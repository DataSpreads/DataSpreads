/*
 * Copyright (c) 2017-2019 Victor Baybekov (DataSpreads@DataSpreads.io, @DataSpreads)
 *
 * This file is part of DataSpreads.
 *
 * DataSpreads is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * DataSpreads is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with DataSpreads.  If not, see <http://www.gnu.org/licenses/>.
 *
 * DataSpreads works well as an embedded realtime database,
 * but it works much better when connected to a network!
 *
 * Please sign up for free at <https://dataspreads.io/HiGitHub>,
 * download our desktop app with UI and background service,
 * get an API token for programmatic access to our network,
 * and enjoy how fast and securely your data spreads to the World!
 */

using Spreads;
using Spreads.LMDB;
using Spreads.Threading;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using static DataSpreads.Buffers.SharedMemory.BufferHeader;

namespace DataSpreads.Buffers
{
    // TODO EnvSync after creating a new. Do not need that when
    // releasing to or taking from free list because we could
    // recover from buffer headers.

    /// <summary>
    /// Virtual buffer allocator. Keeps track of allocated and a free <see cref="BufferRef"/> items.
    /// Must be used together with a <see cref="SharedMemoryBuckets"/> instance that does real allocation.
    /// </summary>
    internal class BufferRefAllocator : IDisposable
    {
        // TODO we could easily detect allocated buffers from dead wpids, but
        // in the default case of block memory we need to do additional work.
        // Add a delegate that returns true if BufferRef could be returned to FreeList.

        private Wpid _wpid;
        private int _pageSize;
        private long _maxTotalSize;
        private long _lastTotalSize;

        /// <summary>
        /// Check total size every this number of allocations. Total size is always checked when <see cref="Utilization"/> is above 90%.
        /// Set to zero to disable the check.
        /// </summary>
        public static byte CheckTotalSizeEveryNAllocation = 100;

        private byte _allocationsWithoutSizeCheckCount;

        private LMDBEnvironment _env;
        private Database _allocatedDb;
        private Database _freeDb;

        // TODO this is all internal with very limited usage, but refactor

        public BufferRefAllocator(string directoryPath, LMDBEnvironmentFlags envFlags, Wpid wpid, int pageSize = 4096, long maxTotalSize = 64L * 1024 * 1024 * 1024)
        {
            _wpid = wpid;
            _pageSize = pageSize;
            _maxTotalSize = maxTotalSize;

            // TODO Log size to settings. Also handle MapFull later and reduce size.
            SetupEnv(directoryPath, 128, envFlags);
            InitDbs();
        }

        public string DirectoryPath => _env.Directory;

        public long MaxTotalSize => _maxTotalSize;

        internal LMDBEnvironment Environment => _env;

        public Wpid Wpid => _wpid;

        /// <summary>
        /// Percent of last known total size vs <see cref="MaxTotalSize"/>.
        /// </summary>
        public double Utilization => Math.Round(100 * (_lastTotalSize / (double)_maxTotalSize), 2);

        public long GetTotalAllocated(int bucketIndex = -1)
        {
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var aC = _allocatedDb.OpenReadOnlyCursor(txn))
            {
                BufferRef lastAllocated = default;
                var totalAllocated = 0L;

                if (bucketIndex == -1)
                {
                    byte bucketIndexByte = 0;
                    if (aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.First))
                    {
                        var dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndexByte, _pageSize);
                        totalAllocated += dataSize * (long)aC.Count();

                        while (aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.NextNoDuplicate))
                        {
                            dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndex, _pageSize);
                            totalAllocated += dataSize * (long)aC.Count();
                        }
                    }
                }
                else
                {
                    BufferRef.EnsureBucketIndexInRange(bucketIndex);
                    var bucketIndexByte = (byte)bucketIndex;
                    if (aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.Set))
                    {
                        var dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndex, _pageSize);
                        totalAllocated += dataSize * (long)aC.Count();
                    }
                }

                _lastTotalSize = totalAllocated;
                return totalAllocated;
            }
        }

        /// <summary>
        /// Allocate a new virtual buffer and
        /// </summary>
        /// <param name="bucketIndex">Bucket index.</param>
        /// <param name="fromFreeList">Set to true if reusing a freed BufferRef.</param>
        /// <param name="buckets"></param>
        /// <exception cref="NotEnoughSpaceException">Thrown when <see cref="GetTotalAllocated"/> exceeds <see cref="MaxTotalSize"/>.</exception>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public BufferRef Allocate(byte bucketIndex, out bool fromFreeList, SharedMemoryBuckets buckets = null)
        {
            BufferRef.EnsureBucketIndexInRange(bucketIndex);

            using (var txn = _env.BeginTransaction())
            {
                try
                {
                    BufferRef freeRef = Allocate(txn, bucketIndex, out fromFreeList, buckets);

                    txn.Commit();
                    // Without packing and returning buffers to pool this kills performance
                    // But it must be done.
                    if (!fromFreeList)
                    {
                        _env.Sync(true);
                    }
                    return freeRef;
                }
                catch
                {
                    txn.Abort();
                    throw;
                }
            }
        }

        protected BufferRef Allocate(Transaction txn, byte bucketIndex, out bool fromFreeList, SharedMemoryBuckets buckets = null)
        {
            fromFreeList = false;
            BufferRef.EnsureBucketIndexInRange(bucketIndex);

            BufferRef freeRef = default;

            using (var fC = _freeDb.OpenCursor(txn))
            using (var aC = _allocatedDb.OpenCursor(txn))
            {
                // If free list is not empty then everything is simple - just move an item from FL to AL

                if (fC.TryGet(ref bucketIndex, ref freeRef, CursorGetOption.Set)
                    && fC.TryGet(ref bucketIndex, ref freeRef, CursorGetOption.FirstDuplicate)
                ) // prefer closer to the bucket start, DRs are sorted by index // TODO test with first with high churn & packer
                {
                    _freeDb.Delete(txn, bucketIndex, freeRef);
                    var ar = freeRef;
                    aC.Put(ref bucketIndex, ref ar, CursorPutOptions.None);
                    fromFreeList = true;
                    if (buckets != null)
                    {
#pragma warning disable 618
                        var directBuffer = buckets.DangerousGet(freeRef);
#pragma warning restore 618
                        var header = directBuffer.Read<SharedMemory.BufferHeader>(0);
                        unchecked
                        {
                            if (header.FlagsCounter != (HeaderFlags.Releasing | HeaderFlags.IsDisposed))
                            {
                                ThrowHelper.ThrowInvalidOperationException(
                                    "Buffer in free list must be (Releasing | IsDisposed).");
                            }

                            // TODO review, we require (Releasing | IsDisposed) in FL. Remove commented code below if that is OK

                            //if (((int)header.FlagsCounter & (int)AtomicCounter.CountMask) != (int)AtomicCounter.CountMask)
                            //{
                            //    ThrowHelper.ThrowInvalidOperationException("Buffer in free list must be disposed.");
                            //}

                            //var ignoreCountAndReleasing = ((int)header.FlagsCounter & ~(int)AtomicCounter.CountMask) & ~((int)SharedMemory.BufferHeader.HeaderFlags.Releasing);
                            //if (ignoreCountAndReleasing != 0)
                            //{
                            //    ThrowHelper.ThrowInvalidOperationException("Non-Releasing flags in free buffer header.");
                            //}

                            var newHeader = header;
                            // newHeader.FlagsCounter = (SharedMemory.BufferHeader.HeaderFlags)(uint)(AtomicCounter.CountMask | (int)SharedMemory.BufferHeader.HeaderFlags.Releasing);
                            newHeader.AllocatorInstanceId = _wpid.InstanceId;

                            // TODO check existing header values
                            // It must be zeros if the buffer was disposed normally
                            //if ((long)header != 0)
                            //{
                            //    Trace.TraceWarning($"Header of free list buffer is not zero: FlagsCounter {header.FlagsCounter}, InstanceId: {header.AllocatorInstanceId}");
                            //}

                            if ((long)header !=
                                directBuffer.InterlockedCompareExchangeInt64(0, (long)newHeader,
                                    (long)header))
                            {
                                // This should only happen if someone if touching the header concurrently
                                // But we are inside write txn now so this should never happen.
                                // TODO could remove expensive CAS for simple write.
                                ThrowHelper.FailFast("Header changed during allocating from free list.");
                            }
                        }
                    }
                }
                else
                {
                    // Free list is empty.
                    // Allocated must be sorted by BufferRef as uint. Since in DUP_SORTED bucket bites are
                    // the same then the last allocated value has the maximum buffer index.

                    int bufferIndex = 1; // if is empty then start from 1

                    BufferRef lastAllocated = default;
                    if (aC.TryGet(ref bucketIndex, ref lastAllocated, CursorGetOption.Set)
                        && aC.TryGet(ref bucketIndex, ref lastAllocated, CursorGetOption.LastDuplicate))
                    {
                        bufferIndex += lastAllocated.BufferIndex;
                    }

                    freeRef = BufferRef.Create(bucketIndex, bufferIndex);

                    lastAllocated = freeRef;
                    // _allocatedDb.Put(txn, bucketIndex, lastAllocated, TransactionPutOptions.AppendDuplicateData);
                    aC.Put(ref bucketIndex, ref lastAllocated, CursorPutOptions.AppendDuplicateData);

                    // We are done, freeRef is set. Reuse cursors and locals.

                    var dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndex, _pageSize);

                    _allocationsWithoutSizeCheckCount++;
                    if (_allocationsWithoutSizeCheckCount == CheckTotalSizeEveryNAllocation &&
                        CheckTotalSizeEveryNAllocation > 0
                        ||
                        _lastTotalSize / (double)_maxTotalSize > 0.9
                        ||
                        dataSize / (double)MaxTotalSize > 0.01 // 1%
                    )
                    {
                        _allocationsWithoutSizeCheckCount = 0;

                        var totalAllocated = 0L;
                        if (aC.TryGet(ref bucketIndex, ref lastAllocated, CursorGetOption.First))
                        {
                            totalAllocated += dataSize * (long)aC.Count();
                        }

                        while (aC.TryGet(ref bucketIndex, ref lastAllocated, CursorGetOption.NextNoDuplicate))
                        {
                            totalAllocated += dataSize * (long)aC.Count();
                        }

                        _lastTotalSize = totalAllocated;

                        if (totalAllocated > _maxTotalSize)
                        {
                            ThrowBufferRefAllocatorFullException();
                        }
                    }

                    if (buckets != null)
                    {
                        unchecked
                        {
#pragma warning disable 618
                            var directBuffer = buckets.DangerousGet(freeRef);
#pragma warning restore 618
                            var header = directBuffer.Read<SharedMemory.BufferHeader>(0);
                            if (header.FlagsCounter != default)
                            {
                                throw new InvalidOperationException($"Allocated buffer has non-empry header.FlagsCounter: {header.FlagsCounter}");
                            }

                            if (header.AllocatorInstanceId != default)
                            {
                                throw new InvalidOperationException($"Allocated buffer has non-empry header.AllocatorInstanceId: {header.AllocatorInstanceId}");
                            }
                            header.FlagsCounter = HeaderFlags.Releasing | HeaderFlags.IsDisposed;
                            header.AllocatorInstanceId = _wpid.InstanceId;

                            // No need to check previous value because it is a new buffer
                            // that was never touched from this BRA perspective.
                            directBuffer.VolatileWriteInt64(0, (long)header);
                        }
                    }
                }
            }

            return freeRef;
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public void Free(BufferRef bufferRef) //, Wpid wpid = default)
        {
            using (var txn = _env.BeginTransaction())
            {
                try
                {
                    Free(txn, bufferRef);

                    txn.Commit();
                }
                catch
                {
                    txn.Abort();
                    throw;
                }
            }
        }

        protected void Free(Transaction txn, BufferRef bufferRef) //, Wpid wpid = default)
        {
            using (var aC = _allocatedDb.OpenCursor(txn))
            {
                byte bucketIndex = checked((byte)bufferRef.BucketIndex);
                var ar = bufferRef;
                if (aC.TryGet(ref bucketIndex, ref ar, CursorGetOption.GetBoth))
                {
                    _allocatedDb.Delete(txn, bucketIndex, ar);
                    _freeDb.Put(txn, bucketIndex, bufferRef);
                }
                else
                {
                    ThrowFreeBufferRefNotAllocated();
                }
            }
        }

        //        [MethodImpl(MethodImplOptions.NoInlining
        //#if NETCOREAPP3_0
        //                    | MethodImplOptions.AggressiveOptimization
        //#endif
        //        )]
        //        public int GetAllocatorInstanceId(BufferRef bufferRef)
        //        {
        //            using (var txn = _env.BeginReadOnlyTransaction())
        //            {
        //                using (var aC = _allocatedDb.OpenReadOnlyCursor(txn))
        //                {
        //                    byte bucketIndex = checked((byte)bufferRef.BucketIndex);
        //                    var ar = bufferRef;
        //                    if (aC.TryGet(ref bucketIndex, ref ar, CursorGetOption.GetBoth))
        //                    {
        //                        return (int)ar.AllocatorInstance;
        //                    }

        //                    return -1;
        //                }
        //            }
        //        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public bool IsInFreeList(BufferRef bufferRef)
        {
            using (var txn = _env.BeginReadOnlyTransaction())
            {
                using (var fC = _freeDb.OpenReadOnlyCursor(txn))
                {
                    byte bucketIndex = checked((byte)bufferRef.BucketIndex);
                    BufferRef value = bufferRef;
                    if (fC.TryGet(ref bucketIndex, ref value, CursorGetOption.GetBoth))
                    {
                        if (bufferRef != value)
                        {
                            ThrowHelper.FailFast("Wrong BRA implementation");
                        }
                        return true;
                    }

                    return false;
                }
            }
        }

        public void PrintAllocated(int bucketIndex = -1, bool printBuffers = false,
            Func<BufferRef, string> additionalInfo = null)
        {
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var aC = _allocatedDb.OpenReadOnlyCursor(txn))
            {
                BufferRef lastAllocated = default;
                var totalAllocated = 0L;

                if (bucketIndex == -1)
                {
                    Console.WriteLine("------------------- ALLOCATED -------------------");
                    byte bucketIndexByte = 0;
                    if (aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.First))
                    {
                        var dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndexByte, _pageSize);
                        var count = (long)aC.Count();
                        var bucketSize = dataSize * count;
                        totalAllocated += bucketSize;
                        Console.WriteLine(
                            $"[{bucketIndexByte}]: count [{count:N0}], buffer size [{dataSize:N0}], bucket size: [{bucketSize:N0}]");
                        if (printBuffers)
                        {
                            PrintBuffers(bucketIndexByte);
                        }

                        while (aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.NextNoDuplicate))
                        {
                            dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndexByte, _pageSize);
                            count = (long)aC.Count();
                            bucketSize = dataSize * count;
                            totalAllocated += bucketSize;
                            Console.WriteLine(
                                $"[{bucketIndexByte}]: count [{count:N0}], buffer size [{dataSize:N0}], bucket size: [{bucketSize:N0}]");

                            if (printBuffers)
                            {
                                PrintBuffers(bucketIndexByte);
                            }
                        }
                    }
                    Console.WriteLine($"------ ALLOCATED {totalAllocated:N0}, {Utilization}% of Max {MaxTotalSize:N0} ------");
                }
                else
                {
                    BufferRef.EnsureBucketIndexInRange(bucketIndex);
                    Console.WriteLine($"------------------- ALLOCATED [{bucketIndex}] -------------------");
                    var bucketIndexByte = (byte)bucketIndex;
                    if (aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.Set))
                    {
                        var dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndexByte, _pageSize);
                        var count = (long)aC.Count();
                        var bucketSize = dataSize * count;
                        totalAllocated += bucketSize;
                        Console.WriteLine(
                            $"[{bucketIndexByte}]: count [{count:N0}], buffer size [{dataSize:N0}], bucket size: [{bucketSize:N0}]");
                        if (printBuffers)
                        {
                            PrintBuffers(bucketIndexByte);
                        }
                    }
                    Console.WriteLine($"------ ALLOCATED [{bucketIndex}] {totalAllocated:N0}, {Utilization}% of Max {MaxTotalSize:N0} ------");
                }

                void PrintBuffers(byte bucketIndexByte)
                {
                    if (aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.SetKey) &&
                        aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.FirstDuplicate))
                    {
                        var info = string.Empty;
                        if (additionalInfo != null)
                        {
                            info = ": " + additionalInfo(lastAllocated);
                        }

                        Console.WriteLine($"   [{lastAllocated.BufferIndex}]{info}");

                        while (aC.TryGet(ref bucketIndexByte, ref lastAllocated, CursorGetOption.NextDuplicate))
                        {
                            info = string.Empty;
                            if (additionalInfo != null)
                            {
                                info = ": " + additionalInfo(lastAllocated);
                            }
                            Console.WriteLine($"   [{lastAllocated.BufferIndex}]{info}");
                        }
                    }
                }

                _lastTotalSize = totalAllocated;
            }
        }

        public void PrintFree(int bucketIndex = -1, bool printBuffers = false,
            Func<BufferRef, string> additionalInfo = null)
        {
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var fC = _freeDb.OpenReadOnlyCursor(txn))
            {
                BufferRef lastFree = default;
                var totalFree = 0L;

                if (bucketIndex == -1)
                {
                    Console.WriteLine("------------------- FREE -------------------");
                    byte bucketIndexByte = 0;
                    if (fC.TryGet(ref bucketIndexByte, ref lastFree, CursorGetOption.First))
                    {
                        var dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndexByte, _pageSize);
                        var count = (long)fC.Count();
                        var bucketSize = dataSize * count;
                        totalFree += bucketSize;
                        Console.WriteLine(
                            $"[{bucketIndexByte}]: count [{count:N0}], buffer size [{dataSize:N0}], bucket size: [{bucketSize:N0}]");
                        if (printBuffers)
                        {
                            PrintBuffers(bucketIndexByte);
                        }

                        while (fC.TryGet(ref bucketIndexByte, ref lastFree, CursorGetOption.NextNoDuplicate))
                        {
                            dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndexByte, _pageSize);
                            count = (long)fC.Count();
                            bucketSize = dataSize * count;
                            totalFree += bucketSize;
                            Console.WriteLine(
                                $"[{bucketIndexByte}]: count [{count:N0}], buffer size [{dataSize:N0}], bucket size: [{bucketSize:N0}]");

                            if (printBuffers)
                            {
                                PrintBuffers(bucketIndexByte);
                            }
                        }
                    }
                    Console.WriteLine($"------ FREE {totalFree:N0}, {Math.Round(100 * (totalFree / (double)MaxTotalSize), 2)}% of Max {MaxTotalSize:N0} ------");
                }
                else
                {
                    BufferRef.EnsureBucketIndexInRange(bucketIndex);
                    Console.WriteLine($"------------------- FREE [{bucketIndex}] -------------------");
                    var bucketIndexByte = (byte)bucketIndex;
                    if (fC.TryGet(ref bucketIndexByte, ref lastFree, CursorGetOption.Set))
                    {
                        var dataSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndexByte, _pageSize);
                        var count = (long)fC.Count();
                        var bucketSize = dataSize * count;
                        totalFree += bucketSize;
                        Console.WriteLine(
                            $"[{bucketIndexByte}]: count [{count}], buffer size [{dataSize}], bucket size: [{bucketSize}]");
                        if (printBuffers)
                        {
                            PrintBuffers(bucketIndexByte);
                        }
                    }
                    Console.WriteLine($"------ FREE [{bucketIndex}] {totalFree:N0}, {Math.Round(100 * (totalFree / (double)MaxTotalSize), 2)}% of Max {MaxTotalSize:N0} ------");
                }

                void PrintBuffers(byte bucketIndexByte)
                {
                    if (fC.TryGet(ref bucketIndexByte, ref lastFree, CursorGetOption.SetKey) &&
                        fC.TryGet(ref bucketIndexByte, ref lastFree, CursorGetOption.FirstDuplicate))
                    {
                        var info = String.Empty;
                        if (additionalInfo != null)
                        {
                            info = ": " + additionalInfo(lastFree);
                        }

                        Console.WriteLine($"   [{lastFree.BufferIndex}]{info}");

                        while (fC.TryGet(ref bucketIndexByte, ref lastFree, CursorGetOption.NextDuplicate))
                        {
                            info = String.Empty;
                            if (additionalInfo != null)
                            {
                                info = ": " + additionalInfo(lastFree);
                            }

                            Console.WriteLine($"   [{lastFree.BufferIndex}]{info}");
                        }
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowBufferRefAllocatorFullException()
        {
            throw new NotEnoughSpaceException();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowFreeBufferRefNotAllocated()
        {
            ThrowHelper.ThrowKeyNotFoundException("bufferRef is not allocated and cannot be freed.");
        }

        private void SetupEnv(string path, uint mapSizeMb, LMDBEnvironmentFlags flags)
        {
            var env = LMDBEnvironment.Create(path, flags, disableAsync: true, disableReadTxnAutoreset: true);
            env.MapSize = mapSizeMb * 1024L * 1024L;
            env.MaxReaders = 1024;
            env.Open();
            var deadReaders = env.ReaderCheck();
            if (deadReaders > 0)
            {
                Trace.TraceWarning($"Cleared {deadReaders} in SharedMemoryBufferPool");
            }
            _env = env;
        }

        private void InitDbs()
        {
            _allocatedDb = _env.OpenDatabase("_allocated",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey | DbFlags.DuplicatesSort | DbFlags.DuplicatesFixed)
                {
                    // compare by the first 4 bytes as integer
                    DupSortPrefix = 32
                });
            _freeDb = _env.OpenDatabase("_free",
                new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey | DbFlags.DuplicatesSort | DbFlags.IntegerDuplicates));
        }

        public virtual void Dispose()
        {
            _allocatedDb.Dispose();
            _freeDb.Dispose();
            _env.Dispose();
        }
    }
}
