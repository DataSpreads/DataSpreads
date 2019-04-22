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

using DataSpreads.StreamLogs;
using Spreads;
using Spreads.Buffers;
using Spreads.LMDB;
using Spreads.Threading;
using Spreads.Utils;
using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using HeaderFlags = DataSpreads.Buffers.SharedMemory.BufferHeader.HeaderFlags;

namespace DataSpreads.Buffers
{
    /// <summary>
    /// A <see cref="RetainableMemoryPool{T}"/> implementation for shared memory
    /// addressable across processes using <see cref="BufferRef"/>.
    /// </summary>
    public class SharedMemoryPool : RetainableMemoryPool<byte>
    {
        /// <summary>
        /// Min Pow2 buffer length.
        /// </summary>
        public const int RmMinPoolBufferLength = 2048;

        /// <summary>
        /// Max possible Pow2 buffer length.
        /// </summary>
        public const int RmMaxPoolBufferLength = RmMinPoolBufferLength * (1 << BufferRef.MaxBucketIdx);

        /// <summary>
        /// The default maximum length of a buffer in the base <see cref="RetainableMemoryPool{T}"/>.
        /// The pool could return buffers up to <see cref="RmMaxPoolBufferLength"/> but buffers
        /// larger than <see cref="RmDefaultMaxBufferLength"/> are stored only in a native pool
        /// so that free ones are available for all processes and not reserved to the current process.
        /// </summary>
        public const int RmDefaultMaxBufferLength = 16 * 1024 * 1024;

        /// <summary>
        /// The default maximum number of reserved buffers per bucket that are available for quick
        /// rent by the current process.
        /// </summary>
        public const int RmDefaultMaxBuffersPerBucket = 64;

        /// <summary>
        /// We limit the number of search buckets (see <see cref="RetainableMemoryPool{T}"/> constructor)
        /// to 0 because most of the streams are infrequent with small number of values.
        /// On average a half of a buffer is empty until rotation and packing so 2+x buffer will
        /// significantly increase the overhead. We calculate rate hint for fast streams and request
        /// a buffer for a next <see cref="StreamBlock"/> in background so allocating a new buffer
        /// from LMDB for a very fast stream does not affects latency and a new large buffer is ready
        /// when needed.
        /// </summary>
        public const int MaxBucketsToTry = 0;

        internal const int PageSize = 4096;

        /// <summary>
        /// Minimum buffer size returned by this pool.
        /// </summary>
        public const int MinBufferSize = PageSize - SharedMemory.BufferHeader.Size;

        private readonly BufferRefAllocator _bra;
        private readonly SharedMemoryBuckets _buckets;

        internal bool PrintBuffersAfterPoolDispose;

        // Note: base RetainableMemoryPool works with Pow2 sizes, this pool adds additional space due to headers

        /// <summary>
        /// SharedMemoryPool constructor.
        /// </summary>
        /// <param name="path">Directory path where shared memory files are stored.</param>
        /// <param name="maxLogSizeMb"></param>
        /// <param name="ownerId"></param>
        /// <param name="rmMaxBufferLength"></param>
        /// <param name="rmMaxBuffersPerBucket"></param>
        /// <param name="rentAlwaysClean"></param>
        public SharedMemoryPool(string path, uint maxLogSizeMb,
            Wpid ownerId,
            int rmMaxBufferLength = RmDefaultMaxBufferLength,
            int rmMaxBuffersPerBucket = RmDefaultMaxBuffersPerBucket, bool rentAlwaysClean = false)
            : this(path, maxLogSizeMb, LMDBEnvironmentFlags.WriteMap, ownerId, rmMaxBufferLength, rmMaxBuffersPerBucket, rentAlwaysClean) { }

        internal unsafe SharedMemoryPool(string path, uint maxLogSizeMb,
            LMDBEnvironmentFlags envFlags,
            Wpid ownerId,
            int rmMaxBufferLength = RmDefaultMaxBufferLength,
            int rmMaxBuffersPerBucket = RmDefaultMaxBuffersPerBucket, bool rentAlwaysClean = false)
            : base(
                (pool, bucketSize) =>
                {
                    if (bucketSize > pool.MaxBufferSize)
                    {
                        BuffersThrowHelper.ThrowBadLength();
                    }
                    var smbp = (SharedMemoryPool)pool;

#pragma warning disable 618
                    var sm = smbp.RentNative(bucketSize);

                    Debug.Assert(!sm.IsDisposed);
                    Debug.Assert(sm.ReferenceCount == 0);
                    Debug.Assert(Unsafe.ReadUnaligned<uint>(sm.HeaderPointer) == HeaderFlags.IsOwned);
#pragma warning restore 618

                    // TODO review if Releasing -> IsOwned should keep disposed state?
                    // RMP calls CreateNew outside lock, so if we call RentNative only here
                    // then it could return IsOwned without IsDisposed. But we a technically
                    // inside the pool until this factory returns. Buffers from RentNative
                    // should be unusable without explicit un-dispose action.

                    // Set counter to zero
                    sm.CounterRef &= ~AtomicCounter.CountMask;

                    return sm;
                },
                RmMinPoolBufferLength,
                Math.Max(RmDefaultMaxBufferLength, Math.Min(RmMaxPoolBufferLength, BitUtil.FindNextPositivePowerOfTwo(rmMaxBufferLength))),
                // from ProcCount to DefaultMaxNumberOfBuffersPerBucket x 2
                Math.Max(Environment.ProcessorCount, Math.Min(RmDefaultMaxBuffersPerBucket * 2, rmMaxBuffersPerBucket)),
                MaxBucketsToTry, rentAlwaysClean: rentAlwaysClean)
        {
            if (ownerId <= 0)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException("ownerId <= 0");
            }

            Directory.CreateDirectory(path);

            _bra = new BufferRefAllocator(path, envFlags, ownerId, PageSize, maxLogSizeMb * 1024 * 1024L);
            _buckets = new SharedMemoryBuckets(Path.Combine(path, "buckets"), pageSize: PageSize, maxBucketIndex: BufferRef.MaxBucketIdx);

            StartMonitoringTask();
        }

        public Wpid Wpid => _bra.Wpid;

        internal SharedMemoryBuckets Buckets => _buckets;

        public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
        {
            if (minBufferSize == -1)
            {
                minBufferSize = RmMinPoolBufferLength;
            }
            return RentMemory(minBufferSize);
        }

        /// <summary>
        /// Rent empty chunk buffer. This is just <see cref="SharedMemory"/> without any SL specific values set.
        /// </summary>
        public new SharedMemory RentMemory(int minDataSize)
        {
            var buffer = (SharedMemory)base.RentMemory(DataSizeToBucketSize(minDataSize));
            return buffer;
        }

        internal const int Pow2ModeIdxLimit = 5;
        internal const int Pow2ModeSizeLimit = RmMinPoolBufferLength * (1 << Pow2ModeIdxLimit) - SharedMemory.BufferHeader.Size;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int DataSizeToBucketSize(int dataSize)
        {
            int bucketSize;
            if (dataSize <= MinBufferSize)
            {
                bucketSize = RmMinPoolBufferLength;
            }
            else if (dataSize <= MinBufferSize + PageSize)
            {
                bucketSize = RmMinPoolBufferLength * 2;
            }
            else if (dataSize <= Pow2ModeSizeLimit)
            {
                bucketSize = BitUtil.FindPreviousPositivePowerOfTwo(dataSize + SharedMemory.BufferHeader.Size);
            }
            else
            {
                var extra = dataSize; // - MinBufferSize;
                var bucketIdx = SelectBucketIndex(extra);
                bucketSize = GetMaxSizeForBucket(bucketIdx);
            }
            return bucketSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int BucketIndexToBufferSize(int bucketIndex, int pageSize)
        {
            int bufferSize;
            if (bucketIndex == 0)
            {
                bufferSize = pageSize;
            }
            else if (bucketIndex < Pow2ModeIdxLimit)
            {
                bufferSize = pageSize << bucketIndex;
            }
            else
            {
                bufferSize = RmMinPoolBufferLength * (1 << bucketIndex) + pageSize;
            }

            return bufferSize;
        }

        [Obsolete("This call could only be made from SMP factory or tests.")]
        internal unsafe SharedMemory RentNative(int bucketSize)
        {
            var bucketIndex = SelectBucketIndex(bucketSize);
            BufferRef.EnsureBucketIndexInRange(bucketIndex);

            // We must init the buffer header before allocating.
            // We know from inside BRA.Allocate txn the value or BufferRef
            // before committing. For new we must init the header,
            // for one from the free list we must assert the buffer is disposed
            // and not owned (it may be in Releasing state though).

            var br = _bra.Allocate((byte)bucketIndex, out _, _buckets);

            var db = _buckets.DangerousGet(br);

            var sm = SharedMemory.Create(db, br, this);

            Debug.Assert(sm.IsDisposed);
            Debug.Assert(Unsafe.Read<uint>(sm.HeaderPointer) == (HeaderFlags.Releasing | HeaderFlags.IsDisposed));

            sm.FromReleasingDisposedToOwned();

            Debug.Assert(!sm.IsDisposed);
            Debug.Assert(sm.ReferenceCount == 0);
            Debug.Assert(Unsafe.Read<uint>(sm.HeaderPointer) == HeaderFlags.IsOwned);

            return sm;
        }

        /// <summary>
        /// Try to return a buffer from allocated to free list if
        /// detected a buffer with (Releasing | Disposed) state
        /// in the allocated list.
        /// </summary>
        internal unsafe void RetryReturnNative(BufferRef bufferRef)
        {
            // It should not be possible that we have SM instance that is in free list
            // If we detected a buffer with Releasing in allocated list we should just
            // re-release it via BRA without creating a SM instance. Otherwise
            // someone could take that Releasing+Free buffer that will become owned
            // but SM finalizer will kill it while thinking it is re-releasing it.
            // If Releasing flag is not cleared after BRA.Free that is not stopping
            // RentNative from taking that buffer.

            // TODO try/catch? or trace warning
            // this operation could be done by any process concurrently, the first that detects
            // the condition tries to clean up.
#pragma warning disable 618
            var db = _buckets.DangerousGet(bufferRef);
#pragma warning restore 618
            if (Unsafe.Read<uint>(db.Data) != (HeaderFlags.Releasing | HeaderFlags.IsDisposed))
            {
                ThrowHelper.ThrowInvalidOperationException($"bufferRef {bufferRef} in not in (Releasing | Disposed) state, cannot retry ReturnNative.");
            }
            if (_bra.IsInFreeList(bufferRef))
            {
                ThrowHelper.ThrowInvalidOperationException($"bufferRef {bufferRef} in in free list.");
            }
            _bra.Free(bufferRef);
        }

        internal void ReturnNative(SharedMemory sm)
        {
            sm.FromOwnedDisposedToReleasingDisposed();

            Debug.Assert(sm.IsDisposed);
            ThrowHelper.AssertFailFast(sm.Header.FlagsCounter == (HeaderFlags.Releasing | HeaderFlags.IsDisposed), "ReturnNative: sm.Header.FlagsCounter == (HeaderFlags.Releasing | HeaderFlags.IsDisposed)");

            // Now the buffer is logically free. To be able to reuse it
            // we must return it to the free list. If the next operation fails
            // the buffer leaks. Since it is in shared memory it will be paged
            // and its backing RAM will be reused. Fails should happen very
            // rarely so leaving a small region of a file is not a big deal.
            // However we still have a logic to detect such cases.
            // Use RetryReturnNative(BufferRef) to try to return

            _bra.Free(sm.BufferRef);

            // Keep Releasing flag, it is ignored by Rent.
            // Do not clear instanceId, BRA.Allocate always overwrites it.
            // It is only needed when a buffer is in allocated list, the
            // value is irrelevant for free buffers.

            SharedMemory.Free(sm);
        }

        // TODO it is not Try... now, it throws. Review usage and refactor to either side
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DirectBuffer TryGetAllocatedNativeBuffer(BufferRef bufferRef)
        {
#if DEBUG
            // we will do atomic header checks later anyway, but keep this in debug
            if (_bra.IsInFreeList(bufferRef))
            {
                ThrowHelper.ThrowInvalidOperationException($"Cannot get non-allocated buffer directly.");
            }
#endif
#pragma warning disable 618
            var nativeBuffer = _buckets.DangerousGet(bufferRef);
#pragma warning restore 618

            // At least is must be owned, other flag checks are done by callers of this method
            // TODO (!) review all usages, ensure that flags are checked depending on the context
            var header = nativeBuffer.Read<SharedMemory.BufferHeader>(0);
            if ((header.FlagsCounter & HeaderFlags.IsOwned) == 0)
            {
                ThrowNotOwned();
            }
            return nativeBuffer;

            void ThrowNotOwned()
            {
                ThrowHelper.ThrowInvalidOperationException($"TryGetAllocatedNativeBuffer: {bufferRef} is not owned.");
            }
        }

        internal Task StartMonitoringTask()
        {
            return Task.Run(async () =>
            {
                while (!_disposed)
                {
                    try
                    {
                        // TODO move to a separate monitoring task + logging
                        Trace.WriteLine($"SMP STATUS: ALLOCATED {_bra.GetTotalAllocated():N0}, {_bra.Utilization}% of Max {_bra.MaxTotalSize:N0}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception in MonitoringTask: " + ex);
                    }

                    await Task.Delay(5000);
                }
            });
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (PrintBuffersAfterPoolDispose)
            {
                Console.WriteLine("------------------ BUFFERS AFTER DISPOSE ------------------");
                PrintBuffers();
            }

            _buckets.Dispose();
            _bra.Dispose();
        }

        internal void PrintBuffers(int bucketIndex = -1, bool printBuffers = false,
            Func<BufferRef, SharedMemoryBuckets, string> additionalInfo = null)
        {
            _bra.PrintAllocated(bucketIndex, printBuffers, (br) => additionalInfo == null ? string.Empty : additionalInfo(br, _buckets));
            _bra.PrintFree(bucketIndex, printBuffers, (br) => additionalInfo == null ? string.Empty : additionalInfo(br, _buckets));
        }
    }
}
