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
using Spreads.Buffers;
using Spreads.Utils;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace DataSpreads.Buffers
{
    /// <summary>
    ///
    /// </summary>
    /// <remarks>
    /// Zero bucket allocates buffers of pageSize that could be arbitrary small power of 2.
    /// The number of buffers per file in the zero bucket is 32,768.
    /// </remarks>
    internal class SharedMemoryBuckets : IDisposable
    {
        private readonly string _directoryPath;
        private readonly int _pageSize;
        private readonly int _maxBucketIndex;
        private SharedMemoryBucket[][] _buckets;
        private DriveInfo _di;

        public SharedMemoryBuckets(string directoryPath, int pageSize = 4096, int maxBucketIndex = BufferRef.MaxBucketIdx)
        {
            if (!BitUtil.IsPowerOfTwo(pageSize))
            {
                ThrowHelper.ThrowArgumentException("pageSize must be a power of 2");
            }

            if ((uint)maxBucketIndex > BufferRef.MaxBucketIdx)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException("maxBucketIndex must be between 0 and BufferRef.MaxBucketIdx = 15");
            }

            if (!Path.IsPathRooted(directoryPath))
            {
                ThrowHelper.ThrowArgumentException("SharedMemoryBuckets: directoryPath must be rooted. Do this before calling ctor.");
            }
            _directoryPath = directoryPath;
            Directory.CreateDirectory(directoryPath);

            var thisDi = new DirectoryInfo(_directoryPath);
            DriveInfo di = null;
            foreach (var driveInfo in DriveInfo.GetDrives())
            {
                if (di != null)
                {
                    break;
                }
                var thisDi1 = thisDi;
                while (thisDi1.Parent != null)
                {
                    if (thisDi1.Parent.FullName == driveInfo.RootDirectory.FullName)
                    {
                        di = driveInfo;
                        break;
                    }

                    thisDi1 = thisDi1.Parent;
                }
            }

            _di = di;

            _pageSize = pageSize;
            _maxBucketIndex = maxBucketIndex;

            _buckets = new SharedMemoryBucket[BufferRef.MaxBucketIdx + 1][];
        }

        [Obsolete("Use only when bufferRef does not need validation, e.g. taken from BRI.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DirectBuffer DangerousGet(BufferRef bufferRef)
        {
            var bucketIndex = bufferRef.BucketIndex;
            var bufferIndex = bufferRef.BufferIndex - 1; // BufferRef is one-based

            SharedMemoryBucket[] bucketFiles = _buckets[bucketIndex];

            var bufferFileIndex = bufferIndex >> (BufferRef.MaxBucketIdx - bucketIndex);
            var bufferIndexInFile = bufferIndex & ((1 << (BufferRef.MaxBucketIdx - bucketIndex)) - 1);

            SharedMemoryBucket bucket = default;
            if (bucketFiles != null)
            {
                // we need < cmp for not throwing (and it could be <), so add >= 0
                // in a *hope* that compiler then eliminates bound check and
                // we have 2 cmp instead of 3. Benchmark inconclusive,
                // and this not that important to spend time on disassembling.
                if (bufferFileIndex >= 0 && bufferFileIndex < bucketFiles.Length)
                {
                    bucket = bucketFiles[bufferFileIndex];
                }
            }

            if (bucket.DirectFile == null)
            {
                bucket = CreateBucket(ref bucketFiles, bufferRef, bufferFileIndex, bucketIndex);
            }

            return bucket.DangerousGet(bufferIndexInFile);
        }

        public DirectBuffer this[BufferRef bufferRef]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var bucketIndex = bufferRef.BucketIndex;
                var bufferIndex = bufferRef.BufferIndex - 1; // BufferRef is one-based

                if (bucketIndex > _maxBucketIndex)
                {
                    ThrowBucketIndexAboveMax();
                }

                SharedMemoryBucket[] bucketFiles = _buckets[bucketIndex];

                var bufferFileIndex = bufferIndex >> (BufferRef.MaxBucketIdx - bucketIndex);
                var bufferIndexInFile = bufferIndex & ((1 << (BufferRef.MaxBucketIdx - bucketIndex)) - 1);

                SharedMemoryBucket bucket = default;
                if (bucketFiles != null && bufferFileIndex < bucketFiles.Length)
                {
                    bucket = bucketFiles[bufferFileIndex];
                }

                if (bucket.DirectFile == null)
                {
                    bucket = CreateBucket(ref bucketFiles, bufferRef, bufferFileIndex, bucketIndex);
                }

                return bucket[bufferIndexInFile];
            }
        }

        private SharedMemoryBucket CreateBucket(ref SharedMemoryBucket[] bucketFiles, BufferRef bufferRef, int bufferFileIndex, int bucketIndex)
        {
            SharedMemoryBucket bucket;
            lock (_buckets)
            {
                if (bucketFiles == null)
                {
                    bucketFiles = new SharedMemoryBucket[4];
                    _buckets[bufferRef.BucketIndex] = bucketFiles;
                }

                if (bufferFileIndex >= bucketFiles.Length)
                {
                    var newSize = BitUtil.FindNextPositivePowerOfTwo(bufferFileIndex + 1);
                    var newbucketFiles = new SharedMemoryBucket[newSize];
                    Array.Copy(bucketFiles, 0, newbucketFiles, 0, bucketFiles.Length);
                    bucketFiles = newbucketFiles;
                    _buckets[bufferRef.BucketIndex] = newbucketFiles;
                }

                bucket = bucketFiles[bufferFileIndex];
                if (bucket.DirectFile == null)
                {
                    var bucketDir = Path.Combine(_directoryPath, bucketIndex.ToString());
                    Directory.CreateDirectory(bucketDir);

                    var buffersPerBucketFile = (1 << 15) >> bucketIndex;
                    var bufferSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndex, _pageSize);
                    var allocationSize = buffersPerBucketFile * bufferSize;

                    // Since we are using sparse files check available free space

                    var available = GetAvailableFreeSpace();

                    // 2x just in case, maybe we need to monitor this and stop any allocations everywhere when below c.200MB or 1GB to be sure
                    if (available >= 0 && allocationSize * 2 > available)
                    {
                        throw new NotEnoughSpaceException(true);
                    }

                    // Console.WriteLine($"allocationSize for bucket {bucketIndex}: {allocationSize}");
                    var df = new DirectFile(Path.Combine(bucketDir, bufferFileIndex + ".smbkt"), allocationSize, true,
                        FileOptions.RandomAccess // Note that we cannot use async with sparse without using valid NativeOverlapped struct in DeviceIoControl function
                        | FileOptions.WriteThrough, // TODO review if we need this
                        true);

                    DirectFile.PrefetchMemory(df.DirectBuffer);

                    Trace.TraceInformation($"Allocated new file in a bucket {bucketIndex}. Total # of files: {bucketFiles.Count(x => x.DirectFile != null)}");
                    bucket = new SharedMemoryBucket(df, bucketIndex, _pageSize);
                    // Console.WriteLine($"File capacity for bucket {bucketIndex}: {bucket.BufferCount}");
                    bucketFiles[bufferFileIndex] = bucket;
                }
            }

            return bucket;
        }

        internal long GetAvailableFreeSpace()
        {
            if (_di != null)
            {
                return _di.AvailableFreeSpace;
            }

            return -1;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowBucketIndexAboveMax()
        {
            ThrowHelper.ThrowArgumentException("bufferRef.BucketIndex > _maxBucketIndex");
        }

        public void Flush()
        {
            foreach (var bucketFiles in _buckets)
            {
                if (bucketFiles != null)
                {
                    foreach (var sharedMemoryBucket in bucketFiles)
                    {
                        sharedMemoryBucket.DirectFile?.Flush(true);
                    }
                }
            }
        }

        public void Dispose()
        {
            var buckets = Interlocked.Exchange(ref _buckets, null);
            if (buckets != null)
            {
                foreach (var bucketFiles in buckets)
                {
                    if (bucketFiles != null)
                    {
                        foreach (var sharedMemoryBucket in bucketFiles)
                        {
                            sharedMemoryBucket.DirectFile?.Dispose();
                        }
                    }
                }
            }
        }

        internal readonly struct SharedMemoryBucket
        {
            public const int DefaultPageSize = 4096;

            public readonly DirectFile DirectFile;
            public readonly DirectBuffer DirectBuffer;
            public readonly int BucketIndex;
            public readonly int PageSize;
            public readonly int BufferSize;
            public readonly int BufferCount;

            public SharedMemoryBucket(DirectFile directFile, int bucketIndex, int pageSize = DefaultPageSize)
            {
                BufferRef.EnsureBucketIndexInRange(bucketIndex);

                DirectFile = directFile;

                var directBuffer = directFile.DirectBuffer;

                var bufferSize = SharedMemoryPool.BucketIndexToBufferSize(bucketIndex, pageSize);

                BufferCount = directBuffer.Length / bufferSize;

                DirectBuffer = directBuffer;
                BucketIndex = bucketIndex;
                PageSize = pageSize;
                BufferSize = bufferSize;
            }

            public DirectBuffer this[int index]
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    if ((uint)index >= BufferCount)
                    {
                        BuffersThrowHelper.ThrowIndexOutOfRange();
                    }
                    // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                    return DirectBuffer.Slice(index * BufferSize, BufferSize);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public unsafe DirectBuffer DangerousGet(int index)
            {
                return new DirectBuffer(BufferSize, DirectBuffer.Data + index * BufferSize);
            }
        }
    }
}
