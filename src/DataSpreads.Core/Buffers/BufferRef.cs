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

using Spreads.Buffers;
using Spreads.Serialization;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DataSpreads.Buffers
{
    /// <summary>
    /// Unique id of a buffer in a shared memory pool stored as <see cref="UInt32"/>.
    /// </summary>
    /// <remarks>
    /// Bit layout:
    ///
    /// <para />
    ///
    /// ```
    /// 0                   1                   2                   3
    /// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    /// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    /// |F| BkIdx |                    BufferIdx                        |
    /// +---------------------------------------------------------------+
    /// ```
    ///
    /// <para />
    ///
    /// F bit is used as an application-specific flag. In DataSpreads it is used
    /// to indicate a location of a buffer (main or cache pool).
    ///
    /// <para />
    ///
    /// BkIdx 4 bits are used for bucket index from 0 to 15. The index 0
    /// corresponds to the smallest buffer size that could fit 2048 bytes.
    /// Subsequent indexes are used for the smallest buffers that could
    /// fit a next power of 2 number of bytes after 2048. The exact buffer
    /// size could be specific to a pool implementation.
    /// See <see cref="SharedMemoryPool"/> for bucket size calculation in DataSpreads.
    ///
    /// <para />
    ///
    /// BufferIdx 27 bits are used for a buffer index inside a bucket. The maximum
    /// number of buffers per bucket is 134,217,726. Count starts from one for ability
    /// to distinguish default value from the first buffer without the flag in the first bucket.
    ///
    /// <para />
    ///
    /// The first bucket could contain at least 256 GB of 2048-bytes buffers.
    /// In DataSpreads the smallest chunk has the size of 4032 bytes (504 GB max)
    /// and all bytes are used for streams storage.
    ///
    /// <para />
    ///
    /// The "power of 2 plus" design is needed for the cases when a large Pow2-sized
    /// buffer is required for fast navigation via bitwise offset calculation.
    /// Since such buffers are usually large the overhead of additional 4032 bytes
    /// quickly diminishes. Also the pow2 span of a buffer is aligned to memory page
    /// and additional 4032 bytes at the beginning of a buffer could be used as
    /// a header. The Pow2 span is available via <see cref="RetainableMemory{T}.PointerPow2"/>,
    /// <see cref="RetainableMemory{T}.LengthPow2"/> and <see cref="RetainableMemory{T}.RetainPow2"/>
    /// members of <see cref="RetainableMemory{T}"/> base class.
    ///
    /// </remarks>
    [StructLayout(LayoutKind.Sequential, Size = 4)]
    [BinarySerialization(blittableSize: 4)]
    [DebuggerDisplay("{" + nameof(ToString) + "()}")]
    internal readonly unsafe struct BufferRef : IEquatable<BufferRef>
    {
        internal const int MaxBucketIdx = 15;
        private const int MaxBufferIdx = (1 << 27) - 1;

        private const int FlagOffset = 31;
        private const uint FlagMask = (uint)1 << FlagOffset;
        private const int BucketIndexOffset = 27;

        private readonly uint _value;

        private BufferRef(uint value)
        {
            _value = value;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="bucketIndex">A number in 0-15 range.</param>
        /// <param name="bufferIndex">A number in 1-134,217,727 range.</param>
        /// <param name="flag">Optional flag.</param>
        /// <returns></returns>
        public static BufferRef Create(int bucketIndex, int bufferIndex, bool flag = false)
        {
            EnsureBucketIndexInRange(bucketIndex);
            EnsureBufferIndexInRange(bufferIndex);

            var f = (*(uint*)&flag) << FlagOffset;
            var bktIdx = (uint)bucketIndex << BucketIndexOffset;
            var value = f | bktIdx | (uint)bufferIndex;
            return new BufferRef(value);
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public static int BucketToSize(int bucketIndex)
        //{
        //    EnsureBucketIndexInRange(bucketIndex);

        //    var bufferSize = bucketIndex == 0
        //        ? PageSize
        //        : BufferRef.MinPow2BufferSize * (1 << bucketIndex) + PageSize;

        //    return bufferSize;
        //}

        /// <summary>
        /// Return a <see cref="BufferRef"/> without a flag. This is what <see cref="SharedMemoryPool"/> expects.
        /// The flag is used outside the pool for context-specific purposes.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BufferRef ClearFlag()
        {
            return new BufferRef(_value & ~FlagMask);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BufferRef SetFlag()
        {
            return new BufferRef(_value | FlagMask);
        }

        public bool Flag
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (_value & FlagMask) != 0;
        }

        /// <summary>
        /// Zero-based bucket index.
        /// </summary>
        public int BucketIndex
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)((_value >> BucketIndexOffset) & MaxBucketIdx);
        }

        /// <summary>
        /// One-based buffer index in a bucket.
        /// </summary>
        public int BufferIndex
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (int)(_value & MaxBufferIdx);
        }

        public bool IsValid
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => BucketIndex > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator uint(BufferRef id)
        {
            return id._value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator BufferRef(uint value)
        {
            return new BufferRef(value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator ==(BufferRef id1, BufferRef id2)
        {
            return id1.Equals(id2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator !=(BufferRef id1, BufferRef id2)
        {
            return !id1.Equals(id2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(BufferRef other)
        {
            return _value == other._value;
        }

        public override bool Equals(object obj)
        {
            if (obj is null) return false;
            return obj is BufferRef bufferRef && Equals(bufferRef);
        }

        public override int GetHashCode()
        {
            return _value.GetHashCode();
        }

        public override string ToString()
        {
            return $"Bucket={BucketIndex}, Buffer={BufferIndex}, Flag={Flag}";
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void EnsureBucketIndexInRange(int bucketIndex)
        {
            if ((uint)bucketIndex > MaxBucketIdx)
            {
                ThrowBucketIndexOutOfRange();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void EnsureBufferIndexInRange(int bufferIndex)
        {
            if ((uint)(bufferIndex - 1) >= MaxBufferIdx)
            {
                ThrowBufferIndexOutOfRange();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowBucketIndexOutOfRange()
        {
            Spreads.ThrowHelper.ThrowArgumentOutOfRangeException("Bucket index is outside 0-15 range.");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowBufferIndexOutOfRange()
        {
            Spreads.ThrowHelper.ThrowArgumentOutOfRangeException("Buffer index is outside 1-134,217,727 range.");
        }
    }
}
