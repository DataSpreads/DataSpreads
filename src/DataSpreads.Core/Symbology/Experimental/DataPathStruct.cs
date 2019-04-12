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

using System;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Spreads;
using Spreads.Buffers;
using Spreads.Serialization;

namespace DataSpreads.Symbology.Experimental
{
    [Obsolete("Only after string-based works ok and only if there is a use case for allocations. Likely will delete this")]
    [DebuggerDisplay("{" + nameof(ToString) + "()}")]
    [StructLayout(LayoutKind.Sequential, Size = Size)]
    [BinarySerialization(Size)]
    internal unsafe struct DataPathStruct : IEquatable<DataPathStruct>
    {
        public const byte NodeSeparator = (byte)'/';
        public const byte ExternalSeparator = (byte)'@';
        public const byte UserSeparator = (byte)'~';

        private const int Size = 256;
        private const int MaxPathSize = 254;

        private byte _length;
        private byte _level;
        // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
        private fixed byte Bytes[MaxPathSize];

        /// <summary>
        /// Symbol constructor.
        /// </summary>
        public DataPathStruct(string path)
        {
            // TODO validate path
            // Normalize xxx@yyy to @yyy/xxx

            _length = 0;
            _level = 0;

            var byteCount = Encoding.UTF8.GetByteCount(path);
            if (byteCount > MaxPathSize)
            {
                ThrowArgumentOutOfRangeException(nameof(path));
            }

            _length = (byte)byteCount;

            fixed (char* charPtr = path)
            {
                var ptr = 2 + (byte*)Unsafe.AsPointer(ref this);

                // we do not need this since we are storing length, but avoid surprises later
                Unsafe.InitBlockUnaligned(ptr, 0, MaxPathSize);

                Encoding.UTF8.GetBytes(charPtr, path.Length, ptr, byteCount);

                var levels = 0;
                for (int i = 0; i < MaxPathSize; i++)
                {
                    if (*(ptr + i) == NodeSeparator)
                    {
                        levels++;
                    }
                }

                _level = (byte)levels;
            }
        }

        public int Level => _level;

        public int Length => _length;

        /// <summary>
        /// Ends with / separator.
        /// </summary>
        public bool IsRepo
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var ptr = 2 + (byte*)Unsafe.AsPointer(ref this);
                return *(ptr + Length - 1) == NodeSeparator;
            }
        }

        public bool IsExternal
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var ptr = 2 + (byte*)Unsafe.AsPointer(ref this);
                return *(ptr) == ExternalSeparator;
            }
        }

        public DirectBuffer DirectBuffer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var ptr = 2 + (byte*)Unsafe.AsPointer(ref this);
                return new DirectBuffer(_length, ptr);
            }
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DataPathStruct Parent()
        {
            if (_level <= 1)
            {
                return default;
            }

            var copy = this;
            if (IsRepo)
            {
                copy._level--;
            }

            var ptr = 2 + (byte*)Unsafe.AsPointer(ref copy);
            *(ptr + _length - 1) = 0;
            for (int i = _length - 2; i >= 0; i--)
            {
                var ptrI = ptr + i;
                if (*(ptr + i) == NodeSeparator)
                {
                    copy._length = (byte)(i + 1);
                    return copy;
                }

                *ptrI = 0;
            }

            ThrowHelper.ThrowInvalidOperationException();
            return default;
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DataPathStruct Append(in DataPathStruct right)
        {
            if (right.IsExternal)
            {
                throw new ArgumentException("Right is external");
            }

            var len = Length;
            var lenRight = right.Length;
            if (len + lenRight + 1 > MaxPathSize)
            {
                ThrowArgumentOutOfRangeException("len");
            }
            var newLevels = _level + right._level + 1;

            var copy = this;
            copy._level = (byte)newLevels;
            copy._length = (byte)(len + lenRight + 1);
            var ptr = 2 + (byte*)Unsafe.AsPointer(ref copy);
            var ptrRight = 2 + (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in right));
            *(ptr + len) = NodeSeparator;
            Unsafe.CopyBlockUnaligned((ptr + len + 1), ptrRight, (uint)lenRight);
            return copy;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(DataPathStruct other)
        {
            if (_length != other._length)
            {
                return false;
            }
            if (_level != other._level)
            {
                return false;
            }
            var ptr = (byte*)Unsafe.AsPointer(ref this);
            var ptrOther = (byte*)Unsafe.AsPointer(ref other);
            var span = new Span<byte>(ptr, _length);
            var spanOther = new Span<byte>(ptrOther, _length);
            return span.SequenceEqual(spanOther);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var ptr = 2 + (byte*)Unsafe.AsPointer(ref this);
            return Encoding.UTF8.GetString(ptr, Length);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (obj is null) return false;
            return obj is DataPathStruct sym && Equals(sym);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            var ptr = 2 + (byte*)Unsafe.AsPointer(ref this);
            return *(int*)ptr;
        }

        //public static implicit operator Symbol256(DataPath path)
        //{
        //    throw new NotImplementedException();
        //}

        /// <summary>
        /// Equals operator.
        /// </summary>
        public static bool operator ==(DataPathStruct x, DataPathStruct y)
        {
            return x.Equals(y);
        }

        /// <summary>
        /// Not equals operator.
        /// </summary>
        public static bool operator !=(DataPathStruct x, DataPathStruct y)
        {
            return !x.Equals(y);
        }

        /// <summary>
        /// Get Symbol as bytes Span.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan()
        {
            var ptr = Unsafe.AsPointer(ref this);
            return new Span<byte>(ptr, Size);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowArgumentOutOfRangeException(string argumentName)
        {
            throw new ArgumentOutOfRangeException(argumentName, "Symbol length is too large");
        }
    }
}