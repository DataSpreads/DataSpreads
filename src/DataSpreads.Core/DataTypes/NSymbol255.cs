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

using Spreads.Serialization;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace DataSpreads.DataTypes
{
    /// <summary>
    /// A struct to store up to 255 UTF8 bytes with length prefix.
    /// </summary>
    [DebuggerDisplay("{" + nameof(ToString) + "()}")]
    [StructLayout(LayoutKind.Sequential, Size = Size)]
    [BinarySerialization(Size)]
    public unsafe struct NSymbol255 : IEquatable<NSymbol255>
    {
        private const int Size = 256;

        private readonly byte Len;
        private fixed byte Bytes[Size - 1];

        /// <summary>
        /// Symbol constructor.
        /// </summary>
        public NSymbol255(string symbol)
        {
            // will throw when length > 255
            Len = checked((byte)Encoding.UTF8.GetByteCount(symbol));
            fixed (char* charPtr = symbol)
            fixed (byte* ptr = Bytes)
            {
                Encoding.UTF8.GetBytes(charPtr, symbol.Length, ptr, Size - 1);
            }
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(NSymbol255 other)
        {
            if (Len != other.Len)
            {
                return false;
            }

            return MemoryExtensions.SequenceEqual(AsSpan(), other.AsSpan());
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var ptr = (byte*)Unsafe.AsPointer(ref this);
            return Encoding.UTF8.GetString(ptr + 1, Len);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is NSymbol255 && Equals((NSymbol255)obj);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            var ptr = Unsafe.AsPointer(ref this);
            return *(int*)((byte*)ptr + 1);
        }

        /// <summary>
        /// Equals operator.
        /// </summary>
        public static bool operator ==(NSymbol255 x, NSymbol255 y)
        {
            return x.Equals(y);
        }

        /// <summary>
        /// Not equals operator.
        /// </summary>
        public static bool operator !=(NSymbol255 x, NSymbol255 y)
        {
            return !x.Equals(y);
        }

        /// <summary>
        /// Get Symbol as bytes Span.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsSpan()
        {
            var ptr = Unsafe.AsPointer(ref this);
            return new ReadOnlySpan<byte>((byte*)ptr + 1, *(byte*)ptr);
        }
    }
}
