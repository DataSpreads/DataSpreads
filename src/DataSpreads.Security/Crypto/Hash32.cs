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

using DataSpreads.Security.Crypto.Native;
using Spreads.Buffers;
using Spreads.Serialization;
using System;
using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace DataSpreads.Security.Crypto
{
    /// <summary>
    /// Blake2b with digest length of 16 bytes.
    /// </summary>
    [DebuggerDisplay("{" + nameof(ToString) + "()}")]
    [StructLayout(LayoutKind.Sequential, Size = Size)]
    [BinarySerialization(Size)]
    public readonly unsafe struct Hash16 : IEquatable<Hash16>
    {
        public const int Size = 16;

        /// <summary>
        ///
        /// </summary>
        public Hash16(string base64)
        {
            Span<byte> bytes = stackalloc byte[base64.Length];

            fixed (char* cPtr = base64)
            fixed (byte* bPtr = bytes)
            {
                Encoding.UTF8.GetBytes(cPtr, base64.Length, bPtr, bytes.Length);
            }

            var status = Base64.DecodeFromUtf8InPlace(bytes, out var bytesWritten);
            if (status != OperationStatus.Done)
            {
                throw new ArgumentException($"Cannot read hash base64 value: status = {status}");
            }

            if (bytesWritten != Size)
            {
                ThrowWrongSize();
            }

            Unsafe.AsRef(in this) = Unsafe.As<byte, Hash16>(ref bytes[0]);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowWrongSize()
        {
            throw new ArgumentException($"Wrong input size");
        }

        public Hash16(ReadOnlySpan<byte> bytes)
        {
            if (bytes.Length != Size)
            {
                ThrowWrongSize();
            }

            Unsafe.AsRef(in this) = Unsafe.As<byte, Hash16>(ref Unsafe.AsRef(in bytes[0]));
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Hash16 other)
        {
            return MemoryExtensions.SequenceEqual(AsSpan(), other.AsSpan());
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return Convert.ToBase64String(AsSpan().ToArray());
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (obj is null) return false;
            return obj is Hash16 o && Equals(o);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode()
        {
            var ptr = Unsafe.AsPointer(ref Unsafe.AsRef(in this));
            return *(int*)((byte*)ptr);
        }

        /// <summary>
        /// Equals operator.
        /// </summary>
        public static bool operator ==(Hash16 x, Hash16 y)
        {
            return x.Equals(y);
        }

        /// <summary>
        /// Not equals operator.
        /// </summary>
        public static bool operator !=(Hash16 x, Hash16 y)
        {
            return !x.Equals(y);
        }

        /// <summary>
        /// Get Hash as <see cref="ReadOnlySpan{T}"/> of bytes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsSpan()
        {
            var ptr = Unsafe.AsPointer(ref Unsafe.AsRef(in this));
            return new ReadOnlySpan<byte>((byte*)ptr, Size);
        }

        internal static Hash16 Random()
        {
            var value = default(Hash16);
            var ptr = Unsafe.AsPointer(ref value);
            Libsodium.randombytes_buf(ptr, (UIntPtr)Size);
            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Hash16 Chain(in Hash16 previous, ReadOnlySpan<byte> input)
        {
            fixed (byte* ptr = input)
            {
                var db = new DirectBuffer(input.Length, ptr);
                return Chain(in previous, db);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Hash16 Chain(in Hash16 previous, DirectBuffer input)
        {
            Span<byte> output = stackalloc byte[Size];
            Spreads.Algorithms.Hash.Blake2b.ComputeAndWriteHash(Size,
                previous.AsSpan(), input, output);
            return Unsafe.As<byte, Hash16>(ref output[0]);
        }

        [Obsolete("Intrinsics version is much faster than native calls for small messages.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Hash16 Chain2(in Hash16 previous, ReadOnlySpan<byte> input)
        {
            fixed (byte* ptr = input)
            {
                var db = new DirectBuffer(input.Length, ptr);
                return Chain2(in previous, db);
            }
        }

        [Obsolete("Intrinsics version is much faster than native calls for small messages.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Hash16 Chain2(in Hash16 previous, DirectBuffer input)
        {
            var output = stackalloc byte[Size];
            Libsodium.crypto_generichash_blake2b(output, (UIntPtr)Size,
                input.Data, (ulong)input.Length,
                (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in previous)), (UIntPtr)Size
                );
            return Unsafe.As<byte, Hash16>(ref output[0]);
        }
    }
}
