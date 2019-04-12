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
using System.Runtime.InteropServices;

namespace DataSpreads.Security.Crypto.Native
{
    internal static partial class Libsodium
    {
        internal const int crypto_generichash_blake2b_BYTES = 32;
        internal const int crypto_generichash_blake2b_BYTES_MAX = 64;
        internal const int crypto_generichash_blake2b_BYTES_MIN = 16;
        internal const int crypto_generichash_blake2b_KEYBYTES = 32;
        internal const int crypto_generichash_blake2b_KEYBYTES_MAX = 64;
        internal const int crypto_generichash_blake2b_KEYBYTES_MIN = 16;

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_generichash_blake2b(
            byte* @out,
            UIntPtr outlen,
            byte* @in,
            ulong inlen,
            byte* key,
            UIntPtr keylen);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_generichash_blake2b_bytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_generichash_blake2b_bytes_max();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_generichash_blake2b_bytes_min();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_generichash_blake2b_final(
            crypto_generichash_blake2b_state* state,
            byte* @out,
            UIntPtr outlen);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_generichash_blake2b_init(
            crypto_generichash_blake2b_state* state,
            byte* key,
            UIntPtr keylen,
            UIntPtr outlen);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_generichash_blake2b_keybytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_generichash_blake2b_keybytes_max();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_generichash_blake2b_keybytes_min();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_generichash_blake2b_statebytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_generichash_blake2b_update(
            crypto_generichash_blake2b_state* state,
            byte* @in,
            ulong inlen);

        [StructLayout(LayoutKind.Explicit, Size = 384)]
        internal struct crypto_generichash_blake2b_state
        {
        }
    }
}
