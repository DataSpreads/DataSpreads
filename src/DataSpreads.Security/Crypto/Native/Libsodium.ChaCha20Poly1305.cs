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
        #region Original

        internal const int crypto_aead_chacha20poly1305_ABYTES = 16;
        internal const int crypto_aead_chacha20poly1305_KEYBYTES = 32;
        internal const int crypto_aead_chacha20poly1305_NPUBBYTES = 8;
        internal const int crypto_aead_chacha20poly1305_NSECBYTES = 0;

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_aead_chacha20poly1305_abytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_aead_chacha20poly1305_decrypt(
            byte* m,
            out ulong mlen_p,
            byte* nsec,
            byte* c,
            ulong clen,
            byte* ad,
            ulong adlen,
            byte* npub,
            byte* k);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_aead_chacha20poly1305_encrypt(
            byte* c,
            out ulong clen_p,
            byte* m,
            ulong mlen,
            byte* ad,
            ulong adlen,
            byte* nsec,
            byte* npub,
            byte* k);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_aead_chacha20poly1305_encrypt_detached(
            byte* c,
            byte* mac,
            out ulong maclen_p,
            byte* m,
            ulong mlen,
            byte* ad,
            ulong adlen,
            byte* nsec,
            byte* npub,
            byte* k);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_aead_chacha20poly1305_decrypt_detached(
            byte* m,
            byte* nsec,
            byte* c,
            ulong clen,
            byte* mac,
            byte* ad,
            ulong adlen,
            byte* npub,
            byte* k);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_aead_chacha20poly1305_keybytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_aead_chacha20poly1305_npubbytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_aead_chacha20poly1305_nsecbytes();

        #endregion Original

        #region IETF

        internal const int crypto_aead_chacha20poly1305_ietf_ABYTES = 16;
        internal const int crypto_aead_chacha20poly1305_ietf_KEYBYTES = 32;
        internal const int crypto_aead_chacha20poly1305_ietf_NPUBBYTES = 12;
        internal const int crypto_aead_chacha20poly1305_ietf_NSECBYTES = 0;

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_aead_chacha20poly1305_ietf_abytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_aead_chacha20poly1305_ietf_decrypt(
            byte* m,
            out ulong mlen_p,
            byte* nsec,
            byte* c,
            ulong clen,
            byte* ad,
            ulong adlen,
            Nonce12* npub,
            byte* k);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_aead_chacha20poly1305_ietf_encrypt(
            byte* c,
            out ulong clen_p,
            byte* m,
            ulong mlen,
            byte* ad,
            ulong adlen,
            byte* nsec,
            Nonce12* npub,
            byte* k);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_aead_chacha20poly1305_ietf_encrypt_detached(
            byte* c,
            byte* mac,
            out ulong maclen_p,
            byte* m,
            ulong mlen,
            byte* ad,
            ulong adlen,
            byte* nsec,
            byte* npub,
            byte* k);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static unsafe extern int crypto_aead_chacha20poly1305_ietf_decrypt_detached(
            byte* m,
            byte* nsec,
            byte* c,
            ulong clen,
            byte* mac,
            byte* ad,
            ulong adlen,
            byte* npub,
            byte* k);

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_aead_chacha20poly1305_ietf_keybytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_aead_chacha20poly1305_ietf_npubbytes();

        [DllImport(Libraries.Libsodium, CallingConvention = CallingConvention.Cdecl)]
        internal static extern UIntPtr crypto_aead_chacha20poly1305_ietf_nsecbytes();

        #endregion IETF
    }
}
