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
using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using DataSpreads.Security.Crypto;
using DataSpreads.Security.Crypto.Native;
using Spreads.Buffers;
using Spreads.Serialization;

namespace DataSpreads.Security.SecureStorage
{
    /// <summary>
    /// Tries to protect from physical access to machine, e.g. stolen laptop.
    /// On Windows it uses DPAPI. Not implemented on Linux and OSX.
    /// </summary>
    /// <remarks>
    /// Note that this does not protect from malware running under OS user account
    /// from accessing the unprotected keys. If that is important then data must be
    /// protected at application level (e.g. encrypt with user password).
    /// This class provides default persistent storage that is reasonably
    /// secure for the kind of data we try to protect in DataSpreads.
    ///
    /// <para />
    /// TODO this para is not related to SecureStorage, just wrote here while having a though. Move to a proper place.
    /// 
    /// In the main use case is protecting identity. For that we use an Ethereum
    /// key as a master key (MK) because there exist quite mature infrastructure (e.g. Metamask, Trezor)
    /// and we want to use Ethereum as PKI. We use MK to sign
    /// a public key (PK) of an locally generated Ed25519 key pair.
    /// Then we store the Ed25519 secret key (SK) in <see cref="SecureStorage"/>
    /// and use it for signing the Ed25519 public key.
    ///
    /// <para />
    /// The signed Ed25519 PK is registered with DSIO during TOFU process.
    /// A client must have an one-time API_TOKEN or have ID_TOKEN to connect
    /// to DSIO server.
    ///
    /// 1. A client connects to the server with API_TOKEN or ID_TOKEN. The
    /// tokens could be acquired via UI or via user/password login if the
    /// client supports the that.
    /// 
    /// 2. The server sends USER_ID and random salt to the client.
    ///
    /// 3. The client signs concatenated SEED_STRING+USER_ID+PK+Salt with MK.
    ///     SEED_STRING is `DATASPREADS_USER_ID:`
    ///     USER_ID is integer.
    ///
    /// 4. The client generates an Ethereum signed transaction to add PK
    /// to DSIO PKI smart contract. The PKI contract onl
    /// 
    /// 
    /// <para />
    /// 
    /// On OSX will probably use keychain to store master password.
    /// On Linux will likely not store plaintext in files by default.
    /// Need to extract interface and provide DI option. Cannot solve the problem in generic way.
    /// </remarks>
    public class SecureStorage 
    {
        // see https://github.com/atom/node-keytar/blob/master/src/keytar_mac.cc

        internal struct StoredKey
        {
            public int Version { get; set; }
            public byte[] Bytes { get; set; }
        }



    }
}
