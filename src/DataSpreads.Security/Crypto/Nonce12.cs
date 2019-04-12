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

using System.Diagnostics;
using System.Runtime.InteropServices;
using Spreads.Serialization;

namespace DataSpreads.Security.Crypto
{
    /// <summary>
    /// Nonce for AEAD. Both AES-GCM and ChaCha20/Poly1305.
    /// </summary>
    [DebuggerDisplay("{" + nameof(FirstVersion) + ("} - {" + nameof(Count) + "}"))]
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    [BinarySerialization(Size)]
    public readonly struct Nonce12
    {
        public const int Size = 12;

        // Layout matches a region of StreamBlock/MessageBlock header. Should be used via ref.

        [FieldOffset(0)]
        public readonly ulong FirstVersion;
        [FieldOffset(8)]
        public readonly int Count;

        public Nonce12(ulong firstVersion, int count)
        {
            FirstVersion = firstVersion;
            Count = count;
        }
    }
}
