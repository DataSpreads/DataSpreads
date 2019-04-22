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
using System.Runtime.CompilerServices;

namespace DataSpreads.StreamLogs
{
    /// <summary>
    /// Static flags that do not change during StreamLog lifetime.
    /// </summary>
    [Flags]
    internal enum StreamLogFlags : byte
    {
        None = 0b_0000_0000,
        IsBinary = 0b_0000_0001,
        NoTimestamp = 0b_0000_0010,

        // TODO should we have a mode when we still do packing but just do not remove from SM?
        // It's easy to block packing by holding a cursor at old block, so no we do not need that.
        NoPacking = 0b_0000_0100,

        /// <summary>
        /// Packing just drop blocks (for temp streams)
        /// </summary>
        DropPacked = 0b_0000_1000,

        /// <summary>
        /// Works only for larger blocks when we have Pow2 shared memory + one page.
        /// With this flag we place the <see cref="StreamBlock.StreamBlockHeader"/>
        /// in the last <see cref="StreamBlock.StreamBlockHeader.Size"/> bytes of
        /// the first page so that payload span is Pow2. This allows for Aeron-style
        /// writing by atomically incrementing current position. Currently used by
        /// <see cref="NotificationLog"/> and could be exposed as an option for
        /// shared streams.
        /// </summary>
        Pow2Payload = 0b_0001_0000
    }

    internal static class StreamLogFlagsExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsBinary(this StreamLogFlags flags)
        {
            return ((byte)flags & (byte)StreamLogFlags.IsBinary) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool NoTimestamp(this StreamLogFlags flags)
        {
            return ((byte)flags & (byte)StreamLogFlags.NoTimestamp) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool NoPacking(this StreamLogFlags flags)
        {
            return ((byte)flags & (byte)StreamLogFlags.NoPacking) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool DropPacked(this StreamLogFlags flags)
        {
            return ((byte)flags & (byte)StreamLogFlags.DropPacked) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Pow2Payload(this StreamLogFlags flags)
        {
            return ((byte)flags & (byte)StreamLogFlags.Pow2Payload) != 0;
        }
    }
}
