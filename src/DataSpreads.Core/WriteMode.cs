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

namespace DataSpreads
{
    /// <summary>
    /// Write synchronization mode.
    /// </summary>
    [Flags]
    public enum WriteMode : byte
    {
        // 0 1 2 3 4 5 6 7 8
        // +-+-+-+-+-+-+-+-+
        // |0|S|E|C|R|A|L|W|
        // +---------------+
        // W - Write
        // L - Local real-time
        // A - AckLocal
        // R - Remote real-time, requires AckLocal
        // C - AckRemote
        // E - Exclusive lock
        // S - Shared write
        // 0 - Reserved

        // Read only mode is implicit as absence of other values.
        // ReadOnly = 0b_0000_0000,

        /// <summary>
        /// 
        /// </summary>
        BatchWrite = 0b_0000_0001, // RT write is additional feature/bit, batch is the simplest

        /// <summary>
        /// Sync data on the current machine in real time and
        /// only synchronize batches to a remote server.
        /// </summary>
        LocalSync = 0b_0000_0010 | BatchWrite,

        /// <summary>
        /// Same as <see cref="LocalSync"/> but waits for acknowledgement that
        /// data is durably written to a local disk or a cluster.
        /// </summary>
        /// <remarks>
        /// Cluster on a very fast network (Aeron/Kafka) could be faster than local
        /// disk persistent write and more reliable. By default WAL writes to a local
        /// disk but it could be configured to write to a network cluster.
        /// </remarks>
        LocalAck = 0b_0000_0100 | LocalSync,

        /// <summary>
        /// Sync data on the current machine in real time and send updates
        /// to a remote server asynchronously (as fast as possible).
        /// Use <see cref="ExclusiveLock"/>  if concurrent writes are
        /// possible but not desirable or <see cref="SharedLocalWrite"/>
        /// if concurrent writes are expected and the remote server
        /// determines the final order of concurrent writes.
        /// Without additional options writes use optimistic concurrency when
        /// the remote server accepts an update only when it's version, checksum
        /// and optional hash indicate that the update is the next value after
        /// the last existing one on the server. <see cref="RemoteAck"/> prevents
        /// any conflicts but is slower than <see cref="ExclusiveLock"/> for live
        /// streaming data.
        /// </summary>
        RemoteSync = 0b_0000_1000 | LocalAck,

        /// <summary>
        /// Same as <see cref="RemoteSync"/> but waits for acknowledgement that
        /// data is durably written to a remote server.
        /// </summary>
        RemoteAck = 0b_0001_0000 | RemoteSync | LocalAck,

        ///// <summary>
        ///// Every write first goes to a remote server (if any). Omitting version allows many
        ///// writers to commit values concurrently to a stream and the remote server
        ///// determines the order of values in FIFO manner.
        ///// This is useful for data collection from lite clients when value has
        ///// a way to identify the writer source.
        ///// For duplicate collectors it's better to create a separate branch per
        ///// collector and then merge streams.
        ///// </summary>

        // TODO Shared remote is RemoteAck or very complicated otherwise. Review it very later

        SharedLocalWrite = 0b_0010_0000,

        // TODO this is how RemoteSync works.
        /// <summary>
        /// Acquire and maintain exclusive remote lock on a stream. Writes
        /// are safe when the lock is held. If remote connection is lost
        /// then writes could continue until the lock is expired (1 minute by default).
        /// </summary>
        ExclusiveLock = 0b_0100_0000
    }

    /// <summary>
    /// Indivisual bits are features, comparing entire enums requires knowledge of bit layout
    /// so do it closer to definition and wrap into methods instead of doing it manually
    /// in every place.
    /// </summary>
    internal static class WriteModeExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsReadOnly(this WriteMode mode)
        {
            return (byte)mode == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasBatchWriteOnly(this WriteMode mode)
        {
            return (byte)mode == 0b_0000_0001;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasLocalSync(this WriteMode mode)
        {
            return (((byte)mode) & 0b_0000_0010) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasLocalAck(this WriteMode mode)
        {
            return (((byte)mode) & 0b_0000_0100) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasRemoteSync(this WriteMode mode)
        {
            return (((byte)mode) & 0b_0000_1000) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasRemoteAck(this WriteMode mode)
        {
            return (((byte)mode) & 0b_0001_0000) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasSharedLocalWrite(this WriteMode mode)
        {
            return (((byte)mode) & 0b_0010_0000) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool HasExclusiveLock(this WriteMode mode)
        {
            return (((byte)mode) & 0b_0100_0000) != 0;
        }
    }
}
