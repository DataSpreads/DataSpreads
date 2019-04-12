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

using DataSpreads.Buffers;
using Spreads.DataTypes;
using Spreads.Serialization.Utf8Json;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Spreads.Serialization;

namespace DataSpreads.StreamLogs
{
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    [BinarySerialization(Size)]
    internal struct StreamBlockRecord
    {
        public const int Size = 20;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamBlockRecord(BufferRef bufferRef)
        {
            Version = default;
            Timestamp = default;
            BufferRef = bufferRef;
        }

        //
        public const int VersionOffset = 0;

        [FieldOffset(VersionOffset)]
        public ulong Version;

        public const int TimestampOffset = 8;

        [FieldOffset(TimestampOffset)]
        public Timestamp Timestamp;

        //
        public const int BufferRefOffset = 16;

        /// <summary>
        /// If not moved, then this is a ref to a shared buffer in LMDB. If moved then this is default.
        /// </summary>
        [FieldOffset(BufferRefOffset)]
        public BufferRef BufferRef;

        /// <summary>
        /// After packing we delete <see cref="BufferRef"/> from the record and the buffer
        /// is returned to the pool. Note that <see cref="BufferRef"/> could still be present
        /// but <see cref="SharedMemory"/> <see cref="SharedMemory.BufferHeader"/> could be
        /// in <see cref="SharedMemory.BufferHeader.HeaderFlags.IsPackedStreamBlock"/> state.
        /// In that case <see cref="BufferRef"/> is invalid and readers must take a block from
        /// storage.
        /// </summary>
        public bool IsPacked
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => BufferRef == default;
        }
    }

    // Could reduce size by 4 byte. Originally was 32 and that was a problem but then
    // we did not calculate rate hint and stored all data in the smallest chunks. Then
    // index started to grow a lot. Keep
    //[Obsolete("Much easier will be to migrate to this, even for existing data.")]
    //[StructLayout(LayoutKind.Explicit, Size = 16, Pack = 4)]
    //internal struct StreamBlockRecordSmall
    //{
    //    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    //    public StreamBlockRecordSmall(BufferRef bufferRef)
    //    {
    //        Version = default;
    //        Timestamp = default;
    //        BufferRef = bufferRef;
    //    }

    //    //
    //    public const int VersionOffset = 0;

    //    [FieldOffset(VersionOffset)]
    //    public uint Version;

    //    public const int TimestampOffset = 4;

    //    [FieldOffset(TimestampOffset)]
    //    public Timestamp Timestamp;

    //    //
    //    public const int BufferRefOffset = 12;

    //    /// <summary>
    //    /// If not moved, then this is a ref to a buffer in LMDB. If moved then this is default.
    //    /// </summary>
    //    [FieldOffset(BufferRefOffset)]
    //    public BufferRef BufferRef;
    //}

    [JsonFormatter(typeof(Formatter))]
    [StructLayout(LayoutKind.Sequential, Size = 32)]
    public struct StreamBlockInfo
    {
        // StreamId is in a context outside this struct.

        public ulong FirstVersion;

        public Timestamp Timestamp;

        public int Count;

        /// <summary>
        /// Logical id of a file in a stream context.
        /// </summary>
        ///
        public int FileId;

        /// <summary>
        /// Start of a chunk payload in the file.
        /// </summary>
        public int PayloadOffset;

        /// <summary>
        /// Payload length
        /// </summary>
        public int PayloadLength;

        public StreamBlockInfo(ulong firstVersion, Timestamp timestamp, int fileId, int payloadOffset)
        {
            FirstVersion = firstVersion;
            Timestamp = timestamp;
            FileId = fileId;
            PayloadOffset = payloadOffset;
            // TODO
            PayloadLength = default;
            Count = default;
        }

        internal class Formatter : IJsonFormatter<StreamBlockInfo>
        {
            public void Serialize(ref JsonWriter writer, StreamBlockInfo value, IJsonFormatterResolver formatterResolver)
            {
                writer.WriteBeginArray();

                writer.WriteUInt64(value.FirstVersion);
                writer.WriteValueSeparator();

                writer.WriteInt64((long)value.Timestamp);
                writer.WriteValueSeparator();

                writer.WriteInt32(value.FileId);
                writer.WriteValueSeparator();

                writer.WriteInt32(value.PayloadOffset);

                writer.WriteEndArray();
            }

            public StreamBlockInfo Deserialize(ref JsonReader reader, IJsonFormatterResolver formatterResolver)
            {
                reader.ReadIsBeginArrayWithVerify();

                var version = reader.ReadUInt64();

                reader.ReadIsValueSeparatorWithVerify();
                var timestamp = (Timestamp)reader.ReadInt64();

                reader.ReadIsValueSeparatorWithVerify();
                var fileId = reader.ReadInt32();

                reader.ReadIsValueSeparatorWithVerify();
                var fileOffset = reader.ReadInt32();

                reader.ReadIsEndArrayWithVerify();

                var message = new StreamBlockInfo(version, timestamp, fileId, fileOffset);

                return message;
            }
        }
    }

    //[StructLayout(LayoutKind.Explicit, Size = 16)]
    //internal struct StreamLogPersistedWalRecord
    //{
    //    //
    //    public const int PositionOffset = 0;

    //    [FieldOffset(PositionOffset)]
    //    public ulong Position;

    //    public const int StreamIdOffset = 8;

    //    [FieldOffset(StreamIdOffset)]
    //    public long StreamId;
    //}
}
