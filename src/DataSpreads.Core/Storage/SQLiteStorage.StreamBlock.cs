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

using DataSpreads.StreamLogs;
using Spreads;
using Spreads.Algorithms.Hash;
using Spreads.Buffers;
using Spreads.Serialization;
using Spreads.SQLite;
using Spreads.SQLite.Fast;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace DataSpreads.Storage
{
    // ReSharper disable once InconsistentNaming
    internal partial class SQLiteStorage : IStreamBlockStorage
    {
        private const string StreamBlockTable = "stream_blocks";

        private static void CreateStreamBlockTable(SqliteConnection connection)
        {
            var commandText =
#pragma warning disable HAA0201 // Implicit string concatenation allocation
                $"CREATE TABLE IF NOT EXISTS `{StreamBlockTable}` (\n" +
                "  `StreamId`        INTEGER NOT NULL,\n" +
                "  `BlockKey`        INTEGER NOT NULL,\n" + // Version
                "  `Timestamp`       INTEGER NOT NULL,\n" +
                "  `Count`           INTEGER NOT NULL,\n" +
                "  `UncompressedLen` INTEGER NOT NULL,\n" +
                "  `WriteEnd`        INTEGER DEFAULT 0,\n" +
                "  `BlobCrc32`       INTEGER NOT NULL,\n" +
                "  `RawData`         BLOB,\n" +
                "  PRIMARY KEY (`StreamId`,`BlockKey`));\n" +
                $" CREATE INDEX IF NOT EXISTS stream_id_timestamp_index ON `{StreamBlockTable}` (`StreamId`, `Timestamp`);";
#pragma warning restore HAA0201 // Implicit string concatenation allocation

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.ExecuteNonQuery();
            command.Dispose();
        }

        private readonly ConcurrentDictionary<long, Task<StreamBlock>> _prefetchDictionary = new ConcurrentDictionary<long, Task<StreamBlock>>();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong LastStreamBlockKey(long streamId)
        {
            var qs = RentQueries();
            try
            {
                return qs.LastStreamBlockKey(streamId, false);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        public void ClearCache(long streamId)
        {
            if (_prefetchDictionary.TryRemove(streamId, out var value))
            {
                value.Dispose();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamBlock TryGetStreamBlock(long streamId, ulong blockKey)
        {
            // TODO this should be on cursor side
            if (_prefetchDictionary.TryRemove(streamId, out var value))
            {
                if (value.Result.IsValid)
                {
                    if (value.Result.FirstVersion == blockKey)
                    {
                        // Console.WriteLine("hit");
                        Prefetch();
                        return value.Result;
                    }
                }

                value.Dispose();
                var chunk1 = GetStreamBlock(streamId, blockKey);
                return chunk1;
            }

            //if (!chunk.IsValid || chunk.StreamId != streamId || chunk.FirstVersion != blockKey)
            //{
            //    ThrowHelper.FailFast(GetFailedWrongStreamIdOrVersionMessage);
            //}

            void Prefetch()
            {
                _prefetchDictionary[streamId] = Task.Run(() =>
                {
                    var nextBlock = GetStreamBlock(streamId, blockKey + 1);
                    if (!nextBlock.IsValid)
                    {
                        Debug.WriteLine("invalid block: " + (blockKey + 1));
                    }
                    return nextBlock;
                    //_prefetchDictionary.AddOrUpdate(streamId, nextBlock, (sid, e) =>
                    //{
                    //    //if (e.ReferenceCount > 0)
                    //    //{
                    //    //    e.Dispose();
                    //    //}

                    //    return nextBlock;
                    //});
                });
            }

            var chunk = GetStreamBlock(streamId, blockKey);
            Prefetch();
            return chunk;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private StreamBlock GetStreamBlock(long streamId, ulong blockKey)
        {
            var qs = RentQueries();
            try
            {
                return qs.GetStreamBlock(streamId, blockKey);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        public bool TryPutStreamBlock(in StreamBlock blockView)
        {
            try
            {
                return Put(in blockView) > 0;
            }
            catch
            {
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Put(in StreamBlock blockView)
        {
            var (inserted, rowId) = InsertBlock(in blockView);
            if (inserted)
            {
                return rowId;
            }
            ThrowHelper.ThrowInvalidOperationException(PutFailMessage);
            return default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (bool inserted, long storageId) InsertBlock(in StreamBlock blockView)
        {
            var qs = RentQueries();
            try
            {
                return qs.InsertBlock(in blockView);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public (bool, long) InsertChunk(StreamBlock blockView, Transaction txn)
        //{
        //    return txn.Queries.InsertChunk(blockView);
        //}

        public partial class StorageQueries
        {
            internal FastQuery InsertBlockQuery;
            internal FastQuery LastVersionQuery;
            internal FastQuery GetStreamBlockQuery;

            private void CreateStreamBlockQueries(SqliteConnection connection, ConnectionPool pool)
            {
                InsertBlockQuery = CreateInsertChunkQuery(connection, pool);
                LastVersionQuery = CreateLastVersionQuery(connection, pool);
                GetStreamBlockQuery = CreateGetStreamBlockQuery(connection, pool);
            }

            private void DisposeStreamBlockQueries()
            {
                InsertBlockQuery.Dispose();
                LastVersionQuery.Dispose();
                GetStreamBlockQuery.Dispose();
            }

            #region InsertBlock

            private FastQuery CreateInsertChunkQuery(SqliteConnection connection, ConnectionPool pool)
            {
                // Atomic insert, cannot insert a chunk whose version is not equal to last completed chunk version + count
                // SQL sample:
                //INSERT INTO test("id1", "version", "count")
                //SELECT 2 as id1, 7 as version, 2 as count
                //WHERE id1 > 0 AND count > 0
                //AND version = coalesce((SELECT version + count as vv FROM test WHERE id1 = 2 ORDER BY version DESC LIMIT 1), 1)

#pragma warning disable HAA0201 // Implicit string concatenation allocation
                var sql = $"INSERT OR REPLACE INTO {StreamBlockTable} " +
                          $"(`StreamId`, `BlockKey`, `Timestamp`, `Count`, `UncompressedLen`, `WriteEnd`, `BlobCrc32`, `RawData`) " +
                          $"SELECT @StreamId as `StreamId`, @BlockKey as `BlockKey`, @Timestamp as `Timestamp`, @Count as `Count`, @UncompressedLen as `UncompressedLen`, @WriteEnd as `WriteEnd`, @BlobCrc32 as `BlobCrc32`, @RawData as `RawData` " +
                          $"WHERE `StreamId` > 0 AND `Count` > 0 " +
                          $"AND `BlockKey` = coalesce((SELECT `BlockKey` + `Count` FROM {StreamBlockTable} WHERE `StreamId` = @StreamId AND `WriteEnd` > 0 ORDER BY `BlockKey` DESC LIMIT 1), 1)";
                // TODO Where WriteEnd > 0
#pragma warning restore HAA0201 // Implicit string concatenation allocation
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public unsafe (bool, long) InsertBlock(in StreamBlock blockView)
            {
                // 1. @StreamId as `StreamId`,
                const int streamIdColumn = 1;

                // 2. @BlockKey as `BlockKey`,
                const int blockKeyColumn = 2;

                // 3. @Timestamp as `Timestamp`,
                const int tsColumn = 3;

                // 4. @Count as `Count`,
                const int countColumn = 4;

                // 5. @UncompressedLen as `UncompressedLen`,
                const int uncompressedLenColumn = 5;

                // 6. @WriteEnd as `WriteEnd`,
                const int writeEndColumn = 6;

                // 7. @BlobCrc32 as `BlobCrc32`,
                const int crc32Column = 7;

                // 8. @RawData as `RawData`
                const int blobColumn = 8;

                if (!blockView.IsValid)
                {
                    ThrowHelper.FailFast("BlockView is invalid, check this before low-level InsertBlock method");
                }

                if (blockView.StreamLogIdLong == 0)
                {
                    ThrowHelper.FailFast("BlockView.StreamId == 0");
                }

                var blobSize = BinarySerializer.SizeOf(in blockView, out var payload,
                    SerializationFormat.BinaryZstd);

                if (payload == null)
                {
                    ThrowHelper.FailFast("retainedMemory.IsEmpty in InsertBlock:BinarySerializer.SizeOf");
                    return default;
                }

                Debug.Assert(blobSize == payload.WrittenLength);
                Debug.Assert(blobSize > 0);

                //byte[] tmpArray = null;
                //MemoryHandle pin = default;
                try
                {
                    //tmpArray = segment.Array;
                    //pin = ((Memory<byte>)tmpArray).Slice(segment.Offset).Pin();

                    var ptr = (IntPtr)payload.WrittenBuffer.Data;
                    // TODO serialize before bind because we need to release temp buffer
                    InsertBlockQuery.Bind((binder, tuple) =>
                    {
                        int rc;
                        var (block, ptrBlob, lenBlob) = tuple;
                        if ((rc = binder.BindInt64(streamIdColumn, block.StreamLogIdLong)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.StreamId: " + rc);
                        }

                        if ((rc = binder.BindInt64(blockKeyColumn, checked((long)block.FirstVersion))) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.FirstVersion: " + rc);
                        }

#pragma warning disable 618
                        if ((rc = binder.BindInt64(tsColumn, (long)block.GetFirstTimestampOrWriteStart())) != 0)
#pragma warning restore 618
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.FirstVersion: " + rc);
                        }

                        if ((rc = binder.BindInt64(countColumn, block.CountVolatile)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.Count: " + rc);
                        }

                        if ((rc = binder.BindInt64(uncompressedLenColumn, block.Length)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.Length: " + rc);
                        }

                        if ((rc = binder.BindInt64(writeEndColumn, (long)block.WriteEnd)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind ch.WriteEnd: " + rc);
                        }

                        // Skip file id

                        var crc32 = Crc32C.CalculateCrc32C((byte*)ptrBlob, lenBlob);
                        if ((rc = binder.BindInt64(crc32Column, crc32)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind crc32: " + rc);
                        }

                        if ((rc = binder.BindBlob(blobColumn, ptrBlob, lenBlob)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk blob: " + rc);
                        }
                    }, (chunk: blockView, ptr, blobSize));

                    return InsertBlockQuery.Step((hasRow, reader, state) =>
                    {
                        if (hasRow)
                        {
                            ThrowHelper.FailFast("InsertChunk should not return rows");
                        }

                        var changes = reader.Changes();
                        if (changes == 0)
                        {
                            Trace.TraceWarning("Cannot insert StreamBlock");
                        }
                        return (changes == 1, reader.LastRowId());
                    }, (object)null);
                }
                catch (Exception ex)
                {
                    ThrowHelper.FailFast("Cannot insert chunk: " + ex);
                    return (false, 0);
                }
                finally
                {
                    payload.Dispose();
                    InsertBlockQuery.ClearAndReset();
                }
            }

            #endregion InsertBlock

            #region LastStreamLogVersion

            private FastQuery CreateLastVersionQuery(SqliteConnection connection, ConnectionPool pool)
            {
                // SELECT coalesce((SELECT version + count FROM test WHERE id1 = 1 ORDER BY version DESC LIMIT 1), 0) as last_version
#pragma warning disable HAA0201 // Implicit string concatenation allocation
                var sql = $"SELECT coalesce((SELECT (`BlockKey` + (`Count` - 1)*@AddChunkCount) FROM {StreamBlockTable} WHERE `StreamId` = @StreamId ORDER BY `BlockKey` DESC LIMIT 1), 0) as last_version";
#pragma warning restore HAA0201 // Implicit string concatenation allocation
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            /// <summary>
            /// Get last block key or last element key.
            /// </summary>
            /// <param name="streamId">Stream id.</param>
            /// <param name="addChunkItemCount">Add block count to the result.</param>
            /// <returns></returns>
            public ulong LastStreamBlockKey(long streamId, bool addChunkItemCount)
            {
                try
                {
                    LastVersionQuery.Bind((binder, state) =>
                    {
                        if (binder.BindInt64(1, state.addChunkItemCount ? 1 : 0) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind addChunkCount");
                        }

                        if (binder.BindInt64(2, state.streamId) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind stream id");
                        }
                    }, (streamId, addChunkItemCount));
                    return LastVersionQuery.Step((hasRow, reader, st) =>
                    {
                        if (hasRow)
                        {
                            return checked((ulong)reader.ColumnInt64(0));
                        }
                        return 0UL;
                    }, (object)null);
                }
                finally
                {
                    LastVersionQuery.ClearAndReset();
                }
            }

            #endregion LastStreamLogVersion

            #region GetStreamBlock

            private FastQuery CreateGetStreamBlockQuery(SqliteConnection connection, ConnectionPool pool)
            {
#pragma warning disable HAA0201 // Implicit string concatenation allocation
                var sql = $"SELECT `RawData` FROM {StreamBlockTable} WHERE `StreamId` = @StreamId AND `BlockKey` >= @BlockKey ORDER BY `BlockKey` ASC LIMIT 1 ";
#pragma warning restore HAA0201 // Implicit string concatenation allocation
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public unsafe StreamBlock GetStreamBlock(long streamId, ulong blockKey)
            {
                try
                {
                    GetStreamBlockQuery.Bind((binder, state) =>
                    {
                        if (binder.BindInt64(1, state.streamId) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind streamId");
                        }

                        if (binder.BindInt64(2, checked((long)state.blockKey)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind blockKey");
                        }
                    }, (streamId, blockKey));
                    return GetStreamBlockQuery.Step((hasRow, reader, st) =>
                    {
                        if (hasRow)
                        {
                            var len = reader.ColumnBytes(0);
                            var ptr = reader.ColumnBlob(0);
                            var db = new DirectBuffer(len, (byte*)ptr);
                            BinarySerializer.Read(db, out StreamBlock chunk);
                            return chunk;
                        }
                        return default(StreamBlock);
                    }, (object)null);
                }
                finally
                {
                    GetStreamBlockQuery.ClearAndReset();
                }
            }

            #endregion GetStreamBlock
        }
    }
}
