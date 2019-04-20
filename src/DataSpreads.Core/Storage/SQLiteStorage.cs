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

using Spreads;
using Spreads.Buffers;
using Spreads.Collections.Concurrent;
using Spreads.SQLite;
using Spreads.SQLite.Fast;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Dapper;

namespace DataSpreads.Storage
{
    // we need to reuse SQLite connection so we use a single class that pools connections, caches prepared statements and implements different storage interfaces

    // ReSharper disable once InconsistentNaming
    internal partial class SQLiteStorage
    {
        private const string TestTable = "test_table";

        private readonly ConnectionPool _connectionPool;

        // private readonly LockedObjectPool<StorageQueries> _queriesObjectPool;
        private readonly BlockingCollection<StorageQueries> _queriesObjectPool;
        private int _allocCount;
        public SQLiteStorage(string connectionString)
        {
            _connectionPool = new ConnectionPool(connectionString);
            _queriesObjectPool= new BlockingCollection<StorageQueries>(new ConcurrentBag<StorageQueries>(), Environment.ProcessorCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Transaction Begin()
        {
            var qs = RentQueries();

            if (qs.Begin())
            {
                return new Transaction(qs, this);
            }
            ThrowHelper.ThrowInvalidOperationException("Cannot begin transaction");
            return default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Transaction BeginConcurrent()
        {
            var qs = RentQueries();

            if (qs.BeginConcurrent())
            {
                return new Transaction(qs, this, true);
            }
            ThrowHelper.ThrowInvalidOperationException("Cannot begin transaction");
            return default;
        }

        public void Checkpoint(bool passive = false)
        {
            var qs = RentQueries();
            try
            {
                qs.Connection.Execute(passive ? "PRAGMA wal_checkpoint(PASSIVE);" : "PRAGMA wal_checkpoint(FULL);");
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (bool, long) InsertTestQuery(long a, long b, RetainedMemory<byte> rawData) //, Transaction txn)
        {
            // return txn.Queries.InsertTestQuery(a, b, rawData);
            var qs = RentQueries();
            try
            {
                return qs.InsertTest(a, b, rawData);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (long, long) SelectTestQuery(long a, long b)
        {
            // return txn.Queries.InsertTestQuery(a, b, rawData);
            var qs = RentQueries();
            try
            {
                return qs.SelectTest(a, b);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private StorageQueries RentQueries()
        {
            if (_queriesObjectPool.TryTake(out var qs))
            {
                return qs;
            }

            if (_allocCount >= Environment.ProcessorCount)
            {
                return _queriesObjectPool.Take();
            }

            Interlocked.Increment(ref _allocCount);
            return new StorageQueries(_connectionPool.Rent(), _connectionPool);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReturnQueries(StorageQueries queriesObject)
        {
            if (!_queriesObjectPool.TryAdd(queriesObject))
            {
                Console.WriteLine("Disposing queries");
                queriesObject.Dispose();
            }

            //if (!_queriesObjectPool.Return(queriesObject))
            //{
            //    Console.WriteLine("Disposing queries");
            //    queriesObject.Dispose();
            //}
        }

        public void Dispose()
        {
            _queriesObjectPool.Dispose();
            _connectionPool.Dispose();
        }

        public struct Transaction : IDisposable
        {
            private SQLiteStorage _storage;
            private StorageQueries _queries;
            private bool _commited;
            private bool _concurrent;

            internal Transaction(StorageQueries queries, SQLiteStorage storage, bool concurrent = false)
            {
                _queries = queries;
                _storage = storage;
                _commited = false;
                _concurrent = concurrent;
            }

            internal StorageQueries Queries
            {
                get => _queries;
                private set => _queries = value;
            }

            public bool Concurrent => _concurrent;

            public bool Commit()
            {
                if (_commited)
                {
                    ThrowHelper.ThrowInvalidOperationException("Already commited");
                }
                _commited = Queries.Commit();
                return _commited;
            }

            public int RawCommit()
            {
                if (_commited)
                {
                    ThrowHelper.ThrowInvalidOperationException("Already commited");
                }
                var rc = Queries.RawCommit();
                _commited = rc == Spreads.SQLite.Interop.Constants.SQLITE_DONE;
                return rc;
            }

            public void Dispose()
            {
                if (!_commited)
                {
                    Queries.Rollback();
                }
                _storage.ReturnQueries(Queries);
                Queries = null;
                _storage = null;
            }
        }

        public partial class StorageQueries : IDisposable
        {
            private SqliteConnection _connection;
            private ConnectionPool _pool;

            // Stream

            internal readonly FastQuery BeginQuery;
            internal readonly FastQuery BeginConcurrentQuery;
            internal readonly FastQuery CommitQuery;
            internal readonly FastQuery RollbackQuery;

            internal readonly FastQuery InsertTestQuery;
            internal readonly FastQuery SelectTestQuery;

            public StorageQueries(SqliteConnection connection, ConnectionPool pool)
            {
                _connection = connection;
                _pool = pool;

                BeginQuery = CreateBeginQuery(connection, pool);
                BeginConcurrentQuery = CreateBeginConcurrentQuery(connection, pool);
                CommitQuery = CreateCommitQuery(connection, pool);
                RollbackQuery = CreateRollbackQuery(connection, pool);

                InsertTestQuery = CreateInsertTestQuery(connection, pool);
                SelectTestQuery = CreateSelectTestQuery(connection, pool);

                CreateStreamBlockQueries(connection, pool);
                CreateContainerBlockQueries(connection, pool);
                CreateMetadataQueries(connection, pool);
            }

            public SqliteConnection Connection => _connection;

            private FastQuery CreateBeginQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"BEGIN;";
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            private FastQuery CreateBeginConcurrentQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"BEGIN CONCURRENT;";
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public bool Begin()
            {
                try
                {
                    return BeginQuery.Step((hasRow, reader, st) => true, (object)null);
                }
                finally
                {
                    BeginQuery.Reset();
                }
            }

            public bool BeginConcurrent()
            {
                try
                {
                    // TODO use raw step
                    return BeginConcurrentQuery.Step((hasRow, reader, st) => true, (object)null);
                }
                finally
                {
                    BeginQuery.Reset();
                }
            }

            private FastQuery CreateCommitQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"COMMIT;";
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public bool Commit()
            {
                try
                {
                    return CommitQuery.Step((hasRow, reader, st) => true, (object)null);
                }
                finally
                {
                    CommitQuery.Reset();
                }
            }

            public int RawCommit()
            {
                try
                {
                    return CommitQuery.RawStep((hasRow, reader, st) => true, (object)null, out _);
                }
                finally
                {
                    CommitQuery.Reset();
                }
            }

            private FastQuery CreateRollbackQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"ROLLBACK;";
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public bool Rollback()
            {
                try
                {
                    return RollbackQuery.Step((hasRow, reader, st) => true, (object)null);
                }
                finally
                {
                    RollbackQuery.Reset();
                }
            }

            private FastQuery CreateInsertTestQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"INSERT OR REPLACE INTO `{TestTable}`(`A`, `B`, `RawData`) VALUES(@A, @B, @RawData);";
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public unsafe (bool, long) InsertTest(long a, long b, RetainedMemory<byte> rawData)
            {
                const int aColumn = 1;

                const int bColumn = 2;

                const int dataColumn = 3;

                try
                {
                    var sw = new SpinWait();

                    // TODO serialize before bind because we need to release temp buffer
                    InsertTestQuery.Bind((binder, tuple) =>
                    {
                        var blobSize = tuple.rawData.Length;
                        var ptr = (IntPtr)tuple.rawData.Pointer;
                        Debug.Assert(blobSize > 0);

                        int rc;
                        if ((rc = binder.BindInt64(aColumn, tuple.a)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.StreamId: " + rc);
                        }

                        if ((rc = binder.BindInt64(bColumn, tuple.b)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.FirstVersion: " + rc);
                        }

                        if ((rc = binder.BindBlob(dataColumn, ptr, blobSize)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk blob: " + rc);
                        }
                    }, (a, b, rawData));

                RETRY:
                //  var rcBegin = BeginConcurrentQuery.RawStep((hasRow, reader, st) => true, (object)null, out _);
                // Console.WriteLine("rcBegin: " + rcBegin);

                RETRY_TEST:
                    var rcTest = InsertTestQuery.RawStep((hasRow, reader, state) =>
                    {
                        if (hasRow)
                        {
                            ThrowHelper.FailFast("InsertChunk should not return rows");
                        }

                        return (reader.Changes() == 1, reader.LastRowId());
                    }, (object)null, out var result);

                //if (rcTest != Spreads.SQLite.Interop.Constants.SQLITE_DONE)
                //{
                //    Console.WriteLine("rcTest: " + rcTest);
                //}

                RETRY_COMMIT:
                    // var rcCommit = CommitQuery.RawStep((hasRow, reader, st) => true, (object)null, out _);
                    if (rcTest != Spreads.SQLite.Interop.Constants.SQLITE_DONE)
                    {
                        // Console.WriteLine(rcCommit);
                        sw.SpinOnce();
                        goto RETRY_TEST;

                        ////// Console.WriteLine(rcTest);
                        //if (rcCommit == Spreads.SQLite.Interop.Constants.SQLITE_BUSY_SNAPSHOT)
                        //{
                        //    // Console.WriteLine("SQLITE_BUSY_SNAPSHOT");
                        //}
                        //else
                        //{
                        //    //Console.WriteLine("RETRY");
                        //    sw.SpinOnce();
                        //    // Console.WriteLine(rcCommit);
                        //    goto RETRY_COMMIT;
                        //}
                        //////// Console.WriteLine("NOT DONE: " + rcTest);

                        //Rollback();

                        //BeginConcurrentQuery.Reset();
                        //TestQuery.Reset();
                        //CommitQuery.Reset();

                        //sw.SpinOnce();
                        //goto RETRY;
                    }

                    return result;
                }
                catch (Exception ex)
                {
                    ThrowHelper.FailFast("Cannot insert chunk: " + ex);
                    return (false, 0);
                }
                finally
                {
                    InsertTestQuery.ClearAndReset();

                    //BeginConcurrentQuery.Reset();
                    //CommitQuery.Reset();
                }
            }

            private FastQuery CreateSelectTestQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"SELECT A,B FROM `{TestTable}` WHERE `A`=@A AND `B`=@B;";
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public (long, long) SelectTest(long a, long b)
            {
                const int aColumn = 1;

                const int bColumn = 2;

                try
                {
                    var sw = new SpinWait();

                    SelectTestQuery.Bind((binder, tuple) =>
                    {
                        int rc;
                        if ((rc = binder.BindInt64(aColumn, tuple.a)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.StreamId: " + rc);
                        }

                        if ((rc = binder.BindInt64(bColumn, tuple.b)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind chunk.FirstVersion: " + rc);
                        }
                    }, (a, b));

                RETRY_TEST:
                    var rcTest = SelectTestQuery.RawStep((hasRow, reader, state) =>
                    {
                        if (hasRow)
                            unsafe
                            {
                                var ax = reader.ColumnInt64(0);
                                var bx = reader.ColumnInt64(1);
                                //var len = reader.ColumnBytes(2);
                                //var ptr = reader.ColumnBlob(2);
                                //var db = new DirectBuffer(len, (byte*)ptr);
                                //var rm = BufferPool.Retain(len, true);
                                //db.Span.CopyTo(rm.Span);
                                return (ax, bx);
                            }

                        return default;
                    }, (object)null, out var result);

                    if (rcTest == Spreads.SQLite.Interop.Constants.SQLITE_BUSY)
                    {
                        Console.WriteLine("Busy");
                        sw.SpinOnce();
                        goto RETRY_TEST;
                    }

                    return result;
                }
                catch (Exception ex)
                {
                    ThrowHelper.FailFast("Cannot insert chunk: " + ex);
                    return default;
                }
                finally
                {
                    SelectTestQuery.ClearAndReset();
                }
            }

            public void Dispose()
            {
                if (_pool == null)
                {
                    ThrowHelper.ThrowObjectDisposedException("ChunkTableConnectionPool");
                    return;
                }

                CommitQuery.Dispose();
                RollbackQuery.Dispose();

                BeginConcurrentQuery.Dispose();
                BeginQuery.Dispose();

                InsertTestQuery.Dispose();
                SelectTestQuery.Dispose();

                DisposeStreamBlockQueries();

                DisposeMetadataQueries();

                _pool.Release(_connection);
                _connection = null;
                _pool = null;
            }
        }

        public class ConnectionPool : Spreads.SQLite.Fast.ConnectionPool
        {
            internal int ConnectionCount;

            public ConnectionPool(string connectionString) : base(connectionString)
            {
            }

            public override void InitConnection(SqliteConnection connection)
            {
                // TODO https://www.sqlite.org/fasterthanfs.html
                // [x] [Added to Makefile] -DSQLITE_DIRECT_OVERFLOW_READ, but we may actually benefit from cache? Likely not, because normally we read fw-only in single pass. Better keep indices in cache.
                // [x] [Using O2] -O3/O2 (check that not Os)
                // https://www.sqlite.org/compile.html#rcmd
                // [x] [Added to Makefile] * Apply 2,4,5,6,7,8,10, they are not needed for DS
                // [x] [Usring cache=shared via uri cs] * Test 9 under heavy load. We likely need shared cache SQLITE_OPEN_SHAREDCACHE and seems like do not use it now.
                // * HAVE_FDATASYNC for servers unless we decide to use lmdb
                // * synchronous = FULL should not be used when DS WAL is used,
                //   but it must be 10x or so larger than SQLite wal so that on crash
                //   DS wal has everything (or call checkpoint before deleting old DS
                //   WAL files just to be safe).
                // [x] * SQLITE_ENABLE_JSON1

                Interlocked.Increment(ref ConnectionCount);
                Trace.TraceInformation("Initializing SQLiteBlockStorage connection, total inits: " + ConnectionCount);

                // Much slower writes in test query with this: connection.Execute("PRAGMA page_size = 16384; ");
                connection.Execute("PRAGMA cache_size = 5000;");
                connection.Execute("PRAGMA synchronous = NORMAL;"); // TODO (review) this is not always needed
                connection.Execute("PRAGMA journal_mode = wal;");
                // TODO review, keep default so far connection.Execute("PRAGMA wal_autocheckpoint=10000;");
                // TODO
                // connection.Execute("PRAGMA read_uncommitted = true;");
                connection.Execute("PRAGMA busy_timeout = 10;");
                // This does not add a lot and not always, risks with IO errors/SIGBUS do not worth it: connection.Execute("PRAGMA mmap_size=268435456;");

                CreateTestTable(connection);
                CreateStreamBlockTable(connection);

                CreateContainerBlockTable(connection);

                CreateMetadataTables(connection);
            }
        }

        private static void CreateTestTable(SqliteConnection connection)
        {
            string commandText;
            SqliteCommand command;
            commandText =
#pragma warning disable HAA0201 // Implicit string concatenation allocation
                $"CREATE TABLE IF NOT EXISTS `{TestTable}` (\n" +
                "  `A`               INTEGER NOT NULL,\n" +
                "  `B`               INTEGER NOT NULL,\n" +
                "  `RawData`         BLOB,\n" +
                "  PRIMARY KEY (`A`,`B`));\n"; // +
            // $"INSERT OR REPLACE INTO `{TestTable}`(rowid, A, B) VALUES(9223372036854775807, 0, 0);";
#pragma warning restore HAA0201 // Implicit string concatenation allocation

            command = connection.CreateCommand();
            command.CommandText = commandText;
            command.ExecuteNonQuery();
            command.Dispose();
        }

        private const string GetFailedWrongStreamIdOrVersionMessage = "Bad chunk stream id or firstVersion";
        private const string PutFailMessage = "Cannot Put StreamLogChunk into the SQLite storage";


        #region Utils

        internal static string PtrToStringUTF8(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero)
            {
                return null;
            }

            var i = 0;
            while (Marshal.ReadByte(ptr, i) != 0)
            {
                i++;
            }

            var bytes = new byte[i];
            Marshal.Copy(ptr, bytes, 0, i);

            return Encoding.UTF8.GetString(bytes, 0, i);
        }

        internal static IntPtr StringToHGlobalUTF8(string s, out int length)
        {
            if (s == null)
            {
                length = 0;
                return IntPtr.Zero;
            }

            var bytes = Encoding.UTF8.GetBytes(s);
            var ptr = Marshal.AllocHGlobal(bytes.Length + 1);
            Marshal.Copy(bytes, 0, ptr, bytes.Length);
            Marshal.WriteByte(ptr, bytes.Length, 0);
            length = bytes.Length;

            return ptr;
        }

        internal static IntPtr StringToHGlobalUTF8(string s)
        {
            int temp;
            return StringToHGlobalUTF8(s, out temp);
        }

        #endregion
    }
}
