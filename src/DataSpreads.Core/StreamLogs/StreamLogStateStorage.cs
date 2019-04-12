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
using DataSpreads.Config;
using Spreads.LMDB;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace DataSpreads.StreamLogs
{
    // StreamId -> LockRecord

    //internal interface IStreamStateTable
    //{
    //    // PL is StreamId

    //    // ExclusiveWriter
    //    // UpstreamVersion
    //    // LastVersion - updated during sync or stream close

    //    bool TryAcquireLock(long streamId, StreamStateRecord streamStateRecord);
    //    bool TryReleaseLock(long streamId, UUID connectionId, out StreamStateRecord streamStateRecord);
    //    bool TryGetLockRecord(long streamId, out StreamStateRecord streamStateRecord);
    //}

    internal unsafe class StreamLogStateStorage : BufferRefAllocator
    {
        private readonly Database _stateDb;
        private SharedMemoryBuckets _buckets;

        public StreamLogState SharedState { [MethodImpl(MethodImplOptions.AggressiveInlining)] get; }

        public StreamLogStateStorage(string directoryPath)
            : base(directoryPath, StartupConfig.StreamStateEnvFlags, default, ProcessConfig.ProcessConfigRecord.BufferSize,
                1024L * 1024 * 1024) // 1GB is way too much but it takes no resources above actually used
        {
            // BRA dbs and _env are set in base class

            // InstanceId -> BufferRef
            _stateDb = Environment.OpenDatabase("_streamLogState", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

            _buckets = new SharedMemoryBuckets(directoryPath, StreamLogState.StreamLogStateRecord.BufferSize, 0);

            using (var txn = Environment.BeginTransaction())
            {
                try
                {
                    long sharedLogId = 0;

                    if (_stateDb.TryGet(txn, ref sharedLogId, out BufferRef sharedBufferRef))
                    {
                        txn.Abort();
                    }
                    else
                    {
                        sharedBufferRef = Allocate(txn, 0, out var fromFreeList, null);
                        _stateDb.Put(txn, sharedLogId, sharedBufferRef, TransactionPutOptions.NoOverwrite);
                        txn.Commit();
                        if (!fromFreeList)
                        {
                            Environment.Sync(true);
                        }
                    }

                    SharedState = new StreamLogState(default, (IntPtr)_buckets[sharedBufferRef].Data);
                }
                catch (Exception ex)
                {
                    txn.Abort();
                    Trace.TraceError(ex.ToString());
                    throw;
                }
            }
        }

        internal long UsedSize => Environment.UsedSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamLogState GetState(StreamLogId slid)
        {
            using (var txn = Environment.BeginReadOnlyTransaction())
            {
                // ReSharper disable once SuggestVarOrType_BuiltInTypes
                long logId = (long)slid;

                try
                {
                    if (_stateDb.TryGet(txn, ref logId, out BufferRef bufferRef))
                    {
                        return new StreamLogState(slid, (IntPtr)_buckets[bufferRef].Data);
                    }
                }
                catch (Exception ex)
                {
                    Trace.TraceError(ex.ToString());
                    throw;
                }
            }

            return CreateState(slid);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private StreamLogState CreateState(StreamLogId slid)
        {
            using (var txn = Environment.BeginTransaction())
            {
                // ReSharper disable once SuggestVarOrType_BuiltInTypes
                long logId = (long)slid;
                try
                {
                    if (_stateDb.TryGet(txn, ref logId, out BufferRef bufferRef))
                    {
                        txn.Abort();
                        return new StreamLogState(slid, (IntPtr)_buckets[bufferRef].Data);
                    }

                    bufferRef = Allocate(txn, 0, out var fromFreeList, null);
                    _stateDb.Put(txn, logId, bufferRef, TransactionPutOptions.NoOverwrite);
                    txn.Commit();
                    if (!fromFreeList)
                    {
                        Environment.Sync(true);
                    }

                    var result = _buckets[bufferRef];
                    if (fromFreeList)
                    {
                        // in Delete we clear the buffer after commit, there is small chance a process died after commit but before cleaning
                        result.Clear(0, result.Length);
                    }
                    else
                    {
                        Debug.Assert(result.IsFilledWithValue(0), "a new ProcessConfig buffer must be clean.");
                    }

                    return new StreamLogState(slid, (IntPtr)_buckets[bufferRef].Data);
                }
                catch (Exception ex)
                {
                    txn.Abort();
                    Trace.TraceError(ex.ToString());
                    throw;
                }
            }
        }

        public override void Dispose()
        {
            _stateDb.Dispose();
            base.Dispose();
        }
    }
}
