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
using DataSpreads.Storage;
using Spreads;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.LMDB;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using static DataSpreads.Buffers.SharedMemory.BufferHeader;

namespace DataSpreads.StreamLogs
{
    internal partial class StreamBlockIndex : IDisposable
    {
        public const ulong CompletedVersion = ulong.MaxValue;
        public const ulong ReadyBlockVersion = ulong.MaxValue - 1;

        private readonly LMDBEnvironment _env;
        private readonly Database _blocksDb;

        internal ReaderBlockCache ReaderBlockCache { [DebuggerStepThrough][MethodImpl(MethodImplOptions.AggressiveInlining)] get; }

        // internal Database _persistedWalPosDb;
        private readonly BlockMemoryPool _blockPool;

        private readonly IStreamBlockStorage _blockStorage;

        public StreamBlockIndex(string path,
            int sizeMb,
            LMDBEnvironmentFlags flags,
            BlockMemoryPool blockPool,
            IStreamBlockStorage blockStorage = null)
        {
            _blockPool = blockPool;
            _blockStorage = blockStorage;
            var env = LMDBEnvironment.Create(path, flags, disableAsync: true, disableReadTxnAutoreset: true);
            env.MapSize = sizeMb * 1024L * 1024L;
            env.MaxReaders = 1024;
            env.Open();
            var deadReaders = env.ReaderCheck();
            if (deadReaders > 0)
            {
                Trace.TraceWarning($"Cleared {deadReaders} in StreamBlockIndex");
            }
            _env = env;
            _blocksDb = _env.OpenDatabase("_streamBlocks",
                new DatabaseConfig(DbFlags.Create | DbFlags.DuplicatesSort | DbFlags.IntegerDuplicates | DbFlags.DuplicatesFixed)
                {
                    DupSortPrefix = 64 * 64
                }
            );

            ReaderBlockCache = new ReaderBlockCache();

            // TODO Review &/| delete
            // OpenDbs();
        }

        //private void OpenDbs()
        //{
        //    //_persistedWalPosDb = _env.OpenDatabase("_persistedWalPos",
        //    //    new DatabaseConfig(DbFlags.Create | DbFlags.IntegerDuplicates | DbFlags.DuplicatesFixed)
        //    //    {
        //    //        DupSortPrefix = 64 // TODO 64x64
        //    //    }
        //    //);
        //}

        public IStreamBlockStorage BlockStorage
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _blockStorage;
        }

        internal StreamBlockRecord[] StreamLogRecords(long streamId)
        {
            using (var txn = _env.BeginReadOnlyTransaction())
            {
                return _blocksDb.AsEnumerable<long, StreamBlockRecord>(txn, streamId).ToArray();
            }
        }

        [Obsolete("This could include completed and ready blocks, review usage, use only in tests")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetBlockCount(StreamLogId slid)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord value = default;
                if (c.TryGet(ref streamId, ref value, CursorGetOption.Set))
                {
                    return (long)c.Count();
                }

                return 0L;
            }
        }

        // SBM X
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GetIsCompleted(StreamLogId streamLogId)
        {
            var streamId = (long)streamLogId;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord value = default;
                if (!c.TryGet(ref streamId, ref value, CursorGetOption.Set)) return false;
                if (!c.TryGet(ref streamId, ref value, CursorGetOption.LastDuplicate)) return false;
                return value.Version == long.MaxValue;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(StreamLogId slid, ulong key, out StreamBlockRecord value)
        {
            var streamId = (long)slid;
            StreamBlockRecord value1 = default;

            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                value1.Version = key;
                if (c.TryFindDup(Lookup.EQ, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(StreamLogId slid, Timestamp key, out StreamBlockRecord value)
        {
            var streamId = (long)slid;
            StreamBlockRecord value1 = default;

            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                value1.Timestamp = key;
                if (c.TryFindDup(Lookup.EQ, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetFirst(StreamLogId streamId, out StreamBlockRecord value)
        {
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord value1 = default;

                if (c.TryGet(ref streamId, ref value1, CursorGetOption.Set))
                {
                    if (c.TryGet(ref streamId, ref value1, CursorGetOption.FirstDuplicate))
                    {
                        value = value1;
                        return true;
                    }
                }

                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNext(StreamLogId slid, ulong key, out StreamBlockRecord value)
        {
            // TODO use SDB method
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord value1 = default;
                value1.Version = key;

                if (c.TryFindDup(Lookup.GT, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }
                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetNext(StreamLogId slid, Timestamp key, out StreamBlockRecord value)
        {
            // TODO use SDB method
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord value1 = default;
                value1.Timestamp = key;

                if (c.TryFindDup(Lookup.GT, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }
                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindAt(StreamLogId slid, ulong key, Lookup direction, out StreamBlockRecord value)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                var value1 = new StreamBlockRecord { Version = key };

                if (c.TryFindDup(direction, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindAt(StreamLogId slid, Timestamp key, Lookup direction, out StreamBlockRecord value)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                var value1 = new StreamBlockRecord { Timestamp = key };

                if (c.TryFindDup(direction, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        // TODO delete these 3 and 2 copies

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindAt3(StreamLogId slid, ulong key, Lookup direction, out StreamBlockRecord value)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                var value1 = new StreamBlockRecord { Version = key };

                if (c.TryGet(ref streamId, ref value1, CursorGetOption.GetBothRange))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindAt2(StreamLogId slid, ulong key, Lookup direction, out StreamBlockRecord value)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            // using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                var value1 = new StreamBlockRecord { Version = key };
                if (_blocksDb.TryFindDup(txn, direction, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetPrevious(StreamLogId slid, ulong key, out StreamBlockRecord value)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord value1 = default;
                value1.Version = key;
                if (c.TryFindDup(Lookup.LT, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetPrevious(StreamLogId slid, Timestamp key, out StreamBlockRecord value)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord value1 = default;
                value1.Timestamp = key;
                if (c.TryFindDup(Lookup.LT, ref streamId, ref value1))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetLast(StreamLogId streamLogId, bool ignoreSentinels, out StreamBlockRecord value)
        {
            var streamId = (long)streamLogId;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord value1 = default;
                if (ignoreSentinels)
                {
                    value1 = new StreamBlockRecord(default) { Version = ReadyBlockVersion };
                    if (c.TryFindDup(Lookup.LT, ref streamId, ref value1))
                    {
                        value = value1;
                        return true;
                    }
                }
                else if (c.TryGet(ref streamId, ref value1, CursorGetOption.Set)
                         && c.TryGet(ref streamId, ref value1, CursorGetOption.LastDuplicate))
                {
                    value = value1;
                    return true;
                }

                value = default;
                return false;
            }
        }

        [Obsolete("Used only by Complete or tests")]
        internal bool TryAddBlockRecord(StreamLogId slid, StreamBlockRecord value)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginTransaction())
            {
                try
                {
                    _blocksDb.Put(txn, streamId, value, TransactionPutOptions.AppendDuplicateData);
                    txn.Commit();
                    return true;
                }
                catch
                {
                    txn.Abort();
                    return false;
                }
            }
        }

        public bool TryDeleteBlockRecord(StreamLogId slid, StreamBlockRecord value)
        {
            var streamId = (long)slid;
            if (value.Version == default)
            {
                return false;
            }

            using (var txn = _env.BeginTransaction(TransactionBeginFlags.NoSync))
            {
                try
                {
                    _blocksDb.Delete(txn, streamId, value);
                    txn.Commit();
                    return true;
                }
                catch
                {
                    txn.Abort();
                    return false;
                }
            }
        }

        public bool Complete(StreamLogId slid)
        {
#pragma warning disable 618
            return TryAddBlockRecord(slid, new StreamBlockRecord(default) { Version = CompletedVersion });
#pragma warning restore 618
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool UpdateBlockRecord(StreamLogId slid, StreamBlockRecord value)
        {
            lock (_env)
            {
                using (var txn = _env.BeginTransaction(TransactionBeginFlags.NoSync))
                {
                    var streamId = (long)slid;

                    var result = false;
                    using (var cursor = _blocksDb.OpenCursor(txn))
                    {
                        StreamBlockRecord record = value;
                        if (cursor.TryGet(ref streamId, ref record, CursorGetOption.GetBoth))
                        {
                            if (record.Version != value.Version)
                            {
                                FailBadVersion();
                            }

                            result = cursor.TryPut(ref streamId, ref value, CursorPutOptions.Current);
                        }
                    }

                    if (result)
                    {
                        txn.Commit();
                    }
                    else
                    {
                        txn.Abort();
                    }

                    return result;
                }
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailBadVersion()
        {
            ThrowHelper.FailFast("UpdateBlockRecord: value1.Version != value.Version");
        }

        /// <summary>
        /// Try to release block and update index record.
        /// </summary>
        /// <param name="slid"></param>
        /// <param name="record"></param>
        /// <param name="delete"></param>
        /// <returns></returns>
        internal unsafe bool TryReleaseBlock(StreamLogId slid, in StreamBlockRecord record,
            bool delete = false)
        {
            var streamId = (long)slid;

            // Debug.Assert(record.BufferRef.Flag);

            Debug.Assert(!record.IsPacked);

            var nativeBuffer = _blockPool.TryGetAllocatedNativeBuffer(record.BufferRef);
            var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer, slid == StreamLogId.Log0Id); // TODO this only works for Log0 but should use SL flags
            // this ctor will throw if
            var blockView = new StreamBlock(blockBuffer); // check manually: slid, expectedFirstVersion: record.Version);
            var header = nativeBuffer.Read<SharedMemory.BufferHeader>(0);

            if (header.FlagsCounter.Flags == HeaderFlags.IsPackedStreamBlock)
            {
                if (blockView.StreamLogIdLong == streamId && blockView.FirstVersion == record.Version)
                {
                    var clearedPackedFlag = SharedMemory.TryFromPackedStreamBlockToStreamBlock(nativeBuffer.Data);
                    if (clearedPackedFlag)
                    {
                        header = nativeBuffer.Read<SharedMemory.BufferHeader>(0);
                    }
                    else
                    {
                        // count is not 1, someone is still using the block
                        return false;
                    }
                }
                else
                {
                    ThrowHelper.FailFast("Wrong slid or version of a packed block.");
                }
            }

            // Note that we compare FlagsCounter, not FlagsCounter.Flags, which implies count == 0
            if (header.FlagsCounter == HeaderFlags.IsStreamBlock)
            {
                // When transitioning from IsPackedStreamBlock
                // instanceId is set in FromStreamBlockToOwned below.
                if (header.AllocatorInstanceId != 0)
                {
                    ThrowHelper.FailFast("header.AllocatorInstanceId != 0");
                }

                // We may have died after TryFromPackedStreamBlockToStreamBlock above
                // and some *future* GC logic could have reclaimed a block in IsStreamBlock
                // state with lingering flag. Currently the current if block is the only such logic,
                // but keep slid & version guards. They uniquely identify a block
                // and if they have changed then this block is no longer used.
                // Note that there is a PackerLock per stream so there should be no
                // concurrent activity here.
                if (blockView.StreamLogIdLong == streamId && blockView.FirstVersion == record.Version)
                {
                    SharedMemory.FromStreamBlockToOwned(nativeBuffer, _blockPool.Wpid.InstanceId);

                    var sharedMemory = SharedMemory.Create(nativeBuffer, record.BufferRef.ClearFlag(), _blockPool);
                    _blockPool.Return(sharedMemory);

                    // ReSharper disable once RedundantAssignment
                    header = nativeBuffer.Read<SharedMemory.BufferHeader>(0);
                    Debug.Assert(header.AllocatorInstanceId == 0 || header.AllocatorInstanceId == _blockPool.Wpid.InstanceId);
                    // Buffer could be taken from the pool immediately, cannot do other asserts (had them but deleted)

                    ReleaseRecord(in record);
                    return true;
                }
                else
                {
                    // TODO remove FF after proper testing
                    ThrowHelper.FailFast("Someone has released a lingering block with IsStreamBlock state. It should have been us.");
                }
            }

            // Note that we compare FlagsCounter, not FlagsCounter.Flags, which implies count == 0
            if (header.FlagsCounter == HeaderFlags.IsOwned)
            {
                // RMPool first clears a buffer and then marks counter as disposed.
                // After cleaning we cannot tell at which stage the buffer is:
                // * Didn't make it to the pool
                // * Was in the pool but was just taken before state transitioned to IsSB.
                // In both cases the buffer state is IsOwned with zero count and dead AllocatorInstanceId,
                // which is easily detectable by (TODO) periodically scanning allocated list for this condition.
                //
                // If disposed we do not need to do anything for sure. This if block checks for that.
                //
                // Dying before cleaning leaves StreamId *and* FirstVersion set. *And* is important,
                // only one of them left means dying during cleaning (probability ~0 but still possible).
                // StreamId & FirstVersion are unique per block, after packing no block could
                // have the same values again. So it is safe to assume that a process died before _blockPool.Return.

                if (blockView.StreamLogIdLong == streamId && blockView.FirstVersion == record.Version)
                {
                    if (_blockPool.Wpid.InstanceId == header.AllocatorInstanceId)
                    {
                        // TODO this is actually possible, remove FF
                        // For temp diagnostic only.
                        // Could be OOM/LMDB/IO exception.
                        ThrowHelper.FailFast("Current process could not finish releasing a block from IsStreamBlock state. That should not be possible.");
                    }
                    var sharedMemory = SharedMemory.Create(nativeBuffer, record.BufferRef.ClearFlag(), _blockPool);
                    _blockPool.Return(sharedMemory);
                }

                // If buffer was cleaned then we just failed to update the record
                ReleaseRecord(in record);
                return true;
            }

            void ReleaseRecord(in StreamBlockRecord r)
            {
                if (delete)
                {
                    var deleted = TryDeleteBlockRecord(slid, r);
                    if (!deleted)
                    {
                        ThrowHelper.FailFast("Cannot delete block record");
                    }
                }
                else
                {
                    var updatedRecord = new StreamBlockRecord(default)
                    {
                        Version = r.Version,
                        Timestamp = r.Timestamp,
                        BufferRef = default
                    };
                    var updated = UpdateBlockRecord(slid, updatedRecord);
                    if (!updated)
                    {
                        ThrowHelper.FailFast("Cannot update block record");
                    }
                }
            }

            ThrowHelper.FailFast($"Invalid header: {header}");
            return false;
        }

        // TODO break this PackBlocks method in stages

        /// <summary>
        /// Tries to pack some blocks and returns true if there could be more blocks to pack.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public unsafe bool PackBlocks(StreamLogState state, Packer.PackAction packAction)
        {
            // Console.WriteLine($"PACKING BLOCKS: slid {state.StreamLogId} action: {packAction}");

            // find records with non-default BufferRef
            // check block state:
            //   validate that it is stream block with correct stream id and version
            //   if block state is already packed then update the record
            //      TODO if there are readers then update BufferRef flag: it means we cannot release the block but no new readers could touch it.
            //      if no readers
            //      note that readers must check the state as well as BufferRef.
            //   if block is not packed then pack it and then update its state
            //      Writing to storage may fail if we are packing the same block again
            //      while failing to update the state after previous pack. Existing
            //      packed block must be equal in that case (TODO hash).
            //      We must detect that we are packing the same block again and update the state.

            var slid = state.StreamLogId;
            var streamId = (long)slid;

            #region Find blocks to pack & release

            if (!TryGetLast(slid, true, out var lastRecord))
            {
                return false;
            }

            var activeBlockVersion = lastRecord.Version;

            // Packing always works forward and always progresses if there are available blocks

            // this is only a hint to avoid full scan from the beginning
            var lastPackedBlockVersionFromState = state.GetLastPackedBlockVersion();
            var lastPackedBlockVersionFromStorage = BlockStorage.LastStreamBlockKey(streamId);

            if (packAction != Packer.PackAction.Delete && lastPackedBlockVersionFromState > lastPackedBlockVersionFromStorage)
            {
                // This happens cross-process, SQLite query doesn't see tha latest block
                // However at the end of this method we do checks that the state version
                // matches the version in the storage. Just return false, a retry should work.
                Trace.TraceWarning($"lastPackedRecordVersion {lastPackedBlockVersionFromState} > lastPackedStorageVersion {lastPackedBlockVersionFromStorage} for Slid {slid}");
                return false;
            }

            if (lastPackedBlockVersionFromState < lastPackedBlockVersionFromStorage)
            {
                Trace.TraceWarning("Missed packed record updates");
            }

            // Find all records that still have BufferRef.
            // They could be already packed, which is indicated by buffer header or lingering flag (BufferRef.Flag).

            const int unpackedCapacity = 16;
            Span<StreamBlockRecord> unpackedRecords = stackalloc StreamBlockRecord[unpackedCapacity]; // 20 bytes each

            // TODO (meta) Move this comment and TODO to a proper place
            // We need to process lingering blocks separately or in the worst case they will block unpacking by consuming all unpackedCapacity.
            // Repeating attempts with lingering blocks is best-effort. It is abnormal situation that is possible only when a reader died ungracefully.
            // TODO we should have a routine that just force-releases all lingering blocks. It could run on server restart or in general when there are no activity in DS at all.

            var unpackedCount = 0;

            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                var record = new StreamBlockRecord
                {
                    Version = 1
                };
                record.Version = lastPackedBlockVersionFromState;

                if (c.TryFindDup(Lookup.GE, ref streamId, ref record)
                    && record.Version < activeBlockVersion)
                {
                    do
                    {
                        if (!record.IsPacked
                            && !record.BufferRef.Flag // not lingering, it is already packed
                           )
                        {
                            unpackedRecords[unpackedCount++] = record;
                        }
                    } while (unpackedCount < unpackedCapacity
                             &&
                             c.TryGet(ref streamId, ref record, CursorGetOption.NextDuplicate)
                             && // order! after TryGet
                             record.Version < activeBlockVersion);
                }
            }

            if (unpackedCount == 0)
            {
                // nothing to pack
                return false;
            }

            #endregion Find blocks to pack & release

            var packedCount = 0;
            var lastInsertedVersion = 0UL;

            #region Do packing

            for (int i = 0; i < unpackedCount; i++)
            {
                ref var record = ref unpackedRecords[i];
                Debug.Assert(!record.BufferRef.Flag);

                Debug.Assert(!record.IsPacked);

                var nativeBuffer = _blockPool.TryGetAllocatedNativeBuffer(record.BufferRef);
                var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer, state.StreamLogFlags.Pow2Payload());

                var isInitialized = StreamBlock.GetIsInitialized(blockBuffer, slid, state.ItemFixedSize, record.Version);

                if (!isInitialized)
                {
                    return false;
                }

                var header = nativeBuffer.Read<SharedMemory.BufferHeader>(0);

                var blockView = new StreamBlock(blockBuffer, null, slid, expectedFirstVersion: record.Version);

                // TODO (meta) Move this comment and TODO to a proper place
                // Steps:
                // 1. Pack to storage, update header to IsPackedStreamBlock
                // 2. Update record: set flag to BufferRef making it lingering + transit state to IsStreamBlock
                // 3. Release block: return buffer to pool if not already (refcount). Clear BufferRef if the record is released.
                //    On step 3 we could see blocks with refcount > 1. If we do so many times that could indicate
                //    that a reader has died. We cannot track every reader per block, that would be too expensive.
                //    TODO We should set Count to zero in such blocks so that if a reader is alive it will detect
                //    that on the next move. However, reader is not moving for a long time for some reason.
                // Block could be at a different stage during this method call. Every call
                // tries to move a block to the next stage.

                if (header.FlagsCounter.Flags == HeaderFlags.IsIndexedStreamBlock)
                {
                    if (blockView.IsCompleted
                    // TODO this needs special treatment, do not set IsPackedSB flag
                    // || header.FlagsCounter.Count == 1 // means that stream log is disposed by all writers and there are no readers as well. Otherwise skip it (it is the last active block)
                    )
                    {
                        // packAction
                        // Pack, update the flag after packing
                        bool packed;

                        switch (packAction)
                        {
                            case Packer.PackAction.Pack:
                                {
                                    packed = BlockStorage.TryPutStreamBlock(blockView); // could throw, TODO need exception handling

                                    if (!packed)
                                    {
                                        Console.WriteLine("!packed");
                                        var lastPackedInStorage = BlockStorage.LastStreamBlockKey(streamId);
                                        if (lastPackedInStorage == blockView.FirstVersion)
                                        {
                                            ThrowHelper.FailFast(
                                                $"Tried to pack already packed block: stream id {streamId}, BufferRef {record.BufferRef}");
                                            packed = true;
                                        }
                                        else if (lastPackedInStorage > blockView.FirstVersion)
                                        {
                                            // TODO
                                            ThrowHelper.FailFast(
                                                "TODO not implemented, review if that is possible and if we could recover");
                                        }
                                        else
                                        {
                                            // TODO this is not hit now with commented condition || header.FlagsCounter.Count == 1
                                            ThrowHelper.FailFast("this is not hit now with commented condition || header.FlagsCounter.Count == 1");
                                        }
                                    }
                                    else
                                    {
                                        lastInsertedVersion = blockView.FirstVersion;
                                    }

                                    break;
                                }
                            case Packer.PackAction.Delete:
                                Debug.WriteLine($"Dropping block: {blockView.StreamLogId} - {blockView.FirstVersion}");
                                if (header.FlagsCounter.Count > 1)
                                {
                                    goto DO_PACKING;
                                }
                                packed = true;
                                break;

                            default:
                                throw new ArgumentOutOfRangeException(nameof(packAction), packAction, null);
                        }

                        if (!packed)
                        {
                            ThrowHelper.FailFast("Cannot pack a block");
                        }
                        else
                        {
                            if (blockView.IsCompleted)
                            {
                                packedCount++;

                                SharedMemory.FromIndexedStreamBlockToPackedStreamBlock(nativeBuffer.Data);

                                // re-read header to enter next if
                                header = nativeBuffer.Read<SharedMemory.BufferHeader>(0);
                                Debug.Assert(header.FlagsCounter.Flags == HeaderFlags.IsPackedStreamBlock);
                            }
                            else
                            {
                                ThrowHelper.FailFast("Same condition in outer if."); // TODO cleanup
                            }
                            // ignore special case of packing active block TODO test that we could pack/unpack
                        }
                    }
                }

                if (header.FlagsCounter.Flags == HeaderFlags.IsPackedStreamBlock)
                {
                    // Packer could have died after setting IsPackedSB flag but before committing the next transaction. We will retry it.

                    // Set lingering flag
                    var updatedRecord = new StreamBlockRecord(default)
                    {
                        Version = record.Version,
                        Timestamp = record.Timestamp,
                        BufferRef = record.BufferRef.SetFlag() // lingering flag
                    };

                    var updated = UpdateBlockRecord(slid, updatedRecord);
                    if (!updated)
                    {
                        ThrowHelper.FailFast("Cannot update block record");
                    }

                    // Now lingering flag is set. BufferRef with this flag must remain in SBI until a block is released for we do not lose track of it.
                }
                else if (header.FlagsCounter.Flags == HeaderFlags.IsStreamBlock)
                {
                    // review relation with record.BufferRef.Flag
                    ThrowHelper.FailFast("Transition to HeaderFlags.IsStreamBlock must be done after lingering flag is set.");
                }
            }

            #endregion Do packing

            DO_PACKING:
            if (packedCount == 0)
            {
                return false;
            }
            // TODO review: was [unpackedCount - 1]
            var lastPackedVersion = unpackedRecords[packedCount - 1].Version;

            //Console.WriteLine($"Stored count: {storedCount}, last version: {lastPackedVersion}");
            ulong fromStorage = 0;
            if ((fromStorage = BlockStorage.LastStreamBlockKey(streamId)) != lastPackedVersion && fromStorage != 0)
            {
                ThrowHelper.FailFast($"XXXXXXX: updating state with wrong key {fromStorage} vs {lastPackedVersion}, cnt {packedCount}, slid {slid}");
            }

            // Console.WriteLine($"UPDATING LAST PACKED STATE: {lastPackedVersion}, cnt {packedCount}, slid {slid}");
            if (fromStorage != 0 && lastInsertedVersion != lastPackedVersion)
            {
                ThrowHelper.FailFast($"YYYYYYYYYYY: lastInsertedVersion {lastInsertedVersion} != lastPackedVersion {lastPackedVersion}, cnt {packedCount}, slid {slid}");
            }
            state.SetLastPackedBlockVersion(lastPackedVersion);

            // only now we can release lingering blocks
            ReleaseBlocks(slid, 0, lastPackedVersion + 1, false);

            ThrowHelper.AssertFailFast(packedCount <= unpackedCapacity, "storedCount <= unpackedCapacity");

            return packedCount == unpackedCapacity;
        }

        // Limited to Packer threads only
        [ThreadStatic]
        private static List<StreamBlockRecord> _releaseCandidates;

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal ulong ReleaseBlocks(StreamLogId slid, ulong fromExclusive, ulong toExclusive, bool delete = false)
        {
            var candidates = _releaseCandidates ?? (_releaseCandidates = new List<StreamBlockRecord>());
            candidates.Clear();

            // Should not release in batch, one by one. We cannot release buffers as a transaction,
            // we managed them via interlocked methods. We also have (TODO) ways to detect dropped buffers
            var streamId = (long)slid;
            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                var record = new StreamBlockRecord { Version = fromExclusive };

                if (c.TryFindDup(Lookup.GT, ref streamId, ref record) && record.Version < toExclusive)
                {
                    do
                    {
                        if (!record.IsPacked && record.BufferRef.Flag)
                        {
                            candidates.Add(record);
                        }
                    } while (record.Version < toExclusive && c.TryGet(ref streamId, ref record, CursorGetOption.NextDuplicate));
                }
                else
                {
                    // no change
                    return fromExclusive;
                }
            }

            if (candidates.Count == 0)
            {
                return fromExclusive;
            }

            var hasGaps = false;
            foreach (var record in candidates)
            {
                var releasedCurrent = TryReleaseBlock(slid, record, delete);

                // We stop only in delete case, where a ref count often intentionally kept to
                // indicate the first block to keep. When packing normally we could have holes,
                // otherwise a single lingering block could prevent releasing all subsequent blocks.
                if (!releasedCurrent)
                {
                    // Console.WriteLine("Gaps");
                    hasGaps = true;
                    if (delete)
                    {
                        return fromExclusive;
                    }
                }

                if (!hasGaps)
                {
                    fromExclusive = record.Version;
                }
            }

            return fromExclusive;
        }

        [Obsolete("Not used, only in old tests")]
        internal bool UpdateReadyBlockVersion(StreamLogId slid, ulong newVersion)
        {
            var streamId = (long)slid;
            using (var txn = _env.BeginTransaction(TransactionBeginFlags.NoSync))
            {
                using (var c = _blocksDb.OpenCursor(txn))
                {
                    var value1 = new StreamBlockRecord { Version = ReadyBlockVersion };
                    if (c.TryGet(ref streamId, ref value1, CursorGetOption.GetBoth))
                    {
                        if (value1.Version != ReadyBlockVersion)
                        {
                            goto ABORT;
                        }

                        var readyBlockRecord = new StreamBlockRecord { Version = ReadyBlockVersion };
                        _blocksDb.Delete(txn, streamId, readyBlockRecord);

                        value1.Version = newVersion;
                        if (!c.TryPut(ref streamId, ref value1, CursorPutOptions.AppendDuplicateData))
                        {
                            goto ABORT;
                        }

                        if (AdditionalCorrectnessChecks.Enabled)
                        {
                            EnsureNoDuplicates(txn, streamId);
                        }

                        goto COMMIT;
                    }

                    goto ABORT;
                }

                COMMIT:
                txn.Commit();
                return true;

                ABORT:
                txn.Abort();
                return false;
            }
        }

        /// <summary>
        /// Rent a <see cref="SharedMemory"/> from pool, set StreamLogId to <see cref="StreamBlock"/> and
        /// transition memory state from IsOwned to IsStreamBlock.
        /// Used during async preparation and during getting a next writable block if there was no prepared block.
        /// </summary>
        /// <param name="minDataSize"></param>
        /// <param name="streamLog"></param>
        /// <returns></returns>
        [Obsolete("Use only from SBI (this class) or tests.")] // TODO Shift+F12 to ensure, + there must be tests for non-happy paths!
        internal SharedMemory GetEmptyStreamBlockMemory(int minDataSize, StreamLog streamLog)
        {
            var sharedMemory = _blockPool.RentMemory(minDataSize);

            // Detach from pool: we cannot return to pool without cleaning SL values
            // but wrong decrement to zero will cause pooling without this.
            // TODO review
            sharedMemory._poolIdx = 0;

            // It is still not owned until in SBI
            SharedMemory.FromOwnedToStreamBlock(sharedMemory.NativeBuffer, streamLog);

            return sharedMemory;
        }

        /// <summary>
        /// Returns a view over a <see cref="StreamBlock"/> that is already in <see cref="StreamBlockIndex"/>.
        /// Throws if buffer state is not <see cref="HeaderFlags.IsIndexedStreamBlock"/>.
        /// View means that <see cref="StreamBlock"/> has no memory manager
        /// and does not own a reference or change reference count.
        /// </summary>
        /// <param name="streamLog"></param>
        /// <param name="bufferRef"></param>
        /// <param name="nativeBuffer"></param>
        /// <param name="expectedFirstVersion"></param>
        /// <param name="ignoreAlreadyIndexed">There could be a race between Prepare/RentNext
        /// to update <see cref="HeaderFlags.IsStreamBlock"/> to <see cref="HeaderFlags.IsIndexedStreamBlock"/>.
        /// This flag tells to ignore failed atomic header swap if existing state is IsISB. Set to true only when
        /// the buffer is known to exist in SBI (we took it from there or just added in a txn).</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        [Obsolete("Use only from SBI (this class) or tests.")] // TODO Shift+F12 to ensure, + there must be tests for non-happy paths!
        internal StreamBlock TryGetIndexedStreamBlockView(StreamLog streamLog, in BufferRef bufferRef, out DirectBuffer nativeBuffer,
            ulong expectedFirstVersion, // TODO unused now. remove if not used in tests later.
            bool ignoreAlreadyIndexed = false)
        {
            if (bufferRef.Flag)
            {
                nativeBuffer = DirectBuffer.Invalid;
                return default;
            }

            var slid = streamLog.Slid;
            var expectedValueSize = streamLog.ItemFixedSize;

            // TODO method called Try... but it throws

            nativeBuffer = _blockPool.TryGetAllocatedNativeBuffer(bufferRef);

            var header = nativeBuffer.Read<SharedMemory.BufferHeader>(0);
            HeaderFlags flagsNoCount = header.FlagsCounter.Flags;
            if (flagsNoCount != HeaderFlags.IsIndexedStreamBlock)
            {
                if (ignoreAlreadyIndexed
                    && flagsNoCount == HeaderFlags.IsStreamBlock
                    && header.AllocatorInstanceId != 0 // transitioning from IsOwned to IsIndexedStreamBlock keeps instance id while a block IsStreamBlock
                    ) // && (IsBufferRefInStreamIndex(bufferRef)))
                {
                    SharedMemory.FromStreamBlockToIndexedStreamBlockClearInstanceId(nativeBuffer, streamLog,
                        firstVersion: 0,
                        isRent: false,
                        ignoreAlreadyIndexed: true);
                }
                else
                {
                    return default;
                    // SharedMemory.ThrowBadInitialState(HeaderFlags.IsIndexedStreamBlock);
                }
            }

            var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer, streamLog.StreamLogFlags.Pow2Payload());

            // We have validated native header, this ctor will validate content
            var blockView = new StreamBlock(blockBuffer, null, slid, expectedValueSize, expectedFirstVersion);

            return blockView;
        }

        /// <summary>
        /// Increments reference count and should only be used by readers. The returned block should be freed via <see cref="StreamBlock.DisposeFree(bool)"/>.
        /// This should never throw bould could return an invalid <see cref="StreamBlock"/> (check <see cref="StreamBlock.IsValid"/> property of the returned value).
        /// </summary>
        [Obsolete("Should use only by readers and released via DisposeFree.")] // TODO Shift+F12 to ensure, + there must be tests for non-happy paths
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe StreamBlock TryRentIndexedStreamBlock(StreamLog streamLog, in BufferRef bufferRef, ulong expectedFirstVersion = 0)
        {
            var blockView = TryGetIndexedStreamBlockView(streamLog, in bufferRef, out var nativeBuffer, expectedFirstVersion,
                // ReSharper disable once RedundantArgumentDefaultValue
                ignoreAlreadyIndexed: false); // TODO review

            if (!blockView.IsValid)
            {
                return blockView;
            }

            SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
            var manager = SharedMemory.Create(nativeBuffer, bufferRef, _blockPool);
            var rentedBlock = new StreamBlock(manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()), manager, streamLog.Slid,
                streamLog.ItemFixedSize, blockView.FirstVersion);
            return rentedBlock;
        }

        /// <summary>
        /// Add uninitialized ready block. Update record of initialized ready block if it is present.
        /// Do nothing if ready block if present and not initialized.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        [Obsolete("Use only from SL.Rotate or tests.")] // TODO Shift+F12 to ensure, + there must be tests for non-happy paths!
        internal void PrepareNextWritableStreamBlock(StreamLog streamLog, int length)
        {
            //return;
            Debug.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] Entered PrepareNextWritableStreamBlock");

            var streamId = (long)streamLog.Slid;

            using (var txn = _env.BeginTransaction())
            {
                // ReSharper disable once RedundantAssignment
                StreamBlock lastBlockView = default;
                StreamBlockRecord record = default;
                SharedMemory nextMemory;

                using (var c = _blocksDb.OpenCursor(txn))
                {
                    if (c.TryGet(ref streamId, ref record, CursorGetOption.Set)
                        && c.TryGet(ref streamId, ref record, CursorGetOption.LastDuplicate)
                        && record.Version != CompletedVersion)
                    {
                        lastBlockView = TryGetIndexedStreamBlockView(streamLog, in record.BufferRef, out _,
                            expectedFirstVersion: 0, // we can't know it
                            ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI

                        Debug.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] PrepareNextWritableStreamBlock: lastBlockView.Count {lastBlockView.CountVolatile}");

                        if (lastBlockView.IsCompleted)
                        {
                            // Disable background activity, there could be many delayed preparation tasks
                            // but preparation must happen before rotated block is complete
                            goto ABORT;
                        }

                        var lastBlockFirstVersion = lastBlockView.FirstVersion;
                        var lastBlockFirstTimestamp = lastBlockView.GetFirstTimestampOrWriteStart();

                        if (record.Version == ReadyBlockVersion)
                        {
                            if (lastBlockFirstVersion > 0)
                            {
                                // CursorPutOptions.Current is not faster but creates risks, we must ensure AppendDuplicateData returns true

                                var readyBlockRecord = new StreamBlockRecord { Version = ReadyBlockVersion };
                                _blocksDb.Delete(txn, streamId, readyBlockRecord);

                                record.Version = lastBlockFirstVersion;
                                record.Timestamp = lastBlockFirstTimestamp;

                                if (!c.TryPut(ref streamId, ref record, CursorPutOptions.AppendDuplicateData))
                                {
                                    StreamBlockRecord r1 = default;

                                    c.TryGet(ref streamId, ref r1, CursorGetOption.LastDuplicate);

                                    ThrowHelper.FailFast("Cannot update ready block version: " + lastBlockFirstVersion);
                                }
                            }
                            else
                            {
                                // Someone has initialized the ready block, we cannot create a block record with version > it
                                // so return it as is. If it is completed then SL will retry.
                                goto ABORT;
                            }
                        }

                        // This will clean newly allocated buffer, which means it is accessed via SM buckets
                        // and a new file is allocated as needed.
                        // LMDB write txn + buffer cleaning + new file allocation when needed = what we are saving
                        // by asynchronously preparing the next block. We are losing space in log buffers
                        // because on average there is 1.5 empty blocks (half of the current + 1 prepared).
                        // Therefore we do not want to do this for every stream, only for the fast ones.
                        // There could be many streams that writes values very infrequently.
                        // Fast streams are those with rateHint provided and the ones that repeatedly exceed
                        // the minimum rate over application lifetime. Explicit rate hint could be very small
                        // and the stream could have few values written infrequently, but it is latency-sensitive
                        // and it's values must propagate as fast as possible.
                        nextMemory = GetEmptyStreamBlockMemory(length, streamLog);

                        var newBlockRecord = new StreamBlockRecord(nextMemory.BufferRef)
                        {
                            Version = ReadyBlockVersion,
                            // need this for correct lookup by timestamp,
                            // otherwise order is broken when lookup version is zero
                            Timestamp = streamLog.NoTimestamp ? default : new Timestamp(unchecked((long)ReadyBlockVersion))
                        };

                        _blocksDb.Put(txn, streamId, newBlockRecord, TransactionPutOptions.AppendDuplicateData);

                        if (AdditionalCorrectnessChecks.Enabled)
                        {
                            EnsureNoDuplicates(txn, streamId);
                        }

                        Debug.Assert(lastBlockView.IsValid);
                    }
                    else
                    {
                        goto ABORT;
                    }
                } // must dispose cursor before commit/abort

                txn.Commit();

                // Note: there is a race with RentNextWritableBlock to update the state.

                // Transition state to indexed. We do not know version so set it to zero.
                SharedMemory.FromStreamBlockToIndexedStreamBlockClearInstanceId(nextMemory.NativeBuffer, streamLog, firstVersion: 0, isRent: false, ignoreAlreadyIndexed: true);

                SharedMemory.Free(nextMemory);

                Debug.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] Returned from PrepareNextWritableStreamBlock");

                return;

                ABORT:
                Debug.Assert(lastBlockView.IsValid);
                txn.Abort();
                Debug.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] Aborted PrepareNextWritableStreamBlock");
            }
        }

        internal unsafe StreamBlock RentInitWritableBlock(StreamLog streamLog)
        {
            var streamId = (long)streamLog.Slid;

            using (var txn = _env.BeginReadOnlyTransaction())
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                StreamBlockRecord record = default;

                if (c.TryGet(ref streamId, ref record, CursorGetOption.Set)
                    && c.TryGet(ref streamId, ref record, CursorGetOption.LastDuplicate))
                {
                    if (record.Version == CompletedVersion)
                    {
                        return default;
                    }

                    var bufferRef = record.BufferRef;
#pragma warning disable 618
                    var lastBlockView = TryGetIndexedStreamBlockView(streamLog, in bufferRef,
                        out var nativeBuffer,
                        expectedFirstVersion: 0, // we are initializing SL and do not know the version
                        ignoreAlreadyIndexed: true);
#pragma warning restore 618

                    var movedPrevious = false;
                    if (record.Version == ReadyBlockVersion // last block record is prepared one
                        && !lastBlockView.IsInitialized     // and is not initialized
                        && !(movedPrevious = c.TryGet(ref streamId, ref record, CursorGetOption.PreviousDuplicate)) // and there is no block before it
                        )
                    {
                        // there is no writable block, will need to create one in a separate call
                        return default;
                    }

                    if (record.BufferRef != bufferRef) //
                    {
                        ThrowHelper.AssertFailFast(movedPrevious, "Must have moved previous if the record is updated.");
#pragma warning disable 618
                        lastBlockView = TryGetIndexedStreamBlockView(streamLog, in record.BufferRef,
                            out nativeBuffer,
                            expectedFirstVersion:
                            0, // we are initializing SL and do not know the version
                            ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI
#pragma warning restore 618
                    }

                    // ReSharper disable once RedundantAssignment
                    var refCount = SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                    ThrowHelper.AssertFailFast(refCount > 1, "Existing block must be owned by the SBI.");

                    var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);

                    var nextBlock = new StreamBlock(
                        manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()), manager,
                        streamLog.Slid,
                        streamLog.ItemFixedSize, lastBlockView.FirstVersion);

                    Debug.WriteLine(
                        $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlockLocked: found prepared block with first version: {nextBlock.FirstVersion}, {record.BufferRef}");

                    return nextBlock;
                }

                // empty, return invalid block
                return default;
            }
        }

        /// <summary>
        /// Get or create the next writeable <see cref="StreamBlock"/> for the given <paramref name="streamLog"/>
        /// and rent that block.
        /// </summary>
        /// <param name="streamLog"></param>
        /// <param name="minimumLength"></param>
        /// <param name="versionToWrite"></param>
        /// <param name="nextBlockFirstTimestamp"></param>
        /// <returns></returns>
        internal unsafe StreamBlock RentNextWritableBlock(
            StreamLog streamLog,
            int minimumLength,
            ulong versionToWrite,
            Timestamp nextBlockFirstTimestamp)
        {
            // Prepare to refactor:
            // 1.
            // NextBlockFirstVersion/nextBlockFirstTimestamp are actually just nextVersion/nextTimestamp,
            // i.e. what the caller of this method is going to write. There is no guarantee that this will
            // be the next block because other processes could have create a new block where the nextVersion
            // should be placed. Currently this is possible for Log0 only, but could be possible for any shared
            // writes. Shared streams could check existence of a writable block in some other way, but calling
            // this method is the supposed way, other ways should only be optimizations (e.g. cache next block by key in Log0).
            // Read transactions are very cheap so we could add some logic here.
            // 2.
            // This index must have records with actual Version and Timestamp, or **both** should be set
            // to ReadyBlockVersion value. Otherwise AsTimeSeries functionality will break.
            // 3.
            // Completed non-empty block is perfectly fine. Current logic missed the cross-process update
            // possibility. We only need to clear completed empty blocks that are only possible for unexpected
            // large values. This case is so rare/edge that we should move it into a separate method
            // that just clears such block and any prepared block after it. This means completed empty is the
            // only condition to clear it.
            // 4.
            // nextVersion == 0 means get the latest writable block. This should be a separate method.

            Debug.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] Entered RentNextWritableBlock");

            var streamId = (long)streamLog.Slid;

            // version == 0 only on Init
            if (versionToWrite == 0)
            {
                // TODO Review usages and use RentInit... directly, throw if version == 0
                return RentInitWritableBlock(streamLog);
            }

            using (var txn = _env.BeginReadOnlyTransaction())
            {
                var block = RentNextWritableBlockExisting(txn, streamLog, versionToWrite,
                    out _); // ignore hasCompletedEmptyBlock, we could do anything with it from write txn
                if (block.IsValid)
                {
                    return block;
                }
            }

            lock (_env) // be nice to LMDB, use it's lock only for x-process access
            {
                return RentNextWritableBlockLocked(streamLog, minimumLength, versionToWrite, nextBlockFirstTimestamp);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowVersionIsNotEqualToNext(ulong version, ulong nextVersion)
        {
            ThrowHelper.ThrowInvalidOperationException($"Version {version} does not equal to the next version {nextVersion} of existing writable block.");
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        private unsafe StreamBlock RentNextWritableBlockExisting(ReadOnlyTransaction txn,
            StreamLog streamLog, ulong versionToWrite, out bool hasCompletedEmptyBlock)
        {
            hasCompletedEmptyBlock = false;
            var streamId = (long)streamLog.Slid;

            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                // This record is used for search by version, which is > 0, so could leave TS as default
                var record = new StreamBlockRecord { Version = versionToWrite };

                var nextVersion = 1UL;
                uint previousChecksum = default;
                Timestamp previousBlockLastTimestamp = default;

                // We first try LE because another process could have created & initialized
                // a block that contains versionToWrite (possible for shared streams).
                // We also need previous checksum and timestamp even if the LE block is completed.
                if (c.TryFindDup(Lookup.LE, ref streamId, ref record))
                {
                    ThrowHelper.AssertFailFast(streamId == (long)streamLog.Slid, "streamId == (long)streamLog.Slid");

#pragma warning disable 618
                    var lastBlockView = TryGetIndexedStreamBlockView(streamLog,
                        in record.BufferRef,
                        out var nativeBuffer,
                        expectedFirstVersion: 0,
                        ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI
#pragma warning restore 618

                    if (!lastBlockView.IsCompleted)
                    {
                        // Found a block that has first version LE @versionToWrite
                        //    *AND is not completed*.
                        // Must return this non-completed block. If it is being
                        // completed now from another thread then will retry.
                        // If there is no more space then the first thread that
                        // detects that must complete the block.

                        var refCount = SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                        ThrowHelper.AssertFailFast(refCount > 1, "Existing block must be owned by the SBI.");
                        var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);
                        var nextBlock = new StreamBlock(
                            manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()),
                            manager, streamLog.Slid, streamLog.ItemFixedSize, lastBlockView.FirstVersion);
                        Debug.WriteLine(
                            $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlock: found non-completed existing block with first version: {nextBlock.FirstVersion}, {record.BufferRef}");
                        return nextBlock;
                    }

                    if (lastBlockView.IsCompleted && lastBlockView.Count == 0)
                    {
                        hasCompletedEmptyBlock = true;
                        return default;
                    }

                    nextVersion = lastBlockView.NextVersion;
                    previousChecksum = lastBlockView.Checksum;
                    previousBlockLastTimestamp = lastBlockView.GetLastTimestampOrDefault();
                }

                // We have tried LE on versionToWrite and the block is completed if exists, otherwise we return it above.
                record.Version = versionToWrite;
                if (c.TryFindDup(Lookup.GT, ref streamId, ref record))
                {
                    ThrowHelper.AssertFailFast(streamId == (long)streamLog.Slid, "streamId == (long)streamLog.Slid");

                    if (record.Version == CompletedVersion)
                    {
                        return default;
                    }

#pragma warning disable 618
                    var lastBlockView = TryGetIndexedStreamBlockView(streamLog, in record.BufferRef,
                        out var nativeBuffer,
                        expectedFirstVersion: 0,
                        ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI
#pragma warning restore 618

                    if (lastBlockView.IsCompleted && lastBlockView.Count == 0)
                    {
                        hasCompletedEmptyBlock = true;
                        return default;
                    }

                    if (!lastBlockView.IsInitialized)
                    {
                        if (versionToWrite != nextVersion)
                        {
                            // We are inside x-process lock, want to write a version that
                            // is not in any existing block and there is uninitialized
                            // prepared block. versionToWrite must be the first version
                            // in the prepared block, otherwise the called is blocked
                            // until another thread/process initializes the prepared block.
                            ThrowVersionIsNotEqualToNext(versionToWrite, nextVersion);
                        }

                        var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer,
                            streamLog.StreamLogFlags.Pow2Payload());

                        // true if we are initializing the block for the first time
                        if (StreamBlock.TryInitialize(blockBuffer, streamLog.Slid,
                            streamLog.BlockInitialVersionAndFlags, streamLog.ItemFixedSize,
                            versionToWrite, previousChecksum, previousBlockLastTimestamp))
                        {
                            if (record.Version != ReadyBlockVersion)
                            {
                                FailUninitializedBlockBadVersion();
                            }

                            SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                            var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);
                            var nextBlock = new StreamBlock(
                                manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()), manager,
                                streamLog.Slid,
                                streamLog.ItemFixedSize, lastBlockView.FirstVersion);
                            Debug.WriteLine(
                                $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlock: found+initialized prepared block with first version: {nextBlock.FirstVersion}, {manager.BufferRef}");
                            return nextBlock;
                        }
                        else
                        {
                            ThrowHelper.FailFast("Block was not initialized and then we failed to initialize it from x-process lock.");
                        }
                    }

                    // Next, not first. Happy path. If not, more thorough checks in locked path
                    var blockVersion = lastBlockView.NextVersion;
                    var isCompleted = lastBlockView.IsCompleted;
                    if (blockVersion == versionToWrite && !isCompleted)
                    {
                        SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                        var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);
                        var nextBlock =
                            new StreamBlock(manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()), manager,
                                streamLog.Slid, streamLog.ItemFixedSize, blockVersion);
                        Debug.WriteLine(
                            $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlock: " +
                            $"found non-completed prepared block with first version: {nextBlock.FirstVersion}, {record.BufferRef}");
                        return nextBlock;
                    }
                }
            }

            return default;
        }

        /// <summary>
        /// Remove a completed empty block and all blocks after it. Subsequent blocks, if any,
        /// must be uninitialized. Normally this is possible only when writing unexpectedly large
        /// value (larger then the active block size).
        /// </summary>
        /// <param name="txn"></param>
        /// <param name="streamLog"></param>
        /// <param name="versionToWrite"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        private void RemoveCompletedEmptyBlock(Transaction txn,
            StreamLog streamLog, ulong versionToWrite)
        {
            // There could be a buffer to clean if we replaced existing one. This should be after commit/abort probably in finally
            BufferRef replacedBufferRef0 = default;
            BufferRef replacedBufferRef1 = default;

            var streamId = (long)streamLog.Slid;
            var record = new StreamBlockRecord { Version = versionToWrite };

            using (var c = _blocksDb.OpenCursor(txn))
            {
                if (c.TryFindDup(Lookup.LE, ref streamId, ref record))
                {
                    ThrowHelper.AssertFailFast(streamId == (long)streamLog.Slid, "streamId == (long)streamLog.Slid");

#pragma warning disable 618
                    var lastBlockView = TryGetIndexedStreamBlockView(streamLog,
                        in record.BufferRef,
                        out var nativeBuffer,
                        expectedFirstVersion: 0,
                        ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI

                    if (lastBlockView.IsCompleted && lastBlockView.Count == 0)
                    {
                        Trace.TraceWarning("Replacing existing empty completed block");

                        var replaceBlockRecord = new StreamBlockRecord { Version = record.Version };
                        _blocksDb.Delete(txn, streamId, replaceBlockRecord);

                        SharedMemory.FromIndexedStreamBlockToStreamBlock(nativeBuffer);
                        replacedBufferRef0 = record.BufferRef;
                        lastBlockView = default;

                        // remove ready block if it is present
                        if (c.TryFindDup(Lookup.GT, ref streamId, ref replaceBlockRecord))
                        {
                            if (replaceBlockRecord.Version != ReadyBlockVersion)
                            {
                                ThrowHelper.FailFast("Block to replace is not the last one, wrong code logic.");
                            }

                            lastBlockView = TryGetIndexedStreamBlockView(streamLog,
                                in replaceBlockRecord.BufferRef,
                                out nativeBuffer, replaceBlockRecord.Version,
                                ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI

                            if (lastBlockView.IsInitialized)
                            {
                                ThrowHelper.FailFast(
                                    "Ready block after an empty completed block is initialized. This should never happen, wrong code logic.");
                            }

                            _blocksDb.Delete(txn, streamId, replaceBlockRecord);

                            SharedMemory.FromIndexedStreamBlockToStreamBlock(nativeBuffer);
                            replacedBufferRef1 = replaceBlockRecord.BufferRef;

                            lastBlockView = default;
                        }

                        if (c.TryFindDup(Lookup.GT, ref streamId, ref replaceBlockRecord))
                        {
                            ThrowHelper.FailFast("Two block records after completed empty. This should not happen.");
                        }
                    }
#pragma warning restore 618
                }
            }

            txn.Commit();

            if (replacedBufferRef0 != default)
            {
                ReturnReplaced(replacedBufferRef0);
            }

            if (replacedBufferRef1 != default)
            {
                ThrowHelper.AssertFailFast(replacedBufferRef0 != default,
                    "replacedBufferRef1 != default is only possible if replacedBufferRef0 != default");
                ReturnReplaced(replacedBufferRef1);
            }

            void ReturnReplaced(BufferRef bufferRef)
            {
                var nativeBuffer = _blockPool.TryGetAllocatedNativeBuffer(bufferRef);
                SharedMemory.FromStreamBlockToOwned(nativeBuffer, _blockPool.Wpid.InstanceId);
                // attach to pool
                var sharedMemory = SharedMemory.Create(nativeBuffer, bufferRef, _blockPool);
                _blockPool.Return(sharedMemory);
            }
        }

        /// <summary>
        /// Get or create a next block after (inclusive) the given version and increase that block reference count.
        /// </summary>
        /// <param name="streamLog"></param>
        /// <param name="minimumLength"></param>
        /// <param name="versionToWrite"></param>
        /// <param name="timestampToWrite">User for record initialization if it was not prepared.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        private StreamBlock RentNextWritableBlockLocked(StreamLog streamLog,
            int minimumLength,
            ulong versionToWrite,
            Timestamp timestampToWrite)
        {
            Debug.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] Entered RentNextWritableBlockLocked");

            if (versionToWrite == 0)
            {
                ThrowHelper.FailFast("RentNextWritableBlockLocked: nextBlockFirstVersion == 0, this condition must have been handled before.");
            }

            var streamId = (long)streamLog.Slid;

            RETRY:
            using (var txn = _env.BeginTransaction())
            {
                var nextBlock =
                    RentNextWritableBlockExisting(txn, streamLog, versionToWrite, out var hasCompletedEmptyBlock);
                if (nextBlock.IsValid)
                {
                    txn.Abort();
                    return nextBlock;
                }

                if (hasCompletedEmptyBlock)
                {
                    // This method commits txn because we could clear buffers only after SBI
                    // is updated, so we start a new txn after calling it.
                    // This should be extremely rare edge case.
                    RemoveCompletedEmptyBlock(txn, streamLog, versionToWrite);
                    goto RETRY;
                }

                StreamBlock lastBlockView = default;
                StreamBlockRecord record = default;

                using (var c = _blocksDb.OpenCursor(txn))
                {
                    #region No block with first version >= version, update index version if needed

                    if (c.TryGet(ref streamId, ref record, CursorGetOption.Set)
                        && c.TryGet(ref streamId, ref record, CursorGetOption.LastDuplicate))
                    {
#pragma warning disable 618
                        lastBlockView = TryGetIndexedStreamBlockView(streamLog, in record.BufferRef, out _,
                            expectedFirstVersion:
                            0, // not record.Version, below we handle the case when record.Version == ReadyBlockVersion
                            ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI
#pragma warning restore 618

                        ThrowHelper.AssertFailFast(lastBlockView.IsCompleted,
                            "Buffer must be completed if we reached there.");
                        ThrowHelper.AssertFailFast(lastBlockView.CountVolatile > 0,
                            "Replacing completed empty buffer is done above.");

                        if (record.Version == ReadyBlockVersion)
                        {
                            var blockFirstVersion = lastBlockView.FirstVersion;

                            ThrowHelper.AssertFailFast(blockFirstVersion != 0, "blockFirstVersion != 0");

                            // CursorPutOptions.Current is not faster but creates risks, we must ensure AppendDuplicateData returns true

                            var readyBlockRecord = new StreamBlockRecord {Version = ReadyBlockVersion};
                            _blocksDb.Delete(txn, streamId, readyBlockRecord);

                            record.Version = blockFirstVersion;
                            record.Timestamp = lastBlockView.GetFirstTimestampOrWriteStart();

                            if (!c.TryPut(ref streamId, ref record, CursorPutOptions.AppendDuplicateData))
                            {
                                ThrowHelper.FailFast("Cannot update ready block version: " + blockFirstVersion);
                            }

                            // now current has correct version in the index, could append new block normally
                        }
                    }

                    #endregion No block with first version >= version, update index version if needed
                } // dispose cursor

                #region Create new block (existing usable cases went to the ABORT label already)

#pragma warning disable 618
                var nextMemory = GetEmptyStreamBlockMemory(minimumLength, streamLog);
                ThrowHelper.AssertFailFast(nextMemory.Header.FlagsCounter == HeaderFlags.IsStreamBlock, "nextMemory.Header.FlagsCounter == HeaderFlags.IsStreamBlock");
#pragma warning restore 618

#if DEBUG
                // Block memory is initialized with slid.
                var block = new StreamBlock(nextMemory.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()));
                Debug.Assert(block.StreamLogIdLong == (long)streamLog.Slid);
                Debug.Assert(block.FirstVersion == 0);
#endif
                
                if (!StreamBlock.TryInitialize(nextMemory.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()),
                    streamLog.Slid, streamLog.BlockInitialVersionAndFlags, streamLog.ItemFixedSize,
                    versionToWrite,
                    lastBlockView.IsValid ? lastBlockView.Checksum : default,
                    lastBlockView.IsValid ? lastBlockView.GetLastTimestampOrDefault() : default)
                )
                {
                    ThrowHelper.FailFast("Cannot initialize next block buffer");
                }

                var newBlockRecord = new StreamBlockRecord(nextMemory.BufferRef)
                {
                    Version = versionToWrite,
                    Timestamp = timestampToWrite
                };

                _blocksDb.Put(txn, streamId, newBlockRecord, TransactionPutOptions.AppendDuplicateData);

                if (AdditionalCorrectnessChecks.Enabled)
                {
                    EnsureNoDuplicates(txn, streamId);
                }

                #endregion Create new block (existing usable cases went to the ABORT label already)

                txn.Commit();
                // txnActive = false;

                // Trace.TraceWarning("PREPARATION FAILED");

                #region After commit state updates & asserts

                // Transition state to indexed.
                SharedMemory.FromStreamBlockToIndexedStreamBlockClearInstanceId(nextMemory.NativeBuffer, streamLog,
                    firstVersion: versionToWrite, // we have just created it with that version, assert that
                    isRent: true, // increment count from 0 to 2: one for SBI and one for the caller of this method
                    ignoreAlreadyIndexed: true);

                nextBlock = new StreamBlock(nextMemory.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()),
                    nextMemory, streamLog.Slid,
                    // More asserts with the values we have just created the block with. Not needed but if they fail then something badly wrong with the code.
                    // TODO (low) after proper testing replace with debug asserts after this call.
                    expectedValueSize: streamLog.ItemFixedSize,
                    expectedFirstVersion: versionToWrite);

                Debug.WriteLine(
                    $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlockLocked: created new block with first version: {nextBlock.FirstVersion}, {nextMemory.BufferRef}");

                if (!lastBlockView.IsValid)
                {
                    ThrowHelper.AssertFailFast(versionToWrite == 1,
                        "Last buffer could be invalid only when we are creating the first block");
                }

                #endregion After commit state updates & asserts

                return nextBlock;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailUninitializedBlockBadVersion()
        {
            ThrowHelper.FailFast("Uninitialized block could only be a prepared one");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void EnsureNoDuplicates(Transaction txn, long streamId)
        {
            StreamBlockRecord r = default;
            using (var c = _blocksDb.OpenReadOnlyCursor(txn))
            {
                // var keyExists = c.TryGet(ref streamId, ref r, CursorGetOption.Set);
                if (!(c.TryGet(ref streamId, ref r, CursorGetOption.Set) &&
                      c.TryGet(ref streamId, ref r, CursorGetOption.FirstDuplicate)))
                {
                    return; // ThrowHelper.FailFast("Cannot set cursor to stream id first SLCR");
                }

                var previous = r;

                while (c.TryGet(ref streamId, ref r, CursorGetOption.NextDuplicate))
                {
                    if (r.Version == previous.Version)
                    {
                        ThrowHelper.FailFast("Duplicate block versions after RentNextWritableBlock: " + r.Version);
                    }

                    previous = r;
                }
            }
        }

        // ReSharper disable once UnusedParameter.Local
        private void Dispose(bool disposing)
        {
            ReaderBlockCache.Dispose();
            // _persistedWalPosDb?.Dispose();
            _blocksDb?.Dispose();
            _env?.Dispose();
            if (_blockStorage != null)
            {
                _blockStorage.Dispose();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~StreamBlockIndex()
        {
            Dispose(false);
        }
    }
}
