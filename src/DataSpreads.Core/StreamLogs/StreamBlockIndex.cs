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
                // we write to storage first.
                ThrowHelper.FailFast($"lastPackedRecordVersion {lastPackedBlockVersionFromState} > lastPackedStorageVersion {lastPackedBlockVersionFromStorage} for Slid {slid}");
            }
            else if (lastPackedBlockVersionFromState < lastPackedBlockVersionFromStorage)
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
                                            Trace.TraceWarning(
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
            if (fromStorage != 0 && (fromStorage = BlockStorage.LastStreamBlockKey(streamId)) != lastPackedVersion)
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

        /// <summary>
        /// Get or create the next writeable <see cref="StreamBlock"/> for the given <paramref name="streamLog"/>
        /// and rent that block.
        /// </summary>
        /// <param name="streamLog"></param>
        /// <param name="minimumLength"></param>
        /// <param name="nextBlockFirstVersion"></param>
        /// <param name="nextBlockFirstTimestamp"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe StreamBlock RentNextWritableBlock(
            StreamLog streamLog,
            int minimumLength,
            ulong nextBlockFirstVersion,
            Timestamp nextBlockFirstTimestamp)
        {
            Debug.WriteLine(
                $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] Entered RentNextWritableBlock");
            var streamId = (long)streamLog.Slid;

            // First check if there is already a prepared buffer

            // version == 0 only on Init, move it to slower path
            if (nextBlockFirstVersion > 0)
            {
                // TODO Ensure that buffer is IndexedStreamBlock with correct parameters
                // TODO Increment count with state check here

                using (var txn = _env.BeginReadOnlyTransaction())
                using (var c = _blocksDb.OpenReadOnlyCursor(txn))
                {
                    // This record is used for search by version, which is > 0, so could leave TS as default
                    var record = new StreamBlockRecord { Version = nextBlockFirstVersion };

                    var nextVersion = 1UL;
                    uint previousChecksum = default;
                    Timestamp previousBlockLastTimestamp = default;
                    if (c.TryFindDup(Lookup.LE, ref streamId, ref record))
                    {
                        ThrowHelper.AssertFailFast(streamId == (long)streamLog.Slid,
                            "streamId == (long)streamLog.Slid");
                        if (record.Version == CompletedVersion)
                        {
                            return default;
                        }

#pragma warning disable 618
                        var lastBlockView = TryGetIndexedStreamBlockView(streamLog,
                            in record.BufferRef,
                            out var nativeBuffer, // TODO use it if !lastBlockView.IsCompleted below
                            expectedFirstVersion: 0,
                            ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI
#pragma warning restore 618

                        if (!lastBlockView.IsCompleted)
                        {
                            // Wanted a block that starts with @version.
                            // Found a block that has first version LE @version
                            //    *AND is not completed*.
                            // Must return this non-completed block. If it is being
                            // completed now from another thread then will retry.
                            // If there is no more space then the first thread that
                            // detect that must complete the block.

                            SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                            var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);
                            var nextBlock = new StreamBlock(
                                manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()),
                                manager, streamLog.Slid, streamLog.ItemFixedSize, lastBlockView.FirstVersion);
                            Debug.WriteLine(
                                $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlock: found non-completed existing block with first version: {nextBlock.FirstVersion}, {record.BufferRef}");
                            return nextBlock;
                        }

                        nextVersion = lastBlockView.NextVersion;
                        previousChecksum = lastBlockView.Checksum;
                        previousBlockLastTimestamp = lastBlockView.GetLastTimestampOrDefault();
                    }

                    record.Version = nextBlockFirstVersion;
                    if (c.TryFindDup(Lookup.GT, ref streamId, ref record))
                    {
                        ThrowHelper.AssertFailFast(streamId == (long)streamLog.Slid,
                            "streamId == (long)streamLog.Slid");
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

                        if (!lastBlockView.IsInitialized)
                        {
                            if (nextBlockFirstVersion != nextVersion)
                            {
                                ThrowVersionIsNotEqualToNext(nextBlockFirstVersion, nextVersion);
                            }

                            var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer,
                                streamLog.StreamLogFlags.Pow2Payload());

                            // true if we are initializing the block for the first time
                            if (StreamBlock.TryInitialize(blockBuffer, streamLog.Slid,
                                streamLog.BlockInitialVersionAndFlags, streamLog.ItemFixedSize,
                                nextBlockFirstVersion, previousChecksum, previousBlockLastTimestamp))
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
                                ThrowHelper.ThrowInvalidOperationException(
                                    "WTF. Block was not initialized and then we failed to ");
                            }
                        }

                        // Next, not first. Happy path. If not, more thorough checks in locked path
                        var blockVersion = lastBlockView.NextVersion;
                        var isCompleted = lastBlockView.IsCompleted;
                        if (blockVersion == nextBlockFirstVersion && !isCompleted)
                        {
                            SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                            var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);
                            var nextBlock =
                                new StreamBlock(manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()), manager,
                                    streamLog.Slid, streamLog.ItemFixedSize, blockVersion);
                            Debug.WriteLine(
                                $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlock: found non-completed prepared block with first version: {nextBlock.FirstVersion}, {record.BufferRef}");
                            return nextBlock;
                        }
                    }
                }
            }

            lock (_env) // be nice to LMDB, use it's lock only for x-process access
            {
                return RentNextWritableBlockLocked(streamLog, minimumLength, nextBlockFirstVersion,
                nextBlockFirstTimestamp);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowVersionIsNotEqualToNext(ulong version, ulong nextVersion)
        {
            ThrowHelper.ThrowInvalidOperationException($"Version {version} does not equal to the next version {nextVersion} of existing writable block.");
        }

        /// <summary>
        /// Get or create a next block after (inclusive) the given version and increase that block reference count.
        /// Used in <see cref="StreamLog.Init"/> with zero version and in <see cref="StreamLog.RotateActiveBlock"/>.
        /// </summary>
        /// <param name="streamLog"></param>
        /// <param name="minimumLength"></param>
        /// <param name="nextBlockFirstVersion"></param>
        /// <param name="nextBlockFirstTimestamp">User for record initialization if it was not prepared.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        private unsafe StreamBlock RentNextWritableBlockLocked(StreamLog streamLog, int minimumLength, ulong nextBlockFirstVersion, Timestamp nextBlockFirstTimestamp)
        {
            Debug.WriteLine(
                $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] Entered RentNextWritableBlockLocked");

            // Note: This is a very large method that is actually 3 or 4 methods,
            // but live with regions for now. This is the most difficult & subtle
            // logic that easily breaks if some assumption is changed,
            // should refactor very carefully.

            // There could be a buffer to clean if we replaced existing one. This should be after commit/abort probably in finally
            BufferRef replacedBufferRef0 = default;
            BufferRef replacedBufferRef1 = default;

            // TODO use txn state
            var txnActive = true;
            var txn = _env.BeginTransaction();
            try
            {
                var streamId = (long)streamLog.Slid;

                StreamBlock lastBlockView = default;
                StreamBlock nextBlock = default;

                using (var c = _blocksDb.OpenCursor(txn))
                {
                    StreamBlockRecord record;

                    #region Init (version == 0)

                    if (nextBlockFirstVersion == 0)
                    {
                        record = default;

                        if (c.TryGet(ref streamId, ref record, CursorGetOption.Set)
                            && c.TryGet(ref streamId, ref record, CursorGetOption.LastDuplicate))
                        {
                            if (record.Version == CompletedVersion)
                            {
                                return default;
                            }

                            var bufferRef = record.BufferRef;
#pragma warning disable 618
                            lastBlockView = TryGetIndexedStreamBlockView(streamLog, in bufferRef,
                                out var nativeBuffer,
                                expectedFirstVersion: 0, // we are initializing SL and do not know the version
                                ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI
#pragma warning restore 618

                            if (record.Version == ReadyBlockVersion
                                && !lastBlockView.IsInitialized
                                && !c.TryGet(ref streamId, ref record, CursorGetOption.PreviousDuplicate))
                            {
                                lastBlockView = default;
                            }
                            else
                            {
                                if (record.BufferRef != bufferRef)
                                {
#pragma warning disable 618
                                    lastBlockView = TryGetIndexedStreamBlockView(streamLog, in record.BufferRef,
                                        out nativeBuffer,
                                        expectedFirstVersion:
                                        0, // we are initializing SL and do not know the version
                                        ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI
#pragma warning restore 618
                                }

                                // ReSharper disable once RedundantAssignment
                                var newCount = SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                                Debug.Assert(newCount > 1);

                                var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);
                                nextBlock = new StreamBlock(
                                    manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()), manager,
                                    streamLog.Slid,
                                    streamLog.ItemFixedSize, lastBlockView.FirstVersion);
                                // Expected numbers and manager.BlockBuffer trip are not needed, checks eat cycles, even if few of them.
                                // But good to have this to ensure that everything works. This is not in any hot loop.

                                Debug.WriteLine(
                                    $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlockLocked: found prepared block with first version: {nextBlock.FirstVersion}, {record.BufferRef}");

                                lastBlockView = default;
                                goto ABORT;
                            }
                        }

                        // empty, return invalid block
                        nextBlock = default;
                        goto ABORT;
                    }

                    #endregion Init (version == 0)

                    #region Use existing last block is it is not completed

                    // Same as read-only above, retry from a lock so that we see all data

                    record = new StreamBlockRecord { Version = nextBlockFirstVersion };

                    if (c.TryFindDup(Lookup.GE, ref streamId, ref record))
                    {
                        if (record.Version == CompletedVersion)
                        {
                            goto ABORT;
                        }

#pragma warning disable 618
                        lastBlockView = TryGetIndexedStreamBlockView(streamLog, in record.BufferRef,
                            out var nativeBuffer,
                            expectedFirstVersion: 0, // it could be not initialized, we check this on the next line
                            ignoreAlreadyIndexed: true); // we took record.BufferRef from SBI
#pragma warning restore 618

                        if (!lastBlockView.IsInitialized)
                        {
                            var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer,
                                streamLog.StreamLogFlags.Pow2Payload());

                            // true if we are initializing the block for the first time
                            if (StreamBlock.TryInitialize(blockBuffer, streamLog.Slid,
                                streamLog.BlockInitialVersionAndFlags, streamLog.ItemFixedSize,
                                nextBlockFirstVersion, lastBlockView.Checksum,
                                lastBlockView.GetLastTimestampOrDefault()))
                            {
                                if (record.Version != ReadyBlockVersion)
                                {
                                    FailUninitializedBlockBadVersion();
                                }

                                SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                                var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);
                                nextBlock = new StreamBlock(
                                    manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()), manager,
                                    streamLog.Slid,
                                    streamLog.ItemFixedSize, lastBlockView.FirstVersion);
                                lastBlockView = default;
                                goto ABORT;
                            }
                            else
                            {
                                ThrowHelper.FailFast("Block was not initialized and then we failed to initialize.");
                            }
                        }

                        var lastBlockFirstVersion = lastBlockView.FirstVersion;
                        var isCompleted = lastBlockView.IsCompleted;
                        if (lastBlockFirstVersion == nextBlockFirstVersion && !isCompleted)
                        {
                            SharedMemory.IncrementIndexedStreamBlock(nativeBuffer.Data);
                            var manager = SharedMemory.Create(nativeBuffer, record.BufferRef, _blockPool);
                            nextBlock = new StreamBlock(
                                manager.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()), manager,
                                streamLog.Slid, streamLog.ItemFixedSize,
                                lastBlockFirstVersion);

                            Debug.WriteLine(
                                $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlockLocked: found existing non-completed block with first version: {nextBlock.FirstVersion}, {record.BufferRef}");

                            lastBlockView = default;
                            goto ABORT;
                        }

                        #region Replace completed empty

                        var requestedVersionExistsAndIsCompleted =
                            lastBlockFirstVersion == nextBlockFirstVersion && isCompleted;

                        // Completed empty means there was not enough space, we need a larger block and replace the empty one.
                        // Note that completed block has no relation to completed stream log (which requires a special record with version = ulong.Max)
                        if (requestedVersionExistsAndIsCompleted)
                        {
#pragma warning disable 618
                            if (lastBlockView.CountVolatile > 0)
                            {
                                var r = new StreamBlockRecord() { Version = lastBlockView.FirstVersion };
                                var x = c.TryFindDup(Lookup.GT, ref streamId, ref r);

                                //lastBlockView = default;
                                //goto ABORT;
                                ThrowHelper.FailFast("requestedVersionExistsAndIsCompleted with count > 0");
                            }

                            Trace.TraceWarning("Replacing existing empty completed block");

                            var replaceBlockRecord = new StreamBlockRecord { Version = record.Version };
                            _blocksDb.Delete(txn, streamId, replaceBlockRecord);

                            // TODO mark it as not indexed
                            // if transaction fails then it will remain in the index
                            // and we will detect that flags are wrong from TryGetIndexedStreamBlock
                            // TODO after txn release it to pool or native. Check if it is possible to return a buffer to pool if we did not take it?
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
                                ThrowHelper.FailFast("Something wrong 3");
                            }
#pragma warning restore 618
                        }
                        else
                        {
                            Debug.Assert(lastBlockView._manager == null,
                                "last block must be just a view over buffer without a memory manager");
                            lastBlockView = default;
                        }

                        #endregion Replace completed empty
                    }

                    #endregion Use existing last block is it is not completed

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

                            var readyBlockRecord = new StreamBlockRecord { Version = ReadyBlockVersion };
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
                Debug.Assert(nextMemory.Header.FlagsCounter == HeaderFlags.IsStreamBlock);
#pragma warning restore 618

#if DEBUG
                // Block memory is initialized with slid.
                var block = new StreamBlock(nextMemory.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()));
                Debug.Assert(block.StreamLogIdLong == (long)streamLog.Slid);
                Debug.Assert(block.FirstVersion == 0);
#endif
                // Initialize block memory with version before commit.
                Debug.Assert(nextBlockFirstVersion > 0);

                if (!StreamBlock.TryInitialize(nextMemory.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()),
                    streamLog.Slid, streamLog.BlockInitialVersionAndFlags, streamLog.ItemFixedSize,
                    nextBlockFirstVersion,
                    lastBlockView.IsValid ? lastBlockView.Checksum : default,
                    lastBlockView.IsValid ? lastBlockView.GetLastTimestampOrDefault() : default)
                )
                {
                    ThrowHelper.FailFast("Cannot initialize next block buffer");
                }

                var newBlockRecord = new StreamBlockRecord(nextMemory.BufferRef)
                {
                    Version = nextBlockFirstVersion,
                    Timestamp = nextBlockFirstTimestamp
                };

                _blocksDb.Put(txn, streamId, newBlockRecord, TransactionPutOptions.AppendDuplicateData);

                if (AdditionalCorrectnessChecks.Enabled)
                {
                    EnsureNoDuplicates(txn, streamId);
                }

                #endregion Create new block (existing usable cases went to the ABORT label already)

                txn.Commit();
                txnActive = false;

                // Trace.TraceWarning("PREPARATION FAILED");

                #region After commit state updates & asserts

                // TODO chaos monkey here: ChaosMonkey.Exception();

                // Transition state to indexed.
                SharedMemory.FromStreamBlockToIndexedStreamBlockClearInstanceId(nextMemory.NativeBuffer, streamLog,
                    firstVersion: nextBlockFirstVersion, // we have just created it with that version, assert that
                    isRent: true, // increment count from 0 to 2: one for SBI and one for the caller of this method
                    ignoreAlreadyIndexed: true);

                nextBlock = new StreamBlock(nextMemory.GetBlockBuffer(streamLog.StreamLogFlags.Pow2Payload()),
                    nextMemory, streamLog.Slid,
                    // More asserts with the values we have just created the block with. Not needed but if they fail then something badly wrong with the code.
                    // TODO (low) after proper testing replace with debug asserts after this call.
                    expectedValueSize: streamLog.ItemFixedSize,
                    expectedFirstVersion: nextBlockFirstVersion);

                Debug.WriteLine(
                    $"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] RentNextWritableBlockLocked: created new block with first version: {nextBlock.FirstVersion}, {nextMemory.BufferRef}");

                if (!lastBlockView.IsValid)
                {
                    ThrowHelper.AssertFailFast(nextBlockFirstVersion == 1,
                        "Last buffer could be invalid only when we are creating the first block");
                }

                if (replacedBufferRef0 != default)
                {
                    ReturnReplaced(replacedBufferRef0);
                }

                if (replacedBufferRef1 != default)
                {
                    Debug.Assert(replacedBufferRef0 != default);
                    ReturnReplaced(replacedBufferRef1);
                }

                #endregion After commit state updates & asserts

                return nextBlock;

                void ReturnReplaced(BufferRef bufferRef)
                {
                    var nativeBuffer = _blockPool.TryGetAllocatedNativeBuffer(bufferRef);
                    SharedMemory.FromStreamBlockToOwned(nativeBuffer, _blockPool.Wpid.InstanceId);
                    // attach to pool
                    var sharedMemory = SharedMemory.Create(nativeBuffer, bufferRef, _blockPool);
                    _blockPool.Return(sharedMemory);
                }

                ABORT:
                txn.Abort();
                txnActive = false;
                ThrowHelper.AssertFailFast(!lastBlockView.IsValid,
                    "LastBuffer is invalid on abort. We go to ABORT only when we could use it.");
                ThrowHelper.AssertFailFast(nextBlock._manager != null || !nextBlock.IsValid,
                    "Returned valid block must have memory manager so that it could be properly disposed later.");
                return nextBlock;
            }
            catch
            {
                // TODO use txn state
                if (txnActive)
                {
                    txn.Abort();
                }

                throw;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailUninitializedBlockBadVersion()
        {
            ThrowHelper.FailFast("Uninitialized block could only be the ready one");
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
