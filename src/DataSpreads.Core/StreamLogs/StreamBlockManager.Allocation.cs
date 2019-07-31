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
using Spreads;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.LMDB;
using Spreads.Utils;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks.Sources;

namespace DataSpreads.StreamLogs
{
    internal partial class StreamBlockManager
    {
        // Leave it as is at 64kb and go on. Just avoid buffers in 16-32kb range 
        // and the waste will be quite small. Packed buffers are as important as active ones.
        internal const int Pow2ModeSizeLimit = 65536 - SharedMemory.BufferHeader.Size;

        /// <summary>
        /// Slices a native buffer to appropriate <see cref="StreamBlock"/> buffer size.
        /// </summary>
        /// <param name="nativeBuffer">Native buffer with header.</param>
        /// <param name="pow2Payload">True if <see cref="StreamBlock"/> data region should be a power of two.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe DirectBuffer NativeToStreamBlockBuffer(DirectBuffer nativeBuffer, bool pow2Payload)
        {
            var memoryLength = nativeBuffer.Length - SharedMemory.BufferHeader.Size;
            int blockLen;
            byte* blockPtr;
            if (memoryLength > Pow2ModeSizeLimit)
            {
                if (pow2Payload)
                {
                    blockLen = StreamBlock.StreamBlockHeader.Size + BitUtil.FindPreviousPositivePowerOfTwo(memoryLength);
                    blockPtr = nativeBuffer.Data + SharedMemory.BufferHeader.Size + (memoryLength - blockLen);
                }
                else
                {
                    // When pow2Payload = false then the entire StreamBlock (including the header)
                    // fits a pow2 buffer so if we unpack it into a RAM-backed pooled buffer
                    // we do not waste 50% of the buffer space.
                    blockLen = BitUtil.FindPreviousPositivePowerOfTwo(memoryLength);
                    blockPtr = nativeBuffer.Data + SharedMemory.BufferHeader.Size + (memoryLength - blockLen);
                }
            }
            else
            {
                if (pow2Payload)
                {
                    ThrowHelper.ThrowNotSupportedException();
                }
                blockLen = memoryLength;
                blockPtr = nativeBuffer.Data + SharedMemory.BufferHeader.Size;
            }

            var blockBuffer = new DirectBuffer(blockLen, blockPtr);
            return blockBuffer;
        }

        // TODO fix/rename throwing method with Try...
        /// <summary>
        /// Returns a view over a <see cref="StreamBlock"/> that is already in <see cref="StreamBlockIndex"/>.
        /// Throws if buffer state is not <see cref="SharedMemory.BufferHeader.HeaderFlags.IsIndexedStreamBlock"/>.
        /// View means that <see cref="StreamBlock"/> has no memory manager
        /// and does not own a reference or change reference count.
        /// </summary>
        /// <param name="streamLog"></param>
        /// <param name="bufferRef"></param>
        /// <param name="nativeBuffer"></param>
        /// <param name="expectedFirstVersion"></param>
        /// <param name="ignoreAlreadyIndexed">There could be a race between Prepare/RentNext
        /// to update <see cref="SharedMemory.BufferHeader.HeaderFlags.IsStreamBlock"/> to <see cref="SharedMemory.BufferHeader.HeaderFlags.IsIndexedStreamBlock"/>.
        /// This flag tells to ignore failed atomic header swap if existing state is IsISB. Set to true only when
        /// the buffer is known to exist in SBI (we took it from there or just added in a txn).</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        private StreamBlock TryGetIndexedStreamBlockView(StreamLog streamLog, in BufferRef bufferRef, out DirectBuffer nativeBuffer,
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

#if DEBUG
            // we will do atomic header checks later anyway, but keep this in debug
            if (IsInFreeList(bufferRef))
            {
                ThrowHelper.ThrowInvalidOperationException($"Cannot get non-allocated buffer directly.");
            }
#endif
#pragma warning disable 618
            nativeBuffer = _buckets.DangerousGet(bufferRef);
#pragma warning restore 618

            // At least is must be owned, other flag checks are done later
            var header = nativeBuffer.Read<SharedMemory.BufferHeader>(index: 0);
            if ((header.FlagsCounter & SharedMemory.BufferHeader.HeaderFlags.IsOwned) == 0)
            {
                return default;
            }

            SharedMemory.BufferHeader.HeaderFlags flagsNoCount = header.FlagsCounter.Flags;
            if (flagsNoCount != SharedMemory.BufferHeader.HeaderFlags.IsIndexedStreamBlock)
            {
                if (ignoreAlreadyIndexed
                    && flagsNoCount == SharedMemory.BufferHeader.HeaderFlags.IsStreamBlock
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
                }
            }

            var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer, streamLog.StreamLogFlags.Pow2Payload());

            // We have validated native header, this ctor will validate content
            var blockView = new StreamBlock(blockBuffer, null, slid, expectedValueSize, expectedFirstVersion);

            return blockView;
        }

        /// <summary>
        /// Get or create a <see cref="StreamBlock"/> that contains <paramref name="version"/> and return its <see cref="StreamBlockProxy"/>.
        /// </summary>
        /// <remarks>
        /// If a block is completed but the version is inside it we return that block anyway.
        /// For normal stream logs that should never happen. For shared logs
        /// (such as NotificationLog) a block completeness is determined by the global version
        /// and not the block flag, so callers must decide what to do.
        /// </remarks>
        /// <param name="streamLog"></param>
        /// <param name="minimumLength"></param>
        /// <param name="version"></param>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        internal unsafe StreamBlock RentNextWritableBlock( // TODO rename to GetOtCreateBlock
            StreamLog streamLog,
            int minimumLength,
            ulong version,
            Timestamp timestamp)
        {
            Debug.WriteLine($"[{Thread.CurrentThread.ManagedThreadId}: {Thread.CurrentThread.Name}] Entered RentNextWritableBlock");
            
            var streamId = (long)streamLog.Slid;

            // version == 0 only on Init
            if (version == 0)
            {
                // TODO Review usages and use RentInit... directly, throw if version == 0
                return RentInitWritableBlock(streamLog);
            }

            using (var txn = Environment.BeginReadOnlyTransaction())
            {
                var block = RentNextWritableBlockExisting(txn, streamLog, version,
                    out _); // ignore hasCompletedEmptyBlock, we could do anything with it from write txn
                if (block.IsValid)
                {
                    return block;
                }
            }

            lock (Environment) // be nice to LMDB, use it's lock only for x-process access
            {
                return RentNextWritableBlockLocked(streamLog, minimumLength, version, timestamp);
            }
        }

        internal unsafe StreamBlock RentInitWritableBlock(StreamLog streamLog)
        {
            var streamId = (long)streamLog.Slid;

            using (var txn = Environment.BeginReadOnlyTransaction())
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
                            ThrowHelper.ThrowInvalidOperationException($"Version {versionToWrite} does not equal to the next version {nextVersion} of existing writable block.");
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
            using (var txn = Environment.BeginTransaction())
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
                ThrowHelper.AssertFailFast(nextMemory.Header.FlagsCounter == SharedMemory.BufferHeader.HeaderFlags.IsStreamBlock, "nextMemory.Header.FlagsCounter == HeaderFlags.IsStreamBlock");
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

#if DEBUG

                EnsureNoDuplicates(txn, streamId);

#endif

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

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailUninitializedBlockBadVersion()
        {
            ThrowHelper.FailFast("Uninitialized block could only be a standby one");
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
    }
}
