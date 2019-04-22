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
using Spreads;
using Spreads.Buffers;
using Spreads.Collections;
using Spreads.Collections.Concurrent;
using Spreads.DataTypes;
using Spreads.Serialization;
using Spreads.Threading;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DataSpreads.Logging;

namespace DataSpreads.StreamLogs
{
    // TODO prepare async when explicit rate hint is given or when calculated one is higher than max buffer size
    // Given RH indicates latency-sensitive stream which could have small number of values
    // Or better to detect continuous streams as ones that rotated at least twice.

    // TODO now we have exclusive per wpid, which is more logical thing
    // We should add IPromiseWritesAreFromSingleThread mode or shared Aeron-like
    // mode. Also batches?

    // Issues:
    // [ ] Low: static find last value, it is always in the active chunk that is never packed.

    // TODO make the size 64 bytes, it's possible now. Below are old comments

    // Note: Current size of SL is 80 bytes. Think of it as "less than 2 cache lines" and not "more than 1".
    // It is possible to reduce the size here, but this adds complexity and saved space will be used in other
    // places so that the total saving could be negative.
    // TODO we have 48 bytes to move stuff from proxy here, or 16 to have 3 SLs in 2 cache lines (however on heap it probably doesn't matter)

    [DebuggerDisplay("{" + nameof(ToString) + "()}")]
    internal unsafe partial class StreamLog : BaseContainer, IStorageIndexed, ISpreadsThreadPoolWorkItem // : IndexedLookup<long, StreamLog>
    {
        // Less than WAL max buffer size (256 KB) with room for some overheads, round down to 10k.
        public const uint MaxItemSize = 250_000; // TODO WAL must depend on it, WAL buffer should never be less (WAL.WALWriter.MaxBufferSize / 10_000) * 10_000;

        // The only consideration is size of logs for many open streams.
        // Empty logs are compressed greatly and are packed when a log is not open.
        private static readonly int MinChunkSize = 1; // will round up to a page size

        /// <summary>
        /// Max size when rate hint is provided
        /// </summary>
        internal const uint MaxChunkSize = StreamLogManager.MaxBufferSize;

        internal const uint MaxChunkAutoGrowSize = MaxItemSize * 4; // 1_000_000;

        private int _storageIndex;
        private byte _smallSizeCount;

        // TODO SyncMode to pack all options in single byte

        internal WriteMode WriteMode;
        private readonly StreamLogFlags _streamLogFlags;

        private readonly StreamLogManager _streamLogManager;

        /// <summary>
        /// Stream log state.
        /// </summary>
        /// <remarks>
        /// State fields are like object instance fields but are visible cross-process.
        /// </remarks>
        internal readonly StreamLogState State;

        /// <summary>
        /// Always the last block unless the log in completed and all blocks are packed, in which case this field has a default (invalid) block.
        /// </summary>
        /// <remarks>
        /// Readers open blocks independently and must never touch this block.
        /// </remarks>
        // [Obsolete("Readers should never touch this fields. Currently only proxy uses it.")] // TODO review the whole thing about proxy
        internal StreamBlock ActiveBlock;

        private readonly string _textId;

        private Task _batchWriteTask;

        internal StreamLog(StreamLogManager streamLogManager,
            StreamLogState state,
            int ratePerMinuteHint = 0,
            string textId = null)
        {
            _streamLogManager = streamLogManager;
            State = state;

            WriteMode = state.GetWriteMode();
            _streamLogFlags = State.StreamLogFlags;

            if (state.GetRatePerMinute() == 0)
            {
                if (ratePerMinuteHint > 0)
                {
                    // hinted rate stored as negative
                    State.SetRatePerMinute(-ratePerMinuteHint);
                }
                else
                {
                    State.SetRatePerMinute(MinChunkSize);
                }
            }
            else
            {
                if (ratePerMinuteHint > 0 && Slid != StreamLogId.Log0Id)
                {
                    // hinted rate stored as negative
                    State.SetRatePerMinute(-ratePerMinuteHint);
                }
            }

            // TODO State init must check for RO
            if (_streamLogManager.BlockIndex.GetIsCompleted(State.StreamLogId))
            {
                State.SetIsCompleted();
            }

            _textId = textId;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~StreamLog()
        {
            Trace.TraceWarning($"{this} is finalized.");
            Dispose(false);
        }

        public override string ToString()
        {
            return $"StreamLog {Slid}: {TextId}";
        }

        internal SharedMemory ActiveBuffer
        {
            // active is always backed by SharedMemory, null only when active is invalid
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ActiveBlock.SharedMemory;
        }

        internal StreamLogManager StreamLogManager
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _streamLogManager;
        }

        internal StreamBlockIndex BlockIndex
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => StreamLogManager.BlockIndex;
        }

        internal StreamLogStateStorage StateStorage
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => StreamLogManager.StateStorage;
        }

        internal ProcessConfig ProcessConfig
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => StreamLogManager.ProcessConfig;
        }

        internal Wpid Wpid
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => StreamLogManager.Wpid;
        }

        public StreamLogId Slid
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => State.StreamLogId;
        }

        public bool IsExclusiveLock
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !WriteMode.HasSharedLocalWrite();
        }

        internal bool DisableNotifications
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !WriteMode.HasLocalSync();
        }

        /// <summary>
        /// Returns true if Packer should pack this stream log.
        /// </summary>
        public bool IsPackable
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => StreamLogManager.IsPackerEnabled && !StreamLogFlags.NoPacking();
        }

        public bool NoTimestamp
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => StreamLogFlags.NoTimestamp();
        }

        /// <summary>
        /// Used to initialize <see cref="StreamBlock"/> version and flags.
        /// Only HasTimestamp affects returned value. The entire block (not items)
        /// is currently always serialized as BinaryZstd.
        /// </summary>
        internal VersionAndFlags BlockInitialVersionAndFlags
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var initialFlags = new VersionAndFlags()
                {
                    SerializationFormat = SerializationFormat.BinaryZstd
                };
                if (!NoTimestamp)
                {
                    initialFlags.HasTimestamp = true;
                }
                return initialFlags;
            }
        }

        public string TextId
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _textId;
        }

        // TODO overlap with container Flags: KeySorting, IsCompleted
        public StreamLogFlags StreamLogFlags
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _streamLogFlags;
        }

        /// <summary>
        /// Container flags.
        /// </summary>
        internal Flags ContainerFlags
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _flags;
        }

        /// <summary>
        /// Expected number of payload in bytes per second in the stream.
        /// </summary>
        public int RatePerMinute
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Math.Abs(State.GetRatePerMinute());
        }

        /// <summary>
        /// Positive for fixed-size payload, negative for variable size (negative value could be a hint of average size, but this is vague idea)
        /// </summary>
        public short ItemFixedSize
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => State.ItemFixedSize;
        }

        /// <summary>
        /// Rate per second limited from both sides with [MinChunkSize, MaxChunkSize]
        /// </summary>
        /// <param name="ratePerMinute"></param>
        /// <returns></returns>
        private static int BoundedChunkSize(int ratePerMinute)
        {
            // NB cast to long to void overflow
            return (int)Math.Max(MinChunkSize, Math.Min(ratePerMinute, MaxChunkSize));
        }

        // TODO implement or remove
        //private static int BoundedChunkSizeAutoGrow(int ratePerSecond)
        //{
        //    // NB cast to long to void overflow
        //    return (int)Math.Max(MinChunkSize, Math.Min(ratePerSecond, MaxChunkAutogrowSize));
        //}

        /// <summary>
        /// Initializes <see cref="ActiveBlock"/> to the last one from <see cref="BlockIndex"/>.
        /// Does not create a new block if there is no existing.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal bool Init()
        {
            if (ActiveBlock.IsValid)
            {
                ThrowHelper.FailFast("Init could only be called when active chunk is invalid");
            }

            var block = BlockIndex.RentInitWritableBlock(this);
            if (block.IsValid)
            {
                ActiveBlock = block;
                return true;
            }
            return false;
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //internal void AcquireExclusiveLock()
        //{
        //    var existing = State.TryAcquireLock(State.ProcessWpid);

        //    if (AdditionalCorrectnessChecks.Enabled)
        //    {
        //        if (existing != 0)
        //        {
        //            ClaimFailFastAcquireLockReturnedNonZero();
        //        }
        //    }

        //    if (!State.TryAcquireExclusiveLock())
        //    {
        //        ThrowHelper.FailFast("Cannot acquire exclusive lock. This should not happen after we took normal lock");
        //    }

        //    _exclusiveLock = true;
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Wpid TryAcquireExlusiveLock(out byte threadToken, int spinLimit = 0)
        {
            return State.TryAcquireExclusiveLock(Wpid, ProcessConfig, out threadToken, spinLimit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Wpid TryAcquireLock(byte threadToken, int spinLimit = 0)
        {
            return IsExclusiveLock
                ? State.TryReEnterExclusiveLock(Wpid, ProcessConfig, threadToken, spinLimit)
                : State.TryAcquireLock(Wpid, ProcessConfig, spinLimit);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ClaimAcquireLock(byte threadToken, int spinLimit = 0)
        {
            var existing = TryAcquireLock(threadToken, spinLimit);

            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (existing != 0)
                {
                    ClaimFailFastAcquireLockReturnedNonZero();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Wpid TryReleaseExclusiveLock()
        {
            return State.TryReleaseLock(Wpid);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Wpid TryReleaseLock(byte threadToken)
        {
            return IsExclusiveLock
                ? State.TryExitExclusiveLock(Wpid, threadToken)
                : State.TryReleaseLock(Wpid);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CommitReleaseLock(byte threadToken)
        {
            var zeroIfReleased = TryReleaseLock(threadToken);
            if (0 != zeroIfReleased)
            {
                FailCannotReleaseStateLock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DirectBuffer LockAndClaim(ulong desiredVersion, int length, byte threadToken)
        {
            ClaimAcquireLock(threadToken);
            return Claim(desiredVersion, length);
        }

        public ref DataTypeHeader ItemDth
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref ActiveBlock.ItemDth;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Memory<byte> ClaimRestOfBlockMemory(int minimumLength)
        {
            var db = ClaimRestOfBlock(minimumLength);
            if (!db.IsValid)
            {
                return Memory<byte>.Empty;
            }
            var memOffset = checked((int)((long)db._pointer - (long)ActiveBlock._manager.Pointer));
            var mem = ActiveBlock._manager.Memory.Slice(memOffset, db.Length);
            return mem;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DirectBuffer ClaimRestOfBlock(int minimumLength)
        {
            var len = Math.Max(ActiveBlock.EndOffset - ActiveBlock.Offset, minimumLength);
            var db = Claim(0, len);
            return db;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DirectBuffer Claim(ulong desiredVersion, int length, Timestamp timestamp = default)
        {
            var buffer = DirectBuffer.Invalid;

            var validLength = unchecked((uint)(length - 1)) < MaxItemSize;

            if (!validLength)
            {
                return buffer;
            }

            while (true)
            {
                buffer = ActiveBlock.Claim(desiredVersion, length);

                if (buffer.IsValid)
                {
                    break;
                }

                if (IsCompleted || (ActiveBlock.IsValid && desiredVersion != 0 && desiredVersion != ActiveBlock.NextVersion))
                {
                    break;
                }

                // Need to rotate here, we are holding lock, in this process no thread could try to rotate while we are here
                RotateActiveBlock(ActiveBlock.IsValid ? ActiveBlock.NextVersion : 0, timestamp, length);
            }

            return buffer;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ClaimFailFastAcquireLockReturnedNonZero()
        {
            ThrowHelper.FailFast("StreamStateView.TryAcquireLock must return zero.");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailCannotReleaseStateLock()
        {
            ThrowHelper.FailFast("Cannot unlock chunk");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AbortValidClaim()
        {
            Commit();
        }

        /// <summary>
        /// No need to abort invalid claim.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AbortValidClaimAndRelease(byte threadToken)
        {
            CommitReleaseLock(threadToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong CommitAndRelease(byte threadToken)
        {
            var version = Commit();
            CommitReleaseLock(threadToken);
            return version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Commit(int length)
        {
            var version = ActiveBlock.Commit(length);

            if (!DisableNotifications)
            {
                NotifyAll();
            }

            return version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Commit()
        {
            var version = ActiveBlock.Commit();

            if (!DisableNotifications)
            {
                NotifyAll();
            }

            return version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void NotifyAll()
        {
            var isPriority = WriteMode.HasLocalAck();
            var log0Version = StreamLogManager.Log0?.Append(new StreamLogNotification(Slid, priority: isPriority));
            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (0 == log0Version)
                {
                    CommitFailFastCannotUpdateZeroLog();
                }
            }
            NotifyUpdate(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void NotifyCurrentProcess(bool force)
        {
            NotifyUpdate(force);
        }

        /// <summary>
        /// Get element at index without bound checks.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DirectBuffer DangerousGetUnpacked(ulong version)
        {
            var pointer = DangerousGetUnpackedPointer(version, out var length);
            return new DirectBuffer(length, pointer);
        }

        /// <summary>
        /// Get element pointer and length at index without bound checks.
        /// Assume active block is not packed during the call.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte* DangerousGetUnpackedPointer(ulong version, out int length)
        {
            var block = ActiveBlock;
            ulong blockFirstVersion;
            if (!(block.IsValid && version >= (blockFirstVersion = block.FirstVersion)))
            {
                return DangerousGetUnpackedPointerNotActive(version, out length);
            }

            return block.DangerousGetPointer((int)(version - blockFirstVersion), out length);
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        private byte* DangerousGetUnpackedPointerNotActive(ulong version, out int length)
        {
            if (!BlockIndex.TryFindAt(Slid, version, Lookup.LE, out var record)
                    || record.BufferRef == default || record.BufferRef.Flag)
            {
                length = default;
                return null;
            }

            // TODO use method GetBlockFromRecord(bool unpack = false), it must include BR.Flag validation is unpack == false

            var nativeBuffer = StreamLogManager.BufferPool.Buckets[record.BufferRef];
            var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer, StreamLogFlags.Pow2Payload());
            var block = new StreamBlock(blockBuffer, null, Slid, ItemFixedSize, record.Version);
            Debug.Assert(block.IsValid);
            var blockFirstVersion = block.FirstVersion;
            return block.DangerousGetPointer((int)(version - blockFirstVersion), out length);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void CommitFailFastCannotUpdateZeroLog()
        {
            ThrowHelper.FailFast("Cannot update zero log");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void RotateActiveBlock(ulong nextVersion, Timestamp nextTimestamp, int minLength)
        {
            var length = Math.Max(Math.Min((int)MaxChunkAutoGrowSize, BoundedChunkSize(minLength * 4)), BoundedChunkSize(RatePerMinute));

            if (!ActiveBlock.IsValid)
            {
                if (Init())
                {
                    Debug.Assert(ActiveBlock.IsValid, "ActiveBlock.IsValid");
                    // If initialized block is completed then will retry
                    return;
                }

                // create the very first block
                if (nextVersion != 1 && nextVersion != 0)
                {
                    ThrowHelper.FailFast($"StreamLog is empty but nextBlockFirstVersion is not 0 or 1, but {nextVersion}.");
                }
                nextVersion = 1;
            }
            else
            {
                if (AdditionalCorrectnessChecks.Enabled)
                {
                    if (ActiveBlock.IsValid && !ActiveBlock.IsCompleted)
                    {
                        ThrowHelper.FailFast("Active chunk is not completed before rotation.");
                    }
                }

                Debug.WriteLine($"[{Thread.CurrentThread.Name}] Entered DoRotateActiveBlock");
                if (!ActiveBlock.IsCompleted)
                {
                    ThrowHelper.FailFast("Cannot rotate incomplete chunk");
                }

                UpdateRate();

                // SL reserved 1 + at least this instance
                Debug.Assert(ActiveBlock.ReferenceCount > 1);

                // TODO if there is no packer then call this: memory will be written to the backing file
                // in blocks gradually and not after all RAM is exhausted or process is closed.
                // TODO this should be in DisposeFree of SB - when refcount is 1 the memory is no longer needed.
                // TOTO all above is wrong, packer always walks over blocks, but if it is disabled then
                // it unlocks memory when refcount == 1 and does not do packing.
                // DirectFile.ReleaseMemory(_activeBlock.SharedMemory.NativeBuffer);

                // Dispose before getting next - important for completed empty ones
#pragma warning disable 618
                ActiveBlock.DisposeFree(releaseMemory: !IsPackable);
#pragma warning restore 618
            }

            _batchWriteTask?.Wait();

            var block = BlockIndex.RentNextWritableBlock(this, length, nextVersion, nextTimestamp);

            if (!block.IsInitialized)
            {
                ThrowHelper.FailFast("!StreamLogChunk.IsInitialized(buffer)");
            }

            // Console.WriteLine($"Rotated block for {Slid}, length {length}");

            LoggingEvents.Write.StreamBlockRotated(Slid.RepoId, Slid.StreamId, block.Length);

            ActiveBlock = block;

            // used as a hint to readers (MoveAt) to avoid index lookups
            State.SetActiveChunkVersion(ActiveBlock.FirstVersion);

            if (WriteMode.HasBatchWriteOnly())
            {
                // No synchronization, we want to reduce shared memory usage.
                // Often this is the case when we are loading existing
                // data into DS. With default settings (compression ratio 1)
                // packer could keep up with many load types, but any slowdown
                // in packer will lead to bloat of shared memory logs.
                // E.g. Zstd scaled level 9 is disproportionally slower
                // compared to compression ratio improvement, but often
                // we do not care about slower load stage if we could still
                // save non-negligible space. Decompression is fast for any level.
                
                _batchWriteTask = Task.Run(() =>
                {
                    var _ = StreamLogManager.Packer.TryPackBlocks(Slid);
                });
            }
            else
            {
                // Pack older chunks and prepare next chunk if the stream is fast
                PackAndPrepareNextBufferAsync();

                // Rotate notification update only from actual "rotator"
                var streamNotification = new StreamLogNotification(Slid, true);
                if (Slid != StreamLogId.Log0Id)
                {
                    if (0 == StreamLogManager.Log0?.Append(streamNotification))
                    {
                        ThrowHelper.FailFast("Cannot update zero log");
                    }
                }
                else
                {
                    StreamLogManager.Packer?.Add(StreamLogId.Log0Id);
                }
            }
        }

        private void UpdateRate()
        {
            var nanos = ActiveBlock.WriteEnd.Nanos - ActiveBlock.WriteStart.Nanos;
            var length = ActiveBlock.Length - StreamBlock.DataOffset;
            var seconds = nanos / 1_000_000_000.0;
            if (seconds <= 0)
            {
                Trace.TraceWarning(
                    $"Non-positive seconds in Stream {Slid}: {TextId}: {ActiveBlock.WriteEnd} - {ActiveBlock.WriteStart}");
            }
            else
            {
                var calcRate = (long)(length / seconds);

                // only when there is no hint (positive value)
                if (State.GetRatePerMinute() >= 0)
                {
                    if (calcRate > ActiveBlock.Length)
                    {
                        _smallSizeCount++;
                    }

                    // if we are rotating faster than once per second it's not a big deal, but if that is heppening
                    // frequently we should adjust
                    if (_smallSizeCount > 10)
                    {
                        _smallSizeCount = 0;
                        State.SetRatePerMinute(ActiveBlock.Length * 2);
                    }
                }
            }
        }

        #region Next block preparation

        /// <summary>
        /// Thread pool work item implementation. Cannot make explicit in the current (bad) design, depends on target framework.
        /// </summary>
        public void Execute()
        {
            PrepareNextBlock();
        }

        internal void PackAndPrepareNextBufferAsync()
        {
#if NETCOREAPP3_0
            // The only overload that does not allocate QueueUserWorkItemCallbackDefaultContext internally in the default thread pool.
            ThreadPool.UnsafeQueueUserWorkItem(this, false);
#else
            ThreadPool.UnsafeQueueUserWorkItem(PackAndPrepareNextBufferDelegate, this);
#endif
        }

#pragma warning disable HAA0603 // Delegate allocation from a method group
        private static readonly WaitCallback PackAndPrepareNextBufferDelegate = PrepareNextBlock;

#pragma warning restore HAA0603 // Delegate allocation from a method group

        internal static void PrepareNextBlock(object slObj)
        {
            var sl = (StreamLog)slObj;
            sl.PrepareNextBlock();
        }

        internal virtual void PrepareNextBlock()
        {
            try
            {
                if (State.GetRatePerMinute() < 0) // && Slid != StreamLogId.Log0Id)
                {
                    var length = BoundedChunkSize(RatePerMinute);
#pragma warning disable 618
                    BlockIndex.PrepareNextWritableStreamBlock(this, length);
#pragma warning restore 618
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Error in async PrepareNextBlock: " + ex);
            }
        }

        #endregion Next block preparation

        #region Move to other parts

        #region Server

        /// <summary>
        /// Used to detect if a chunk is still needed for sending to WAL:
        /// if block's last version is above this value then chunk is no
        /// longer needed.
        /// </summary>
        internal ulong LastProcessedVersion
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => State.GetLastVersionSentToWal();
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => State.SetLastVersionSentToWal(value);
        }

        /// <summary>
        /// If WAL's written position is above this value then LastVersionSentToWal is written to disk.
        /// </summary>
        internal ulong SentToWalPosition
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => State.GetWalPosition();
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => State.SetWalPosition(value);
        }

        internal ulong WalPersistedPosition
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => StateStorage.SharedState.GetWalPosition();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsVersionInWal(ulong version)
        {
            return version <= LastProcessedVersion && SentToWalPosition <= WalPersistedPosition;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal AckRequest WaitForAck(ulong streamLogVersion)
        {
            var ackRequest = new AckRequest(this, streamLogVersion);
            StreamLogManager.AckCompletions.Enqueue(ackRequest);
            return ackRequest;
        }

        // This SLM instance in the writer. Used to skip duplicate notifications from ZL listener
        internal bool CurrentWpidIsWriter
        {
            get { return false; }
        }

        // we are producing data and not receiving from upstream
        internal bool UpstreamIsWriter
        {
            get { return false; }
        }

        #endregion Server

        #endregion Move to other parts

        public int StorageIndex
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _storageIndex;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => _storageIndex = value;
        }

        // TODO sort out lock release.

        protected override void Dispose(bool disposing)
        {
            if (ActiveBlock.IsValid)
            {
#pragma warning disable 618
                ActiveBlock.DisposeFree();
#pragma warning restore 618
            }

            ActiveBlock = default;

            // TODO Ensure that the last incomplete block is packed
            StreamLogManager.Packer.Add(Slid);
        }

        public StreamLogCursor GetCursor()
        {
            return new StreamLogCursor(this, false);
        }

        // TODO should have base method
        public Task Complete()
        {
            if (!IsCompleted)
            {
                if (ActiveBlock.IsValid)
                {
                    ActiveBlock.Complete();
                }

                var added = BlockIndex.Complete(State.StreamLogId);
                if (added)
                {
                    Trace.TraceInformation($"Completed LogChunkTable for stream {Slid}: {TextId}");
                    if (0 == StreamLogManager.Log0?.Append(new StreamLogNotification(Slid, false, true)))
                    {
                        ThrowHelper.FailFast("Cannot update ZeroLog");
                    }
                }
                State.SetIsCompleted();
            }
            NotifyUpdate(true);
            return Task.CompletedTask;
        }

        public bool IsCompleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => State.GetIsCompleted();
        }

        public long Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => CurrentVersion;
        }

        public long CurrentVersion
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!ActiveBlock.IsValid)
                {
                    if (!Init())
                    {
                        return 0;
                    }
                }
                return checked((long)ActiveBlock.CurrentVersion);
            }
        }
    }
}
