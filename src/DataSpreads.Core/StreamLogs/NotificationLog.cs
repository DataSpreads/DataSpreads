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
using Spreads.DataTypes;
using Spreads.Utils;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace DataSpreads.StreamLogs
{
    internal sealed unsafe class NotificationLog : StreamLog
    {
        public const int Log0BufferSize = 8 * 1024 * 1024;
        public const ulong BlockVersionMask = ~(((ulong)Log0BufferSize >> 3) - 1);

        private const long InvalidIdxMask = ~((StreamLogManager.MaxBufferSize / 8) - 1L);

        internal long RotateCount;
        internal long StaleVersionCount;

        /// <summary>
        /// This stores previous block or prepared next (TBD).
        /// </summary>
        private readonly ConcurrentDictionary<ulong, StreamBlock> _blocks = new ConcurrentDictionary<ulong, StreamBlock>();

        public NotificationLog(StreamLogManager streamLogManager)
            : base(streamLogManager, streamLogManager.Log0State, StreamLogManager.MaxBufferSize, "_NotificationLog")
        {
            _ = State.TryAcquireLock(streamLogManager.Wpid, streamLogManager.ProcessConfig);
            if (!Init())
            {
                RotateActiveBlock(1, default, StreamLogManager.MaxBufferSize);
            }
            _ = State.TryReleaseLock(streamLogManager.Wpid);

            _blockCapacity = ActiveBlock.PayloadLength / StreamLogNotification.Size;
        }

        // internal Reader DefaultReader;
        private readonly int _blockCapacity;

        // internal ulong Log0StartVersion => State.GetLog0Version();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* Log0ItemPosition(byte* blockPointer, long version, out long idx)
        {
            var firstVersion = *(long*)(blockPointer + StreamBlock.StreamBlockHeader.FirstVersionOffset);

            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (firstVersion == 0)
                {
                    FailBlockVersionIsZero();
                }
            }

            idx = version - firstVersion;

            if ((idx & InvalidIdxMask) != 0)
            {
                return null;
            }

            return blockPointer + StreamBlock.StreamBlockHeader.Size + (idx << 3);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong BlockFirstVersion(ulong version)
        {
            return (version & BlockVersionMask) + 1;
        }

        [MethodImpl(MethodImplOptions.NoInlining // it has contented interlocked, which is heavy vs method call
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public ulong Append(StreamLogNotification payload)
        {
            var ulongPayload = (ulong)payload;
            if (ulongPayload == 0)
            {
                FailAppendEmptyPayload();
            }
            AGAIN:
            byte* position;
            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            var version = State.IncrementLog0Version();

            var blockPointer = ActiveBlock.Pointer;
            while ((position = Log0ItemPosition(blockPointer, (long)version, out _)) == null)
            {
                blockPointer = UpdateBlockPointer(version);
                if (blockPointer == null)
                {
                    goto AGAIN;
                }
            }

            if (IntPtr.Size == 8)
            {
                if (AdditionalCorrectnessChecks.Enabled)
                {
                    if (Volatile.Read(ref *(ulong*)(position)) != 0)
                    {
                        ThrowHelper.FailFast("Attempted to write to non-empty Log0 position");
                    }
                }

                Volatile.Write(ref *(ulong*)(position), ulongPayload);
            }
            else
            {
                // x86 atomic write
                Interlocked.Exchange(ref *(long*)(position), unchecked((long)ulongPayload));
            }

            // This writer thread could have been preempted anywhere before payload write.
            // This thread could have been low priority or from low-priority process.
            // Other writers could have added millions of new values.
            // We need to make sure that we are not writing to a block that readers
            // already ignore. Such case is easily reproduced in stress tests,
            // so we should leave no chance for that happening in actual apps.
            // Small additional cost is justified, performance is still OK.
            // (Log0 is used only for real time notifications or per-block notifications;
            // Also see #3 on possibility to use multiple notification channels)

            if (IsVersionStale(version))
            {
                goto AGAIN;
            }

            return version;
        }

        /// <summary>
        /// The version is older than the previous block.
        /// </summary>
        /// <param name="version"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsVersionStale(ulong version)
        {
            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            var globalVersion = State.GetLog0VersionVolatile();
            if (globalVersion - version > (ulong)_blockCapacity // this check is much faster & still rare
                && // the second condition is only possible when the first is true and not evaluated otherwise
                BlockFirstVersion(globalVersion) - BlockFirstVersion(version) > (ulong)_blockCapacity)
            {
                Interlocked.Increment(ref StaleVersionCount);
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        private byte* UpdateBlockPointer(ulong version)
        {
            byte* blockPointer = null;

            var blockKey = BlockFirstVersion(version);

            if (!TryUpdate())
            {
                if (!IsVersionStale(version))
                {
                    Rotate();
                    // if we cannot get block after rotate then we are in the old block
                    var _ = TryUpdate();
                    // TODO Assert that version is 2 blocks old if TryUpdate after Rotate returned false
                }
                else
                {
                    ThrowHelper.FailFast("Detected stale version");
                    // TODO increment monitoring counter
                }
            }

            return blockPointer;

            bool TryUpdate()
            {
                if (_blocks.TryGetValue(blockKey, out var block))
                {
                    blockPointer = block.Pointer;
                    return true;
                }

                return false;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailAppendEmptyPayload()
        {
            ThrowHelper.FailFast("NotificationLog.Append: cannot append empty payload");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailBlockVersionIsZero()
        {
            ThrowHelper.FailFast("Block version is zero");
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal void Rotate()
        {
            // For Log0 Rotate is always for global version and we update
            // ActiveBlock to the latest one, while keeping the previous one.
            // If after rotation we cannot find a block in the cache then
            // the version is stale, so we retry to write to the latest version.

            var existing = State.TryAcquireExclusiveLock(Wpid, ProcessConfig, out _, 0);

            if (AdditionalCorrectnessChecks.Enabled)
            {
                ThrowHelper.AssertFailFast(existing == 0, "TryAcquireLock returned non-zero");
            }
            Debug.WriteLine($"[{Thread.CurrentThread.Name}] Rotate lock taken: ");

            // we took the lock but do not know what other threads could have done

            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            var globalVersion = State.GetLog0VersionInterlocked();
            var globalBlockVersion = BlockFirstVersion(globalVersion);

            if (globalBlockVersion > ActiveBlock.FirstVersion)
            {
                *(int*)(ActiveBlock.Pointer + StreamBlock.StreamBlockHeader.CountOffset) = _blockCapacity;
                ActiveBlock.Complete();
            }

            if (ActiveBlock.IsCompleted)
            {
                Interlocked.Increment(ref RotateCount);

                var version = BlockFirstVersion(globalVersion + 1); // ActiveBlock.NextVersion; //

                Debug.Assert(NoTimestamp, "NotificationLog does not have timestamps.");
                Timestamp timestamp = default;

                RotateActiveBlock(version, timestamp, StreamLogManager.MaxBufferSize);

                _blocks[ActiveBlock.FirstVersion] = new StreamBlock(
                    new DirectBuffer(ActiveBlock.Length, ActiveBlock.Pointer),
                    null,
                    ActiveBlock.StreamLogId,
                    ActiveBlock.ItemFixedSize,
                    ActiveBlock.FirstVersion);

                // delete all keys below than this
                var keyToDelete = (long)ActiveBlock.FirstVersion - (long)_blockCapacity * 2;
                if (keyToDelete > 0)
                {
                    while (keyToDelete > 0 && _blocks.Count > 2)
                    {
                        if (_blocks.TryRemove((ulong)keyToDelete, out _))
                        {
                            Debug.WriteLine($"Removed Log0 old block from cache with key {keyToDelete}");
                        }
                        keyToDelete -= _blockCapacity;
                    }
                }

                if (!ActiveBlock.IsValid)
                {
                    ThrowHelper.FailFast("Equal pointers after rotate");
                }

                if (!BitUtil.IsPowerOfTwo(ActiveBlock.PayloadLength)
                    ||
                    !BitUtil.IsAligned((long)ActiveBlock.Pointer + StreamBlock.DataOffset, 4096))
                {
                    ThrowHelper.FailFast("Log0 payload length is not a power of 2");
                }
            }

            existing = State.TryReleaseLock(Wpid);
            Debug.WriteLine($"[{Thread.CurrentThread.Name}] Rotate lock released");
            if (AdditionalCorrectnessChecks.Enabled)
            {
                ThrowHelper.AssertFailFast(existing == 0, "TryAcquireLock returned non-zero");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StreamBlock RentChunkFromRecord(StreamBlockRecord record)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (record.BufferRef == default)
                {
                    ThrowHelper.FailFast("Log0 is not archived");
                }
            }

#pragma warning disable 618
            return StreamLogManager.BlockIndex.TryRentIndexedStreamBlock(this, record.BufferRef);
#pragma warning restore 618
        }

        // TODO There is a lot of commented code that tried to mitigate
        // writer preemption. But it was too complex. A simpler solution
        // would be:
        // 0. check current value
        // 1. if current is zero check next value before spinning (or check count)
        // 2. if next value is there check current, if not then spin. Only one value needs to be checked because Interlocked has barriers.
        // 3. if next is present but the current is not then add current idx to stalled queue
        //    the queue should be limited in size, we even have unused space in a block
        // 4. if current position - queue first position > some limit try read queued positions
        // 5. if queued positions are still zero fire a task that tries to wake up waiters that missed an update
        //    this is stupid but effective. This should be extremely rare and we need to minimize
        //    impact on normal case. Due to XADD contention updates are limited to c.30 Mops now, we have a little bit cycles to spare.
        // Do queue check only instead of spinning. Do not measure time. Give up earlier with wake up.
        // Missed updates should not slow down normal progress.
        // Async waiters must support false notifications instead of timeouts.
        // TODO we should not change priorities and yield more often. Never skip yield
        // or prefer UDP unblocking from Spreads SharedSpinLock.

        public class Reader : IEnumerator<StreamLogNotification>
        {
            public long Stalled;
            public long StallCount;
            public long MaxStall;

            private readonly NotificationLog _log0;
            private readonly CancellationToken _ct;
            private readonly bool _doNotWaitForNew;

            private ulong _currentKey;
            private StreamLogNotification _currentValue;

            private StreamBlock _currentBlock;
            // private int _currentChunkCapacity;
            // private ulong _currentChunkFirstVersion;
            // private ulong _lastMissed;

            private Queue<ulong> _skippedSlots = new Queue<ulong>();

            public Reader(NotificationLog log0, CancellationToken ct, bool doNotWaitForNew = false)
            {
                _log0 = log0;
                _ct = ct;
                _doNotWaitForNew = doNotWaitForNew;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private ulong TryReadPosition(byte* position)
            {
                if (IntPtr.Size == 8)
                {
                    return Volatile.Read(ref *(ulong*)position);
                }
                // x86 case
                return (ulong)Interlocked.Read(ref *(long*)position);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private ulong ReadPosition(byte* position, bool waitOnYield, out bool nextWillSpinYield)
            {
                nextWillSpinYield = false;
                ulong notificationValue = 0;
                // TODO review spinner
                var spinner = new SpinWait();
                while ((notificationValue = TryReadPosition(position)) == 0)
                {
                    spinner.SpinOnce();
                    if (spinner.NextSpinWillYield)
                    {
                        if (waitOnYield & !_ct.IsCancellationRequested)
                        {
                            spinner.Reset();
                            if (!Thread.Yield())
                            {
                                Thread.Sleep(0);
                            }
                        }
                        else
                        {
                            nextWillSpinYield = true;
                            return default;
                        }
                    }
                }

                return notificationValue;
            }

            /// <summary>
            /// Try read only current chunk
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryMoveNextFast(out bool spinYield)
            {
                spinYield = false;
                if (_currentKey == 0)
                {
                    return false;
                }
                var key = _currentKey + 1;

                var position = Log0ItemPosition(_currentBlock.Pointer, (long)key, out _);

                if (position != null)
                {
                    var notificationValue = ReadPosition(position, waitOnYield: false, out spinYield);
                    if (notificationValue != 0)
                    {
                        _currentValue = (StreamLogNotification)notificationValue;
                        _currentKey = key;
                        return true;
                    }
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                // TODO next chunk is prepared in advance, we could get it in advance. `This` is class, could work with as ref from ThreadPool
                var spinner = new SpinWait();

                while (!_ct.IsCancellationRequested)
                {
                    if (TryMoveNextFast(out var spinYield))
                    {
                        return true;
                    }

                    if (!spinYield) // finished current chunk
                    {
                        if (MoveGT(_currentKey)) // move to next chunk
                        {
                            return true;
                        }

                        // MoveGT waits if key must be there
                        if (_doNotWaitForNew)
                        {
                            return false;
                        }
                    }

                    // we cannot read value fast enough, probably writer thread was paused after incrementing counter

                    if (_doNotWaitForNew)
                    {
                        var maxKey = (ulong)_log0.CurrentVersion;

                        if (_currentKey + 1 > maxKey)
                        {
                            return false;
                        }
                    }

                    spinner.SpinOnce();
                    if (spinner.NextSpinWillYield)
                    {
                        spinner.Reset();
                        if (!Thread.Yield())
                        {
                            Thread.Sleep(0);
                        }
                        _currentValue = StreamLogNotification.WalSignal;
                        return true;
                    }
                }

                // Console.WriteLine("Log0.Reader CANCELLED");
                return false;

                //                if (_currentKey == 0)
                //                {
                //                    return MoveGT(0);
                //                }

                //                ulong notificationValue;

                //                var nanos = 0L;
                //                long delay = 0;

                //                // if (_missedUpdates.Count < 2)
                //                {
                //                    // goto RETRY;
                //                }
                //                //

                //                RETRY_WITH_MISSED_CHECK:

                //                if (_missedUpdates.Count > 0 && (ProcessConfig.CurrentTime.Nanos - _missedUpdates.Peek().Timestamp.Nanos > 1000_000))
                //                {
                //                    var missed = _missedUpdates.Dequeue();
                //                    var missedKey = missed.Version;

                //                    var chunk = _currentChunk;
                //                    var lookedUp = false;

                //                    MISSED_HAS_CHUNK:

                //                    if (missedKey >= chunk.FirstVersion &&
                //                        missedKey < chunk.FirstVersion + (ulong)chunk.Count)
                //                    {
                //                        // Console.WriteLine("X");
                //                        var db = chunk.GetUnchecked((int)((long)missedKey - (long)chunk.FirstVersion));
                //                        if ((notificationValue = db.VolatileReadUInt64(0)) != default)
                //                        {
                //                            // Console.WriteLine("Y");
                //                            _currentValue = (StreamLogNotification)notificationValue;
                //                            // Console.WriteLine("Recovered key: " + key);
                //                            if (lookedUp)
                //                            {
                //#pragma warning disable 618
                //                                chunk.DisposeFree();
                //#pragma warning restore 618
                //                            }
                //                            goto RETURN_TRUE;
                //                        }
                //                        else
                //                        {
                //                            // Console.WriteLine($"Z {missedKey}: {db.VolatileReadUInt64(0)}");
                //                        }
                //                    }

                //                    if (!lookedUp && _log0.State.StreamLogManager.LogChunkIndex.TryFindAt(StreamLogId.Log0Id, missedKey, Lookup.LE, out var record))
                //                    {
                //                        chunk = _log0.GetChunkFromRecord(record);
                //                        lookedUp = true;
                //                        goto MISSED_HAS_CHUNK;
                //                        // Console.WriteLine("Lookup on write");
                //                    }
                //                    if (lookedUp)
                //                    {
                //                        Console.WriteLine("Missed dispose");
                //#pragma warning disable 618
                //                        chunk.DisposeFree();
                //#pragma warning restore 618
                //                    }
                //                    _missedUpdates.Enqueue(missed);
                //                    //Console.WriteLine($"Dropped key: {missedKey}, ch fv: {chunk.FirstVersion}, ch count: {chunk.Count}");
                //                }

                //                var movedGt = false;

                //                RETRY:

                //                if (key >= _currentChunkFirstVersion && key < _currentChunkFirstVersion + (ulong)_currentChunkCapacity)
                //                {
                //                    var currentBuffer = _currentChunk.GetUnchecked(checked((int)(key - _currentChunkFirstVersion)));

                //                    while ((notificationValue = currentBuffer.VolatileReadUInt64(0)) == default) // _currentBuffer.VolatileReadUInt64(0)) == default)
                //                    {
                //                        if (_doNotWaitForNew)
                //                        {
                //                            return false;
                //                        }

                //                        if (nanos == 0)
                //                        {
                //                            // Do something globally useful that does not yield and has a memory barrier
                //                            nanos = _log0.StreamLogManager.ProcessConfig.UpdateTime();
                //                        }
                //                        else
                //                        {
                //                            spinner.SpinOnce();
                //                        }

                //                        if (_ct.IsCancellationRequested)
                //                        {
                //                            return false;
                //                        }

                //                        if (spinner.NextSpinWillYield)
                //                        {
                //                            // Console.WriteLine("Spin yield");
                //                            var currentMaxKey = _log0.State.GetLog0Version();
                //                            if (key >= currentMaxKey)
                //                            {
                //                                do // Busy wait could be set here by resetting spinner
                //                                {
                //                                    // Console.WriteLine("No key: " + key);
                //                                    spinner.SpinOnce(); // the reader thread is likely with high priority, do not just Thread.Sleep(0)

                //                                    if (_missedUpdates.Count > 0)
                //                                    {
                //                                        Thread.Sleep(1);
                //                                        // Console.WriteLine("XXX");
                //                                        goto RETRY_WITH_MISSED_CHECK;
                //                                    }

                //                                    if (_ct.IsCancellationRequested)
                //                                    {
                //                                        return false;
                //                                    }
                //                                } while (key > _log0.State.GetLog0Version());
                //                            }
                //                            else
                //                            {
                //                                // spinner.Reset();

                //                                delay = ProcessConfig.CurrentTime.Nanos - nanos;

                //                                Volatile.Write(ref Stalled, delay);
                //                                Interlocked.Increment(ref StallCount);

                //                                // Console.WriteLine("Must have key: " + key);
                //                                if (delay >= 5_00) // 0_000_000
                //                                {
                //                                    _missedUpdates.Enqueue(new MissedUpdate() { Timestamp = (Timestamp)nanos, Version = key });
                //                                    // ThrowHelper.FailFast("missed key");
                //                                    // Console.WriteLine("Skipping key: " + key);

                //                                    if (_missedUpdates.Count > 1 && (nanos - _missedUpdates.Peek().Timestamp.Nanos > 1_000))
                //                                    {
                //                                        goto RETRY_WITH_MISSED_CHECK;
                //                                    }
                //                                    else
                //                                    {
                //                                        key++;
                //                                        goto RETRY;
                //                                    }
                //                                }
                //                            }
                //                        }
                //                    }

                //                    _currentValue = (StreamLogNotification)notificationValue;

                //                    _currentKey = key;

                //                    goto RETURN_TRUE;
                //                }

                //                if (_ct.IsCancellationRequested)
                //                {
                //                    return false;
                //                }

                //                nanos = _log0.StreamLogManager.ProcessConfig.UpdateTime();
                //                try
                //                {
                //                    if (movedGt)
                //                    {
                //                        _currentChunk.DisposeFree();
                //                    }
                //                    movedGt = MoveGT(key);
                //                }
                //                catch (Exception ex)
                //                {
                //                    ThrowHelper.FailFast("Muted exception: " + ex);
                //                }

                //                // Console.WriteLine("MGT 2");
                //                //if (!MoveGT(key))
                //                //{
                //                //    ThrowHelper.FailFast("After key > _log0.State.GetZeroLogVersion() we must find a chunk");
                //                //}
                //                goto RETRY;

                //                RETURN_TRUE:
                //                if (nanos > 0)
                //                {
                //                    delay = ProcessConfig.CurrentTime.Nanos - nanos;
                //                    if (delay > MaxStall)
                //                    {
                //                        MaxStall = delay;
                //                    }
                //                    if (delay > 100_000)
                //                    {
                //                        // Console.WriteLine($"Stalled at key [{key}]: " + delay.ToString("N"));
                //                    }
                //                }
                //                Volatile.Write(ref Stalled, 0);
                //                return true;
            }

            /// <summary>
            /// Move at position greater than the given key.
            /// </summary>
            [MethodImpl(MethodImplOptions.NoInlining)]
            // ReSharper disable once InconsistentNaming
            public bool MoveGT(ulong searchKey)
            {
                var maxKey = _log0.CurrentVersion;

                var key = searchKey + 1;

                // TODO Ensure Log0 writes dummy value on Init()
                //if (key > maxKey)
                //{
                //    return false;
                //}

                byte* position;

                if (_currentBlock.IsValid
                    && (position = Log0ItemPosition(_currentBlock.Pointer, (long)key, out _)) != null
                   )
                {
                    _currentKey = key;
                    _currentValue = (StreamLogNotification)ReadPosition(position, waitOnYield: true, out _);
                    return true;
                }

                StreamBlock newChunk = default;
                // This doesn;t work reliably
                //                var sm = _currentChunk.SharedMemory;
                //                if (sm != null && sm.NextBuffer != default)
                //                {
                //                    // Console.WriteLine("Via NextBuffer");
                //#pragma warning disable 618
                //                    newChunk = _log0.StreamLogManager.BufferPool.TryRentExistingChunk(StreamLogId.Log0Id, sm.NextBuffer,
                //                        _log0.State.ValueSize);
                //#pragma warning restore 618
                //                }

                if (!newChunk.IsValid)
                {
                    if (!_log0.BlockIndex.TryFindAt(StreamLogId.Log0Id, key, Lookup.GE,
                        out var record) || record.IsPacked)
                    {
                        return false;
                    }

                    newChunk = _log0.RentChunkFromRecord(record);
                }

                if (!newChunk.IsValid)
                {
                    return false;
                }

                if (newChunk.FirstVersion <= _currentKey)
                {
                    // not started to write to the next chunk
#pragma warning disable 618
                    newChunk.DisposeFree();
#pragma warning restore 618
                    return false;
                }

                var prev = _currentBlock;

                position = Log0ItemPosition(newChunk.Pointer, (long)key, out _);

                if (position == null)
                {
                    // We are looking for GT, any larger value will work
                    position = Log0ItemPosition(newChunk.Pointer, (long)newChunk.FirstVersion, out _);
                    // ThrowHelper.FailFast($"We have found GE chunk but cannot find key inside it, that should not happen: key={key}, firstKey={newChunk.FirstVersion}");
                }

                _currentKey = key;
                _currentValue = (StreamLogNotification)ReadPosition(position, waitOnYield: true, out _);

                _currentBlock = newChunk;

                if (prev.IsValid)
                {
#pragma warning disable 618
                    prev.DisposeFree();
#pragma warning restore 618
                }

                return true;
            }

            public void Reset()
            { }

            public ulong CurrentVersion
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Volatile.Read(ref _currentKey);
            }

            public StreamLogNotification Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _currentValue;
            }

            public int MissedQueueLength => _skippedSlots.Count;

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                if (_currentBlock.IsValid)
                {
#pragma warning disable 618
                    _currentBlock.DisposeFree();
#pragma warning restore 618
                }
            }
        }
    }
}
