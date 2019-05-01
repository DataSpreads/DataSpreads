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
using Spreads.Serialization;
using Spreads.Utils;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace DataSpreads.StreamLogs
{
    // TODO review TryPack in dispose. We could just send rotate notification, packing no longer depends on SL instance
    // The state could be non-readonly field and indicate if SL is disposed

    internal partial class StreamLog
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal StreamBlock GetBlockFromRecord(StreamBlockRecord record)
        {
            StreamBlock newActiveBlock;
            // we cannot touch a block when it is stored
            if (record.BufferRef != default)
            {
#pragma warning disable 618
                newActiveBlock = StreamLogManager.BlockIndex.TryRentIndexedStreamBlock(this, record.BufferRef);
#pragma warning restore 618

                if (newActiveBlock.IsValid || record.Version == StreamBlockIndex.ReadyBlockVersion)
                {
                    return newActiveBlock;
                }

                // TODO review why this was needed
                //if (_streamLog.StreamLogManager.BlockIndex.TryGetValue(StreamLogId, record.Version, out record)
                //    && record.BufferRef != default && !record.BufferRef.Flag
                //)
                //{
                //    FailCannotGetChunk(record);
                //}
            }
            // throw new NotImplementedException(); // TODO Storage
            newActiveBlock =
                    StreamLogManager
                    .BlockIndex
                    .BlockStorage
                    .TryGetStreamBlock((long)Slid, record.Version);

            if (newActiveBlock.IsValid)
            {
                return newActiveBlock;
            }

            FailCannotGetBlock();
            return default;

            void FailCannotGetBlock()
            {
                ThrowHelper.FailFast($"Cannot get chunk that should exist: bufferRef {record.BufferRef}, version: {record.Version}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReaderBlockCache.StreamBlockProxy GetBlockProxyFromRecord(StreamBlockRecord record)
        {
            StreamBlock newActiveBlock;
            // we cannot touch a block when it is stored
            if (record.BufferRef != default)
            {




#pragma warning disable 618
                newActiveBlock = StreamLogManager.BlockIndex.TryRentIndexedStreamBlock(this, record.BufferRef);
#pragma warning restore 618

                if (newActiveBlock.IsValid || record.Version == StreamBlockIndex.ReadyBlockVersion)
                {
                    return newActiveBlock;
                }

                // TODO review why this was needed
                //if (_streamLog.StreamLogManager.BlockIndex.TryGetValue(StreamLogId, record.Version, out record)
                //    && record.BufferRef != default && !record.BufferRef.Flag
                //)
                //{
                //    FailCannotGetChunk(record);
                //}
            }
            // throw new NotImplementedException(); // TODO Storage
            newActiveBlock =
                StreamLogManager
                    .BlockIndex
                    .BlockStorage
                    .TryGetStreamBlock((long)Slid, record.Version);

            if (newActiveBlock.IsValid)
            {
                return newActiveBlock;
            }

            FailCannotGetBlock();
            return default;

            void FailCannotGetBlock()
            {
                ThrowHelper.FailFast($"Cannot get chunk that should exist: bufferRef {record.BufferRef}, version: {record.Version}");
            }
        }

        public struct StreamLogCursor : ISpecializedCursor<ulong, DirectBuffer, StreamLogCursor> // TODO , ISpecializedCursor<Timestamp, DirectBuffer, StreamLogCursor>
        {
            // Note: this is special implementation of a container cursor
            // that is different from BaseContainer<TKey> because keys
            // here are not only of the known type ulong but also
            // consecutive - start from 1 and always incremented by 1.
            // We could use this info to simplify and speed up things.
            // Only MoveAt/TryGet for Timestamp are implemented
            // using nested search extracted from BaseContainer.

            // TODO maybe we should store SM here, less risks of missed decrement
            // Also we could use thread pool for prefetch next blocks. With struct
            // there is no object where to store a new block.
            private StreamBlock _currentBlock;      // 16

            internal readonly StreamLog StreamLog;     // 8

            private ulong _currentKey;                  // 8

            // TODO review what this is for. It makes the struct size 40, with 7 bytes unused, bad alignment and 2x32 = one cache line
            private readonly bool _asyncEnumeratorMode; // 1

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StreamLogCursor(StreamLog streamLog, bool asyncEnumeratorMode)
            {
                _currentBlock = default;
                StreamLog = streamLog;
                _currentKey = 0UL;
                _asyncEnumeratorMode = asyncEnumeratorMode;
            }

            private StreamBlockIndex BlockIndex
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => StreamLog.BlockIndex;
            }

            public StreamLogId StreamLogId
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => StreamLog.Slid;
            }

            public ref DataTypeHeader ItemDth
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => ref _currentBlock.ItemDth;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                ThrowHelper.ThrowNotSupportedException();
                return default;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                if (_currentKey == 0)
                {
                    return MoveFirst();
                }

                // Cursors are not thread safe, _currentChunk is set if _currentKey == 0 in MoveFirst
                if (_currentBlock.CurrentVersion > _currentKey)
                {
                    _currentKey = _currentKey + 1;
                    return true;
                }

                // we could have reached the end but chunk is not filled
                // this property is set during rotate, before it is set
                // we cannot change chunks
                if (!_currentBlock.IsCompleted)
                {
                    return false;
                }

                return MoveAt(_currentKey + 1, Lookup.EQ);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                _currentKey = 0UL;
                if (_currentBlock.IsValid)
                {
#pragma warning disable 618
                    _currentBlock.DisposeFree();
#pragma warning restore 618
                }
                _currentBlock = default;
            }

            public KeyValuePair<ulong, DirectBuffer> Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new KeyValuePair<ulong, DirectBuffer>(_currentKey, CurrentValue);
            }

            object IEnumerator.Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Current;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
                Reset();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync()
            {
                Reset();
                return new ValueTask(Task.CompletedTask);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveAt(ulong key, Lookup direction)
            {
                // keys are sequential
                if (key == 0)
                {
                    if (direction == Lookup.GE || direction == Lookup.GT)
                    {
                        key = 1;
                        direction = Lookup.EQ;
                    }
                    else
                    {
                        return false;
                    }
                }

                // reduce direction options
                if (direction == Lookup.LT)
                {
                    key--;
                    direction = Lookup.LE;
                }

                if (direction == Lookup.GT)
                {
                    if (key > ulong.MaxValue - 2)
                    {
                        return false;
                    }
                    key++;
                    direction = Lookup.GE;
                }

                if (_currentBlock.IsValid && key >= _currentBlock.FirstVersion && key <= _currentBlock.CurrentVersion)
                {
                    _currentKey = key;
                    return true;
                }

                Debug.Assert(direction != Lookup.GT && direction != Lookup.LT);

                var prev = _currentBlock;
                StreamBlock newBlock = default;

                var activeBlockVersion = StreamLog.State.GetActiveBlockVersion();

                // our search always includes equality so if this condition is true we will always find the correct block
                // by GE search because:
                // 1) activeBlockVersion exists as the index key, it may be temporarily stored as ReadyBlockVersion
                // 2) if a new chunk is added G*E* will find the equal one, if the chunk is ready, *G*E will find it and another equal could not exist
                if (activeBlockVersion > 0 && key >= activeBlockVersion)
                {
                    if (!BlockIndex.TryFindAt(StreamLogId, activeBlockVersion, Lookup.GE, out var record))
                    {
                        ThrowHelper.FailFast("Cannot get active chunk than must exist");
                    }

                    // active block must be valid
                    newBlock = StreamLog.GetBlockFromRecord(record);

                    if (!newBlock.IsValid || key < newBlock.FirstVersion)
                    {
                        ThrowHelper.FailFast("!newBlock.IsValid");
                    }

                    // only for LE we could lookup previous chunk
                    if (newBlock.CountVolatile == 0 && direction != Lookup.LE)
                    {
#pragma warning disable CS0618 // Type or member is obsolete
                        newBlock.DisposeFree();
#pragma warning restore CS0618 // Type or member is obsolete
                        return false;
                    }
                }

                if (!newBlock.IsValid)
                {
                    if (BlockIndex.TryFindAt(StreamLogId, key, Lookup.LE,
                        out var record))
                    {
                        newBlock = StreamLog.GetBlockFromRecord(record);
                    }
                }

                if (newBlock.IsValid && key >= newBlock.FirstVersion)
                {
                    var found = false;
                    if (key <= newBlock.CurrentVersion)
                    {
                        found = true;
                        _currentKey = key;
                    }
                    else if (direction == Lookup.LE)
                    {
                        found = true;
                        _currentKey = newBlock.CurrentVersion;
                    }

                    if (found)
                    {
                        _currentBlock = newBlock;
                        if (prev.IsValid)
                        {
#pragma warning disable 618
                            prev.DisposeFree();
#pragma warning restore 618
                        }

                        return true;
                    }
                }

#pragma warning disable 618
                newBlock.DisposeFree();
#pragma warning restore 618

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveAt(Timestamp key, Lookup direction)
            {
                // TODO MA with TS should check current block before hitting the index
                if (NestedLookupHelper.TryFindBlockAt(ref key, direction, out var sbwts, out int blockIndex, StreamLog))
                {
                    _currentBlock = sbwts.Block;
                    _currentKey = sbwts.Block.FirstVersion + (ulong)blockIndex;
                    return true;
                }

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveFirst()
            {
                return MoveAt(0, Lookup.GT);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveLast()
            {
                return MoveAt(StreamBlockIndex.ReadyBlockVersion - 1, Lookup.LE);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long MoveNext(long stride, bool allowPartial)
            {
                ThrowHelper.ThrowNotImplementedException();
                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MovePrevious()
            {
                if (_currentKey > 1)
                {
                    return MoveAt(_currentKey - 1, Lookup.EQ);
                }

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long MovePrevious(long stride, bool allowPartial)
            {
                ThrowHelper.ThrowNotImplementedException();
                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Task<bool> MoveNextBatch()
            {
                // TODO here is the IO!
                return TaskUtil.FalseTask;
            }

            public bool IsIndexed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public bool IsCompleted
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => StreamLog.IsCompleted;
            }

            public IAsyncCompleter AsyncCompleter
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => StreamLog;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StreamLogCursor Initialize()
            {
                return new StreamLogCursor(StreamLog, _asyncEnumeratorMode);
            }

            public StreamLogCursor Clone()
            {
                var c = this;
                c._currentBlock = default;
                return c;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ICursor<ulong, DirectBuffer> ICursor<ulong, DirectBuffer>.Clone()
            {
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
                return Clone();
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryGetValue(ulong key, out DirectBuffer value)
            {
                var c = this;
                var moved = c.MoveAt(key, Lookup.EQ);
                if (moved)
                {
                    value = c.CurrentValue;
                    return true;
                }
                value = DirectBuffer.Invalid;
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryGetValue(Timestamp key, out DirectBuffer value)
            {
                // TODO MA with TS should check current block before hitting the index
                var c = this;
                var moved = c.MoveAt(key, Lookup.EQ);
                if (moved)
                {
                    value = c.CurrentValue;
                    return true;
                }
                value = DirectBuffer.Invalid;
                return false;
            }

            public CursorState State
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _currentKey == 0 ? CursorState.Initialized : CursorState.Moving;
            }

            public KeyComparer<ulong> Comparer
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => KeyComparer<ulong>.Default;
            }

            public ulong CurrentKey
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _currentKey;
            }

            public DirectBuffer CurrentValue
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    Debug.Assert(_currentKey >= _currentBlock.FirstVersion);
                    return _currentBlock.DangerousGet(checked((int)(_currentKey - _currentBlock.FirstVersion)));
                }
            }

            public ISeries<ulong, DirectBuffer> CurrentBatch
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => default;
            }

            ISeries<ulong, DirectBuffer> ICursor<ulong, DirectBuffer>.Source
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Source;
            }

            Series<ulong, DirectBuffer, StreamLogCursor> ISpecializedCursor<ulong, DirectBuffer, StreamLogCursor>.Source
            {
                [Pure]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Source;
            }

            public Series<ulong, DirectBuffer, StreamLogCursor> Source
            {
                [Pure]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new Series<ulong, DirectBuffer, StreamLogCursor>(this);
            }

            public bool IsContinuous
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            /// <summary>
            /// Could merge DirectBuffers after using this method because they are in the same chunk and adjacent.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool MoveNextWithinChunk()
            {
                if (_currentBlock.CurrentVersion > _currentKey)
                {
                    _currentKey = _currentKey + 1;
                    return true;
                }
                return false;
            }

            //[MethodImpl(MethodImplOptions.AggressiveInlining)]
            //internal bool ConsumeNextWithinChunk(int lengthLimit, out RetainedMemory<byte> dataItems)
            //{
            //    if (_currentChunk.CurrentVersion > _currentKey)
            //    {
            //        _currentKey = _currentKey + 1;

            //        // Offsets are from buffer start

            //        var offset = _currentChunk.Offset;
            //        var idx = checked((int)(_currentKey - _currentChunk.FirstVersion));
            //        var currentOffset = _currentChunk.GetNthOffset(idx);
            //        if (offset - currentOffset <= lengthLimit)
            //        {
            //            _currentChunk.
            //        }
            //        return true;
            //    }
            //    return false;
            //}

            /// <summary>
            /// Call this method only after MoveNextWithinChunk() returned true. Together they allow for
            /// kind of Peek() operation but move back should be less frequent.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool MovePreviousWithinChunk()
            {
                _currentKey = _currentKey - 1;
                Debug.Assert(_currentKey >= _currentBlock.FirstVersion);
                return true;
            }

            // TODO copy directly to WAL head, must be given with DirectBuffer where to write
            // return codes:
            // positive int - written so many values
            // negative int - cannot write even a single value, need (-result) size
            // zero - cannot move next (possible if we eagerly consume values on previous move)
            // This method moves the cursor!

            ///// <summary>
            ///// Produce RequestAppendDataToStream
            ///// </summary>
            ///// <param name="maxSizeSoftLimit"></param>
            ///// <param name="dto"></param>
            ///// <returns></returns>
            //internal bool MoveNextDto(int maxSizeSoftLimit, out RequestAppendDataToStream dto)
            //{
            //    throw new NotImplementedException();

            //    //maxSizeSoftLimit = maxSizeSoftLimit - RequestAppendDataToStream.FixedFieldsSize;

            //    //dto = default;
            //    //// MN changes active chunk as needed
            //    //if (!MoveNext())
            //    //{
            //    //    return false;
            //    //}

            //    //// we moved next and always consuming the current value

            //    //int length;

            //    //var currentChunkIndex = checked((int)(_currentKey - _currentChunk.FirstVersion));

            //    //var currentOffset = _currentChunk.GetNthOffset(currentChunkIndex);

            //    //var count = 1;

            //    //// always exists, we are at an existing key
            //    //int nextOffset = _currentChunk.GetNthOffset(currentChunkIndex + count);

            //    //var maxCount = checked((int)(_currentChunk.CurrentVersion - _currentKey + 1));

            //    //// TODO refactor, test for off by one errors
            //    //while (true)
            //    //{
            //    //    if (nextOffset - currentOffset > maxSizeSoftLimit)
            //    //    {
            //    //        count--;
            //    //        nextOffset = _currentChunk.GetNthOffset(currentChunkIndex + count);
            //    //        break;
            //    //    }
            //    //    if (count == maxCount)
            //    //    {
            //    //        break;
            //    //    }
            //    //    count++;
            //    //    nextOffset = _currentChunk.GetNthOffset(currentChunkIndex + count);
            //    //}

            //    //length = nextOffset - currentOffset;

            //    //var dataItems = _currentChunk.RetainedMemory.Retain(currentOffset, length);

            //    //dto = new RequestAppendDataToStream(this._streamLog.StreamId, _currentKey, count, dataItems);
            //    //// NB after creating dto
            //    //_currentKey += checked((ulong)(count - 1));

            //    //return true;
            //}
        }

        public struct StreamLogCursor2 : ISpecializedCursor<ulong, DirectBuffer, StreamLogCursor2> // TODO , ISpecializedCursor<Timestamp, DirectBuffer, StreamLogCursor>
        {
            // Note: this is special implementation of a container cursor
            // that is different from BaseContainer<TKey> because keys
            // here are not only of the known type ulong but also
            // consecutive - start from 1 and always incremented by 1.
            // We could use this info to simplify and speed up things.
            // Only MoveAt/TryGet for Timestamp are implemented
            // using nested search extracted from BaseContainer.

            // TODO maybe we should store SM here, less risks of missed decrement
            // Also we could use thread pool for prefetch next blocks. With struct
            // there is no object where to store a new block.
            private ReaderBlockCache.StreamBlockProxy _currentBlockProxy;      // 16

            internal readonly StreamLog StreamLog;     // 8

            private ulong _currentKey;                  // 8

            // TODO review what this is for. It makes the struct size 40, with 7 bytes unused, bad alignment and 2x32 = one cache line
            private readonly bool _asyncEnumeratorMode; // 1

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StreamLogCursor2(StreamLog streamLog, bool asyncEnumeratorMode)
            {
                _currentBlockProxy = default;
                StreamLog = streamLog;
                _currentKey = 0UL;
                _asyncEnumeratorMode = asyncEnumeratorMode;
            }

            private StreamBlockIndex BlockIndex
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => StreamLog.BlockIndex;
            }

            public StreamLogId StreamLogId
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => StreamLog.Slid;
            }

            public ref DataTypeHeader ItemDth
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => ref _currentBlockProxy.Block.ItemDth;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                ThrowHelper.ThrowNotSupportedException();
                return default;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                if (_currentKey == 0)
                {
                    return MoveFirst();
                }

                // Cursors are not thread safe, _currentChunk is set if _currentKey == 0 in MoveFirst
                if (_currentBlockProxy.Block.CurrentVersion > _currentKey)
                {
                    _currentKey = _currentKey + 1;
                    return true;
                }

                // we could have reached the end but chunk is not filled
                // this property is set during rotate, before it is set
                // we cannot change chunks
                if (!_currentBlockProxy.Block.IsCompleted)
                {
                    return false;
                }

                return MoveAt(_currentKey + 1, Lookup.EQ);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                _currentKey = 0UL;
                if (_currentBlockProxy.Block.IsValid)
                {
#pragma warning disable 618
                    _currentBlockProxy.DisposeFree();
#pragma warning restore 618
                }
                _currentBlockProxy = default;
            }

            public KeyValuePair<ulong, DirectBuffer> Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new KeyValuePair<ulong, DirectBuffer>(_currentKey, CurrentValue);
            }

            object IEnumerator.Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Current;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
                Reset();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync()
            {
                Reset();
                return new ValueTask(Task.CompletedTask);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveAt(ulong key, Lookup direction)
            {
                // keys are sequential
                if (key == 0)
                {
                    if (direction == Lookup.GE || direction == Lookup.GT)
                    {
                        key = 1;
                        direction = Lookup.EQ;
                    }
                    else
                    {
                        return false;
                    }
                }

                // reduce direction options
                if (direction == Lookup.LT)
                {
                    key--;
                    direction = Lookup.LE;
                }

                if (direction == Lookup.GT)
                {
                    if (key > ulong.MaxValue - 2)
                    {
                        return false;
                    }
                    key++;
                    direction = Lookup.GE;
                }

                if (_currentBlockProxy.IsValid && key >= _currentBlockProxy.FirstVersion && key <= _currentBlockProxy.CurrentVersion)
                {
                    _currentKey = key;
                    return true;
                }

                Debug.Assert(direction != Lookup.GT && direction != Lookup.LT);

                var prev = _currentBlockProxy;
                StreamBlock newBlock = default;

                var activeBlockVersion = StreamLog.State.GetActiveBlockVersion();

                // our search always includes equality so if this condition is true we will always find the correct block
                // by GE search because:
                // 1) activeBlockVersion exists as the index key, it may be temporarily stored as ReadyBlockVersion
                // 2) if a new chunk is added G*E* will find the equal one, if the chunk is ready, *G*E will find it and another equal could not exist
                if (activeBlockVersion > 0 && key >= activeBlockVersion)
                {
                    if (!BlockIndex.TryFindAt(StreamLogId, activeBlockVersion, Lookup.GE, out var record))
                    {
                        ThrowHelper.FailFast("Cannot get active chunk than must exist");
                    }

                    // active block must be valid
                    newBlock = StreamLog.GetBlockFromRecord(record);

                    if (!newBlock.IsValid || key < newBlock.FirstVersion)
                    {
                        ThrowHelper.FailFast("!newBlock.IsValid");
                    }

                    // only for LE we could lookup previous chunk
                    if (newBlock.CountVolatile == 0 && direction != Lookup.LE)
                    {
#pragma warning disable CS0618 // Type or member is obsolete
                        newBlock.DisposeFree();
#pragma warning restore CS0618 // Type or member is obsolete
                        return false;
                    }
                }

                if (!newBlock.IsValid)
                {
                    if (BlockIndex.TryFindAt(StreamLogId, key, Lookup.LE,
                        out var record))
                    {
                        newBlock = StreamLog.GetBlockFromRecord(record);
                    }
                }

                if (newBlock.IsValid && key >= newBlock.FirstVersion)
                {
                    var found = false;
                    if (key <= newBlock.CurrentVersion)
                    {
                        found = true;
                        _currentKey = key;
                    }
                    else if (direction == Lookup.LE)
                    {
                        found = true;
                        _currentKey = newBlock.CurrentVersion;
                    }

                    if (found)
                    {
                        _currentBlockProxy = newBlock;
                        if (prev.IsValid)
                        {
#pragma warning disable 618
                            prev.DisposeFree();
#pragma warning restore 618
                        }

                        return true;
                    }
                }

#pragma warning disable 618
                newBlock.DisposeFree();
#pragma warning restore 618

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveAt(Timestamp key, Lookup direction)
            {
                // TODO MA with TS should check current block before hitting the index
                if (NestedLookupHelper.TryFindBlockAt(ref key, direction, out var sbwts, out int blockIndex, StreamLog))
                {
                    _currentBlockProxy = sbwts.Block;
                    _currentKey = sbwts.Block.FirstVersion + (ulong)blockIndex;
                    return true;
                }

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveFirst()
            {
                return MoveAt(0, Lookup.GT);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveLast()
            {
                return MoveAt(StreamBlockIndex.ReadyBlockVersion - 1, Lookup.LE);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long MoveNext(long stride, bool allowPartial)
            {
                ThrowHelper.ThrowNotImplementedException();
                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MovePrevious()
            {
                if (_currentKey > 1)
                {
                    return MoveAt(_currentKey - 1, Lookup.EQ);
                }

                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long MovePrevious(long stride, bool allowPartial)
            {
                ThrowHelper.ThrowNotImplementedException();
                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Task<bool> MoveNextBatch()
            {
                // TODO here is the IO!
                return TaskUtil.FalseTask;
            }

            public bool IsIndexed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public bool IsCompleted
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => StreamLog.IsCompleted;
            }

            public IAsyncCompleter AsyncCompleter
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => StreamLog;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StreamLogCursor2 Initialize()
            {
                return new StreamLogCursor2(StreamLog, _asyncEnumeratorMode);
            }

            public StreamLogCursor2 Clone()
            {
                var c = this;
                c._currentBlockProxy = default;
                return c;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ICursor<ulong, DirectBuffer> ICursor<ulong, DirectBuffer>.Clone()
            {
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
                return Clone();
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryGetValue(ulong key, out DirectBuffer value)
            {
                var c = this;
                var moved = c.MoveAt(key, Lookup.EQ);
                if (moved)
                {
                    value = c.CurrentValue;
                    return true;
                }
                value = DirectBuffer.Invalid;
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryGetValue(Timestamp key, out DirectBuffer value)
            {
                // TODO MA with TS should check current block before hitting the index
                var c = this;
                var moved = c.MoveAt(key, Lookup.EQ);
                if (moved)
                {
                    value = c.CurrentValue;
                    return true;
                }
                value = DirectBuffer.Invalid;
                return false;
            }

            public CursorState State
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _currentKey == 0 ? CursorState.Initialized : CursorState.Moving;
            }

            public KeyComparer<ulong> Comparer
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => KeyComparer<ulong>.Default;
            }

            public ulong CurrentKey
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _currentKey;
            }

            public DirectBuffer CurrentValue
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    Debug.Assert(_currentKey >= _currentBlockProxy.FirstVersion);
                    return _currentBlockProxy.DangerousGet(checked((int)(_currentKey - _currentBlockProxy.FirstVersion)));
                }
            }

            public ISeries<ulong, DirectBuffer> CurrentBatch
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => default;
            }

            ISeries<ulong, DirectBuffer> ICursor<ulong, DirectBuffer>.Source
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Source;
            }

            Series<ulong, DirectBuffer, StreamLogCursor2> ISpecializedCursor<ulong, DirectBuffer, StreamLogCursor2>.Source
            {
                [Pure]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Source;
            }

            public Series<ulong, DirectBuffer, StreamLogCursor2> Source
            {
                [Pure]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new Series<ulong, DirectBuffer, StreamLogCursor2>(this);
            }

            public bool IsContinuous
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            /// <summary>
            /// Could merge DirectBuffers after using this method because they are in the same chunk and adjacent.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool MoveNextWithinChunk()
            {
                if (_currentBlockProxy.CurrentVersion > _currentKey)
                {
                    _currentKey = _currentKey + 1;
                    return true;
                }
                return false;
            }



            /// <summary>
            /// Call this method only after MoveNextWithinChunk() returned true. Together they allow for
            /// kind of Peek() operation but move back should be less frequent.
            /// </summary>
            /// <returns></returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal bool MovePreviousWithinChunk()
            {
                _currentKey = _currentKey - 1;
                Debug.Assert(_currentKey >= _currentBlockProxy.FirstVersion);
                return true;
            }
        }
    }
}
