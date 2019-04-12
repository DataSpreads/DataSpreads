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
using Spreads.DataTypes;
using Spreads.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace DataSpreads.StreamLogs
{
    internal partial class StreamBlockIndex
    {
        // TODO These methods are not used, review & delete if they are not needed

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamBlockRecordCursor GetBlockRecordCursor(StreamLogId streamId)
        {
            return new StreamBlockRecordCursor(this, streamId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamBlockRecordTimestampCursor GetBlockRecordTimestampCursor(StreamLogId streamId)
        {
            return new StreamBlockRecordTimestampCursor(this, streamId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Series<ulong, StreamBlockRecord, StreamBlockRecordCursor> GetBlockRecordSeries(StreamLogId streamId)
        {
            return GetBlockRecordCursor(streamId).Source;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Series<Timestamp, StreamBlockRecord, StreamBlockRecordTimestampCursor> GetBlockRecordTimeSeries(StreamLogId streamId)
        {
            return GetBlockRecordTimestampCursor(streamId).Source;
        }

        public struct StreamBlockRecordCursor : ISpecializedCursor<ulong, StreamBlockRecord, StreamBlockRecordCursor>
        {
            private readonly StreamBlockIndex _blockIndex;
            private readonly StreamLogId _slid;
            private ulong _cursorCurrentKey;
            private StreamBlockRecord _chunkCursorCurrent;
            private CursorState _chunkCursorState;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StreamBlockRecordCursor(StreamBlockIndex blockIndex, StreamLogId slid)
            {
                _blockIndex = blockIndex;
                _slid = slid;
                _chunkCursorState = CursorState.Initialized;
                _cursorCurrentKey = default;
                _chunkCursorCurrent = default;
            }

            public void Dispose()
            {
                // noop
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Task DisposeAsync()
            {
                return Task.CompletedTask;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                _cursorCurrentKey = default;
                _chunkCursorCurrent = default;
                _chunkCursorState = CursorState.None;
            }

            public StreamBlockRecordCursor Initialize()
            {
                return new StreamBlockRecordCursor(_blockIndex, _slid);
            }

            public CursorState State
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _chunkCursorState;
            }

            public KeyComparer<ulong> Comparer
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => KeyComparer<ulong>.Default;
            }

            public ulong CurrentKey
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _cursorCurrentKey;
            }

            public StreamBlockRecord CurrentValue
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _chunkCursorCurrent;
            }

            public ISeries<ulong, StreamBlockRecord> CurrentBatch
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => null;
            }

            public KeyValuePair<ulong, StreamBlockRecord> Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new KeyValuePair<ulong, StreamBlockRecord>(CurrentKey, CurrentValue);
            }

            object IEnumerator.Current => Current;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveFirst()
            {
                if (_blockIndex.TryGetFirst(_slid, out var value))
                {
                    if (value.Version < ReadyBlockVersion)
                    {
                        _chunkCursorState = CursorState.Moving;
                        _cursorCurrentKey = value.Version;
                        _chunkCursorCurrent = value;
                        return true;
                    }
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                if (_chunkCursorState < CursorState.Moving) return MoveFirst();

                if (_blockIndex.TryGetNext(_slid, _cursorCurrentKey, out var value))
                {
                    if (value.Version < ReadyBlockVersion)
                    {
                        _chunkCursorState = CursorState.Moving;
                        _cursorCurrentKey = value.Version;
                        _chunkCursorCurrent = value;
                        return true;
                    }
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long MoveNext(long stride, bool allowPartial)
            {
                throw new NotImplementedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveAt(ulong key, Lookup direction)
            {
                if (_blockIndex.TryFindAt(_slid, key, direction, out var value))
                {
                    if (value.Version < ReadyBlockVersion)
                    {
                        _cursorCurrentKey = value.Version;
                        _chunkCursorCurrent = value;
                        _chunkCursorState = CursorState.Moving;
                        return true;
                    }
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MovePrevious()
            {
                if (State < CursorState.Moving) return MoveLast();

                if (_blockIndex.TryGetPrevious(_slid, _cursorCurrentKey, out var value))
                {
                    _chunkCursorState = CursorState.Moving;
                    _cursorCurrentKey = value.Version;
                    _chunkCursorCurrent = value;
                    return true;
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveLast()
            {
                if (_blockIndex.TryGetLast(_slid, true, out var value))
                {
                    _chunkCursorState = CursorState.Moving;
                    _cursorCurrentKey = value.Version;
                    _chunkCursorCurrent = value;
                    return true;
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long MovePrevious(long stride, bool allowPartial)
            {
                throw new NotImplementedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Task<bool> MoveNextBatch()
            {
                return TaskUtil.FalseTask;
            }

            public StreamBlockRecordCursor Clone()
            {
                var clone = new StreamBlockRecordCursor(_blockIndex, _slid)
                {
                    _cursorCurrentKey = _cursorCurrentKey,
                    _chunkCursorCurrent = _chunkCursorCurrent,
                    _chunkCursorState = _chunkCursorState
                };
                return clone;
            }

            public bool IsIndexed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public bool IsCompleted
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _blockIndex.GetIsCompleted(_slid);
            }

            public IAsyncCompleter AsyncCompleter
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => throw new NotSupportedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask<bool> MoveNextAsync()
            {
                ThrowHelper.ThrowNotSupportedException();
                return default;
            }

            ICursor<ulong, StreamBlockRecord> ICursor<ulong, StreamBlockRecord>.Clone()
            {
                return Clone();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryGetValue(ulong key, out StreamBlockRecord value)
            {
                return _blockIndex.TryGetValue(_slid, key, out value);
            }

            ISeries<ulong, StreamBlockRecord> ICursor<ulong, StreamBlockRecord>.Source => Source;

            public Series<ulong, StreamBlockRecord, StreamBlockRecordCursor> Source
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new Series<ulong, StreamBlockRecord, StreamBlockRecordCursor>(this);
            }

            public bool IsContinuous
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }
        }

        public struct StreamBlockRecordTimestampCursor : ISpecializedCursor<Timestamp, StreamBlockRecord, StreamBlockRecordTimestampCursor>
        {
            private readonly StreamBlockIndex _blockIndex;
            private readonly StreamLogId _slid;
            private Timestamp _currentKey;
            private StreamBlockRecord _currentValue;
            private CursorState _chunkCursorState;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StreamBlockRecordTimestampCursor(StreamBlockIndex blockIndex, StreamLogId slid)
            {
                _blockIndex = blockIndex;
                _slid = slid;
                _chunkCursorState = CursorState.Initialized;
                _currentKey = default;
                _currentValue = default;
            }

            public void Dispose()
            {
                // noop
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Task DisposeAsync()
            {
                return Task.CompletedTask;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                _currentKey = default;
                _currentValue = default;
                _chunkCursorState = CursorState.None;
            }

            public StreamBlockRecordTimestampCursor Initialize()
            {
                return new StreamBlockRecordTimestampCursor(_blockIndex, _slid);
            }

            public CursorState State
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _chunkCursorState;
            }

            public KeyComparer<Timestamp> Comparer
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => KeyComparer<Timestamp>.Default;
            }

            public Timestamp CurrentKey
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _currentKey;
            }

            public StreamBlockRecord CurrentValue
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _currentValue;
            }

            public ISeries<Timestamp, StreamBlockRecord> CurrentBatch
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => null;
            }

            public KeyValuePair<Timestamp, StreamBlockRecord> Current
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new KeyValuePair<Timestamp, StreamBlockRecord>(CurrentKey, CurrentValue);
            }

            object IEnumerator.Current => Current;

            // TODO
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveFirst()
            {
                if (_blockIndex.TryGetFirst(_slid, out var value))
                {
                    if (value.Version < ReadyBlockVersion)
                    {
                        _chunkCursorState = CursorState.Moving;
                        _currentKey = value.Timestamp;
                        _currentValue = value;
                        return true;
                    }
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveNext()
            {
                if (_chunkCursorState < CursorState.Moving) return MoveFirst();

                if (_blockIndex.TryGetNext(_slid, _currentKey, out var value))
                {
                    if (value.Version < ReadyBlockVersion)
                    {
                        _chunkCursorState = CursorState.Moving;
                        _currentKey = value.Timestamp;
                        _currentValue = value;
                        return true;
                    }
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long MoveNext(long stride, bool allowPartial)
            {
                throw new NotImplementedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveAt(Timestamp key, Lookup direction)
            {
                if (_blockIndex.TryFindAt(_slid, key, direction, out var value))
                {
                    if (value.Version < ReadyBlockVersion)
                    {
                        _currentKey = value.Timestamp;
                        _currentValue = value;
                        _chunkCursorState = CursorState.Moving;
                        return true;
                    }
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MovePrevious()
            {
                if (State < CursorState.Moving) return MoveLast();

                if (_blockIndex.TryGetPrevious(_slid, _currentKey, out var value))
                {
                    _chunkCursorState = CursorState.Moving;
                    _currentKey = value.Timestamp;
                    _currentValue = value;
                    return true;
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool MoveLast()
            {
                if (_blockIndex.TryGetLast(_slid, true, out var value))
                {
                    _chunkCursorState = CursorState.Moving;
                    _currentKey = value.Timestamp;
                    _currentValue = value;
                    return true;
                }
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long MovePrevious(long stride, bool allowPartial)
            {
                throw new NotImplementedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public Task<bool> MoveNextBatch()
            {
                return TaskUtil.FalseTask;
            }

            public StreamBlockRecordTimestampCursor Clone()
            {
                var clone = new StreamBlockRecordTimestampCursor(_blockIndex, _slid)
                {
                    _currentKey = _currentKey,
                    _currentValue = _currentValue,
                    _chunkCursorState = _chunkCursorState
                };
                return clone;
            }

            public bool IsIndexed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }

            public bool IsCompleted
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _blockIndex.GetIsCompleted(_slid);
            }

            public IAsyncCompleter AsyncCompleter
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => throw new NotSupportedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask<bool> MoveNextAsync()
            {
                ThrowHelper.ThrowNotSupportedException();
                return default;
            }

            ICursor<Timestamp, StreamBlockRecord> ICursor<Timestamp, StreamBlockRecord>.Clone()
            {
                return Clone();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryGetValue(Timestamp key, out StreamBlockRecord value)
            {
                return _blockIndex.TryGetValue(_slid, key, out value);
            }

            ISeries<Timestamp, StreamBlockRecord> ICursor<Timestamp, StreamBlockRecord>.Source => Source;

            public Series<Timestamp, StreamBlockRecord, StreamBlockRecordTimestampCursor> Source
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => new Series<Timestamp, StreamBlockRecord, StreamBlockRecordTimestampCursor>(this);
            }

            public bool IsContinuous
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => false;
            }
        }
    }
}
