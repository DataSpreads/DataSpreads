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
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace DataSpreads
{
    // TODO finalize/dispose

    public struct TimeStreamCursor<T> : ICursor<Timestamp, T, TimeStreamCursor<T>>
    {
        internal StreamLog.StreamLogCursor Inner;
        private (bool, Timestamped<T>) _currentValue;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TimeStreamCursor(StreamLog.StreamLogCursor inner)
        {
            Inner = inner;
            _currentValue = default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Timestamped<T> Deserialize(DirectBuffer db)
        {
            Timestamp timestamp = default;
            Debug.Assert(!Inner.StreamLog.NoTimestamp);

            timestamp = db.Read<Timestamp>(0);
            db = db.Slice(Timestamp.Size);

            var len = BinarySerializer.Read<T>(Inner.ItemDth, db, out var value);
            if (len <= 0)
            {
                ThrowCannotDeserialize();
            }
            return new Timestamped<T>(timestamp, value);

            void ThrowCannotDeserialize()
            {
                throw new InvalidDataException("Cannot deserialize data");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> MoveNextAsync()
        {
            _currentValue = default;
            return Inner.MoveNextAsync();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            _currentValue = default;
            return Inner.MoveNext();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            _currentValue = default;
            Inner.Reset();
        }

        KeyValuePair<Timestamp, T> IEnumerator<KeyValuePair<Timestamp, T>>.Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new KeyValuePair<Timestamp, T>(CurrentKey, CurrentValue);
        }

        public KeyValuePair<Timestamp, T> Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!_currentValue.Item1)
                {
                    _currentValue = (true, Deserialize(Inner.CurrentValue));
                }
                return new KeyValuePair<Timestamp, T>(_currentValue.Item2.Timestamp, _currentValue.Item2.Value);
            }
        }

        object IEnumerator.Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
            get => ((IEnumerator)Inner).Current;
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            _currentValue = default;
            Inner.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            _currentValue = default;
            return Inner.DisposeAsync();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveAt(Timestamp key, Lookup direction)
        {
            _currentValue = default;
            return Inner.MoveAt(key, direction);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveFirst()
        {
            _currentValue = default;
            return Inner.MoveFirst();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveLast()
        {
            _currentValue = default;
            return Inner.MoveLast();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long MoveNext(long stride, bool allowPartial)
        {
            _currentValue = default;
            return Inner.MoveNext(stride, allowPartial);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MovePrevious()
        {
            _currentValue = default;
            return Inner.MovePrevious();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long MovePrevious(long stride, bool allowPartial)
        {
            _currentValue = default;
            return Inner.MovePrevious(stride, allowPartial);
        }

        public bool IsIndexed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.IsIndexed;
        }

        public bool IsCompleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.IsCompleted;
        }

        public IAsyncCompleter AsyncCompleter
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.AsyncCompleter;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TimeStreamCursor<T> Initialize()
        {
            return new TimeStreamCursor<T>(Inner);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TimeStreamCursor<T> Clone()
        {
            return new TimeStreamCursor<T>(Inner.Clone());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ICursor<Timestamp, T> ICursor<Timestamp, T>.Clone()
        {
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
            return Clone();
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(Timestamp key, out T value)
        {
            if (Inner.TryGetValue(key, out var value1))
            {
                value = Deserialize(value1);
                return true;
            }

            value = default;
            return false;
        }

        public CursorState State
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.State;
        }

        public KeyComparer<Timestamp> Comparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => KeyComparer<Timestamp>.Default;
        }

        public Timestamp CurrentKey
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Current.Key;
        }

        public T CurrentValue
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Current.Value;
        }

        ISeries<Timestamp, T> ICursor<Timestamp, T>.Source
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
            get => Source;
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
        }

        public Series<Timestamp, T, TimeStreamCursor<T>> Source
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new Series<Timestamp, T, TimeStreamCursor<T>>(this);
        }

        public bool IsContinuous
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.IsContinuous;
        }
    }

    public struct DataStreamCursor<T> : ICursor<ulong, Timestamped<T>, DataStreamCursor<T>>
    {
        internal StreamLog.StreamLogCursor Inner;

        private (bool, Timestamped<T>) _currentValue;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DataStreamCursor(StreamLog.StreamLogCursor inner)
        {
            Inner = inner; // TODO WTF is async batch mode? need to use it without bool
            _currentValue = default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Timestamped<T> Deserialize(DirectBuffer db)
        {
            Timestamp timestamp = default;
            // TODO should cache this value directly in _inner
            if (!Inner.StreamLog.NoTimestamp)
            {
                timestamp = db.Read<Timestamp>(0);
                db = db.Slice(Timestamp.Size);
            }

            var len = BinarySerializer.Read<T>(Inner.ItemDth, db, out var value);
            if (len <= 0)
            {
                ThrowCannotDeserialize();
            }
            return new Timestamped<T>(timestamp, value);

            void ThrowCannotDeserialize()
            {
                throw new InvalidDataException("Cannot deserialize data");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<bool> MoveNextAsync()
        {
            _currentValue = default;
            return Inner.MoveNextAsync();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            _currentValue = default;
            return Inner.MoveNext();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            _currentValue = default;
            Inner.Reset();
        }

        KeyValuePair<ulong, Timestamped<T>> IEnumerator<KeyValuePair<ulong, Timestamped<T>>>.Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new KeyValuePair<ulong, Timestamped<T>>(CurrentKey, CurrentValue);
        }

        public KeyValuePair<ulong, Timestamped<T>> Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new KeyValuePair<ulong, Timestamped<T>>(Inner.CurrentKey, CurrentValue);
        }

        object IEnumerator.Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
            get => ((IEnumerator)Inner).Current;
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            _currentValue = default;
            Inner.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync()
        {
            _currentValue = default;
            return Inner.DisposeAsync();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveAt(ulong key, Lookup direction)
        {
            _currentValue = default;
            return Inner.MoveAt(key, direction);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveFirst()
        {
            _currentValue = default;
            return Inner.MoveFirst();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveLast()
        {
            _currentValue = default;
            return Inner.MoveLast();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long MoveNext(long stride, bool allowPartial)
        {
            _currentValue = default;
            return Inner.MoveNext(stride, allowPartial);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MovePrevious()
        {
            _currentValue = default;
            return Inner.MovePrevious();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long MovePrevious(long stride, bool allowPartial)
        {
            _currentValue = default;
            return Inner.MovePrevious(stride, allowPartial);
        }

        public bool IsIndexed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.IsIndexed;
        }

        public bool IsCompleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.IsCompleted;
        }

        public IAsyncCompleter AsyncCompleter
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.AsyncCompleter;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DataStreamCursor<T> Initialize()
        {
            return new DataStreamCursor<T>(Inner);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DataStreamCursor<T> Clone()
        {
            return new DataStreamCursor<T>(Inner.Clone());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ICursor<ulong, Timestamped<T>> ICursor<ulong, Timestamped<T>>.Clone()
        {
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
            return Clone();
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(ulong key, out Timestamped<T> value)
        {
            if (Inner.TryGetValue(key, out var value1))
            {
                value = Deserialize(value1);
                return true;
            }

            value = default;
            return false;
        }

        public CursorState State
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.State;
        }

        public KeyComparer<ulong> Comparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.Comparer;
        }

        public ulong CurrentKey
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.CurrentKey;
        }

        public Timestamped<T> CurrentValue
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!_currentValue.Item1)
                {
                    _currentValue = (true, Deserialize(Inner.CurrentValue));
                }
                return _currentValue.Item2;
            }
        }

        ISeries<ulong, Timestamped<T>> ICursor<ulong, Timestamped<T>>.Source
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable HAA0601 // Value type to reference type conversion causing boxing allocation
            get => Source;
#pragma warning restore HAA0601 // Value type to reference type conversion causing boxing allocation
        }

        public Series<ulong, Timestamped<T>, DataStreamCursor<T>> Source
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new Series<ulong, Timestamped<T>, DataStreamCursor<T>>(this);
        }

        public bool IsContinuous
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.IsContinuous;
        }
    }

    public readonly struct DataStream<T> : ISeries<ulong, Timestamped<T>, DataStreamCursor<T>>
    {
        private readonly Series<ulong, Timestamped<T>, DataStreamCursor<T>> _series;
        private readonly Metadata _metadata;

        internal DataStream(DataStreamCursor<T> cursor, Metadata metadata)
        {
            _series = new Series<ulong, Timestamped<T>, DataStreamCursor<T>>(cursor);
            _metadata = metadata;
        }

        public Metadata Metadata => _metadata;

        public string TextId => _series._cursor.Inner.StreamLog.TextId;

        #region Implicit conversions

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator Series<ulong, Timestamped<T>, DataStreamCursor<T>>(DataStream<T> ds)
        {
            return ds._series;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator Series<ulong, Timestamped<T>, Cursor<ulong, Timestamped<T>>>(DataStream<T> ds)
        {
            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            var c = new Cursor<ulong, Timestamped<T>>(ds._series.GetCursor());
            return new Series<ulong, Timestamped<T>, Cursor<ulong, Timestamped<T>>>(c);
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public static implicit operator Series<Timestamp, T, TimeStreamCursor<T>>(DataStream<T> ds)
        //{
        //    return ds.AsTimeSeries();
        //}

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public static implicit operator Series<Timestamp, T, Cursor<Timestamp, T>>(DataStream<T> ds)
        //{
        //    // ReSharper disable once ImpureMethodCallOnReadonlyValueField
        //    var c = new Cursor<Timestamp, T>(ds.AsTimeSeries().GetCursor());
        //    return new Series<Timestamp, T, Cursor<Timestamp, T>>(c);
        //}

        #endregion Implicit conversions

        public Series<Timestamp, T, TimeStreamCursor<T>> AsTimeSeries()
        {
            if (_series._cursor.Inner.StreamLog.State.GetTrackedKeySorting() != KeySorting.Strong)
            {
                throw new InvalidOperationException($"KeySorting must be strong to open a data stream as time series, current sort order is: {_series._cursor.Inner.StreamLog.State.GetTrackedKeySorting()}");
            }
            var tsc = new TimeStreamCursor<T>(_series._cursor.Inner);
            return tsc.Source;
        }

        #region ISeries delegated to _series

        // ReSharper disable ImpureMethodCallOnReadonlyValueField

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Spreads.IAsyncEnumerator<KeyValuePair<ulong, Timestamped<T>>> GetAsyncEnumerator()
        {
            return _series.GetAsyncEnumerator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator<KeyValuePair<ulong, Timestamped<T>>> IEnumerable<KeyValuePair<ulong, Timestamped<T>>>.GetEnumerator()
        {
            return _series.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)_series).GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DataStreamCursor<T> GetEnumerator()
        {
            return _series.GetEnumerator();
        }

        public void Dispose()
        {
            _series.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ICursor<ulong, Timestamped<T>> ISeries<ulong, Timestamped<T>>.GetCursor()
        {
            return _series.GetCursor();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AsyncCursor<ulong, Timestamped<T>, DataStreamCursor<T>> GetAsyncCursor()
        {
            return _series.GetAsyncCursor();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DataStreamCursor<T> GetCursor()
        {
            return _series.GetCursor();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IAsyncCursor<ulong, Timestamped<T>> ISeries<ulong, Timestamped<T>>.GetAsyncCursor()
        {
            return _series.GetAsyncCursor();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(ulong key, out Timestamped<T> value)
        {
            return _series.TryGetValue(key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetAt(long index, out KeyValuePair<ulong, Timestamped<T>> kvp)
        {
            return _series.TryGetAt(index, out kvp);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindAt(ulong key, Lookup direction, out KeyValuePair<ulong, Timestamped<T>> kvp)
        {
            return _series.TryFindAt(key, direction, out kvp);
        }

        public bool IsCompleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series.IsCompleted;
        }

        public bool IsIndexed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series.IsIndexed;
        }

        public KeyComparer<ulong> Comparer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series.Comparer;
        }

        public Opt<KeyValuePair<ulong, Timestamped<T>>> First
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series.First;
        }

        public Opt<KeyValuePair<ulong, Timestamped<T>>> Last
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series.Last;
        }

        public Timestamped<T> LastValueOrDefault
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series.LastValueOrDefault;
        }

        public Timestamped<T> this[ulong key]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series[key];
        }

        public IEnumerable<ulong> Keys
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series.Keys;
        }

        public IEnumerable<Timestamped<T>> Values
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _series.Values;
        }

        // ReSharper restore ImpureMethodCallOnReadonlyValueField

        #endregion ISeries delegated to _series
    }

}
