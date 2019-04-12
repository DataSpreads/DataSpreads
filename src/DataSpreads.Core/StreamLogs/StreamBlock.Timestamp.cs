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
using Spreads.Algorithms;
using Spreads.Collections;
using Spreads.DataTypes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

// ReSharper disable ImpureMethodCallOnReadonlyValueField

namespace DataSpreads.StreamLogs
{
    internal readonly partial struct StreamBlock
    {
        public unsafe Timestamp PreviousLastTimestamp
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return (Timestamp)(*(long*)(Pointer + StreamBlockHeader.PreviousBlockLastTimestampOffset));
            }
        }

        public Timestamp GetFirstTimestampOrWriteStart()
        {
            if (VersionAndFlags.HasTimestamp)
            {
                var ts = DangerousGetTimestampAtIndex(0);
                return ts;
            }

            return WriteStart;
        }

        public Timestamp GetLastTimestampOrDefault()
        {
            if (VersionAndFlags.HasTimestamp && Count > 0)
            {
                var ts = DangerousGetTimestampAtIndex(Count - 1);
                return ts;
            }

            return default;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Timestamp DangerousGetTimestampAtIndex(int index)
        {
            var firstItem = DangerousGet(index);
            var ts = firstItem.Read<Timestamp>(0);
            return ts;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int TimestampSortedSearch(Timestamp value)
        {
            if (!VersionAndFlags.HasTimestamp)
            {
                ThrowHelper.ThrowInvalidOperationException("StreamBlock values do not have timestamp.");
            }

            var sbts = new StreamBlockWithTimestamp(this);

            return VectorSearch.SortedSearch(ref sbts, 0, CountVolatile, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int TimestampSortedLookup(Timestamp value, Lookup direction)
        {
            if (!VersionAndFlags.HasTimestamp)
            {
                ThrowHelper.ThrowInvalidOperationException("StreamBlock values do not have timestamp.");
            }

            var sbts = new StreamBlockWithTimestamp(this);

            return VectorSearch.SortedLookup(ref sbts, 0, CountVolatile, value, direction);
        }
    }

    internal readonly struct StreamBlockWithTimestamp : IVector<Timestamp>, IEquatable<StreamBlockWithTimestamp>
    {
        public readonly StreamBlock Block;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamBlockWithTimestamp(StreamBlock block)
        {
            Block = block;
        }

        public IEnumerator<Timestamp> GetEnumerator()
        {
            throw new NotSupportedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Block.CountVolatile;
        }

        public Timestamp this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Get(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Timestamp Get(int index)
        {
            if (!Block.VersionAndFlags.HasTimestamp)
            {
                ThrowHelper.ThrowInvalidOperationException("StreamBlock values do not have timestamp.");
            }
            return Block.DangerousGetTimestampAtIndex(index);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Timestamp DangerousGet(int index)
        {
            return Block.DangerousGetTimestampAtIndex(index);
        }

        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Count;
        }

        public unsafe bool Equals(StreamBlockWithTimestamp other)
        {
            return (long)Block.Pointer == (long)other.Block.Pointer;
        }

        public override bool Equals(object obj)
        {
            if (obj is null) return false;
            return obj is StreamBlockWithTimestamp other && Equals(other);
        }

        public override int GetHashCode()
        {
            return Block.GetHashCode();
        }
    }
}
