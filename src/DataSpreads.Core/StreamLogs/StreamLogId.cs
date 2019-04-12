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
using Spreads.Serialization;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DataSpreads.StreamLogs
{
    [DebuggerDisplay("{" + nameof(ToString) + "()}")]
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    [BinarySerialization(8)]
    internal readonly struct StreamLogId : IEquatable<StreamLogId>
    {
        // For nay other special internal streams use step 100 so that we
        // could change id without modifying existing data

        // There are issues with 0 == default, also 0 mostly because it looks circular
        // We should keep zero as sentinel for absence of StreamId.
        // All internal to DB service stream ids should be negative.

        public static readonly StreamLogId Log0Id = (StreamLogId)(-1);

        /// <summary>
        /// Local <see cref="Repo"/> id. One-based.
        /// </summary>
        [FieldOffset(0)]
        public readonly int RepoId;

        /// <summary>
        /// Stream id inside a local <see cref="Repo"/>. Zero value used for repo identity log that contains metadata events.
        /// </summary>
        [FieldOffset(4)]
        public readonly int StreamId;

        /// <summary>
        /// Only for conversions to/from long.
        /// </summary>
        [FieldOffset(0)]
        private readonly long _asLong;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private StreamLogId(long asLong)
        {
            this = default;
            if (asLong == 0)
            {
                ThrowZeroStreamIdValue();
            }
            _asLong = asLong;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamLogId(int repoId, int streamId)
        {
            _asLong = 0;
            RepoId = repoId;
            StreamId = streamId;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowZeroStreamIdValue()
        {
            ThrowHelper.ThrowArgumentOutOfRangeException("StreamLogId == 0. Must check validity.");
        }

        public int ContainerId
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => StreamIdToContainerId(StreamId);
        }

        public bool IsInternal
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _asLong < 0;
        }

        public bool IsValid
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _asLong != 0;
        }

        public bool IsUser
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _asLong > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator long(StreamLogId id)
        {
            var value = id._asLong;

            if (AdditionalCorrectnessChecks.Enabled)
            {
                // protect from default value
                if (value == 0)
                {
                    ThrowZeroStreamIdValue();
                }
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator StreamLogId(long value)
        {
            return new StreamLogId(value);
        }

        public static bool operator ==(StreamLogId id1, StreamLogId id2)
        {
            return id1.Equals(id2);
        }

        public static bool operator !=(StreamLogId id1, StreamLogId id2)
        {
            return !id1.Equals(id2);
        }

        public bool Equals(StreamLogId other)
        {
            return _asLong == other._asLong;
        }

        public override bool Equals(object obj)
        {
            if (obj is null) return false;
            return obj is StreamLogId slid && Equals(slid);
        }

        public override int GetHashCode()
        {
            return _asLong.GetHashCode();
        }

        public override string ToString()
        {
            return $"SLid: {RepoId} - {StreamId}";
        }

        public static int ContainerIdToStreamId(int containerId)
        {
            // TODO docs
            // We have 4 stream log ids per container id.
            // For now 2 are understandable (even/odd as in QUIC protocol).
            // But since it will be almost impossible to change 2 to 4
            // for existing stores it's better to provision for
            // unforeseen use cases. We have (U)Int32 stream ids per RepoId,
            // which corresponds to 1 billion containers ids per repo.
            // We could create a container every second for 34 years before
            // we hit this limit. Containers are identities which by their
            // nature are very limited in existence. For KV storage we (will)
            // have PersistentMap container.
            return containerId << 2;
        }

        public static int StreamIdToContainerId(int streamId)
        {
            return streamId >> 2;
        }
    }
}
