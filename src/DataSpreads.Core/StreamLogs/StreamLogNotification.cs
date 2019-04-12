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

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DataSpreads.StreamLogs
{
    [DebuggerDisplay("StreamLogId={" + nameof(StreamLogId) + ("}, Tag={" + nameof(Tag) + "}"))]
    [StructLayout(LayoutKind.Sequential, Size = Size)]
    internal readonly struct StreamLogNotification
    {
        public const int Size = 8;

        private const int TagOffset = 56;

        private const byte PriorityMask = 0b_0000_0001;
        private const byte RotateMask = 0b_0000_0010;
        private const byte CompleteMask = 0b_0000_0100;
        private const byte UpstreamMask = 0b_0000_1000;
        private const byte InternalMask = 0b_0001_0000;

        // ReSharper disable once UnusedMember.Local
        private const byte ReservedMask = 0b_0010_0000;

        private const byte WalPositionMask = 0b_0100_0000;

        // TODO (?, low) can have 7 bits for append batch size, but then this mask must be checked before all others
        // ReSharper disable once UnusedMember.Local
        private const byte AppendMask = 0b_1000_0000;

        public const ulong MaxStreamId = (1LU << TagOffset) - 1UL;
        private const ulong StreamIdMask = ((1LU << (64 - TagOffset)) - 1UL) << TagOffset;

        private readonly ulong _value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private StreamLogNotification(ulong value)
        {
            _value = value;
        }

        // When WAL reaches 2^56 bytes add a ctor overload to expand range by 6 bits ;)

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamLogNotification(StreamLogId slid,
            bool rotated = false,
            bool completed = false,
            bool fromUpstream = false,
            bool priority = false)
        {
            byte tag = 0;
            ulong ustreamId;

            if (slid.IsInternal)
            {
                tag |= InternalMask;
                ustreamId = (ulong)-(long)(slid);
            }
            else
            {
                ustreamId = (ulong)(long)slid;
            }

            if (ustreamId > MaxStreamId)
            {
                ThrowArgumentEx();
            }

            if (priority)
            {
                tag |= PriorityMask;
            }

            if (rotated)
            {
                tag |= RotateMask;
            }

            if (completed)
            {
                tag |= CompleteMask;
            }

            if (fromUpstream)
            {
                tag |= UpstreamMask;
            }

            _value = (ulong)tag << TagOffset | ustreamId;
        }

        public static StreamLogNotification WalSignal = new StreamLogNotification((ulong)WalPositionMask << TagOffset);

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowArgumentEx()
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentOutOfRangeException("streamId");
        }

        public StreamLogId StreamLogId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var asLong = (long)(_value & ~StreamIdMask);
                return (StreamLogId)(IsInternal ? -asLong : asLong);
            }
        }

        public byte Tag
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => checked((byte)(_value >> TagOffset));
        }

        public bool IsPriority
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ((_value >> TagOffset) & PriorityMask) != 0;
        }

        public bool IsRotated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ((_value >> TagOffset) & RotateMask) != 0;
        }

        public bool IsCompleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ((_value >> TagOffset) & CompleteMask) != 0;
        }

        public bool IsInternal
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ((_value >> TagOffset) & InternalMask) != 0;
        }

        public bool IsWalPosition
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ((_value >> TagOffset) & WalPositionMask) != 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator ulong(StreamLogNotification id)
        {
            return id._value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static explicit operator StreamLogNotification(ulong value)
        {
            return new StreamLogNotification(value);
        }
    }
}
