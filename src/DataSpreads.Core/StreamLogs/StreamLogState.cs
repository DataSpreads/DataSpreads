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
using Spreads.Serialization;
using Spreads.Threading;
using Spreads.Utils;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Unsafe = System.Runtime.CompilerServices.Unsafe;

namespace DataSpreads.StreamLogs
{
    [StructLayout(LayoutKind.Sequential, Size = 8)]
    internal readonly unsafe partial struct StreamLogState
    {
        // Have 3 long slots reserved

        /// <summary>
        /// Current StreamLog state.
        /// </summary>
        [BinarySerialization(BufferSize)]
        [StructLayout(LayoutKind.Explicit, Size = BufferSize)]
        internal readonly struct StreamLogStateRecord
        {
            public const int BufferSize = 128;

            // Two cache lines. Keep hot values in the first one, it will be in L2
            // probably always when a SL is active.

            //////////////////// Cache line 1 //////////////////////////

            //
            public const int LockerOffset = 0;

            [FieldOffset(LockerOffset)]
            public readonly Wpid Locker;

            //
            public const int StreamLogIdOffset = 8;

            [FieldOffset(StreamLogIdOffset)]
            public readonly StreamLogId StreamLogId;

            #region Format, Flags & IsCompleted

            //
            public const int ReservedXOffset = 16 + 0;

            [FieldOffset(ReservedXOffset)]
            public readonly int ReservedX;

            //
            public const int ItemFixedSizeOffset = 16 + 4;

            [FieldOffset(ItemFixedSizeOffset)]
            public readonly short ItemFixedSize;

            // TODO this could be static flags
            public const int StreamLogFlagsOffset = 16 + 6;

            [FieldOffset(StreamLogFlagsOffset)]
            public readonly StreamLogFlags StreamLogFlags;

            // Keep as a dedicated byte because we read this value a lot.
            public const int IsCompletedOffset = 16 + 7;

            [FieldOffset(IsCompletedOffset)]
            public readonly byte IsCompleted;

            #endregion Format, Flags & IsCompleted

            #region Rate & dynamic flags

            // TODO Store rate as Pow2, we do not care about precise rate value
            public const int WriteModeOffset = 24 + 0;

            /// <summary>
            /// This is set during writer open. If multiple shared writers have different modes than
            /// the most strict one wins. TODO decide what to do
            /// </summary>
            [FieldOffset(WriteModeOffset)]
            public readonly WriteMode WriteMode;

            public const int RatePerMinuteOffset = 24 + 1;

            [FieldOffset(RatePerMinuteOffset)]
            public readonly sbyte RatePerMinute;


            public const int TrackedKeySortingOffset = 24 + 2;

            [FieldOffset(RatePerMinuteOffset)]
            public readonly KeySorting TrackedKeySorting;

            // TODO Unused 1 byte

            // TODO
            public const int DynamicFlagsOffset = 24 + 4;

            [FieldOffset(DynamicFlagsOffset)]
            public readonly uint DynamicFlags;

            #endregion Rate & dynamic flags

            #region WAL version & position

            //
            public const int LastVersionSentToWalOffset = 32;

            [FieldOffset(LastVersionSentToWalOffset)]
            public readonly ulong LastVersionSentToWal;

            //
            public const int WalPositionOffset = 40;

            [FieldOffset(WalPositionOffset)]
            public readonly ulong WalPosition;

            #endregion WAL version & position

            // TODO review usage of the two active chunk fields. Updates are not atomic, need retry/recover logic

            public const int ActiveChunkVersionOffset = 48;

            [FieldOffset(ActiveChunkVersionOffset)]
            public readonly ulong ActiveChunkVersion;

            public const int ReservedLong1Offset = 56;

            [FieldOffset(ReservedLong1Offset)]
            public readonly ulong ReservedLong1;

            //////////////////// Cache line 2 //////////////////////////

            //
            public const int ExclusiveWriterOffset = 64 + 0;

            [FieldOffset(ExclusiveWriterOffset)]
            public readonly UUID ExclusiveWriterConnectionId; // UUID 16 bytes size

            //
            public const int ExclusiveWriterTimestampOffset = 64 + 16;

            [FieldOffset(ExclusiveWriterTimestampOffset)]
            public readonly long ExclusiveWriterTimestamp;

            //
            public const int UpstreamVersionOffset = 64 + 24;

            [FieldOffset(UpstreamVersionOffset)]
            public readonly ulong UpstreamVersion;

            // This should be updated after all index records are updated.
            // The only purpose is to speedup lookup of initial position.
            public const int LastPackedChunkVersionOffset = 64 + 32;

            [FieldOffset(LastPackedChunkVersionOffset)]
            public readonly ulong LastPackedChunkVersion;

            //  for packer lock with expiration
            public const int PackerTimestampOffset = 64 + 40;

            [FieldOffset(PackerTimestampOffset)]
            public readonly long PackerTimestamp;

            //
            public const int ReservedLong2Offset = 64 + 48;

            [FieldOffset(ReservedLong2Offset)]
            public readonly ulong ReservedLong2;

            //
            public const int ReservedLong3Offset = 64 + 56;

            [FieldOffset(ReservedLong3Offset)]
            public readonly ulong ReservedLong3;
        }

        private readonly byte* _statePointer;

        internal StreamLogState(StreamLogId streamLogId, IntPtr statePointer)
        {
            Debug.Assert(BitUtil.IsAligned((long)statePointer, 64), "StreamLogState buffer must be aligned to 64 bytes.");

            _statePointer = (byte*)statePointer;

            if (StreamLogId != default && StreamLogId != streamLogId)
            {
                ThrowHelper.ThrowArgumentException($"Wrong existing StreamId {StreamLogId} in StreamLogState vs expected {streamLogId}.");
            }
        }

        internal bool IsValid
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _statePointer != null;
        }

        internal IntPtr StatePointer
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (IntPtr)_statePointer;
        }

        public StreamLogId StreamLogId
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Unsafe.Read<StreamLogId>(_statePointer + StreamLogStateRecord.StreamLogIdOffset);
        }

        internal short ItemFixedSize
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled)
                { AssertValidPointer(); }

                return Unsafe.ReadUnaligned<short>(_statePointer + StreamLogStateRecord.ItemFixedSizeOffset);
            }
        }

        internal StreamLogFlags StreamLogFlags
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled)
                { AssertValidPointer(); }

                return (StreamLogFlags)Unsafe.ReadUnaligned<byte>(_statePointer + StreamLogStateRecord.StreamLogFlagsOffset);
            }
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool GetIsCompleted()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            return 0 != Volatile.Read(ref *(_statePointer + StreamLogStateRecord.IsCompletedOffset));
        }

        /// <summary>
        /// One-way operation
        /// </summary>
        [System.Diagnostics.Contracts.Pure]
        internal void SetIsCompleted()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            Volatile.Write(ref *(_statePointer + StreamLogStateRecord.IsCompletedOffset), 1);
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal WriteMode GetWriteMode()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            return (WriteMode)Unsafe.ReadUnaligned<byte>(_statePointer + StreamLogStateRecord.WriteModeOffset);
        }

        [System.Diagnostics.Contracts.Pure]
        internal void SetWriteMode(WriteMode writeMode)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            Unsafe.WriteUnaligned<byte>(_statePointer + StreamLogStateRecord.WriteModeOffset, (byte)writeMode);
        }

        /// <summary>
        /// When returned value is negative the rate is explicitly hinted and a next buffer is pre-allocated in advance.
        /// </summary>
        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetRatePerMinute()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            var raw = Unsafe.ReadUnaligned<sbyte>(_statePointer + StreamLogStateRecord.RatePerMinuteOffset);
            var pow2 = Math.Abs(raw);
            var absValue = 1 << pow2;
            return raw < 0 ? -absValue : absValue;
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void HintRatePerMinute(uint value)
        {
            SetRatePerMinute(-(int)value);
        }

        /// <summary>
        /// Hinted values must be negative, calculated values are positive.
        /// </summary>
        /// <param name="value"></param>
        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetRatePerMinute(int value)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            sbyte raw = 0;
            if (value != 0)
            {
                var abs = Math.Abs(value);
                var nextPow2 = (32 - IntUtil.NumberOfLeadingZeros(abs - 1));
                var pow2 = Math.Min(24, nextPow2);

                raw = checked((sbyte)(value < 0 ? -pow2 : pow2));
            }

            Unsafe.WriteUnaligned<sbyte>(_statePointer + StreamLogStateRecord.RatePerMinuteOffset, raw);
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal KeySorting GetTrackedKeySorting()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            var keySorting = Unsafe.ReadUnaligned<KeySorting>(_statePointer + StreamLogStateRecord.TrackedKeySortingOffset);
            return keySorting;
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetTrackedKeySorting(KeySorting value)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }
            Unsafe.Write<KeySorting>(_statePointer + StreamLogStateRecord.TrackedKeySortingOffset, value);
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ulong GetLastVersionSentToWal()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            if (IntPtr.Size == 8)
            {
                return Volatile.Read(ref *(ulong*)(_statePointer + StreamLogStateRecord.LastVersionSentToWalOffset));
            }
            else
            {
                return unchecked((ulong)Interlocked.Read(ref *(long*)(_statePointer + StreamLogStateRecord.LastVersionSentToWalOffset)));
            }
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetLastVersionSentToWal(ulong value)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            if (IntPtr.Size == 8)
            {
                Volatile.Write(ref *(ulong*)(_statePointer + StreamLogStateRecord.LastVersionSentToWalOffset), value);
            }
            else
            {
                Interlocked.Exchange(ref *(long*)(_statePointer + StreamLogStateRecord.LastVersionSentToWalOffset), unchecked((long)value));
            }
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ulong GetWalPosition()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            if (IntPtr.Size == 8)
            {
                return Volatile.Read(ref *(ulong*)(_statePointer + StreamLogStateRecord.WalPositionOffset));
            }
            else
            {
                return unchecked((ulong)Interlocked.Read(ref *(long*)(_statePointer + StreamLogStateRecord.WalPositionOffset)));
            }
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetWalPosition(ulong value)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            if (IntPtr.Size == 8)
            {
                Volatile.Write(ref *(ulong*)(_statePointer + StreamLogStateRecord.WalPositionOffset),
                    value);
            }
            else
            {
                Interlocked.Exchange(
                    ref *(long*)(_statePointer + StreamLogStateRecord.WalPositionOffset),
                    unchecked((long)value));
            }
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ulong GetActiveBlockVersion()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            if (IntPtr.Size == 8)
            {
                return Volatile.Read(ref *(ulong*)(_statePointer + StreamLogStateRecord.ActiveChunkVersionOffset));
            }
            else
            {
                return unchecked((ulong)Interlocked.Read(ref *(long*)(_statePointer + StreamLogStateRecord.ActiveChunkVersionOffset)));
            }
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetActiveChunkVersion(ulong value)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            if (value == 0)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(nameof(value));
            }

            if (IntPtr.Size == 8)
            {
                Volatile.Write(ref *(ulong*)(_statePointer + StreamLogStateRecord.ActiveChunkVersionOffset),
                    value);
            }
            else
            {
                Interlocked.Exchange(
                    ref *(long*)(_statePointer + StreamLogStateRecord.ActiveChunkVersionOffset),
                    unchecked((long)value));
            }
        }

        // TODO Volatile if Packer becomes multithreaded
        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ulong GetLastPackedBlockVersion()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            // we do not need volatile but need atomic write just in case of torn read
            if (IntPtr.Size == 8)
            {
                return Volatile.Read(ref *(ulong*)(_statePointer + StreamLogStateRecord.LastPackedChunkVersionOffset));
            }
            else
            {
                return (ulong)Interlocked.Read(ref *(long*)(_statePointer + StreamLogStateRecord.LastPackedChunkVersionOffset));
            }
        }

        [System.Diagnostics.Contracts.Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetLastPackedBlockVersion(ulong value)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            if (IntPtr.Size == 8)
            {
                Volatile.Write(ref *(ulong*)(_statePointer + StreamLogStateRecord.LastPackedChunkVersionOffset), value);
            }
            else
            {
                Interlocked.Exchange(ref *(long*)(_statePointer + StreamLogStateRecord.LastPackedChunkVersionOffset),
                    (long)value);
            }
        }

        [Obsolete("Old overload, used a lot in tests.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CheckInit(StreamLogId slid, short valueSize, SerializationFormat format)
        {
            CheckInit(slid, valueSize, format.IsBinary() ? StreamLogFlags.IsBinary : StreamLogFlags.None);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CheckInit(StreamLogId slid, short valueSize, StreamLogFlags streamLogFlags)
        {
            var streamId = (long)slid;
            if (StreamLogId == default)
            {
                Unsafe.WriteUnaligned(
                    _statePointer + StreamLogStateRecord.StreamLogIdOffset, streamId);
                Unsafe.WriteUnaligned(
                    _statePointer + StreamLogStateRecord.ItemFixedSizeOffset, valueSize);
                Unsafe.WriteUnaligned(
                    _statePointer + StreamLogStateRecord.StreamLogFlagsOffset,
                    (byte)streamLogFlags);

                Unsafe.WriteUnaligned(
                    _statePointer + StreamLogStateRecord.TrackedKeySortingOffset,
                    (byte)KeySorting.Strong); // until we have 2 values sorting is always strong

                return true;
            }

            if (valueSize != ItemFixedSize || StreamLogFlags != streamLogFlags)
            {
                ThrowHelper.ThrowInvalidOperationException($"Existing state doesn't match provided parameters: valueSize {valueSize} != ValueSize {ItemFixedSize} || StreamLogFlags {StreamLogFlags} != streamLogFlags {streamLogFlags}");
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AssertValidPointer()
        {
            ThrowHelper.AssertFailFast(_statePointer != null, "_pointer == IntPtr.Zero in StreamLogState");
        }

        [Obsolete("Won't release memory. Only for tests.")]
        internal static StreamLogState GetDummyState(StreamLogId slid = default)
        {
            var ptr = Marshal.AllocHGlobal(StreamLogStateRecord.BufferSize);
            new Span<byte>((void*)ptr, StreamLogStateRecord.BufferSize).Clear();
            return new StreamLogState(slid, ptr);
        }

        #region NotificationLog

        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [Pure]
        internal ulong IncrementLog0Version()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            return (ulong)Interlocked.Increment(ref *(long*)(_statePointer + StreamLogStateRecord.WalPositionOffset));
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ulong GetLog0Version()
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            return (ulong)Interlocked.Add(ref *(long*)(_statePointer + StreamLogStateRecord.WalPositionOffset), 0);
        }

        #endregion NotificationLog
    }
}
