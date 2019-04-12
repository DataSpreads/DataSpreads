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
using Spreads.Collections.Concurrent;
using Spreads.Threading;
using Spreads.Utils;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Spreads.Serialization;
using static System.Runtime.CompilerServices.Unsafe;

#pragma warning disable 618

namespace DataSpreads.Buffers
{
    /// <summary>
    ///
    /// </summary>
    [DebuggerDisplay("{" + nameof(ToString) + "()}")]
    public unsafe class SharedMemory : RetainableMemory<byte>
    {
        [DebuggerDisplay("{" + nameof(ToString) + "()}")]
        [StructLayout(LayoutKind.Explicit, Size = Size)]
        public struct BufferHeader : IEquatable<BufferHeader>
        {
            public const int Size = 8;

            // Note that state transition only possible before or after a BRA txn, not "inside".
            // Successful commit operation is the transaction. State transition in code before
            // commit is "before" transaction. "Inside" transaction only gives a global lock.

            [DebuggerDisplay("{" + nameof(ToString) + "()}")]
            [StructLayout(LayoutKind.Sequential, Size = 4)]
            public readonly struct HeaderFlags
            {
                private readonly uint _value;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                private HeaderFlags(uint value)
                {
                    _value = value;
                }

                // 3 bits left

                /// <summary>
                /// A buffer is a <see cref="IsStreamBlock"/>.
                /// </summary>
                /// <remarks>
                /// This is ephemeral state only when we are going to transit into IsIndexedStreamBlock state.
                /// But it is needed because we could die after SBI txn but before updating the block flags.
                /// When a block with this flag is detected we must check <see cref="StreamBlockIndex"/> and update the flag.
                /// Usually we detect this from SBI operations.
                /// </remarks>
                public const uint IsStreamBlock = IsOwned | 0b0010_0000_00000000_00000000_00000000;

                // IsStreamBlock -> IsIndexedStreamBlock:
                // After adding to SBI. If flag update fails we will find such buffer in SBI as correct.

                /// <summary>
                /// A buffer is a <see cref="IsStreamBlock"/> and is owned by a stream (included in <see cref="StreamBlockIndex"/>).
                /// </summary>
                public const uint IsIndexedStreamBlock = IsStreamBlock | 0b0001_0000_00000000_00000000_00000000;

                // IsIndexedStreamBlock -> IsPackedStreamBlock:
                // Packer sets this flag after durably persisting data.

                public const uint IsPackedStreamBlock = IsIndexedStreamBlock | 0b0000_1000_00000000_00000000_00000000;

                // TODO (meta, low) review comments, write a spec for buffer lifetime
                // IsPackedStreamBlock -> IsOwned:
                // Must be IsPackedStreamBlock. Cannot release not packed SB, will lose data otherwise.
                // If packed then atomically unset the two bits and decrement counter (both in a single atomic op).
                // Counter must be equal to one before this transition. If CAS is unsuccessful and counter is >1
                // then a reader is active on this block. Will have to retry later.
                // TODO: Readers must not touch packed blocks but unpack into a cache
                // If a cursor is not moving for a very long time we cannot do anything,
                // but we should mark a buffer so that next move will fail but in a recoverable way and the cursor
                // could detect this situation and recover.
                // Need to carefully review when cursor move returns false.
                // Looks like setting count to zero could work. CurrentValue will work but cursors won't be able to move.
                //
                // IsOwned -> IsStreamBlock:
                // First need to initialize SB over the buffer, then set the flag. If SB is not added to the SBI
                // we could detect that during scan. TODO is a buffer not in SBI IsStreamBlock at all?
                // TODO IsStreamBlock must be very short-lived (as IsReleasing), if we find a block
                // with this value we must check SBI (from write txn to ensure we see the latest).
                // If not in SBI then IsStreamBlock with dead owner is just the same as IsOwned.

                /////////////////   BlockMemoryPool  /////////////////
                // ------------------------------------------------ //
                /////////////////  SharedMemoryPool  /////////////////

                // SMP must check that only IsOwned flag is set when it receives a buffer for return
                // or allocates a new one. These checks are done in RentFromPool/ReturnToPool methods.

                // Just owned but not used block is pooled in RetainableMemoryPool.
                // If a process dies then pooled buffers leak, but we store process instance id
                // in allocated list and could scan all allocated IsOwned-only buffers
                // and check if their owner is alive, then free leaked buffers.

                public const uint IsOwned = 0b0100_0000_00000000_00000000_00000000;

                // Transitions:
                //   [x] (IsOwned | IsDisposed) -> (Releasing | IsDisposed):
                //       Before BRA.Free txn. If txn fails a buffer is in allocated list with Releasing flag.
                //   [x] (Releasing | IsDisposed) -> IsOwned:
                //       After BRA.Allocate txn. If flag update fails a buffer is in allocated list with Releasing flag.
                // SM_GC:
                //   [ ] An allocated buffer with Releasing flag is not used, it's allocating or freeing process died.
                // Checks:
                //   Releasing is only possible together with IsDisposed, should fail fast is this is not the case at any point.
                //   [x] When allocating from free list.
                public const uint Releasing = 0b1000_0000_00000000_00000000_00000000;

                /// <summary>
                /// Disposed without any flags.
                /// </summary>
                public const uint IsDisposed = AtomicCounter.CountMask; // 0b0000_0000_11111111_11111111_11111111

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static bool operator ==(HeaderFlags h1, HeaderFlags h2)
                {
                    return h1.Equals(h2);
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static bool operator !=(HeaderFlags h1, HeaderFlags h2)
                {
                    return !h1.Equals(h2);
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public bool Equals(HeaderFlags other)
                {
                    return _value == other._value;
                }

                public override bool Equals(object obj)
                {
                    if (ReferenceEquals(null, obj)) return false;
                    return obj is HeaderFlags other && Equals(other);
                }

                public override int GetHashCode()
                {
                    ThrowHelper.ThrowInvalidOperationException();
                    return 0;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static implicit operator uint(HeaderFlags h)
                {
                    return h._value;
                }

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static implicit operator HeaderFlags(uint value)
                {
                    return new HeaderFlags(value);
                }

                /// <summary>
                /// Value with cleared counter bits. Not shifted.
                /// </summary>
                public uint Flags
                {
                    [MethodImpl(MethodImplOptions.AggressiveInlining)]
                    get => this & ~IsDisposed;
                }

                /// <summary>
                /// Count part only without flags.
                /// </summary>
                public int Count
                {
                    [MethodImpl(MethodImplOptions.AggressiveInlining)]
                    get => (int)(this & IsDisposed);
                }

                public override string ToString()
                {
                    return Convert.ToString(this >> 24, 2).PadLeft(8, '0') + " - " + Count;
                }
            }

            //
            public const int FlagsCounterOffset = 0;

            [FieldOffset(FlagsCounterOffset)]
            public HeaderFlags FlagsCounter;

            //
            public const int AllocatorInstanceIdOffset = 4;

            [FieldOffset(AllocatorInstanceIdOffset)]
            public uint AllocatorInstanceId;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static bool operator ==(BufferHeader h1, BufferHeader h2)
            {
                return h1.Equals(h2);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static bool operator !=(BufferHeader h1, BufferHeader h2)
            {
                return !h1.Equals(h2);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(BufferHeader other)
            {
                return FlagsCounter == other.FlagsCounter
                    && AllocatorInstanceId == other.AllocatorInstanceId;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                return obj is BufferHeader other && Equals(other);
            }

            public override int GetHashCode()
            {
                ThrowHelper.ThrowInvalidOperationException();
                return 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static explicit operator long(BufferHeader h)
            {
                return *(long*)&h;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static explicit operator BufferHeader(long value)
            {
                return *(BufferHeader*)&value;
            }

            public override string ToString()
            {
                return FlagsCounter.ToString() + " | " + AllocatorInstanceId;
            }
        }

        private static readonly ObjectPool<SharedMemory> ObjectPool = new ObjectPool<SharedMemory>(() => new SharedMemory(), Environment.ProcessorCount * 4);

        private SharedMemory()
        {
        }

        internal BufferRef BufferRef
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => unchecked((BufferRef)(uint)_counterOrReserved);
        }

        internal BufferHeader Header
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Read<BufferHeader>(HeaderPointer);
        }

        internal DirectBuffer NativeBuffer
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new DirectBuffer(_length + BufferHeader.Size, HeaderPointer);
        }

        [DebuggerStepThrough]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DirectBuffer GetBlockBuffer(bool pow2Payload)
        {
            return BlockMemoryPool.NativeToStreamBlockBuffer(NativeBuffer, pow2Payload);
        }

        [Obsolete("Use only inside SM or in tests")]
        internal byte* HeaderPointer
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (byte*)_pointer - BufferHeader.Size;
        }

        [Obsolete("Use only inside SM or in tests")]
        [DebuggerStepThrough]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static BufferHeader CompareExchangeHeader(byte* headerPointer, BufferHeader newValue, BufferHeader expected)
        {
            var existing = Interlocked.CompareExchange(ref *(long*)headerPointer, (long)newValue, (long)expected);
            return (BufferHeader)existing;
        }

        #region Shared Memory state transitions: require SM instances

        /// <summary>
        /// Transition state from (Releasing | IsDisposed) to IsOwned with zero counter.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal void FromReleasingDisposedToOwned()
        {
            var hptr = HeaderPointer;
            var header = Read<BufferHeader>(hptr);

            const uint requiredFlags = BufferHeader.HeaderFlags.Releasing | BufferHeader.HeaderFlags.IsDisposed;
            if (header.FlagsCounter != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            var newHeader = header;

            newHeader.FlagsCounter = BufferHeader.HeaderFlags.IsOwned;

            if (header != CompareExchangeHeader(hptr, newHeader, header))
            {
                ThrowFlagsCounterChanged();
            }
        }

        /// <summary>
        /// Transition state from (IsOwned | IsDisposed) to (Releasing | IsDisposed).
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal void FromOwnedDisposedToReleasingDisposed()
        {
            var hptr = HeaderPointer;
            var header = Read<BufferHeader>(hptr);

            const uint requiredFlags = BufferHeader.HeaderFlags.IsOwned | BufferHeader.HeaderFlags.IsDisposed;
            if (header.FlagsCounter != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            var newHeader = header;

            newHeader.FlagsCounter = BufferHeader.HeaderFlags.Releasing | BufferHeader.HeaderFlags.IsDisposed;

            if (header != CompareExchangeHeader(hptr, newHeader, header))
            {
                ThrowFlagsCounterChanged();
            }
        }

        #endregion Shared Memory state transitions: require SM instances

        #region StreamBlock state transitions

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static void FromOwnedToStreamBlock(DirectBuffer nativeBuffer, StreamLog streamLog)
        {
            var hptr = nativeBuffer.Data;

            var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer, streamLog.StreamLogFlags.Pow2Payload());

            var header = Read<BufferHeader>(hptr);

            // this implies a check that count == 0
            const uint requiredFlags = BufferHeader.HeaderFlags.IsOwned;
            if (header.FlagsCounter != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            // Initialize SB with slid before transitioning state
            // Note that a new block has no version (it is ReadyBlockVersion or version is initialized before IsStreamBlock -> IsIndexedStreamBlock transition
            var initialized = StreamBlock.TryInitializeEmpty(blockBuffer, streamLog);
            if (!initialized)
            {
                ThrowHelper.ThrowInvalidOperationException("Buffer was already initialized for stream.");
            }

            var newHeader = header;
            newHeader.FlagsCounter = BufferHeader.HeaderFlags.IsStreamBlock;

            if (header != CompareExchangeHeader(hptr, newHeader, header))
            {
                ThrowFlagsCounterChanged();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static void FromStreamBlockToOwned(DirectBuffer nativeBuffer, uint instanceId)
        {
            var hptr = nativeBuffer.Data;

            var header = Read<BufferHeader>(hptr);

            const uint requiredFlags = BufferHeader.HeaderFlags.IsStreamBlock;
            if (header.FlagsCounter != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            var newHeader = header;
            newHeader.FlagsCounter = BufferHeader.HeaderFlags.IsOwned;
            newHeader.AllocatorInstanceId = instanceId;

            if (header != CompareExchangeHeader(hptr, newHeader, header))
            {
                ThrowFlagsCounterChanged();
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="nativeBuffer"></param>
        /// <param name="streamLog"></param>
        /// <param name="firstVersion"></param>
        /// <param name="isRent"></param>
        /// <param name="ignoreAlreadyIndexed">There could be a race between Prepare/RentNext
        /// to update <see cref="BufferHeader.HeaderFlags.IsStreamBlock"/> to <see cref="BufferHeader.HeaderFlags.IsIndexedStreamBlock"/>.
        /// This flag tells to ignore failed atomic header swap if existing state is IsISB. Set to true only when
        /// the buffer is known to exist in SBI (we took it from there or just added in a txn).</param>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static void FromStreamBlockToIndexedStreamBlockClearInstanceId(DirectBuffer nativeBuffer,
            StreamLog streamLog, ulong firstVersion = 0, bool isRent = false, bool ignoreAlreadyIndexed = false)
        {
            var slid = streamLog.Slid;
            var itemFixedSize = streamLog.ItemFixedSize;

            var hptr = nativeBuffer.Data;

            var blockBuffer = BlockMemoryPool.NativeToStreamBlockBuffer(nativeBuffer, streamLog.StreamLogFlags.Pow2Payload());

            var newCount = isRent ? 2 : 1;
            var alreadyIndexed = false;

            var header = Read<BufferHeader>(hptr);

            // this implies a check that count == 0
            const uint requiredFlags = BufferHeader.HeaderFlags.IsStreamBlock;
            if (header.FlagsCounter != requiredFlags)
            {
                if (ignoreAlreadyIndexed && header.FlagsCounter.Flags == BufferHeader.HeaderFlags.IsIndexedStreamBlock)
                {
                    // Trace.TraceInformation($"Already indexed race 1: isRent {isRent} count: {header.FlagsCounter.Count}");
                    alreadyIndexed = true;
                    newCount = header.FlagsCounter.Count;
                }
                else
                {
                    ThrowBadInitialState(requiredFlags);
                }
            }

            // TODO (!) this has side effects, add static IsInitialized
            var initialized = StreamBlock.GetIsInitialized(blockBuffer, slid, itemFixedSize, firstVersion);
            if (!initialized)
            {
                ThrowHelper.ThrowInvalidOperationException("Buffer was not initialized before transition to IndexedStreamBlock.");
            }

            var newHeader = header;
            newHeader.FlagsCounter = BufferHeader.HeaderFlags.IsIndexedStreamBlock | (uint)newCount;
            newHeader.AllocatorInstanceId = 0; // same as shared wpid instance id

            while (true)
            {
                if (alreadyIndexed)
                {
                    IncrementIndexedStreamBlock(hptr);
                    break;
                }

                var existing = CompareExchangeHeader(hptr, newHeader, header);
                if (header != existing)
                {
                    if (ignoreAlreadyIndexed && existing.FlagsCounter.Flags == BufferHeader.HeaderFlags.IsIndexedStreamBlock)
                    {
                        // Trace.TraceInformation($"Already indexed race 2: isRent {isRent} count: {header.FlagsCounter.Count}");
                        // race was after initial check and before CAS. Set alreadyIndexed to true and
                        // go to the beginning of the loop where we will go to IncrementIndexedStreamBlock.
                        alreadyIndexed = true;
                        continue;
                    }

                    Trace.TraceError($"FLAG CHANGED: existing: [{existing}], header: [{header}], ignoreAlreadyIndexed: [{ignoreAlreadyIndexed}]");
                    ThrowFlagsCounterChanged();
                }
                break;
            }
        }

        /// <summary>
        /// Note that this keeps InstanceId == 0 and we could detect in which direction
        /// a block was going when it is found in IsStreamBlock state:
        /// If InstanceId is not zero the block was just taken from the pool (IsOwned).
        /// If InstanceId is zero then the block was just released from IsIndexedSB or IsPackedSB.
        /// </summary>
        /// <param name="nativeBuffer"></param>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static void FromIndexedStreamBlockToStreamBlock(DirectBuffer nativeBuffer)
        {
            var hptr = nativeBuffer.Data;

            var header = Read<BufferHeader>(hptr);

            // this implies a check that count == 1
            const uint requiredFlags = BufferHeader.HeaderFlags.IsIndexedStreamBlock | (uint)1;
            if (header.FlagsCounter != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            var newHeader = header;
            newHeader.FlagsCounter = BufferHeader.HeaderFlags.IsStreamBlock;
            Debug.Assert(newHeader.AllocatorInstanceId == 0);

            if (header != CompareExchangeHeader(hptr, newHeader, header))
            {
                ThrowFlagsCounterChanged();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static int IncrementIndexedStreamBlock(byte* headerPointer)
        {
            var header = Read<BufferHeader>(headerPointer);

            const uint requiredFlags = BufferHeader.HeaderFlags.IsIndexedStreamBlock;
            if (header.FlagsCounter.Flags != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            var count = header.FlagsCounter.Count;
            if (count == 0)
            {
                ThrowBadInitialIndexedStreamBlockCount(count, false);
            }

            var newHeader = header;
            while (true)
            {
                newHeader.FlagsCounter = header.FlagsCounter + 1;
                var existing = CompareExchangeHeader(headerPointer, newHeader, header);
                if (header != existing)
                {
                    if (existing.FlagsCounter.Flags == BufferHeader.HeaderFlags.IsIndexedStreamBlock)
                    {
                        header = existing;
                        continue;
                    }

                    ThrowFlagsCounterChanged();
                }
                break;
            }

            return (int)(newHeader.FlagsCounter & BufferHeader.HeaderFlags.IsDisposed);
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static int DecrementIndexedStreamBlock(byte* headerPointer)
        {
            var header = Read<BufferHeader>(headerPointer);

            const uint requiredFlags = BufferHeader.HeaderFlags.IsIndexedStreamBlock;
            if (header.FlagsCounter.Flags != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            var count = header.FlagsCounter.Count;
            if (count <= 1)
            {
                ThrowBadInitialIndexedStreamBlockCount(count, true);
            }

            var newHeader = header;
            while (true)
            {
                newHeader.FlagsCounter = header.FlagsCounter - 1;
                var existing = CompareExchangeHeader(headerPointer, newHeader, header);
                if (header != existing)
                {
                    if (existing.FlagsCounter.Flags == BufferHeader.HeaderFlags.IsIndexedStreamBlock)
                    {
                        header = existing;
                        continue;
                    }

                    ThrowFlagsCounterChanged();
                }
                break;
            }

            return (int)(newHeader.FlagsCounter & BufferHeader.HeaderFlags.IsDisposed);
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static void FromIndexedStreamBlockToPackedStreamBlock(byte* headerPointer)
        {
            var header = Read<BufferHeader>(headerPointer);

            const uint requiredFlags = BufferHeader.HeaderFlags.IsIndexedStreamBlock;
            if (header.FlagsCounter.Flags != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            var count = header.FlagsCounter.Count;
            if (count == 0)
            {
                ThrowBadInitialIndexedStreamBlockCount(count, false);
            }

            var newHeader = header;
            while (true)
            {
                newHeader.FlagsCounter = BufferHeader.HeaderFlags.IsPackedStreamBlock | (uint)header.FlagsCounter.Count;
                var existing = CompareExchangeHeader(headerPointer, newHeader, header);
                if (header != existing)
                {
                    if (existing.FlagsCounter.Flags == BufferHeader.HeaderFlags.IsIndexedStreamBlock)
                    {
                        header = existing;
                        continue;
                    }

                    ThrowFlagsCounterChanged();
                }
                break;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static bool TryFromPackedStreamBlockToStreamBlock(byte* headerPointer)
        {
            var header = Read<BufferHeader>(headerPointer);

            const uint requiredFlags = BufferHeader.HeaderFlags.IsPackedStreamBlock;
            if (header.FlagsCounter.Flags != requiredFlags)
            {
                ThrowBadInitialState(requiredFlags);
            }

            var count = header.FlagsCounter.Count;
            if (count == 0)
            {
                ThrowBadInitialIndexedStreamBlockCount(count, false);
            }

            // there are users of this block
            if (count > 1)
            {
                return false;
            }

            Debug.Assert(count == 1); // this is owned by SBI

            var newHeader = header;
            newHeader.FlagsCounter = BufferHeader.HeaderFlags.IsStreamBlock;

            var existing = CompareExchangeHeader(headerPointer, newHeader, header);
            if (header != existing)
            {
                if (existing.FlagsCounter.Flags == BufferHeader.HeaderFlags.IsPackedStreamBlock)
                {
                    Debug.Assert(existing.FlagsCounter.Count > 1);
                    return false;
                }

                ThrowFlagsCounterChanged();
            }

            return true;
        }

        #endregion StreamBlock state transitions

        #region State transition throw helpers

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowFlagsCounterChanged()
        {
            ThrowHelper.ThrowInvalidOperationException("Flags changed during transition.");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void ThrowBadInitialState(BufferHeader.HeaderFlags requiredFlags)
        {
            ThrowHelper.ThrowInvalidOperationException($"State must be [{requiredFlags}] before transition");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowBadInitialIndexedStreamBlockCount(int count, bool decrement)
        {
            if (decrement)
            {
                ThrowHelper.ThrowInvalidOperationException(
                    $"Initial count must be greater than 1, current is [{count}]");
            }
            else
            {
                Debug.Assert(count == 0);
                ThrowHelper.ThrowInvalidOperationException(
                    $"Initial count must be non-zero be [{count}] before transition");
            }
        }

        #endregion State transition throw helpers

        [Obsolete("TODO delete")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal RetainedMemory<byte> RetainBlockMemory(bool isLog0)
        {
            if (isLog0)
            {
                // TODO remove hard coded numbers,
                // Log0 uses Pool's largest buffer
                if (Length < 192 + 8 * 1024 * 1024)
                {
                    ThrowHelper.FailFast("Wrong chunk buffer length for Log0:" + Length);
                }

                var len0 = LengthPow2 + StreamBlock.DataOffset;
                var rm0 = new RetainedMemory<byte>(this, Length - len0, len0, true);
                if (!BitUtil.IsAligned((long)((byte*)rm0.Pointer + StreamBlock.DataOffset), SharedMemoryPool.PageSize))
                {
                    ThrowHelper.FailFast("Wrong Log0 pointer alignment:" + rm0.Length);
                }

                if (rm0.Length != 192 + 8 * 1024 * 1024)
                {
                    ThrowHelper.FailFast("Wrong retained memory length for Log0:" + rm0.Length);
                }
                return rm0;
            }

            // Note: this is needed for the opposite reason why blocks are Pow2 + Page
            // We cannot avoid a SM header so far and need Pow2 SM spans with minimal overhead
            // for multi-writer Aeron-style logs.
            // But then we use the entire SM buffer length for SL, pack it and unpack in
            // RAM that allocates in Pow2 chunks. With the header we will lose near 50%
            // of RAM:
            // [         unused         SMH[SBHP + payload 1st page]|[                  SM Pow2 remaining payload                 ]].
            // Instead we prefer to leave the header page of SM unused, not RAM:
            // [SMH 4088 b unused [SBHP|  SM payload  ]]

            if (Length > SharedMemoryPool.Pow2ModeSizeLimit)
            {
                return RetainPow2();
            }

            return Retain();
        }

        #region Lifecycle

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SharedMemory Create(DirectBuffer nativeBuffer, BufferRef bufferRef, SharedMemoryPool pool)
        {
            var memoryBuffer = ObjectPool.Allocate();

            memoryBuffer.Init(nativeBuffer, bufferRef, pool);
#if DEBUG
            memoryBuffer.Tag = Environment.StackTrace;
#endif
            return memoryBuffer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Init(DirectBuffer nativeBuffer, BufferRef bufferRef, SharedMemoryPool pool)
        {
            _isNativeWithHeader = true;
            // after _isNativeWithHeader is set _counterOrReserved is not used for counter and could be reused for other purposes.
            _counterOrReserved = unchecked((int)(uint)bufferRef);

            _poolIdx = pool.PoolIdx; // NB order matters, we then access properties with assert

            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (BufferHeader.Size != NativeHeaderSize)
#pragma warning disable 162
            // ReSharper disable once HeuristicUnreachableCode
            {
                // ReSharper disable once HeuristicUnreachableCode
                FailWrongHeaderSize();
            }
#pragma warning restore 162

            _pointer = nativeBuffer.Data + BufferHeader.Size;
            _length = nativeBuffer.Length - BufferHeader.Size;

            void FailWrongHeaderSize()
            {
                ThrowHelper.FailFast("SharedMemory.BufferHeader.Size must be equal to RetainableMemory.NativeHeaderSize");
            }
        }

        /// <summary>
        /// This is only for <see cref="SharedMemory"/> .NET object and doesn't care about the shared memory.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Free(SharedMemory buffer)
        {
            buffer._poolIdx = default;
            buffer._counterOrReserved = default;

            // Does this:
            //buffer._pointer = default;
            //buffer._length = default;
            buffer.ClearAfterDispose();

            // if we missed the pool the state of the object is already clean,
            // just drop it and do not try finalization logic.
            GC.SuppressFinalize(buffer);

            ObjectPool.Free(buffer);
        }

        /// <summary>
        /// This is for disposing SM buffer and not the object (we have Free for detaching and pooling the object).
        /// We need to check flags: only IsOwned could be released - it is just pooled ready for use or temp one.
        /// When in the pool flags value is: IsOwned | InFreeList
        /// When rented but not yet used for SB: IsOwned
        /// IsOwned means that the buffer is owned by the current process and until the process is detected dead
        /// no other process could use the buffer.
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                var pool = Pool;
                if (pool != null)
                {
                    pool.ReturnInternal(this, clearMemory: false);
                    // pool calls Dispose(false) is a bucket is full
                    return;
                }

                // RMP is disposing, it detaches buffers from itself to finalized them
                GC.SuppressFinalize(this);
            }

            // Either pool is full and called this method with false or just a finalizer.
            // Buffer is not pooled, we are going to return in to the native shared pool.

            AtomicCounter.Dispose(ref CounterRef);

            if (Header.FlagsCounter != (BufferHeader.HeaderFlags.IsOwned | BufferHeader.HeaderFlags.IsDisposed))
            {
                // TODO throw, not fail, review when this is possible
                ThrowHelper.FailFast("Header.FlagsCounter != (BufferHeader.HeaderFlags.IsOwned | BufferHeader.HeaderFlags.IsDisposed)");
            }

            ((SharedMemoryPool)Pool).ReturnNative(this);
        }

        #endregion Lifecycle

        public override string ToString()
        {
            return $"SM: {BufferRef.BucketIndex} ({(_length):N0}) - {BufferRef.BufferIndex:N0}";
        }

        [Conditional("DEBUG")]
        internal void InvariantCheck()
        {
            if (_length <= 0)
            {
                BuffersThrowHelper.ThrowBadLength();
            }

            if (_pointer == null)
            {
                ThrowHelper.ThrowArgumentNullException(nameof(_pointer));
            }

            if (!BitUtil.IsPowerOfTwo(LengthPow2))
            {
                ThrowHelper.ThrowInvalidOperationException("!BitUtil.IsPowerOfTwo(LengthPow2)");
            }

            if (!BitUtil.IsAligned((long)PointerPow2, 2048))
            {
                ThrowHelper.ThrowInvalidOperationException("!BitUtil.IsPowerOfTwo(LengthPow2)");
            }

            if (LengthPow2 > 2048 && !BitUtil.IsAligned((long)PointerPow2, 4096))
            {
                ThrowHelper.ThrowInvalidOperationException("!BitUtil.IsPowerOfTwo(LengthPow2)");
            }
        }
    }
}
