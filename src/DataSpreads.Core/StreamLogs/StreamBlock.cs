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

// This kills performance and produces a lot of garbage & GC.
// #define DETECT_LEAKS

using DataSpreads.Buffers;
using DataSpreads.Config;
using DataSpreads.Security.Crypto;
using Spreads;
using Spreads.Algorithms.Hash;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.Serialization;
using Spreads.Utils;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static DataSpreads.Buffers.SharedMemory.BufferHeader;
using static System.Runtime.CompilerServices.Unsafe;

namespace DataSpreads.StreamLogs
{
    /// <summary>
    /// A view over stream block buffer.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)] // Two pointer objects
    [DebuggerDisplay("StreamBlock: {" + nameof(DisplayValue) + "}")]
    [BinarySerialization(typeof(Serializer))]
    internal readonly unsafe partial struct StreamBlock : IDisposable
    {
        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
        internal readonly struct StreamBlockHeader
        {
            public const int Size = 128;

            #region AppState (1st cache line) - not part of data, temp storage whole block is active (not packed)

            //

            public const int InitTimestampOffset = 0;

            [FieldOffset(InitTimestampOffset)]
            public readonly Timestamp InitTimestamp;

            //
            public const int PreviousChecksumOffset = 8;

            [FieldOffset(PreviousChecksumOffset)]
            public readonly uint PreviousChecksum;

            //
            public const int ReservedStateIntOffset = 12;

            [FieldOffset(ReservedStateIntOffset)]
            public readonly short ReservedStateInt;

            //
            public const int PreviousHashOffset = 16;

            [FieldOffset(PreviousHashOffset)]
            public readonly Hash16 PreviousHash;

            //
            public const int HashCountOffset = 32;

            [FieldOffset(HashCountOffset)]
            public readonly int HashCount;

            //
            public const int HashChecksumOffset = 36;

            [FieldOffset(HashChecksumOffset)]
            public readonly uint HashChecksum;

            // Note that we could reuse it after count > 0
            public const int PreviousBlockLastTimestampOffset = 40;

            [FieldOffset(PreviousBlockLastTimestampOffset)]
            public readonly uint PreviousBlockLastTimestamp;

            // Reserved 2*8 slots

            #endregion AppState (1st cache line) - not part of data, temp storage whole block is active (not packed)

            #region Additional data

            //
            public const int VersionAndFlagsOffset = 64;

            [FieldOffset(VersionAndFlagsOffset)]
            public readonly VersionAndFlags VersionAndFlags;

            //
            public const int AdditionalFlagsOffset = 65;

            [FieldOffset(AdditionalFlagsOffset)]
            public readonly byte AdditionalFlags;

            //
            public const int ItemFixedSizeOffset = 66;

            [FieldOffset(ItemFixedSizeOffset)]
            public readonly short ItemFixedSize;

            //
            public const int PayloadLengthOffset = 68;

            [FieldOffset(PayloadLengthOffset)]
            public readonly int PayloadLength;

            //
            public const int StreamLogIdOffset = 72;

            [FieldOffset(StreamLogIdOffset)]
            public readonly StreamLogId StreamLogId;

            //
            public const int FirstVersionOffset = 80;

            public const int Nonce12Offset = FirstVersionOffset;

            [FieldOffset(FirstVersionOffset)]
            public readonly ulong FirstVersion;

            //

            public const int CountOffset = 88;

            [FieldOffset(CountOffset)]
            public readonly int Count;

            //

            public const int ChecksumOffset = 92;

            [FieldOffset(ChecksumOffset)]
            public readonly uint Checksum;

            //

            public const int WriteEndTimestampOffset = 96;

            [FieldOffset(WriteEndTimestampOffset)]
            public readonly Timestamp WriteEnd;

            //

            public const int HashOffset = 104;

            [FieldOffset(HashOffset)]
            public readonly Hash16 Hash;

            //

            public const int PkContextIdOffset = 120;

            [FieldOffset(PkContextIdOffset)]
            public readonly uint PkContextId;

            //

            public const int ItemDthOffset = 124;

            [FieldOffset(ItemDthOffset)]
            public readonly DataTypeHeader ItemDth;

            #endregion Additional data
        }

        // 1. Cached pointer is significantly faster.
        // 2. It is not the same as _manager.Pointer, _manager is stored for disposing and could
        //    be null for block view that do not own a ref count.
        internal readonly byte* Pointer;

        /// <summary>
        /// Memory manager for the underlying block. Could be null for read-only views.
        /// </summary>
        internal readonly RetainableMemory<byte> _manager;

        /// <summary>
        /// Use this ctor when:
        /// * a reference is already atomically incremented with IsIndexedStreamBlock flag checked.
        /// * we only create a temp view to read existing data (memoryManager must be null and expectedSlid must be default to avoid checks)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamBlock(DirectBuffer blockBuffer,
            RetainableMemory<byte> memoryManager = null,
            StreamLogId expectedSlid = default,
            int expectedValueSize = 0,
            ulong expectedFirstVersion = 0)
        {
            Debug.Assert(SizeOf<StreamBlock>() == 16);
            Debug.Assert(SizeOf<StreamBlockHeader>() == 128);

            if (memoryManager != null)
            {
                if (expectedSlid == default)
                {
                    ThrowSlidZeroWithNonNullManager();
                }

                Debug.Assert(BitUtil.IsAligned((long)blockBuffer.Data, 8));

                if (memoryManager.ReferenceCount < 1)
                {
                    FailWrongRefCount(memoryManager.ReferenceCount);
                }

                if (memoryManager is SharedMemory sm)
                {
                    // We could open packed block but only if we increment ref count before
                    if ((sm.Header.FlagsCounter & ~HeaderFlags.IsDisposed & ~HeaderFlags.IsPackedStreamBlock) != 0)
                    {
                        // has other flags than IsIndexedStreamBlock
                        ThrowWrongFlags(sm.Header.FlagsCounter);
                    }
                }
            }

            var isValid = blockBuffer.IsValid;

            _manager = memoryManager;
            Pointer = blockBuffer.Data;

#if DETECT_LEAKS
            _finalizeChecker = new PanicOnFinalize();
#endif

            if (expectedSlid != default)
            {
                // cast checks that slid is valid
                var expectedStreamId = (long)expectedSlid;

                if (expectedStreamId != 0 && StreamLogIdLong != expectedStreamId)
                {
                    FailWrongStreamId(expectedStreamId);
                }

                if (!expectedSlid.IsUser && ItemFixedSize != StreamLogNotification.Size)
                {
                    isValid = false;
                }
            }

            if (expectedValueSize > 0 && expectedValueSize != ItemFixedSize)
            {
                FailWrongValueSize(expectedValueSize);
            }

            var firstVersion = FirstVersion;
            if (expectedFirstVersion > 0 && expectedFirstVersion != firstVersion)
            {
                FailWrongFirstVersion(expectedFirstVersion);
            }

            // NB actual buffer could be bigger,
            // but we deal with it as if it has original buffer length
            // Indexes for var-sized values are stored before the end of the original buffer

            if (blockBuffer.Length < Length)
            {
                FailWrongBufferLength(blockBuffer.Length);
            }

            if (!isValid)
            {
                // clear so that Dispose has no effect and IsValid returns false.
                _manager = default;
                Pointer = default;
            }
        }

        [Obsolete("Use overload with DirectBuffer or remove this attribute if we really need RM")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamBlock(RetainedMemory<byte> retainedMemory,
            StreamLogId expectedSlid,
            int expectedValueSize = 0,
            ulong expectedFirstVersion = 0) : this(new DirectBuffer(retainedMemory), retainedMemory._manager, expectedSlid, expectedValueSize, expectedFirstVersion)
        {
            // we take ownership over retainedMemory, it no longer exists
            retainedMemory.Forget();
        }

        private string DisplayValue => IsValid ? $"SB: Sid: {StreamLogIdLong}, FV: {FirstVersion}, ptrmod: {(((IntPtr)Pointer).ToInt64() % 1000)}" : "SB.Invalid";

        /// <summary>
        /// Try initialize a new empty block without knowledge about previous block,
        /// e.g. for the first block in a stream log or during preparation when the
        /// previous block is not completed and we do not have previous checksum
        /// and last Timestamp.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static bool TryInitializeEmpty(DirectBuffer blockBuffer, StreamLog streamLog)
        {
            return TryInitialize(blockBuffer, streamLog.Slid, streamLog.BlockInitialVersionAndFlags, streamLog.ItemFixedSize, default, default, default);
        }

        /// <summary>
        /// For tests, old signature
        /// </summary>
        /// <param name="blockBuffer"></param>
        /// <param name="slid"></param>
        /// <param name="itemFixedSize"></param>
        /// <param name="firstVersion"></param>
        /// <returns></returns>
        internal static bool TryInitialize(DirectBuffer blockBuffer,
            StreamLogId slid,
            short itemFixedSize,
            ulong firstVersion)
        {
            return TryInitialize(blockBuffer, slid, default, itemFixedSize, firstVersion, default, default);
        }

        /// <summary>
        /// Try to set initial <see cref="StreamBlock"/> header values.
        /// </summary>
        /// <param name="blockBuffer">Memory used for <see cref="StreamBlock"/>. Only a Pow2 span of <see cref="SharedMemory"/>> if length is above <see cref="SharedMemoryPool.Pow2ModeSizeLimit"/></param>
        /// <param name="slid"></param>
        /// <param name="blockFlags"></param>
        /// <param name="itemFixedSize"></param>
        /// <param name="firstVersion"></param>
        /// <param name="previousChecksum"></param>
        /// <param name="previousBlockLastTimestamp">Used to track timestamp order by <see cref="DataStreamWriter{T}"/></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static bool TryInitialize(DirectBuffer blockBuffer, 
            StreamLogId slid,
            VersionAndFlags blockFlags,
            short itemFixedSize,
            ulong firstVersion,
            uint previousChecksum,
            Timestamp previousBlockLastTimestamp)
        {
            var len = blockBuffer.Length;
            if (len > SharedMemoryPool.Pow2ModeSizeLimit && !BitUtil.IsPowerOfTwo(len) && slid != StreamLogId.Log0Id) // TODO Pow2 now only limited to Log0
            {
                BuffersThrowHelper.ThrowBadLength();
            }

            var initializedInThisCall = false;
            var wasInitialized = false;

            // cast will throw if default
            var streamId = (long)slid;

            // TODO CAS is overkill, but replace it with read/write only after auditing that this method is called without races.
            // Note that this a call per block rotation so could keep CAS just to be safe.

            var existingStreamId = Interlocked.CompareExchange(
                ref *(long*)(blockBuffer.Data + StreamBlockHeader.StreamLogIdOffset),
                streamId,
                0L);

            if (existingStreamId == 0)
            {
                initializedInThisCall = true;
            }
            else
            {
                if (existingStreamId != streamId)
                {
                    ThrowHelper.ThrowInvalidOperationException(
                        $"Existing stream id is not zero [{existingStreamId}] and is not equal to the provided one [{streamId}].");
                }
            }

            if (firstVersion != 0)
            {
                var existingVersion = unchecked((ulong)Interlocked.CompareExchange(
                    ref *(long*)(blockBuffer.Data + StreamBlockHeader.FirstVersionOffset),
                    unchecked((long)firstVersion),
                    0L));

                if (existingVersion == 0L)
                {
                    initializedInThisCall = true;
                }
                else
                {
                    if (existingVersion != firstVersion)
                    {
                        ThrowHelper.ThrowInvalidOperationException(
                            $"Existing first version is not zero [{existingVersion}] and is not equal to the provided one [{firstVersion}].");
                    }
                    else
                    {
                        initializedInThisCall = false;
                        wasInitialized = true;
                    }
                }
            }

            if (initializedInThisCall)
            {
                // keep <types>
                WriteUnaligned<VersionAndFlags>(blockBuffer.Data + StreamBlockHeader.VersionAndFlagsOffset, blockFlags);
                WriteUnaligned<int>(blockBuffer.Data + StreamBlockHeader.PayloadLengthOffset, blockBuffer.Length - StreamBlockHeader.Size);
                WriteUnaligned<long>((blockBuffer.Data + StreamBlockHeader.InitTimestampOffset), (long)ProcessConfig.CurrentTime);
                    
                // Checksum is updated in-place on commit, but we must store initial value for validation
                WriteUnaligned<uint>((blockBuffer.Data + StreamBlockHeader.ChecksumOffset), previousChecksum);
                WriteUnaligned<uint>((blockBuffer.Data + StreamBlockHeader.PreviousChecksumOffset), previousChecksum);
                WriteUnaligned<short>((short*)(blockBuffer.Data + StreamBlockHeader.ItemFixedSizeOffset), itemFixedSize);

                WriteUnaligned<Timestamp>((blockBuffer.Data + StreamBlockHeader.PreviousBlockLastTimestampOffset), previousBlockLastTimestamp);
            }
            else if (wasInitialized)
            {
#if DEBUG
                Debug.Assert(ReadUnaligned<int>(blockBuffer.Data + StreamBlockHeader.PayloadLengthOffset) + StreamBlockHeader.Size == blockBuffer.Length);
                Debug.Assert(ReadUnaligned<short>((short*)(blockBuffer.Data + StreamBlockHeader.ItemFixedSizeOffset)) == checked((short)itemFixedSize));
#endif
            }

            return initializedInThisCall;
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static bool GetIsInitialized(DirectBuffer blockBuffer, StreamLogId slid, short itemFixedSize,
            ulong firstVersion, uint previousChecksum = 0)
        {
            // cast will throw if default
            var streamId = (long)slid;

            var existingStreamId = Read<long>((long*)(blockBuffer.Data + StreamBlockHeader.StreamLogIdOffset));

            if (existingStreamId == 0)
            {
                return false;
            }
            else
            {
                if (existingStreamId != streamId)
                {
                    ThrowHelper.ThrowInvalidOperationException($"Existing stream id is not zero [{existingStreamId}] and is not equal to the provided one [{streamId}].");
                }
            }

            var existingValueSize =
                ReadUnaligned<short>((short*)(blockBuffer.Data + StreamBlockHeader.ItemFixedSizeOffset));
            if (itemFixedSize != existingValueSize)
            {
                ThrowHelper.ThrowInvalidOperationException($"Existing value size is not zero [{existingValueSize}] and is not equal to the provided one [{itemFixedSize}].");
            }

            if (firstVersion != 0)
            {
                var existingVersion = Read<ulong>((ulong*)(blockBuffer.Data + StreamBlockHeader.FirstVersionOffset));

                if (existingVersion == 0UL)
                {
                    return false;
                }
                else
                {
                    if (existingVersion != firstVersion)
                    {
                        ThrowHelper.ThrowInvalidOperationException($"Existing first version is not zero [{existingVersion}] and is not equal to the provided one [{firstVersion}].");
                    }
                }
            }

            return true;
        }

        public void Dispose()
        {
#if DETECT_LEAKS
            _finalizeChecker?.Dispose();
#endif
            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            _manager?.Decrement();
        }

        internal int ReferenceCount
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _manager.Pointer != null ? _manager.ReferenceCount : -1;
        }

        internal SharedMemory SharedMemory
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _manager as SharedMemory;
        }

        // TODO set releaseMemory to true when packer is disabled, otherwise will consume all RAM eventually
        [Obsolete("Only for cursors that own SharedMemory object")]
        internal void DisposeFree(bool releaseMemory = false)
        {
            Debug.Assert(!IsValid || _manager != null, "DisposeFree must not be called on a StreamBlock view");
#if DETECT_LEAKS
            _finalizeChecker?.Dispose();
#endif
            if (_manager is SharedMemory sm)
            {
                // was reserved: cannot pack until this instance release its rc
                var remaining = sm.Decrement();

                // No one is using the block, it is only owned by SBI.
                if (releaseMemory && remaining == 1)
                {
                    DirectFile.ReleaseMemory(sm.NativeBuffer);
                }

                Debug.Assert(remaining > 0); // TODO rework. This is always true, otherwise will throw because flags won't allow to return to the pool.
                if (remaining > 0)
                {
                    SharedMemory.Free(sm);
                }
            }
            else
            {
                Dispose();
            }
        }

#if DETECT_LEAKS

        internal class PanicOnFinalize : IDisposable
        {
            public bool Disposed;
            public string Callstack = Environment.StackTrace;

            ~PanicOnFinalize()
            {
                if (Disposed)
                {
                    // sanity check
                    ThrowHelper.ThrowInvalidOperationException(
                        $"Finalizer was called despite being disposed: {Callstack}");
                }
                else
                {
                    ThrowHelper.ThrowInvalidOperationException(
                        $"Chunk was not properly disposed and is being finalized: {Callstack}");
                }
            }

            public void Dispose()
            {
                if (Disposed)
                {
                    ThrowHelper.ThrowInvalidOperationException(
                        $"Chunk was already disposed. {Callstack}");
                }
                GC.SuppressFinalize(this);
                Disposed = true;
            }
        }

        internal readonly PanicOnFinalize _finalizeChecker;
#endif

        public const int DataOffset = StreamBlockHeader.Size;

        public bool IsValid
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Pointer != null;
        }

        // TODO remove volatile, review if race is ever possible
        public StreamLogId StreamLogId
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (StreamLogId)Volatile.Read(ref *(long*)(Pointer + StreamBlockHeader.StreamLogIdOffset));
        }

        internal long StreamLogIdLong
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref *(long*)(Pointer + StreamBlockHeader.StreamLogIdOffset));
        }

        // TODO remove volatile, review usages where race is possible (TryInit?)
        // TODO at least Rename to volatile and read normally in claim
        public ulong FirstVersion
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return Volatile.Read(ref *(ulong*)(Pointer + StreamBlockHeader.FirstVersionOffset));
            }
        }

        // TODO Review usages and replace with non-volatile where it is safe
        public int CountVolatile
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                var count = Volatile.Read(ref *(int*)(Pointer + StreamBlockHeader.CountOffset));
                return count;
            }
        }

        public int Count
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                var count = *(int*)(Pointer + StreamBlockHeader.CountOffset);
                return count;
            }
        }

        public uint Checksum
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                var checksum = *(uint*)(Pointer + StreamBlockHeader.ChecksumOffset);
                return checksum;
            }
        }

        private uint PreviousChecksum
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                var checksum = Volatile.Read(ref *(uint*)(Pointer + StreamBlockHeader.PreviousChecksumOffset));
                return checksum;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ValidateChecksum()
        {
            var checksum = PreviousChecksum;
            var count = CountVolatile;
            for (int i = 0; i < count; i++)
            {
                unchecked
                {
                    var ptr = DangerousGetPointer(i, out var len);
                    checksum = Crc32C.CalculateCrc32C(ptr, len, checksum);
                }
            }
            return checksum == Checksum;
        }

        // TODO remove volatile
        // TODO review usages - this should be payload length
        // TODO Rename to Payload length when adjusted usage or to BufferLength
        internal int Length
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return (StreamBlockHeader.Size) + Volatile.Read(ref *(int*)(Pointer + StreamBlockHeader.PayloadLengthOffset));
            }
        }

        internal int PayloadLength
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return *(int*)(Pointer + StreamBlockHeader.PayloadLengthOffset);
            }
        }

        internal StreamBlockHeader Header
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ReadUnaligned<StreamBlockHeader>(Pointer);
        }

        public VersionAndFlags VersionAndFlags
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return Read<VersionAndFlags>((VersionAndFlags*)(Pointer + StreamBlockHeader.VersionAndFlagsOffset));
            }
        }

        public short ItemFixedSize
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return Read<short>((short*)(Pointer + StreamBlockHeader.ItemFixedSizeOffset));
            }
        }

        // TODO remove volatile
        // TODO rename to Init...
        public Timestamp WriteStart
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return (Timestamp)Volatile.Read(ref *(long*)(Pointer + StreamBlockHeader.InitTimestampOffset));
            }
        }

        public Timestamp WriteEnd
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return (Timestamp)Volatile.Read(ref *(long*)(Pointer + StreamBlockHeader.WriteEndTimestampOffset));
            }
        }

        private long WriteEndLongVolatile
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return Volatile.Read(ref *(long*)(Pointer + StreamBlockHeader.WriteEndTimestampOffset));
            }
        }

        public Hash16 Hash
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return Read<Hash16>(Pointer + StreamBlockHeader.HashOffset);
            }
        }

        public uint PkContextId
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return Read<uint>(Pointer + StreamBlockHeader.PkContextIdOffset);
            }
        }

        public ref DataTypeHeader ItemDth
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return ref As<byte, DataTypeHeader>(ref *(Pointer + StreamBlockHeader.PkContextIdOffset));
            }
        }

        public bool IsCompleted
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return WriteEndLongVolatile != 0;
            }
        }

        public bool IsInitialized
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => FirstVersion != 0;
        }

        public bool IsRented
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _manager != null;
        }

        /// <summary>
        /// First byte after free space.
        /// </summary>
        public int EndOffset
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (ItemFixedSize > 0)
                {
                    return Length;
                }
                else
                {
                    // for var-length types we keep index at the end of the term
                    // +1 because we reserve space for the next value and use EndOffset - Offset to check if we have space
                    return Length - 4 * (CountVolatile + 1);
                }
            }
        }

        /// <summary>
        /// Current position where next item will be written.
        /// </summary>
        public int Offset
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => GetNthOffset(CountVolatile);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetNthOffset(int index)
        {
            if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

            if (ItemFixedSize > 0)
            {
                return DataOffset + index * ItemFixedSize;
            }
            else
            {
                if (index == 0)
                {
                    return DataOffset;
                }
                var nextValueOffset = *(int*)(Pointer + Length - index * 4);
                return nextValueOffset;
            }
        }

        public ulong CurrentVersion
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                // TODO review if this two memory reads and if we could make them one
                // FV should not be volatile here

                return *(ulong*)(Pointer + StreamBlockHeader.FirstVersionOffset) + (ulong)CountVolatile - 1;
            }
        }

        internal ulong NextVersion
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

                return FirstVersion + (ulong)CountVolatile;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Memory<byte> ClaimMemory(ulong version, int claimLength)
        {
            var db = Claim(version, claimLength);
            if (!db.IsValid)
            {
                return Memory<byte>.Empty;
            }
            var memOffset = checked((int)((long)db._pointer - (long)_manager.Pointer));
            var mem = _manager.Memory.Slice(memOffset, db.Length);
            return mem;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DirectBuffer Claim(ulong version, int claimLength)
        {
            // TODO review why this check is needed. It must be done once when creating block. 3% for fixed size
            if (!IsValid)
            {
                return DirectBuffer.Invalid;
            }

            // volatile reads count
            var count = CountVolatile;

            var nextVersion = FirstVersion + (ulong)count; // NextVersion manually inlined to reuse count;

            if (version > 0 && version != nextVersion)
            {
                return DirectBuffer.Invalid;
            }

            var valueSize = ItemFixedSize;

            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (valueSize > 0 && claimLength != valueSize)
                {
                    FailClaimWrongFixedLength();
                }
            }

            int offset;
            if (valueSize > 0)
            {
                // manually inlined GetNthOffset + EndOffset to avoid valueSize branch and Count read gives c.10% perf for fixed size case.
                offset = DataOffset + count * valueSize;
                var endOffset = Length;

                var offsetPlusLength = offset + claimLength;
                if (offsetPlusLength > endOffset || WriteEndLongVolatile != default)
                {
                    return ClaimCompleteReturnInvalid();
                }
            }
            else
            {
                offset = GetNthOffset(count); // Reuse count

                // We could skip checking if WriteEnd is set ONLY FOR FIXED SIZE TYPE because
                // 1) When it is set normally there is not enough space already
                // 2) TODO If we are kicked out here, everything is ignored and commit should fail, we are probably
                //    failing somewhere after this place or after Claim returns and during writes to the buffer
                //    so any check here doesn't help but adds a volatile write
                // But we do not do so to be safe.

                var endOffset = EndOffset;
                // NB > EndOffset not _length
                var offsetPlusLength = offset + claimLength;
                if (offsetPlusLength > endOffset || WriteEndLongVolatile != default)
                {
                    return ClaimCompleteReturnInvalid();
                }

                {
                    // NB! write only after length check, otherwise could corrupt existing
                    *(int*)(Pointer + endOffset) = offsetPlusLength;
                }
            }
            var pointer = Pointer + offset;
            var db = new DirectBuffer(claimLength, pointer);
            return db;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private DirectBuffer ClaimCompleteReturnInvalid()
        {
            Complete();
            return DirectBuffer.Invalid;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowSlidZeroWithNonNullManager()
        {
            ThrowHelper.ThrowInvalidOperationException(
                "expectedSlid could only be default when memoryManager is null. In such case it is used to create a block view to read existing data.");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailClaimWrongFixedLength()
        {
            ThrowHelper.FailFast("length != valueSize");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Commit(int length)
        {
            // EndOffset depends only on Count, which is not updated before commit
            var endOffset = EndOffset;
            var offset = GetNthOffset(CountVolatile);
            var offsetPlusLength = offset + length;
            var reservedoffsetPlusLength = *(int*)(Pointer + endOffset);
            if ((uint)offsetPlusLength > reservedoffsetPlusLength || ItemFixedSize > 0)
            {
                ThrowHelper.FailFast("Committing length larger than reserved or ItemFixedSize > 0");
            }

            // as if we claimed initially the actual length
            *(int*)(Pointer + endOffset) = offsetPlusLength;

            return Commit();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Commit()
        {
            if (AdditionalCorrectnessChecks.Enabled) { EnsurePointerNotNull(); }

            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (WriteEndLongVolatile != default)
                {
                    // NB: Fail fast. If we are alive but someone finalized the chunk then something is wrong.
                    // This should be exceptionally rare. If someone kicked this writer out we ignore what this writer wrote.
                    CommitFailFastCompleted();
                }
            }

            var count = CountVolatile;
            var count1 = (ulong)(count + 1);

            unchecked
            {
                var ptr = DangerousGetPointer(count, out var len);
                var checksum = Checksum;
                var updatedChecksum = (ulong)Crc32C.CalculateCrc32C(ptr, len, checksum);
                // On little-endian count bytes go first
                var commitValue = count1 | (updatedChecksum << 32);

                // TODO review: if we always lock around claim/commit then Volatile/Interlocked is not needed

                if (IntPtr.Size == 8)
                {
                    Volatile.Write(ref *(ulong*)(Pointer + StreamBlockHeader.CountOffset), commitValue);
                }
                else
                {
                    Interlocked.Exchange(ref *(long*)(Pointer + StreamBlockHeader.CountOffset),
                        (long)commitValue);
                }
            }

            // avoid additional Count access, calculate the same way as in CurrentVersion
            var currentVersion = FirstVersion + (ulong)count;

            Debug.Assert(currentVersion == CurrentVersion);

            return currentVersion;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void CommitFailFastCompleted()
        {
            ThrowHelper.FailFast("Commiting to completed block");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        public void Complete()
        {
            // must be idempotent
            if (0 != Interlocked.CompareExchange(ref *(long*)(Pointer + StreamBlockHeader.WriteEndTimestampOffset),
                    (long)ProcessConfig.CurrentTime, 0))
            {
                // Console.WriteLine("Chunk already completed");
                // ThrowHelper.FailFast("Cannot complete chunk, other writer already completed");
            }

            if (StreamLogIdLong != (long)StreamLogs.StreamLogId.Log0Id)
            {
                ClearFreeSpace();
                // TODO rolling checksum is repeatable, we do work twice
                // We do not checksum index at the end because if it's wrong
                // then the rolling checksum will be wrong
                // On complete we should take existing checksum and append header value to it.
                // Complettion happens during rotation and adds latency to already slowish (if pool misses) process
                // CalculateChecksum();
            }
        }

        public DirectBuffer this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (unchecked((uint)index) >= CountVolatile)
                {
                    BuffersThrowHelper.ThrowIndexOutOfRange();
                }

                return DangerousGet(index);
            }
        }

        /// <summary>
        /// Get element at index without bound checks.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal DirectBuffer DangerousGet(int index)
        {
            var pointer = DangerousGetPointer(index, out var length);
            return new DirectBuffer(length, pointer);
        }

        /// <summary>
        /// Get element pointer and length at index without bound checks.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* DangerousGetPointer(int index, out int length)
        {
            int valueSize = ItemFixedSize;
            byte* pointer;
            if (valueSize > 0)
            {
                pointer = Pointer + DataOffset + valueSize * index;
            }
            else
            {
                var len = Length;
                // TODO just add 4, save 2 ops
                var nextValueOffset = *(int*)(Pointer + len - (index + 1) * 4);
                var thisValueOffset = index == 0 ? DataOffset : *(int*)(Pointer + len - index * 4);
                valueSize = nextValueOffset - thisValueOffset;
                if (valueSize <= 0)
                {
                    DangerousGetFailFastWrongOffset();
                }
                pointer = Pointer + thisValueOffset;
            }

            length = valueSize;
            return pointer;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void DangerousGetFailFastWrongOffset()
        {
            ThrowHelper.FailFast("Wrong offset calculation");
        }

        [Obsolete("1. we do chained checksum, free space is not a part of it. 2. Pool must return empty. So we should assert that free space is empty instead of clearing it.")]
        internal void ClearFreeSpace()
        {
            var offset = Offset;
            var endOffset = EndOffset;
            if (endOffset > offset)
            {
                var span = new Span<byte>(Pointer + offset, EndOffset - offset);
                span.Clear();
            }
        }

        #region Binary converter

        public class Serializer : InternalSerializer<StreamBlock>
        {
            public override int SizeOf(in StreamBlock value, BufferWriter bufferWriter)
            {
                return value.Length;
            }

            public override int Write(in StreamBlock value, DirectBuffer destination)
            {
                var sBuffer = new DirectBuffer(value.Length, value.Pointer);

                var spos = 0;
                var dpos = 0;

                var copySize = StreamBlockHeader.Size;
                sBuffer.Span.Slice(spos, copySize).CopyTo(destination.Span.Slice(dpos, copySize));
                spos = dpos = copySize;

                // for converter version = 1 we skip
                copySize = value.Length - DataOffset;
                if (value.ItemFixedSize > 1)
                {
                    var sourceSpan = new Span<byte>(value.Pointer + DataOffset, value.Length - DataOffset);
                    BinarySerializer.Shuffle(sourceSpan, destination.Slice(dpos, copySize).Span, (byte)value.ItemFixedSize);
                }
                else
                {
                    sBuffer.Span.Slice(spos, copySize).CopyTo(destination.Span.Slice(dpos, copySize));
                }

                return value.Length;
            }

            public override int Read(DirectBuffer sBuffer, out StreamBlock value)
            {
                var bufferLength = sBuffer.ReadInt32(StreamBlockHeader.PayloadLengthOffset) + StreamBlockHeader.Size;
                var valueSize = sBuffer.ReadInt16(StreamBlockHeader.ItemFixedSizeOffset);
                var streamId = sBuffer.ReadInt64(StreamBlockHeader.StreamLogIdOffset);

                var retainedMemory = BufferPool.Retain(bufferLength);

                var destination = new DirectBuffer(retainedMemory.Length, (byte*)retainedMemory.Pointer);

                var spos = 0;
                var dpos = 0;

                var copySize = StreamBlockHeader.Size;
                sBuffer.Span.Slice(spos, copySize).CopyTo(destination.Span.Slice(dpos, copySize));
                spos = dpos = copySize;

                copySize = bufferLength - DataOffset;
                if (valueSize > 1)
                {
                    var sourceSpan = new Span<byte>(sBuffer.Data + DataOffset, bufferLength - DataOffset);
                    BinarySerializer.Unshuffle(sourceSpan, destination.Slice(dpos, copySize).Span, (byte)valueSize);
                }
                else
                {
                    sBuffer.Span.Slice(spos, copySize).CopyTo(destination.Span.Slice(dpos, copySize));
                }

                value = new StreamBlock(retainedMemory, (StreamLogId)streamId, valueSize, 0);
                return bufferLength;
            }

            public override short FixedSize => 0;

            public byte ConverterVersion
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => 1;
            }
        }

        #endregion Binary converter

        #region Throw Helpers

        // These conditions indicate wrong usage/code and are not recoverable

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsurePointerNotNull()
        {
            if (Pointer == null)
            {
                ThrowHelper.FailFast("StreamBlock: _pointer == null");
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailWrongRefCount(int refCount)
        {
            ThrowHelper.ThrowInvalidOperationException(
                $"retainedMemory.ReferenceCount {refCount} < 1. At least SL must own the chunk");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowWrongFlags(HeaderFlags flags)
        {
            ThrowHelper.ThrowInvalidOperationException(
                $"Flags {flags} must be IsIndexedStreamBlock");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void FailWrongStreamId(long streamId)
        {
            ThrowHelper.FailFast($"Wrong StreamId of existing block: expected {(StreamLogId)streamId} existing {(StreamLogId)StreamLogIdLong}");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void FailWrongValueSize(int expectedValueSize)
        {
            ThrowHelper.FailFast($"Wrong ValueSize of existing chunk: expected {expectedValueSize} existing {ItemFixedSize}");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void FailWrongFirstVersion(ulong expectedFirstVersion)
        {
            ThrowHelper.FailFast(
                $"Wrong FirstVersion of existing chunk: expected {expectedFirstVersion} existing {FirstVersion}");
        }

        //[MethodImpl(MethodImplOptions.NoInlining)]
        //private void FailNotInitialized()
        //{
        //    ThrowHelper.FailFast($"SLC is not initialized: ValueSize={ValueSize} FirstVersion={FirstVersion}");
        //}

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void FailWrongBufferLength(int length)
        {
            ThrowHelper.FailFast(
                $"Wrong BufferLength of block memory: expected >= {Length} but given {length}");
        }

        #endregion Throw Helpers
    }

    internal static class StreamLogChunkExtensions
    {
        // TODO Remove
    }
}
