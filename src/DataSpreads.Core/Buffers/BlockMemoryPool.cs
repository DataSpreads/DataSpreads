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

using Spreads.Buffers;
using Spreads.LMDB;
using Spreads.Threading;
using Spreads.Utils;
using System.Runtime.CompilerServices;
using DataSpreads.StreamLogs;
using Spreads;

namespace DataSpreads.Buffers
{
    internal sealed unsafe class BlockMemoryPool : SharedMemoryPool
    {
        internal BlockMemoryPool(string path, uint maxLogSizeMb,
            LMDBEnvironmentFlags envFlags,
            Wpid ownerId,
            int maxBufferLength = RmDefaultMaxBufferLength,
            int maxBuffersPerBucket = RmDefaultMaxBuffersPerBucket)
            : base(path, maxLogSizeMb, envFlags, ownerId, maxBufferLength, maxBuffersPerBucket, rentAlwaysClean: true)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static DirectBuffer NativeToStreamBlockBuffer(DirectBuffer nativeBuffer, bool pow2Payload)
        {
            var memoryLength = nativeBuffer.Length - SharedMemory.BufferHeader.Size;
            int blockLen;
            byte* blockPtr;
            if (memoryLength > Pow2ModeSizeLimit)
            {
                if (pow2Payload)
                {
                    blockLen = StreamBlock.StreamBlockHeader.Size + BitUtil.FindPreviousPositivePowerOfTwo(memoryLength);
                    blockPtr = nativeBuffer.Data + SharedMemory.BufferHeader.Size + (memoryLength - blockLen);
                }
                else
                {
                    // When pow2Payload = false then the entire StreamBlock (including the header)
                    // fits a pow2 buffer so if we unpack it into a RAM-backed pooled buffer 
                    // we do not waste 50% of the buffer space.
                    blockLen = BitUtil.FindPreviousPositivePowerOfTwo(memoryLength);
                    blockPtr = nativeBuffer.Data + SharedMemory.BufferHeader.Size + (memoryLength - blockLen);
                }
            }
            else
            {
                if (pow2Payload)
                {
                    ThrowHelper.ThrowNotSupportedException();
                }
                blockLen = memoryLength;
                blockPtr = nativeBuffer.Data + SharedMemory.BufferHeader.Size;
            }

            var blockBuffer = new DirectBuffer(blockLen, blockPtr);
            return blockBuffer;
        }
    }
}
