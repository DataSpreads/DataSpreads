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

using DataSpreads.Storage;
using Spreads;
using Spreads.Collections.Concurrent;
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace DataSpreads.StreamLogs
{
    internal class ReaderBlockCache
    {
        // GCHandle holds a weak reference to StreamBlockProxy.
        // Readers (SLCursors) keep a strong reference.
        // When all readers go out of scope SBP is finalized.
        // SBP has a single count in SB ref count.

        private readonly ConcurrentDictionary<BlockKey, GCHandle> _blocks = new ConcurrentDictionary<BlockKey, GCHandle>();
        private readonly StreamLogManager _streamLogManager;

        // private readonly Func<BlockKey,GCHandle> _factoryFuncX;

        public ReaderBlockCache(StreamLogManager streamLogManager)
        {
            _streamLogManager = streamLogManager;
            // _factoryFunc = HandleFactory;
        }

        private StreamBlockIndex BlockIndex => _streamLogManager.BlockIndex;
        // private IStreamBlockStorage BlockStorage => _streamLogManager.BlockIndex.BlockStorage;

        public StreamBlockProxy TryRentIndexedStreamBlockProxy(StreamLog streamLog, StreamBlockRecord record)
        {
            StreamBlockProxy p = null;
            while (p == null)
            {
                var blockKey = new BlockKey(streamLog.Slid, record.Version);
                // ReSharper disable once InconsistentlySynchronizedField
                if (_blocks.TryGetValue(blockKey, out var handle))
                {
                    p = handle.Target as StreamBlockProxy;
                }
                else
                {
                    // CD.GetOrAdd factory is not atomic, lock manually.
                    // Lock on SL, not a global object. This also helps 
                    // to avoid a capturing lambda and the risk of collecting
                    // an object held only by a weak reference during that lambda return.
                    lock (streamLog)
                    {
                        if (_blocks.TryGetValue(blockKey, out handle))
                        {
                            p = handle.Target as StreamBlockProxy;
                        }
                        else
                        {
                            p = StreamBlockProxy.Create(blockKey, this);

#pragma warning disable 618
                            var sb = BlockIndex.TryRentIndexedStreamBlock(streamLog, record.BufferRef); // TODO review if we should pass record.Version
#pragma warning restore 618

                            // Could set directly without SetStreamBlock
                            p.Block = sb;
                            handle = GCHandle.Alloc(p, GCHandleType.Weak);
                            if (!_blocks.TryAdd(blockKey, handle))
                            {
                                // this should never happen unless there are different streamLog instances for the same slid,
                                // which in turn should never happen because SLs are stored in SLM dictionary.
                                ThrowHelper.FailFast("Cannot add newly created StreamBlockProxy to cache from inside a lock.");
                            }
                        }
                    }
                }
            }

            return p;
        }

        public readonly struct BlockKey : IEquatable<BlockKey>
        {
            public readonly StreamLogId Slid;
            public readonly ulong Version;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public BlockKey(StreamLogId slid, ulong version)
            {
                Slid = slid;
                Version = version;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(BlockKey other)
            {
                return Slid.Equals(other.Slid) && Version == other.Version;
            }

            public override bool Equals(object obj)
            {
                return obj is BlockKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (Slid.GetHashCode() * 397) ^ Version.GetHashCode();
                }
            }
        }

        public sealed class StreamBlockProxy : IDisposable
        {
            private static ObjectPool<StreamBlockProxy> _pool = new ObjectPool<StreamBlockProxy>(() => new StreamBlockProxy());

            private BlockKey _key;
            private ReaderBlockCache _cache;

            // 1. SB has 2 pointer+reference fields, each of them is
            // assigned atomically, only the entire struct could be torn.
            // Reader normal operations depend only on the pointer.
            // The reference to memory manager is only used during dispose/free.
            // Using lock in dispose and replace method does not affect
            // normal read operations.
            // 2. Public mutable field could be replaced by ref readonly property,
            // but for now just avoid setting it other then in cache factory and 
            // SetStreamBlock method. It's hard to do by accident,
            // while access to SB is performance critical.
            public StreamBlock Block;

            private int _rc;

            private volatile bool _isSharedMemory;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static StreamBlockProxy Create(BlockKey key, ReaderBlockCache cache)
            {
                var instance = _pool.Allocate();
                instance._key = key;
                instance._cache = cache;
                return instance;
            }

            public void SetStreamBlock(StreamBlock newBlock)
            {
                // The new block is already rented from shared memory,
                // this object owns the reference on behalf of the current process.
                // Proxy usage is tracked by _rc field.

                lock (this)
                {
                    var previous = Block;
                    Block = newBlock;
                    if (previous.IsValid)
                    {
#pragma warning disable 618
                        previous.DisposeFree();
#pragma warning restore 618
                    }
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public StreamBlockProxy Retain()
            {
                var current = Interlocked.Increment(ref _rc);
                ThrowHelper.AssertFailFast(current > 0, "current > 0");
                return this;
            }

            public void Dispose()
            {
                Dispose(true);
            }

            public void Dispose(bool disposing)
            {
                if (!_isSharedMemory)
                {
                    DoDispose();
                }
                else
                {
                    lock (this)
                    {
                        DoDispose();
                    }
                }

                void DoDispose()
                {
                    var remaining = Interlocked.Decrement(ref _rc);
                    ThrowHelper.AssertFailFast(remaining >= 0, "remaining >= 0"); // TODO replace with trace or add && disposing
                    if (remaining == 0 || !disposing)
                    {
                        // remove self from cache
                        _cache._blocks.TryRemove(_key, out var handle);
                        if (handle.IsAllocated)
                        {
                            handle.Free();
                        }

#pragma warning disable 618
                        // ReSharper disable once InconsistentlySynchronizedField
                        Block.DisposeFree();
#pragma warning restore 618

                        if (disposing)
                        {
                            GC.SuppressFinalize(this);
                            // ReSharper disable once InconsistentlySynchronizedField
                            Block = default;
                            _key = default;
                            _pool = default;
                            _rc = default;
                            _isSharedMemory = default;
                        }
                    }
                }
            }

            ~StreamBlockProxy()
            {
                Dispose(false);
            }
        }
    }
}
