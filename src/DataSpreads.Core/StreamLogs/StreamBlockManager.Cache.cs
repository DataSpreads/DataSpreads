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

using DataSpreads.Buffers;
using Spreads;
using Spreads.Collections.Concurrent;
using Spreads.Threading;
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace DataSpreads.StreamLogs
{
    internal partial class StreamBlockManager
    {
        internal readonly ConcurrentDictionary<StreamBlockProxy.CacheKey, GCHandle> BlocksCache = new ConcurrentDictionary<StreamBlockProxy.CacheKey, GCHandle>();
    }

    /// <summary>
    ///
    /// </summary>
    internal sealed class StreamBlockProxy : IDisposable
    {
        private static readonly ObjectPool<StreamBlockProxy> Pool = new ObjectPool<StreamBlockProxy>(() => new StreamBlockProxy());

        /// <summary>
        /// When we prepare or prefetch a block we store it's proxy as an ephemeral property of the last block.
        /// Whenever we set the next value we first must get the key proxy from the cache. If it's not cached do nothing, do not revive it.
        /// </summary>
        private static readonly ConditionalWeakTable<StreamBlockProxy, StreamBlockProxy> NextTable = new ConditionalWeakTable<StreamBlockProxy, StreamBlockProxy>();

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

        private int _rc = AtomicCounter.CountMask; // disposed state

        private BufferRef _bufferRef;

        private CacheKey _key;

        private StreamBlockManager _blockManager;

        [Obsolete]
        internal volatile bool IsSharedMemory;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StreamBlockProxy Create(CacheKey key, StreamBlockManager cache)
        {
            var instance = Pool.Allocate();
            ThrowHelper.AssertFailFast(AtomicCounter.GetIsDisposed(ref instance._rc), "Pooled proxy must be in disposed state.");
            instance._key = key;
            instance._blockManager = cache;
            // cache owns one ref
            instance._rc = 1;
            return instance;
        }

        public StreamBlockProxy Next => NextTable.TryGetValue(this, out var next) ? next : default;


        [Obsolete("This methods assumes possible existence of multiple proxies for the same block, which is wrong")]
        public bool TrySetNext(StreamBlockProxy next)
        {
            lock (this)
            {
                if (!NextTable.TryGetValue(this, out _))
                {
                    NextTable.Add(this, next);
                    return true;
                }

                return false;
            }
        }

        public void SetStreamBlock(StreamBlock newBlock)
        {
            // The new block is already rented from shared memory,
            // this object owns the reference on behalf of the current process.
            // Proxy usage is tracked by _rc field.

            lock (this)
            {
                if (AtomicCounter.GetIsDisposed(ref _rc))
                {
                    // because we inside the lock that protects from concurrent call to dispose,
                    // the proxy could be disposed by now and the new block is not needed
#pragma warning disable 618
                    newBlock.DisposeFree();
#pragma warning restore 618
                }
                else
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
        }

        /// <summary>
        /// Returns null as false in Try... pattern.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public StreamBlockProxy TryRetain()
        {
            var current = AtomicCounter.IncrementIfRetained(ref _rc);
            return current <= 0 ? null : this;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool disposing)
        {
            if (!IsSharedMemory)
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
                if (!disposing)
                {
                    // Cache has a weak reference, so finalizer could start running while a handle is still in the cache
                    // As if _rc was 1 and we called DecrementIfOne - if successful, no resurrect is possible
                    // because Retain uses IncrementIfRetained.

                    var current = Volatile.Read(ref _rc);
                    var existing = Interlocked.CompareExchange(ref _rc, 0, current);
                    if (existing != current)
                    {
                        // Resurrected while we tried to set rc to zero.
                        // What if rc was wrong and not 1? At some point all new users will
                        // dispose the proxy and it will be in the cache with
                        // positive rc but without GC root, then it will be
                        // collected and finalized and will return to this
                        // place where we will try to set rc to 0 again.
                        // TODO trace this condition, it indicates dropped proxies
                        // From user code it could be possible only when manually using cursors
                        // and forgetting to dispose them, so all blame is on users, but we should not fail.

                        ThrowHelper.AssertFailFast(existing > 1, "existing > 1 when resurrected");
                        return;
                    }
                }
                else
                {
                    var remaining = AtomicCounter.Decrement(ref _rc);
                    if (remaining > 1)
                    {
                        return;
                    }

                    if (AtomicCounter.DecrementIfOne(ref _rc) != 0)
                    {
                        return;
                    }
                }

                ThrowHelper.AssertFailFast(_rc == 0, "_rc must be 0 to proceed with proxy disposal");

                try
                {
                    // remove self from cache
                    _blockManager.BlocksCache.TryRemove(_key, out var handle);
                    if (handle.IsAllocated)
                    {
                        handle.Free();
                    }
                }
                finally
                {
                    // If we are shutting down, e.g. unhandled exception in other threads
                    // increase the chances we do release shared memory ref with try/finally.

#pragma warning disable 618
                    // ReSharper disable once InconsistentlySynchronizedField
                    Block.DisposeFree();
#pragma warning restore 618
                }

                // Do not pool finalized objects.
                // TODO (review) proxy does not have ref type fields,
                // probably could add to pool without thinking about GC/finalization order.
                // However, this case should be very rare (e.g. unhandled exception)
                // and we care about releasing RC of shared memory above all.
                if (disposing)
                {
                    GC.SuppressFinalize(this);
                    // ReSharper disable once InconsistentlySynchronizedField
                    Block = default;
                    _key = default;
                    AtomicCounter.Dispose(ref _rc);
                    IsSharedMemory = default;
                }
            }
        }

        ~StreamBlockProxy()
        {
            Dispose(false);
        }

        public readonly struct CacheKey : IEquatable<CacheKey>
        {
            public readonly StreamLogId Slid;
            public readonly ulong Version;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public CacheKey(StreamLogId slid, ulong version)
            {
                Slid = slid;
                Version = version;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Equals(CacheKey other)
            {
                return Slid.Equals(other.Slid) && Version == other.Version;
            }

            public override bool Equals(object obj)
            {
                return obj is CacheKey other && Equals(other);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (Slid.GetHashCode() * 397) ^ Version.GetHashCode();
                }
            }
        }
    }
}
