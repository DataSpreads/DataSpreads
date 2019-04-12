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
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Spreads;
using Spreads.Collections.Concurrent;
using Spreads.Collections.Internal;

namespace DataSpreads.Storage.Experimental
{
    [Obsolete]
    internal readonly struct DataBlockKey : IEquatable<DataBlockKey>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DataBlockKey(long streamId, long blockKey)
        {
            StreamId = streamId;
            BlockKey = blockKey;
        }

        public long StreamId { [MethodImpl(MethodImplOptions.AggressiveInlining)] get; }

        public long BlockKey { [MethodImpl(MethodImplOptions.AggressiveInlining)] get; }

        public bool Equals(DataBlockKey other)
        {
            return StreamId == other.StreamId && BlockKey == other.BlockKey;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            return obj is DataBlockKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (StreamId.GetHashCode() * 397) ^ BlockKey.GetHashCode();
            }
        }
    }

    // TODO we have to implement IMutableSeries<TKey,WeakReference<DataBlock>> from scratch. We already had very similar thing in SQLiteProvider and RawPersistentChunksSeries
    // Problem with that solution was too tight coupling with "Provider"

    // * we need to guarantee that one logical block is always one instance, atomic GetOrAdd
    // * we do not have a method to clean a particular block from cache so it is either GC or timer scanning or keeping all and dispose
    // * main use case is one-pass computation, so prefer to keep minimum live blocks
    //   which leads to the first issue of always returning the same instance.
    //

    // Design notes:
    // Most methods should be implemented in the base class.
    // A virtual method must take DataBlock from storage. Inside this method
    // we call the appropriate storage method depending on implementation.
    // We could start with strongly-typed containers and implement raw-to-storage deserialization directly.
    // Then we need to review IDataView and Schema from ML.NET, adapt Vec.RuntimeTypeId to use a correct type
    // and use schema to deserialize raw bytes.
    // Open issues:
    // * column names for partial access

    internal abstract class BlockSeries<TRowKey> : IMutableSeries<TRowKey, DataBlock> // TODO mutable series
    {
        internal readonly LockedWeakDictionary<long> _blockCache = new LockedWeakDictionary<long>();

        internal IContainerBlockStorage _blockStorage;

        internal readonly KeyComparer<TRowKey> _comparer;
        internal long _streamId;

        public BlockSeries(long streamId, KeyComparer<TRowKey> comparer, IContainerBlockStorage blockStorage)
        {
            _streamId = streamId;
            _comparer = comparer;
            _blockStorage = blockStorage;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private TRowKey LongToKey(long key)
        {
            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            return _comparer.Add(default(TRowKey), key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long KeyToLong(TRowKey key)
        {
            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            return _comparer.Diff(key, default(TRowKey));
        }

        #region Cache logic

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool GetDataBlockFromCache(long lk, out DataBlock block)
        {
            if (_blockCache.TryGetValue(lk, out var value))
            {
                if (value is DataBlock b)
                {
                    block = b;
                    return true;
                }

                FailWrongCacheBehavior();
            }

            block = null;
            return false;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailWrongCacheBehavior()
        {
            ThrowHelper.FailFast(
                "Assumption about or implementation of LockedWeakDictionary is wrong. TGV returning true must return a live object.");
        }

        private DataBlock GetDataBlock(TRowKey key)
        {
            var lk = KeyToLong(key);
            if (GetDataBlockFromCache(lk, out var block))
            {
                return block;
            }

            return GetDataBlockSlow(lk);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private DataBlock GetDataBlockSlow(long lk)
        {
            // Note that we lock on this, reads on _blockCache could continue
            lock (this)
            {
                if (GetDataBlockFromCache(lk, out var block))
                {
                    return block;
                }

                // this method takes time on deserialization which is definitely more that the lock overhead, so uncontended lock is ok
                // but should we maybe deserialize twice and dispose on duplication later? TODO review
                block = GetBlockFromStorage(lk);

                if (block != null)
                {
                    if (!_blockCache.TryAdd(lk, block))
                    {
                        ThrowHelper.FailFast("Cannot add block to cache while holding a lock and checking that block was not in the cache.");
                    }
                }

                return block;
            }
        }

        /// <summary>
        /// TODO when we support IDV.Schema or similar then this could become virtual with default implementation using schema.
        /// Very likely non-generic performance with schema will be very close, so we could later delete generic child classes.
        /// However, now we do not have schema and need to figure out the right implementation the hard way.
        /// </summary>
        /// <param name="lk"></param>
        /// <returns></returns>
        protected abstract DataBlock GetBlockFromStorage(long lk);

        #endregion Cache logic

        public Spreads.IAsyncEnumerator<KeyValuePair<TRowKey, DataBlock>> GetAsyncEnumerator()
        {
            throw new NotImplementedException();
        }

        public IEnumerator<KeyValuePair<TRowKey, DataBlock>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public ICursor<TRowKey, DataBlock> GetCursor()
        {
            throw new NotImplementedException();
        }

        public IAsyncCursor<TRowKey, DataBlock> GetAsyncCursor()
        {
            throw new NotImplementedException();
        }

        public bool TryGetValue(TRowKey key, out DataBlock value)
        {
            throw new NotImplementedException();
        }

        public bool TryGetAt(long index, out KeyValuePair<TRowKey, DataBlock> kvp)
        {
            throw new NotImplementedException();
        }

        public bool TryFindAt(TRowKey key, Lookup direction, out KeyValuePair<TRowKey, DataBlock> kvp)
        {
            throw new NotImplementedException();
        }

        public bool IsCompleted => throw new NotImplementedException();

        public bool IsIndexed => throw new NotImplementedException();

        public KeyComparer<TRowKey> Comparer => _comparer;

        public Opt<KeyValuePair<TRowKey, DataBlock>> First => throw new NotImplementedException();

        public Opt<KeyValuePair<TRowKey, DataBlock>> Last => throw new NotImplementedException();

        public DataBlock LastValueOrDefault => throw new NotImplementedException();

        public DataBlock this[TRowKey key] => throw new NotImplementedException();

        public IEnumerable<TRowKey> Keys => throw new NotImplementedException();

        public IEnumerable<DataBlock> Values => throw new NotImplementedException();

        public Task<bool> TryAddLast(TRowKey key, DataBlock value)
        {
            throw new NotImplementedException();
        }

        public Task Complete()
        {
            throw new NotImplementedException();
        }

        public Task<bool> Set(TRowKey key, DataBlock value)
        {
            throw new NotImplementedException();
        }

        public Task<bool> TryAdd(TRowKey key, DataBlock value)
        {
            throw new NotImplementedException();
        }

        public Task<bool> TryAddFirst(TRowKey key, DataBlock value)
        {
            throw new NotImplementedException();
        }

        public ValueTask<Opt<DataBlock>> TryRemove(TRowKey key)
        {
            throw new NotImplementedException();
        }

        public ValueTask<Opt<KeyValuePair<TRowKey, DataBlock>>> TryRemoveFirst()
        {
            throw new NotImplementedException();
        }

        public ValueTask<Opt<KeyValuePair<TRowKey, DataBlock>>> TryRemoveLast()
        {
            throw new NotImplementedException();
        }

        public ValueTask<Opt<KeyValuePair<TRowKey, DataBlock>>> TryRemoveMany(TRowKey key, Lookup direction)
        {
            throw new NotImplementedException();
        }

        public Task<bool> TryRemoveMany(TRowKey key, DataBlock updatedAtKey, Lookup direction)
        {
            throw new NotImplementedException();
        }

        public ValueTask<long> TryAppend(ISeries<TRowKey, DataBlock> appendMap, AppendOption option = AppendOption.RejectOnOverlap)
        {
            throw new NotImplementedException();
        }

        public long Count => throw new NotImplementedException();

        public long Version => throw new NotImplementedException();

        public bool IsAppendOnly => throw new NotImplementedException("TODO delete old iface member");

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }

    // Each container type should implement GetBlockFromStorage that just redirects to the right storage method
    internal sealed class BlockSeriesForSeries<TRowKey, TValue> : BlockSeries<TRowKey>
    {
        public BlockSeriesForSeries(long streamId, KeyComparer<TRowKey> comparer, IContainerBlockStorage blockStorage) : base(streamId, comparer, blockStorage)
        {
        }

        protected override DataBlock GetBlockFromStorage(long lk)
        {
            return _blockStorage.TryGetSeriesBlocks<TRowKey, TValue>(_streamId, lk, Lookup.EQ, limit: 1);
        }
    }

    //internal class BlockSeries<TRowKey, TColumnKey, TValue> : BlockSeries<TRowKey, TValue>
    //{
    //}
}
