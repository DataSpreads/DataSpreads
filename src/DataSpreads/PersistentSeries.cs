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
using System.Collections.Generic;
using System.Threading.Tasks;
using Spreads;

namespace DataSpreads
{
    /// <summary>
    /// Persistent append series
    /// </summary>
    public interface IPersistentAppendSeries<TKey, TValue> : ISeries<TKey, TValue>
    {
        /// <summary>
        /// Adds key and value to the end of the series.
        /// </summary>
        /// <returns>True on successful addition. False if the <paramref name="key"/>  is null or breaks sorting order or series <see cref="Mutability"/> is <see cref="Mutability.ReadOnly"/>.</returns>
        ValueTask<bool> TryAddLast(TKey key, TValue value);

        /// <summary>
        /// Adds key and value to the end of the series.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="key"/> is null or breaks sorting order.</exception>
        /// <exception cref="InvalidOperationException">Series <see cref="Mutability"/> is <see cref="Mutability.ReadOnly"/></exception>
        ValueTask AddLast(TKey key, TValue value);

        /// <summary>
        /// Make the map read-only and disable all subsequent Add/Remove/Set methods (they will throw InvalidOperationException)
        /// </summary>
        Task Complete();
    }

    /// <summary>
    /// Mutable series
    /// </summary>
    public interface IMutableSeries<TKey, TValue> : IAppendSeries<TKey, TValue>
    {
        /// <summary>
        /// Set value at key. Returns true if key did not exist before.
        /// </summary>
        ValueTask<bool> Set(TKey key, TValue value);

        /// <summary>
        /// Attempts to add new key and value to map.
        /// </summary>
        ValueTask<bool> TryAdd(TKey key, TValue value);

        /// <summary>
        /// Checked addition, checks that new element's key is smaller/earlier than the First element's key
        /// and adds element to this map
        /// throws ArgumentOutOfRangeException if new key is larger than the first
        /// </summary>
        ValueTask<bool> TryAddFirst(TKey key, TValue value);

        /// <summary>
        /// Returns Value deleted at the given key on success.
        /// </summary>
        ValueTask<Opt<TValue>> TryRemove(TKey key);

        /// <summary>
        /// Returns KeyValue deleted at the first key.
        /// </summary>
        ValueTask<Opt<KeyValuePair<TKey, TValue>>> TryRemoveFirst();

        /// <summary>
        /// Returns KeyValue deleted at the last key (with version before deletion)
        /// </summary>
        ValueTask<Opt<KeyValuePair<TKey, TValue>>> TryRemoveLast();

        /// <summary>
        /// Removes all keys from the given key towards the given direction and returns the nearest removed key.
        /// </summary>
        ValueTask<Opt<KeyValuePair<TKey, TValue>>> TryRemoveMany(TKey key, Lookup direction);

        /// <summary>
        /// Update value at key and remove other elements according to direction.
        /// Value of updatedAtKey could be invalid (e.g. null for reference types) and ignored,
        /// i.e. there is no guarantee that the given key is present after calling this method.
        /// This method is only used for atomic RemoveMany operation in SCM and may be not implemented/supported.
        /// </summary>
        ValueTask<bool> TryRemoveMany(TKey key, TValue updatedAtKey, Lookup direction);

        /// <summary>
        /// Add values from appendMap to the end of this map.
        /// </summary>
        ValueTask<long> TryAppend(ISeries<TKey, TValue> appendMap, AppendOption option = AppendOption.RejectOnOverlap);

        // TODO Methods
        // MarkComplete
        // MarkAppendOnly
        // All mutating methods must check mutability
    }
}