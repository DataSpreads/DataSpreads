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
using Spreads;
using Spreads.Collections.Internal;

namespace DataSpreads.Storage
{
    // Container blocks could become invalid.
    // However only writers invalidate them.
    // If an entry is updated writers could find existing entry
    // in cache and mark it as invalid so that other outstanding
    // users could detect that the block is stale and update it.
    // Change in Version in read-lock should trigger that check,
    // if version is not changing no blocks are updated.
    // Every block entry should have container version and/or order version. TODO and or or?
    // If order version is unchanged then old blocks are always valid.
    // All container instances in the same process MUST always work with
    // the same block instance, but we need cross-process checks.
    // 1. Check cache for block key.
    // 2. If not present then get it from storage.
    // 3. ...

    internal interface IContainerBlockStorage : IDisposable
    {
        // This method is required for atomic directional lookup.
        // We do not want to cache all keys because it's hard to keep them in-sync.
        // Potential design should be LMDB dupsorted index that acts as cached keys but
        // still is atomic across processes. Then SQLite role becomes KV storage.
        // LMDB should act as commit confirmation: even if we write smth to SQLite
        // it does not exists unless it is stored in LMDB. For that we need
        // block key and version. We could quickly compare all blocks and detect
        // when a block was written to SQLite but not into LMDB.

        /// <summary>
        /// Lookup block key according to direction.
        /// </summary>
        long TryGetBlockKey(long streamId, long blockKey, Lookup direction = Lookup.EQ);

        // Generic methods are used by typed DataSource block series
        // and use cached delegates in VectorStorage<T> static fields.
        // TODO currently implemented only for typed data, will need more
        // work for frames with different column types. Probably need to pass
        // delegates array cached somewhere or limit support to known types.

        // TODO start with series, others will be trivial

        /// <summary>
        /// Get data block with all columns. Blocks could form a linked list via <see cref="DataBlock.NextBlock"/> according to direction (TODO review)
        /// </summary>
        DataBlock TryGetSeriesBlocks<TKey, TValue>(long streamId, long blockKey, Lookup direction = Lookup.EQ, int limit = 1);

        bool TryPutSeriesBlock<TKey, TValue>(DataBlock block);
    }
}
