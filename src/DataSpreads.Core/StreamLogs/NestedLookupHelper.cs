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
using Spreads.Algorithms;
using Spreads.Collections;
using Spreads.DataTypes;
using System;
using System.Runtime.CompilerServices;

namespace DataSpreads.StreamLogs
{
    /// <summary>
    /// Nested search implementation extracted from <see cref="BaseContainer{TKey}"/>.
    /// Do not change any logic here without changing in the original place.
    /// </summary>
    internal static class NestedLookupHelper
    {
        /// <summary>
        /// When found, updates key by the found key if it is different, returns block and index whithin the block where the data resides.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool TryFindBlockAt<TKey, TVec>(
            ref TKey key,
            Lookup lookup,
            out TVec block,
            out int blockIndex,
            KeyComparer<TKey> comparer,
            ISeries<TKey, TVec> dataSource) where TVec : struct, IVector<TKey>, IEquatable<TVec>
        {
            // This is non-obvious part:
            // For most searches we could find the right block
            // by searching with LE on the source:
            // o - EQ match, x - non-existing target key
            // | - start of a block, there is no space between start of block and it's first element (TODO review, had issues with that)
            // * for LE
            //      - [...] |[o...] |[<..]
            //      - [...] |[.o..] |[<..]
            //      - [...] |[ox..] |[<..]
            //      - [...] |[...o]x|[<..]
            // * for EQ
            //      - [...] |[x...] |[<..] - not possible, because with LE this would be [..o]x|[....] | [<..] since the first key must be LE, if it is not !EQ we find previous block
            //      - [...] |[o...] |[<..]
            //      - [...] |[..o.] |[<..]
            //      - [...] |[..x.] |[<..]
            //      - [...] |[....]x|[<..]
            // * for GE
            //      - [...] |[o...] |[<..]
            //      - [...] |[xo..] |[<..]
            //      - [...] |[..o.] |[<..]
            //      - [...] |[..xo] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE, SLE+SS do not find key but it is in the next block if it exist
            //      - [...] |[....]x|[o..] SPECIAL CASE
            // * for GT
            //      - [...] |[xo..] |[<..]
            //      - [...] |[.xo.] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE
            //      - [...] |[..xo] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE
            //      - [...] |[....]x|[o..] SPECIAL CASE

            // for LT we need to search by LT

            // * for LT
            //      - [..o] |[x...] |[<..]
            //      - [...] |[ox..] |[<..]
            //      - [...] |[...o]x|[<..]

            // So the algorithm is:
            // Search source by LE or by LT if lookup is LT
            // Do SortedSearch on the block
            // If not found check if complement is after the end

            // Notes: always optimize for LE search, it should have least branches and could try speculatively
            // even if we could detect special cases in advance. Only when we cannot find check if there was a
            // special case and process it in a slow path as non-inlined method.

            bool retryOnGt = default;
            block = default;

            if (dataSource != null)
            {
                retryOnGt = true;
                TryFindBlock_ValidateOrGetBlockFromSource(ref block,
                    key, lookup, lookup == Lookup.LT ? Lookup.LT : Lookup.LE,
                    comparer, dataSource);
            }

        RETRY:

            if (!block.Equals(default))
            {
                blockIndex = VectorSearch.SortedLookup(ref block, 0,
                    block.Length, key, lookup, comparer);

                if (blockIndex >= 0)
                {
                    return true;
                }

                // Check for SPECIAL CASE from the comment above
                if (retryOnGt &&
                    (~blockIndex) == block.Length
                    && ((int)lookup & (int)Lookup.GT) != 0)
                {
                    retryOnGt = false;
                    TVec nextBlock = default;

                    TryFindBlock_ValidateOrGetBlockFromSource(ref nextBlock,
                        block.DangerousGet(0), lookup, Lookup.GT, comparer, dataSource);

                    if (!nextBlock.Equals(default))
                    {
                        block = nextBlock;
                        goto RETRY;
                    }
                }
            }
            else
            {
                blockIndex = -1;
            }

            return false;
        }

        // TODO Test multi-block case and this attribute impact. Maybe direct call is OK without inlining
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void TryFindBlock_ValidateOrGetBlockFromSource<TKey, TVec>(
            ref TVec block,
            TKey key,
            Lookup direction,
            Lookup sourceDirection,
            KeyComparer<TKey> comparer,
            ISeries<TKey, TVec> dataSource) where TVec : struct, IVector<TKey>, IEquatable<TVec>
        {
            // for single block this should exist, for sourced blocks this value is updated by a last search
            // Take reference, do not work directly. Reference assignment is atomic in .NET

            if (!block.Equals(default))
            {
                // Check edge cases if key is outside the block and we may need to retrieve
                // the right one from storage. We do not know anything about other blocks, so we must
                // be strictly in range so that all searches will work.

                if (block.Length <= 1) // with 1 there are some edge cases that penalize normal path, so make just one comparison
                {
                    block = default;
                }
                else
                {
                    var firstC = comparer.Compare(key, block.DangerousGet(0));

                    if (firstC < 0 // not in this block even if looking LT
                        || direction == Lookup.LT // first value is >= key so LT won't find the value in this block
                                                  // Because rowLength >= 2 we do not need to check for firstC == 0 && GT
                    )
                    {
                        block = default;
                    }
                    else
                    {
                        var lastC = comparer.Compare(key, block.DangerousGet(block.Length - 1));

                        if (lastC > 0
                            || direction == Lookup.GT
                        )
                        {
                            block = default;
                        }
                    }
                }
            }
            // if block is null here we have rejected it and need to get it from source
            // or it is the first search and cached block was not set yet
            if (block.Equals(default))
            {
                // Lookup sourceDirection = direction == Lookup.LT ? Lookup.LT : Lookup.LE;
                // TODO review: next line will eventually call this method for in-memory case, so how inlining possible?
                // compiler should do magic to convert all this to a loop at JIT stage, so likely it does not
                // and the question is where to break the chain. We probably could afford non-inlined
                // DataSource.TryFindAt if this method will be faster for single-block and cache-hit cases.
                if (!dataSource.TryFindAt(key, sourceDirection, out var kvp))
                {
                    block = default;
                }
                else
                {
                    if (AdditionalCorrectnessChecks.Enabled)
                    {
                        if (kvp.Value.Length <= 0 || comparer.Compare(kvp.Key, kvp.Value.DangerousGet(0)) != 0)
                        {
                            ThrowBadBlockFromSource();
                        }
                    }

                    block = kvp.Value;
                }
            }
        }

        #region Same code adapted for StreamLog cursor types. DO NOT CHANGE HERE WITHOUT CHANGING SOURCE.

        // We only use TryFindAt from series and this is redirected to BlockIndex

        /// <summary>
        /// When found, updates key by the found key if it is different, returns block and index whithin the block where the data resides.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
            | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal static bool TryFindBlockAt(
            ref Timestamp key,
            Lookup lookup,
            out StreamBlockWithTimestamp block,
            out int blockIndex,
            StreamLog dataSource)
        {
            // This is non-obvious part:
            // For most searches we could find the right block
            // by searching with LE on the source:
            // o - EQ match, x - non-existing target key
            // | - start of a block, there is no space between start of block and it's first element (TODO review, had issues with that)
            // * for LE
            //      - [...] |[o...] |[<..]
            //      - [...] |[.o..] |[<..]
            //      - [...] |[ox..] |[<..]
            //      - [...] |[...o]x|[<..]
            // * for EQ
            //      - [...] |[x...] |[<..] - not possible, because with LE this would be [..o]x|[....] | [<..] since the first key must be LE, if it is not !EQ we find previous block
            //      - [...] |[o...] |[<..]
            //      - [...] |[..o.] |[<..]
            //      - [...] |[..x.] |[<..]
            //      - [...] |[....]x|[<..]
            // * for GE
            //      - [...] |[o...] |[<..]
            //      - [...] |[xo..] |[<..]
            //      - [...] |[..o.] |[<..]
            //      - [...] |[..xo] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE, SLE+SS do not find key but it is in the next block if it exist
            //      - [...] |[....]x|[o..] SPECIAL CASE
            // * for GT
            //      - [...] |[xo..] |[<..]
            //      - [...] |[.xo.] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE
            //      - [...] |[..xo] |[<..]
            //      - [...] |[...x] |[o..] SPECIAL CASE
            //      - [...] |[....]x|[o..] SPECIAL CASE

            // for LT we need to search by LT

            // * for LT
            //      - [..o] |[x...] |[<..]
            //      - [...] |[ox..] |[<..]
            //      - [...] |[...o]x|[<..]

            // So the algorithm is:
            // Search source by LE or by LT if lookup is LT
            // Do SortedSearch on the block
            // If not found check if complement is after the end

            // Notes: always optimize for LE search, it should have least branches and could try speculatively
            // even if we could detect special cases in advance. Only when we cannot find check if there was a
            // special case and process it in a slow path as non-inlined method.

            bool retryOnGt = default;
            block = default;

            if (dataSource != null)
            {
                retryOnGt = true;
                TryFindBlock_ValidateOrGetBlockFromSource(ref block,
                    key, lookup, lookup == Lookup.LT ? Lookup.LT : Lookup.LE,
                    dataSource);
            }

        RETRY:

            if (!block.Equals(default))
            {
                blockIndex = VectorSearch.SortedLookup(ref block, start: 0, block.Length, key, lookup);

                if (blockIndex >= 0)
                {
                    return true;
                }

                // Check for SPECIAL CASE from the comment above
                if (retryOnGt &&
                    (~blockIndex) == block.Length
                    && ((int)lookup & (int)Lookup.GT) != 0)
                {
                    retryOnGt = false;
                    StreamBlockWithTimestamp nextBlock = default;

                    TryFindBlock_ValidateOrGetBlockFromSource(ref nextBlock,
                        block.DangerousGet(0), lookup, Lookup.GT, dataSource);

                    if (!nextBlock.Equals(default))
                    {
                        block = nextBlock;
                        goto RETRY;
                    }
                }
            }
            else
            {
                blockIndex = -1;
            }

            return false;
        }

        // TODO Test multi-block case and this attribute impact. Maybe direct call is OK without inlining
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void TryFindBlock_ValidateOrGetBlockFromSource(
            ref StreamBlockWithTimestamp block,
            Timestamp key,
            Lookup direction,
            Lookup sourceDirection,
            StreamLog dataSource)
        {
            // for single block this should exist, for sourced blocks this value is updated by a last search
            // Take reference, do not work directly. Reference assignment is atomic in .NET

            if (!block.Equals(default))
            {
                // Check edge cases if key is outside the block and we may need to retrieve
                // the right one from storage. We do not know anything about other blocks, so we must
                // be strictly in range so that all searches will work.

                if (block.Length <= 1) // with 1 there are some edge cases that penalize normal path, so make just one comparison
                {
                    block = default;
                }
                else
                {
                    var firstC = KeyComparer<Timestamp>.Default.Compare(key, block.DangerousGet(0));

                    if (firstC < 0 // not in this block even if looking LT
                        || direction == Lookup.LT // first value is >= key so LT won't find the value in this block
                                                  // Because rowLength >= 2 we do not need to check for firstC == 0 && GT
                    )
                    {
                        block = default;
                    }
                    else
                    {
                        var lastC = KeyComparer<Timestamp>.Default.Compare(key, block.DangerousGet(block.Length - 1));

                        if (lastC > 0
                            || direction == Lookup.GT
                        )
                        {
                            block = default;
                        }
                    }
                }
            }
            // if block is null here we have rejected it and need to get it from source
            // or it is the first search and cached block was not set yet
            if (block.Equals(default))
            {
                // Lookup sourceDirection = direction == Lookup.LT ? Lookup.LT : Lookup.LE;
                // TODO review: next line will eventually call this method for in-memory case, so how inlining possible?
                // compiler should do magic to convert all this to a loop at JIT stage, so likely it does not
                // and the question is where to break the chain. We probably could afford non-inlined
                // DataSource.TryFindAt if this method will be faster for single-block and cache-hit cases.

                if (dataSource.BlockIndex.TryFindAt(dataSource.Slid, key, sourceDirection, out var record))
                {
                    var sb = dataSource.GetBlockFromRecord(record);
                    var recordKey = record.Timestamp;
                    var value = new StreamBlockWithTimestamp(sb);

                    if (AdditionalCorrectnessChecks.Enabled)
                    {
                        if (value.Length <= 0 ||
                            KeyComparer<Timestamp>.Default.Compare(recordKey, value.DangerousGet(0)) != 0)
                        {
                            ThrowBadBlockFromSource();
                        }
                    }

                    block = value;
                }
                else
                {
                    block = default;
                }
            }
        }

        #endregion Same code adapted for StreamLog cursor types. DO NOT CHANGE HERE WITHOUT CHANGING SOURCE.

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowBadBlockFromSource()
        {
            ThrowHelper.ThrowInvalidOperationException("BaseContainer.DataSource.TryFindAt " +
                                                       "returned an empty block or key that doesn't match the first row index value");
        }
    }
}
