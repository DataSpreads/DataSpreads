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
using DataSpreads.StreamLogs;

namespace DataSpreads.Storage
{
    internal interface IStreamBlockStorage : IDisposable
    {
        /// <summary>
        /// Get StreamBlock.
        /// </summary>
        StreamBlock TryGetStreamBlock(long streamId, ulong blockKey);

        /// <summary>
        /// Returns true if a block is written to the storage.
        /// Returns false if block version does not match tha last version in th storage.
        /// It does not swallows exceptions and could throw due to bad input.
        /// </summary>
        /// <param name="blockView"></param>
        bool TryPutStreamBlock(in StreamBlock blockView);

        /// <summary>
        /// FirstVersion of the last chunk stored in this storage.
        /// </summary>
        ulong LastStreamBlockKey(long streamId);

        /// <summary>
        /// Clears all temporary resources if storage caches or prefetches results of <see cref="TryGetStreamBlock"/>.
        ///
        /// </summary>
        /// <remarks>
        /// Call this on every <see cref="StreamLog.StreamLogCursor"/> disposal.
        /// </remarks>
        /// <param name="streamId"></param>
        void ClearCache(long streamId);
    }
}
