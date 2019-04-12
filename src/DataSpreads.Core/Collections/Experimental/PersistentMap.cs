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
using System.Collections.Concurrent;
using DataSpreads.StreamLogs;
using Spreads.Buffers;

namespace DataSpreads.Collections.Experimental
{
    internal class PMap
    {
        public enum EntryTag
        {
            // Value was set/added but not merged yet
            None = 0,

            Deleted = 1
        }

        public struct Entry
        {
            // TODO tag & version is the same int64, version is limited by int53
            public ulong Version;

            public EntryTag Tag;
        }

        private StreamBlock _activeBlock;
        private StreamBlock _previousBlock;
        private ulong _mergedVersion;

        private Spreads.LMDB.LMDBEnvironment _env;
        private Spreads.LMDB.Database _db;

        // state in db must be as if entry deleted before deleting the entry
        // first delete

        // this maps unmerged records
        private ConcurrentDictionary<DirectBuffer, Entry> _cache =
            new ConcurrentDictionary<DirectBuffer, Entry>();

        private void Set(DirectBuffer key, DirectBuffer value)
        {
            // TODO write to log first and get entry
            // _cache.up
        }

        private DirectBuffer GetValueForEntry(Entry entry)
        {
            throw new NotImplementedException();
        }

        private bool TryGetValue(DirectBuffer key, out DirectBuffer value)
        {
            value = DirectBuffer.Invalid;
            if (_cache.TryGetValue(key, out var entry))
            {
                // Version could be tagged as deleted if
                // we removed existing but not yet merged
                // the deletion. If we query _db it will
                // return a previous one.
                // Note that for Deleted entry to exist
                // TryRemove must had returned true when
                // this entry was added.
                if (entry.Tag == EntryTag.Deleted)
                {
                    return false;
                }

                value = GetValueForEntry(entry);
                return true;
            }

            using (var txn = _env.BeginReadOnlyTransaction())
            {
                return _db.TryGet(txn, ref key, out value);
            }
        }
    }
}
