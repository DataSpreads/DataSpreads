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
using DataSpreads.Storage;
using Spreads.LMDB;
using Spreads.Threading;
using System.IO;
using System.Runtime.CompilerServices;

namespace DataSpreads.StreamLogs
{
    // This class has these main functions:
    // * Block allocation/releasing
    // * Block navigation
    // * Block caching (tracking)
    // It's a big single class because it uses a single LMDB environment

    internal partial class StreamBlockManager : BufferRefAllocator
    {
        internal const int PageSize = 4096;

        // Sentinels for completed and prepared index records

        public const ulong CompletedVersion = ulong.MaxValue;
        public const ulong ReadyBlockVersion = ulong.MaxValue - 1; // TODO rename to standby, ready/prepared is ambiguous, add ...Sentinel suffix to both

        private readonly Database _blocksDb;
        private readonly Database _pidDb;
        private readonly SharedMemoryBuckets _buckets;
        private readonly IStreamBlockStorage _blockStorage;

        public StreamBlockManager(string path,
            LMDBEnvironmentFlags envFlags,
            Wpid wpid,
            IStreamBlockStorage blockStorage = null,
            long maxTotalSize = 1024L * 1024 * 1024 * 1024 * 1024,
            uint envSizeMb = 1024) // 1Gb takes 2 MB of TLB, do not make it too large TODO calculate realistic required size that is derived from maxTotalSize when using smallest buffers
            : base(path, envFlags, wpid, PageSize, maxTotalSize, envSizeMb)
        {
            _blocksDb = Environment.OpenDatabase("_streamBlocks",
                new DatabaseConfig(DbFlags.Create | DbFlags.DuplicatesSort | DbFlags.IntegerDuplicates | DbFlags.DuplicatesFixed)
                {
                    DupSortPrefix = 64 * 64
                }
            );

            _pidDb = Environment.OpenDatabase("_streamLogState", new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

            _buckets = new SharedMemoryBuckets(Path.Combine(path, "buckets"), pageSize: PageSize, maxBucketIndex: BufferRef.MaxBucketIdx);

            _blockStorage = blockStorage;
        }

        public IStreamBlockStorage BlockStorage
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _blockStorage;
        }

        
    }
}
