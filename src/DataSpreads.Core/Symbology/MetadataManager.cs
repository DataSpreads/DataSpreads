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
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using DataSpreads.Storage;
using DataSpreads.StreamLogs;
using Spreads;
using Spreads.Serialization;

namespace DataSpreads.Symbology
{
    // TODO Extract interface or base class. Base class is probably better because we then could have devirt, but... this is IO bound, doesn't matter.

    // TODO check that stream or repo with the same name already exists
    // Now we could create repo/sub stream and repo/sub/ repo

    internal interface IMetadataManager
    {
        #region Quick lookup

        ValueTask<long> TryGetStreamId(DataPath dataPath);

        // These methods need to make additional validation if TypeCoded

        ValueTask<long> TryGetStreamId<T>(DataPath dataPath);

        ValueTask<long> TryGetMatrixId<T>(DataPath dataPath);

        ValueTask<long> TryGetSeriesId<TRow, TValue>(DataPath dataPath);

        ValueTask<long> TryGetFrameId<TRow, TColumn>(DataPath dataPath);

        ValueTask<long> TryGetFrameTId<TRow, TColumn, TValue>(DataPath dataPath);

        ValueTask<long> TryGetPanelId<TRow, TColumn>(DataPath dataPath);

        #endregion Quick lookup
    }

    internal class MetadataManager : IDisposable
    {
        internal const string DataPathPrefix = ":";

        internal const string AccountRepo = "~/";

        internal const string LocalRepo = "_/"; // TODO we need some local repo that always works without sync. Or stashing mechanism when disconnected.

        // we do not care what exactly symbol is after : for the
        // global root so have some smile :)
        internal const string MetadataTableStoreRootPk = ":)";

        private readonly SQLiteStorage _metadataStorage;
        private readonly StreamLogStateStorage _streamStateStorage;

        // for disposal from tests
        private readonly bool _ownMetadataStorage;

        private readonly bool _ownstreamStateStorage;

        /// <summary>
        /// Cache repos on their materialized path. Client side caching should always work regardless of storage implementation.
        /// Repo events will keeps it in sync. Repo events are quite rare so we could take a lock and do things slowly if needed.
        /// </summary>
        internal ConcurrentDictionary<string, MetadataRecord> RepoPathCache = new ConcurrentDictionary<string, MetadataRecord>();

        // TODO (low) quick lookup
        //private readonly string _metadataDirectory;
        //private readonly LMDBEnvironment _env;
        //private readonly Database _quickLookupDb;

        // TODO metadataStorage interface
        public MetadataManager(string dataStorePath, SQLiteStorage metadataStorage = null, StreamLogStateStorage streamStateStorage = null)
        {
            if (metadataStorage == null)
            {
                var dataStoragePath = Path.Combine(dataStorePath, "storage");
                Directory.CreateDirectory(dataStoragePath);
                var path = Path.GetFullPath(Path.Combine(dataStoragePath, "data.db"));
                var uri = new Uri(path);
                var absoluteUri = uri.AbsoluteUri;

                metadataStorage = new SQLiteStorage($@"Data Source={absoluteUri}?cache=shared");
                _ownMetadataStorage = true;
            }
            _metadataStorage = metadataStorage;

            if (streamStateStorage == null)
            {
                var logStateStoragePath = Path.Combine(dataStorePath, "log", "logstate");
                streamStateStorage = new StreamLogStateStorage(logStateStoragePath);
                _ownstreamStateStorage = true;
            }
            _streamStateStorage = streamStateStorage;

            var zeroRepo = new MetadataRecord
            {
                PK = MetadataTableStoreRootPk,
                Metadata = new Metadata("Virtual root repo, keeps counter for all other repos and nothing else."),
                Counter = 1
            };
            _metadataStorage.InsertMetadataRow(zeroRepo);

            #region Init account repo

            var accountRepo = new MetadataRecord()
            {
                PK = DataPathPrefix + AccountRepo,
                RepoId = 1,

                SchemaType = 0,
                Metadata = new Metadata("Current account root repo")
            };
            _metadataStorage.InsertMetadataRow(accountRepo);

            accountRepo = _metadataStorage.GetExactMetadata(accountRepo.PK);
            if (accountRepo.RepoId != 1)
            {
                ThrowHelper.FailFast("Account repo must be initialized with RepoId = 1");
            }

            RepoPathCache[DataPath.CurrentAccountPrefix] = accountRepo;

            #endregion Init account repo

            #region Init local repo

            var localRepo = new MetadataRecord()
            {
                PK = DataPathPrefix + LocalRepo,
                RepoId = -1,

                SchemaType = 0,
                Metadata = new Metadata("Local root repo")
            };
            _metadataStorage.InsertMetadataRow(localRepo);

            localRepo = _metadataStorage.GetExactMetadata(localRepo.PK);
            if (localRepo.RepoId != -1)
            {
                ThrowHelper.FailFast("Local repo must be initialized with RepoId = 2");
            }

            RepoPathCache[DataPath.LocalPrefix] = localRepo;

            #endregion Init local repo

            // TODO Implement later. Need to compare with default lookup.
            //_metadataDirectory = Path.Combine(dataStorePath, "metadata");
            //Directory.CreateDirectory(_metadataDirectory);
            //_env = OpenEnv(_metadataDirectory);
            //_quickLookupDb = OpenQuickLookupDb(_env);
        }

        private string RepoIdToDpPrefix(int repoId)
        {
            // :0:~/        - local user root
            // :0:@vb/      - external root
            // :0:_/        - local root
            // :1:repo      - repo path with parent repo id 1
            // :1:stream    - stream path with parent repo id 1
            // path does not distinguish between repo and stream
            return $":{repoId}:";
        }

        public async ValueTask<MetadataRecord> GetMetadataRecordByPathAsync(DataPath path)
        {
            var isRepo = path.IsRepo;
            var currentRepoPath = path;
            if (!isRepo)
            {
                currentRepoPath = currentRepoPath.Parent;
            }

        CURRENT_REPO_CACHED:
            if (RepoPathCache.TryGetValue(currentRepoPath.ToString(), out var currentRepoRecord))
            {
                if (isRepo)
                {
                    return currentRepoRecord;
                }
                // TODO
                var parentId = currentRepoRecord.RepoId;
                var currentStreamPk = RepoIdToDpPrefix(parentId) + path.Name;
                var currentStreamRecord = _metadataStorage.GetExactMetadata(currentStreamPk);
                if (currentStreamRecord.PK != null)
                {
                    return currentStreamRecord;
                }

                return default;
            }
            else if (currentRepoPath.IsRoot)
            {
                if (currentRepoPath.IsExternalAccount)
                {
                    throw new NotImplementedException();
                }
                else
                {
                    var parentId = 0;
                    var currentRepoPk = RepoIdToDpPrefix(parentId) + currentRepoPath.Name;
                    currentRepoRecord = _metadataStorage.GetExactMetadata(currentRepoPk);
                    if (currentRepoRecord.PK != null)
                    {
                        RepoPathCache[currentRepoPath.ToString()] = currentRepoRecord;
                        // no current repo is cached, go to the lookup as if it was cached already
                        goto CURRENT_REPO_CACHED;
                    }
                }
            }

        PARENT_REPO_CACHED:
            // current repo is not cached, find it
            var parentRepoPath = currentRepoPath.Parent;
            if (RepoPathCache.TryGetValue(parentRepoPath.ToString(), out var parentMdr))
            {
                // has found parent of the current
                var parentId = parentMdr.RepoId;
                var currentRepoPk = RepoIdToDpPrefix(parentId) + currentRepoPath.Name;
                currentRepoRecord = _metadataStorage.GetExactMetadata(currentRepoPk);
                if (currentRepoRecord.PK != null)
                {
                    RepoPathCache[currentRepoPath.ToString()] = currentRepoRecord;
                    // no current repo is cached, go to the lookup as if it was cached already
                    goto CURRENT_REPO_CACHED;
                }
                else
                {
                    // we found parent but cannot find current repo
                    // which means it does not exists
                    return default;
                }
            }

            var parentRecord = await GetMetadataRecordByPathAsync(parentRepoPath);
            RepoPathCache[parentRepoPath.ToString()] = parentRecord;
            goto PARENT_REPO_CACHED;
        }

        public async ValueTask<MetadataRecord> CreateRepoAsync(DataPath dataPath, Metadata metadata = null)
        {
            if (!dataPath.IsRepo)
            {
                throw new ArgumentException($"{dataPath} is not a repo path.");
            }

            if (dataPath.IsRooted)
            {
                if (dataPath.IsExternalAccount)
                {
                    throw new ArgumentException("Cannot create external repo.");
                }
                Debug.Assert(dataPath.IsCurrentAccount || dataPath.IsLocal);
            }
            else
            {
                throw new ArgumentException($"Data path must be rooted: {dataPath}");
            }

            if (dataPath.Parent.ToString() == string.Empty)
            {
                throw new ArgumentException("Cannot create root repo.");
            }

            var parent = await GetMetadataRecordByPathAsync(dataPath.Parent);
            if (parent.PK == null)
            {
                throw new ArgumentException($"Parent repo does not exist: {dataPath}");
            }

            using (var txn = _metadataStorage.BeginConcurrent())
            {
                var newRepoId = txn.Queries.IncrementCounter(MetadataTableStoreRootPk);
                if (dataPath.IsLocal)
                {
                    newRepoId = -newRepoId;
                }
                var newRepoRecord = new MetadataRecord
                {
                    PK = RepoIdToDpPrefix(parent.RepoId) + dataPath.Name,
                    RepoId = newRepoId,
                    ParentId = parent.RepoId,
                    SchemaType = 0,
                    Metadata = metadata
                };

                var inserted = txn.Queries.InsertMetadataRow(newRepoRecord);
                if (inserted)
                {
                    txn.RawCommit();
                    return newRepoRecord;
                }
            }

            return default;
        }

        public async ValueTask<MetadataRecord> CreateStreamAsync(DataPath dataPath, Metadata metadata, ContainerSchema schema)
        {
            if (dataPath.IsRepo)
            {
                throw new ArgumentException($"{dataPath} is not a stream path.");
            }

            if (dataPath.IsRooted)
            {
                if (dataPath.IsExternalAccount)
                {
                    throw new ArgumentException("Cannot create external repo.");
                }
                Debug.Assert(dataPath.IsCurrentAccount || dataPath.IsLocal);
            }
            else
            {
                throw new ArgumentException($"Data path must be rooted: {dataPath}");
            }

            var parent = await GetMetadataRecordByPathAsync(dataPath.Parent);
            if (parent.PK == null)
            {
                throw new ArgumentException($"Parent repo does not exist: {dataPath}");
            }

            using (var txn = _metadataStorage.BeginConcurrent())
            {
                var newStreamId = txn.Queries.IncrementCounter(parent.PK);
                
                var newStreamRow = new MetadataRecord
                {
                    PK = RepoIdToDpPrefix(parent.RepoId) + dataPath.Name,
                    RepoId = parent.RepoId,
                    ContainerId = newStreamId,
                    ParentId = parent.RepoId,
                    SchemaType = 0,
                    Schema = schema,
                    Metadata = metadata
                };

                var inserted = txn.Queries.InsertMetadataRow(newStreamRow);
                if (inserted)
                {
                    txn.RawCommit();
                    return newStreamRow;
                }
            }

            return default;
        }

        public void Dispose()
        {
            if (_ownMetadataStorage)
            {
                _metadataStorage.Dispose();
            }

            if (_ownstreamStateStorage)
            {
                _streamStateStorage.Dispose();
            }

            GC.SuppressFinalize(this);

            //_quickLookupDb.Dispose();
            //_env.Dispose();
        }

        #region Quick lookup

        //public bool TryQuickLookup(in DataPathStruct path, out MetadataRecord record)
        //{
        //    var ptr = (byte*)Unsafe.AsPointer(ref Unsafe.AsRef(in path));
        //    var db = new DirectBuffer(path.Length, ptr);
        //    using (var txn = _env.BeginReadOnlyTransaction())
        //    {
        //        if (_quickLookupDb.TryGet(txn, ref db, out var value))
        //        {
        //            record = value.Read<MetadataRecord>(0);
        //            return true;
        //        }
        //    }
        //    record = default;
        //    return false;
        //}

        //private static LMDBEnvironment OpenEnv(string path)
        //{
        //    var env = LMDBEnvironment.Create(path, LMDBEnvironmentFlags.None,
        //        disableAsync: true, disableReadTxnAutoreset: false);

        //    env.MapSize = 128 * 1024 * 1024;
        //    env.MaxReaders = 1024;
        //    env.Open();
        //    var deadReaders = env.ReaderCheck();
        //    if (deadReaders > 0)
        //    {
        //        Trace.TraceWarning($"Cleared {deadReaders} in ProcessConfig");
        //    }
        //    return env;
        //}

        //private static Database OpenQuickLookupDb(LMDBEnvironment env)
        //{
        //    return env.OpenDatabase("_quickLookup", new DatabaseConfig(DbFlags.Create));
        //}

        #endregion Quick lookup
    }
}
