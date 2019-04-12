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

using DataSpreads.Symbology;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace DataSpreads
{
    /// <summary>
    /// Logical unit of data storage.
    /// </summary>
    public class Repo
    {
        // Repo is a thin wrapper over DataStore with DataPath prefix.
        // All methods are implemented on DataStore that accepts full
        // data path. Repo methods accept data paths relative to
        // the prefix. In future we could move up with ../siblingRepo.
        // Rooted paths are OK as in a terminal.

        private readonly DataStore _dataStore;
        private readonly DataPath _dataPathPrefix;
        internal readonly MetadataRecord MetadataRecord;

        private Repo _parent;

        internal Repo(DataStore dataStore, DataPath repoPath, MetadataRecord metadataRecord)
        {
            _dataStore = dataStore;
            _dataPathPrefix = repoPath;
            MetadataRecord = metadataRecord;
        }

        public DataNodeType Type => DataNodeType.Repo;

        public string DataPath => _dataPathPrefix.ToString();

        public string Name => _dataPathPrefix.Name;

        // TODO Root. Review if null is OK for parent or `this` is better.

        public Repo Parent
        {
            get
            {
                if (_parent == null && _dataPathPrefix.Parent.ToString() != string.Empty)
                {
                    // if this repo exists then parent must be already cached
                    var mdr = _dataStore.MetadataManager.RepoPathCache[_dataPathPrefix.Parent.ToString()];
                    _parent = new Repo(_dataStore, _dataPathPrefix.Parent, mdr);
                }

                return _parent;
            }
        }

        public Metadata Metadata => MetadataRecord.Metadata;

        /// <summary>
        /// Create new or get existing sub-repo from the current repo.
        /// </summary>
        /// <remarks>
        /// When a sub-repo already exists the <paramref name="metadata"/> parameter is ignored.
        /// Returning existing repo is for convenience instead of throwing when a repo already exists.
        /// This call is idempotent and could be used at the beginning of a program many times.
        /// </remarks>
        /// <param name="newRepoName">Repo name inside the current repo.</param>
        /// <param name="metadata">
        /// Optional metadata. Provide description with relevant keywords for easier full-text search.
        /// Provide tags that you could use for search filtering.
        /// </param>
        /// <returns></returns>
        public async ValueTask<Repo> CreateRepoAsync(string newRepoName, Metadata metadata = null)
        {
            var dataPath = _dataPathPrefix.Combine(newRepoName, true);
            var mdr = await _dataStore.MetadataManager.CreateRepoAsync(dataPath, metadata);

            if (mdr.PK == null)
            {
                mdr = await _dataStore.MetadataManager.GetMetadataRecordByPathAsync(dataPath);
            }

            if (mdr.PK != null)
            {
                var repo = new Repo(_dataStore, dataPath, mdr);
                return repo;
            }

            throw new ArgumentException($"Cannot create a new Repo {dataPath}.");
        }

        /// <summary>
        /// Open existing sub-repo from the current repo.
        /// </summary>
        public async ValueTask<Repo> OpenRepoAsync(string path)
        {
            var dataPath = _dataPathPrefix.Combine(path, true);
            var mdr = await _dataStore.MetadataManager.GetMetadataRecordByPathAsync(dataPath);

            if (mdr.PK != null)
            {
                var repo = new Repo(_dataStore, dataPath, mdr);
                return repo;
            }

            throw new KeyNotFoundException($"Repo {dataPath} does not exist.");
        }

        public ValueTask<bool> CreateStreamAsync<T>(
            string newStreamName,
            Metadata metadata,
            DataStreamCreateFlags flags = DataStreamCreateFlags.None)
        {
            var dp = _dataPathPrefix.Combine(newStreamName, false);
            return _dataStore.CreateStreamAsync<T>(dp, metadata, flags);
        }

        public ValueTask<DataStream<T>> OpenStreamAsync<T>(string streamName)
        {
            var dataPath = _dataPathPrefix.Combine(streamName, false);
            return _dataStore.OpenStreamAsync<T>(dataPath);
        }

        /// <summary>
        /// Open existing data stream writer.
        /// </summary>
        /// <typeparam name="T">Type of data stream values.</typeparam>
        /// <param name="streamName">Data stream name in this repo.</param>
        /// <param name="writeMode"></param>

        /// <param name="bytesPerMinuteHint">
        /// For latency-sensitive streams provide an
        /// estimated number of bytes per minute produced. Any positive
        /// value will pre-allocate a shared memory block prepared for writes
        /// in advance in background and helps to avoid potentially
        /// blocking IO operation during data stream writes.
        /// Please be conservative and do not provide a value for every stream
        /// "just in case", because it increases memory and disk usage.
        /// Performance without this hint is already good for most use cases.
        /// </param>
        /// <returns></returns>
        public ValueTask<DataStreamWriter<T>> OpenStreamWriterAsync<T>(
            string streamName,
            WriteMode writeMode = WriteMode.LocalSync,
            int bytesPerMinuteHint = 0)
        {
            var dp = _dataPathPrefix.Combine(streamName, false);
            return _dataStore.OpenStreamWriterAsync<T>(dp, writeMode, bytesPerMinuteHint);
        }

        #region Static entry points My & Local

        private static Repo _currentAccountRepo;
        private static Repo _localRepo;

        internal static Repo My
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_currentAccountRepo == null)
                {
                    Init();
                }
                return _currentAccountRepo;

                void Init()
                {
                    lock (DataStore.StaticLockObject)
                    {
                        if (_currentAccountRepo == null)
                        {
                            _currentAccountRepo = new Repo(DataStore.Default, new DataPath(Symbology.DataPath.CurrentAccountPrefix), default);
                        }
                    }
                }
            }
        }

        internal static Repo Local
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_localRepo == null)
                {
                    Init();
                }
                return _localRepo;

                void Init()
                {
                    lock (DataStore.StaticLockObject)
                    {
                        if (_localRepo == null)
                        {
                            _localRepo = DataStore.Default.Local; // new Repo(DataStore.Default, new DataPath(Symbology.DataPath.LocalPrefix), default);
                        }
                    }
                }
            }
        }

        #endregion Static entry points My & Local
    }
}
