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

using DataSpreads.Config;
using DataSpreads.Storage;
using DataSpreads.StreamLogs;
using DataSpreads.Symbology;
using Spreads;
using Spreads.DataTypes;
using Spreads.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataSpreads
{
    /// <summary>
    /// Physical unit of data storage.
    /// </summary>
    public class DataStore : CriticalFinalizerObject, IDisposable
    {
        internal static object StaticLockObject = new object();

        internal static DataStore _default;

        internal static DataStore Default
        {
            get
            {
                if (_default == null)
                {
                    _default = OpenDefault();
                }

                return _default;
            }
        }

        private static int _counter;

        public static DataStore Create(string name, string path)
        {
            return new DataStore(ProcessConfig.Default, (uint)Interlocked.Increment(ref _counter), name, path);
        }

        public readonly uint Id;
        public readonly string Name;
        public string DataStoreDirectory { get; }
        public string WALDirectory { get; }
        public string ArchiveDirectory { get; }

        // DataStore should not have complex logic, just delegate
        // calls to these managers and keep shared config & resources.

        internal readonly StreamLogManager StreamLogManager;
        internal readonly MetadataManager MetadataManager;

        // DataStore is CriticalFinalizerObject therefore its finalizer
        // will called last after all other finalizers. We keep GCHandles
        // on managers so that they are not collected independently, and
        // as long as the managers are alive their resource are as well.
        // Only StreamLogs are weakly referenced from SLM and could be
        // finalized as soon as nobody uses them.
        private GCHandle _slmHandle;

        private GCHandle _mmHandle;

        private Repo _rootRepo;

        internal DataStore(ProcessConfig processConfig, uint id, string dataStoreName,
            string dataStoreDirectory = null,
            string walDirectory = null,
            string archiveDirectory = null,
            int maxLogSizeMb = 10 * 1024,
            Opt<bool> disableNotificationLog = default, Opt<bool> disablePacker = default)
        {
            var processConfig1 = processConfig;
            Id = id;

            Name = dataStoreName;

            dataStoreDirectory = Path.Combine(dataStoreDirectory ?? processConfig.DataRootPath, dataStoreName);

            Directory.CreateDirectory(dataStoreDirectory);
            DataStoreDirectory = dataStoreDirectory;

            if (walDirectory == null)
            {
                walDirectory = Path.Combine(DataStoreDirectory, "wal");
            }
            else
            {
                walDirectory = Path.Combine(walDirectory, "wal_" + dataStoreName);
            }
            WALDirectory = walDirectory;
            Directory.CreateDirectory(WALDirectory);

            if (archiveDirectory != null)
            {
                ArchiveDirectory = Path.Combine(archiveDirectory, dataStoreName);
                Directory.CreateDirectory(ArchiveDirectory);
            }

            if (!processConfig1.IsLiteClient)
            {
                if (maxLogSizeMb < StartupConfig.MinimumLogSizeMb)
                {
                    maxLogSizeMb = StartupConfig.MinimumLogSizeMb;
                }

                if (maxLogSizeMb > StartupConfig.StreamBlockTableMaxSizeMb)
                {
                    maxLogSizeMb = StartupConfig.StreamBlockTableMaxSizeMb;
                }

                var disableNotificationLog1 = disableNotificationLog.IsPresent && disableNotificationLog.Present;
                var disablePacker1 = disablePacker.IsPresent && disablePacker.Present;

                var dataStoragePath = Path.Combine(DataStoreDirectory, "storage");
                Directory.CreateDirectory(dataStoragePath);

                var path = Path.GetFullPath(Path.Combine(dataStoragePath, "data.db"));
                var uri = new Uri(path);
                var absoluteUri = uri.AbsoluteUri;
                var sqLiteStorage = new SQLiteStorage($@"Data Source={absoluteUri}?cache=shared");

                StreamLogManager = new StreamLogManager(
                    processConfig1,
                    Name,
                    dataStoreDirectory,
                    (uint)maxLogSizeMb, disableNotificationLog1, disablePacker1, sqLiteStorage);

                _slmHandle = GCHandle.Alloc(StreamLogManager, GCHandleType.Normal);

                MetadataManager = new MetadataManager(DataStoreDirectory, sqLiteStorage, StreamLogManager.StateStorage);

                _mmHandle = GCHandle.Alloc(MetadataManager, GCHandleType.Normal);
                // TODO Connection manager
            }
            else
            {
                throw new NotImplementedException("Lite client is not implemented.");
            }
        }

        private static DataStore OpenDefault()
        {
            var ds = new DataStore(ProcessConfig.Default, 0,
                ProcessConfig.DefaultDataStoreName,
                ProcessConfig.DataStoreConfig?.DataStoreDirectory,
                ProcessConfig.DataStoreConfig?.SyncerDirectory,
                ProcessConfig.DataStoreConfig?.ArchiveDirectory,
                ProcessConfig.DataStoreConfig?.MaxLogSizeMb ?? 10 * 1024
                );
            return ds;
        }

        private void Dispose(bool disposing)
        {
            MetadataManager.Dispose();
            StreamLogManager.Dispose();

            _slmHandle.Free();
            _mmHandle.Free();
            // TODO other members
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~DataStore()
        {
            Dispose(false);
        }

        internal async ValueTask<bool> CreateStreamAsync<T>(DataPath dataPath,
            Metadata metadata, DataStreamCreateFlags flags)
        {
            var slf = (StreamLogFlags)flags;
            short fixedSize = 0;
            if (slf.IsBinary() && TypeEnumHelper<T>.FixedSize > 0)
            {
                if (slf.NoTimestamp())
                {
                    fixedSize = TypeEnumHelper<T>.FixedSize;
                }
                else
                {
                    fixedSize = checked((short)(Timestamp.Size + TypeEnumHelper<T>.FixedSize));
                }
            }

            // TODO this is only first take to move further and start writing stuff,
            // needs review and proper ContainerSchema generation from types T.
            // Also there are so many different flags that they need review.
            var schema = new ContainerSchema()
            {
                ContainerType = (byte)ContainerLayout.None,
                ContainerFlags = (byte)slf,
                ValuesSchema = new TypeSchema()
                {
                    DataTypeHeader = TypeEnumHelper<T>.DataTypeHeader,
                    TypeName = typeof(T).Name,
                    TypeFullName = typeof(T).FullName,
                    FixedSize = fixedSize,
                }
            };

            var mdr = await MetadataManager.CreateStreamAsync(dataPath, metadata, schema).ConfigureAwait(false);
            if (mdr.PK != null)
            {
                var containerId = mdr.ContainerId;
                var streamId = StreamLogId.ContainerIdToStreamId(containerId);
                var slid = new StreamLogId(mdr.RepoId, streamId);

                var state = StreamLogManager.StateStorage.GetState(slid);

                var lockHolder = state.LockHolder;

                state.CheckInit(slid, fixedSize, slf);

                return true;
            }

            mdr = await MetadataManager.GetMetadataRecordByPathAsync(dataPath).ConfigureAwait(false);
            if (mdr.PK != null)
            {
                return false;
            }

            throw new ArgumentException($"Cannot create a new data stream {dataPath}.");
        }

        internal async ValueTask<DataStreamWriter<T>> OpenStreamWriterAsync<T>(DataPath dataPath,
            WriteMode writeMode = WriteMode.LocalSync,
            int rateHint = 0)
        {
            var mdr = await MetadataManager.GetMetadataRecordByPathAsync(dataPath).ConfigureAwait(false);
            if (mdr.PK != null)
            {
                var containerId = mdr.ContainerId;
                var streamId = StreamLogId.ContainerIdToStreamId(containerId);
                var slid = new StreamLogId(mdr.RepoId, streamId);

                var sl = StreamLogManager.OpenStreamLog(slid, rateHint, dataPath.ToString());
                // var state = StreamLogManager.StateStorage.GetState(slid);
                var writer = new DataStreamWriter<T>(sl, KeySorting.NotEnforced, writeMode);
                return writer;
            }

            throw new KeyNotFoundException($"Data stream {dataPath} does not exist.");
        }

        internal async ValueTask<DataStream<T>> OpenStreamAsync<T>(DataPath dataPath)
        {
            var mdr = await MetadataManager.GetMetadataRecordByPathAsync(dataPath).ConfigureAwait(false);
            if (mdr.PK != null)
            {
                var containerId = mdr.ContainerId;
                var streamId = StreamLogId.ContainerIdToStreamId(containerId);
                var slid = new StreamLogId(mdr.RepoId, streamId);

                var sl = StreamLogManager.OpenStreamLog(slid, textId: dataPath.ToString());
                var ds = new DataStream<T>(new DataStreamCursor<T>(sl.GetCursor()), mdr.Metadata);
                return ds;
            }

            throw new KeyNotFoundException($"Data stream {dataPath} does not exist.");
        }

        public Repo Local
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_rootRepo == null)
                {
                    Init();
                }
                return _rootRepo;

                void Init()
                {
                    lock (StaticLockObject)
                    {
                        if (_rootRepo == null)
                        {
                            _rootRepo = new Repo(this, new DataPath(DataPath.LocalPrefix), default);
                        }
                    }
                }
            }
        }
    }
}
