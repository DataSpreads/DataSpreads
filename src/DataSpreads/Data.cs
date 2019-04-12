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
using DataSpreads.Extensions;
using Spreads;
using Spreads.DataTypes;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace DataSpreads
{
    /// <summary>
    /// Entry and extension point to DataSpreads.
    /// </summary>
    public static class Data
    {
        public static Repo My = Repo.My;
        public static Repo Local = Repo.Local;

        internal static readonly ProcessConfig ProcessConfig = ProcessConfig.Default;

        public static Timestamp CurrentTime
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ProcessConfig.CurrentTime;
        }

        static Data()
        {
        }

        public static readonly PersistentStorage Storage = PersistentStorage.Instance;
        internal static readonly QueryData Q = default;

        public sealed class PersistentStorage
        {
            public static PersistentStorage Instance = new PersistentStorage();

            private PersistentStorage()
            {
                // TODO Init default repo
            }

            public StorageConfig Config => StorageConfig.Instance;

            public sealed class StorageConfig
            {
                public static StorageConfig Instance = new StorageConfig();

                private StorageConfig()
                {
                }

                /// <summary>
                /// Set custom location and max log size of the default data store.
                ///
                /// <para />
                ///
                /// If you have a single disk on your system then changing default
                /// location will have no effect. If you have several SSD disks
                /// then use the one with the largest free space for <paramref name="dataStoreDirectory"/>
                /// and a different one with the fastest sequential IO performance (writes per second)
                /// for <paramref name="syncerDirectory"/>.
                /// </summary>
                /// <param name="dataStoreDirectory">
                /// A directory where data is stored.
                /// Choose a location on an SSD disk with the largest free space.
                /// </param>
                /// <param name="syncerDirectory">
                /// A directory where write-ahead is stored.
                /// Choose a location on an SSD disk with the fastest sequential IO performance (writes per second).
                /// NVMe disks are best for this.
                /// If you have a single SSD disk leave this parameter as null.
                /// </param>
                /// <param name="archiveDirectory">
                /// A directory where all write-ahead logs are copied to instead of being deleted.
                /// Choose a location on any disk with a lot of free space.
                /// The disk must be different from disks used for <paramref name="dataStoreDirectory"/>
                /// and <paramref name="syncerDirectory"/>.
                /// </param>
                /// <param name="maxLogSizeMb">Hard limit on log buffers size.</param>
                /// <returns>True if the parameters are saved for the first time. False if default store was already initialized.</returns>
                public bool InitializeDefaultStore(string dataStoreDirectory = null,
                    string syncerDirectory = null,
                    string archiveDirectory = null,
                    int maxLogSizeMb = 10 * 1024)
                {
                    if (ProcessConfig.DataStoreConfig != null)
                    {
                        // already initialized
                        return false;
                    }

                    if (!string.IsNullOrWhiteSpace(dataStoreDirectory))
                    {
                        Directory.CreateDirectory(dataStoreDirectory);
                    }
                    else
                    {
                        dataStoreDirectory = null;
                    }

                    if (!string.IsNullOrWhiteSpace(syncerDirectory))
                    {
                        Directory.CreateDirectory(syncerDirectory);
                    }
                    else
                    {
                        syncerDirectory = null;
                    }

                    if (!string.IsNullOrWhiteSpace(archiveDirectory))
                    {
                        Directory.CreateDirectory(archiveDirectory);
                    }
                    else
                    {
                        archiveDirectory = null;
                    }

                    if (maxLogSizeMb < StartupConfig.MinimumLogSizeMb)
                    {
                        maxLogSizeMb = StartupConfig.MinimumLogSizeMb;
                    }

                    return ProcessConfig.SaveDataStoreConfig(new DataStoreConfig
                    {
                        DataStoreDirectory = dataStoreDirectory == null ? null : Path.GetFullPath(dataStoreDirectory),
                        SyncerDirectory = syncerDirectory == null ? null : Path.GetFullPath(syncerDirectory),
                        ArchiveDirectory = archiveDirectory == null ? null : Path.GetFullPath(archiveDirectory),
                        MaxLogSizeMb = maxLogSizeMb
                    });
                }
            }
        }
    }

    // TODO dynamic operations + interface
    public readonly struct DataObject : IData
    {
        private void Test()
        {
            var x = Data.Q["asd"];
        }

        public Mutability Mutability => throw new NotImplementedException();
    }

    namespace Extensions
    {
        internal interface IQueryHandler
        {
            DataObject Query(string query);
        }

        public readonly struct QueryData
        {
            internal static IQueryHandler QueryHandler;

            public DataObject this[string query]
            {
                get
                {
                    if (QueryHandler == null)
                    {
                        throw new NotImplementedException("Query handler is not implemented.");
                    }
                    return QueryHandler.Query(query);
                }
            }
        }
    }

    internal class QueryHandler : IQueryHandler
    {
        private QueryHandler()
        {
        }

        public DataObject Query(string query)
        {
            return default;
        }
    }
}
