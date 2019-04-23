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

using Spreads.LMDB;
using System;
using System.IO;

namespace DataSpreads.Config
{
    // This class should not be exposed to used directly, all fields are internally settable without checks
    // In some places code checks valid/sensible ranges, in others it does not.
    // TODO a way to configure stuff from user code

    // TODO internal
    public static class StartupConfig
    {
        public const string AppName = "DataSpreads";
        public const string AppNameLowCase = "dataspreads";

        static StartupConfig()
        {
            var local = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.DoNotVerify), AppName);
            Directory.CreateDirectory(local);
            AppDataPath = local;

            var roaming = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData, Environment.SpecialFolderOption.DoNotVerify), AppName);
            Directory.CreateDirectory(roaming);
            AppConfigPath = roaming;
        }

        /// <summary>
        /// Store app data, including process data. This is AppData/Local/DataSpreads on Windows and home/.local/share/DataSpreads.
        /// </summary>
        internal static string AppDataPath;

        /// <summary>
        /// Store config data. This is AppData/Roaming/DataSpreads folder on Windows and home/.config/DataSpreads on Linux.
        /// </summary>
        internal static string AppConfigPath;

        public static bool IsLiteClient = false;

        internal static int AssumeDeadAfterSeconds { get; set; } = 60;

        // TODO review
        internal static LMDBEnvironmentFlags ProcessConfigEnvFlags { get; set; } = LMDBEnvironmentFlags.WriteMap;

        internal static LMDBEnvironmentFlags StreamStateEnvFlags { get; set; } = LMDBEnvironmentFlags.WriteMap;

        /// <summary>
        /// By default we prefer NoSync. It keeps ACI property on journaled file systems.
        /// http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340
        /// Performance difference is too large vs (WriteMap | MapAsync) so we are ready
        /// to lose durability of last transaction in case of a (very rare) system crash.
        /// TODO reserach which FS are safe. We cannot afford DB corruption, but can lose last txns.
        ///
        /// </summary>
        internal static LMDBEnvironmentFlags StreamLogBufferPoolFlags { get; set; } = LMDBEnvironmentFlags.NoSync;

        /// <summary>
        /// Same as <see cref="StreamLogBufferPoolFlags"/>.
        /// TODO Consider LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.MapAsync if it is safer. It is slower but kind of tolerable with our auto rate adjustment.
        /// </summary>
        internal static LMDBEnvironmentFlags StreamBlockIndexFlags { get; set; } = LMDBEnvironmentFlags.NoSync;

        // 16 GB should be plenty for high load instances - we try to offload and reused shared memory
        // and the table should not grow that large. On low-load systems it will never grow large
        // and mmaped file will be small since it growths on demand.
        private static int _streamBlockTableMaxSizeMb = 64 * 1024;

        public const int MinimumLogSizeMb = 256;

        /// <summary>
        /// Default is 16 GB. Minimum value in 256 MB. Set this value to max space available if there is no
        /// data persistence service configured for a data store.
        /// </summary>
        // TODO this is data store setting and not Process config setting
        public static int StreamBlockTableMaxSizeMb
        {
            get => _streamBlockTableMaxSizeMb;
            set
            {
                //
                if (value < MinimumLogSizeMb)
                {
                    _streamBlockTableMaxSizeMb = MinimumLogSizeMb;
                }
                _streamBlockTableMaxSizeMb = value;
            }
        }

        // WAL
        public static int WALChunkMinSize { get; set; } = 16 * 1024;

        //
        public static int PackedChunkSize { get; set; } = 1000_000;

        internal static double PackerTimeoutSeconds { get; set; } = 60;

        //public static bool WalCompress { get; set; } = true;
        //public static bool WALCompressLz4 { get; set; } = false;
    }
}
