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

using System.Runtime.Serialization;

namespace DataSpreads
{
    public class DataStoreConfig
    {
        [DataMember(Name = "WARNING")]
        public string Warn { get; set; } = "Changing values in this file manually will not have any effect and will break existing configuration. Please read docs on how to move existing data store to a different location.";

        [DataMember(Name = "dataStoreDirectory")]
        public string DataStoreDirectory { get; set; }

        [DataMember(Name = "syncerDirectory")]
        public string SyncerDirectory { get; set; }

        [DataMember(Name = "archiveDirectory")]
        public string ArchiveDirectory { get; set; }

        [DataMember(Name = "maxLogSizeMb")]
        public int MaxLogSizeMb { get; set; } = 10 * 1024;
    }
}