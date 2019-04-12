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

using Spreads.Serialization;

namespace DataSpreads.Symbology
{
    internal struct MetadataRecord
    {
        public string PK { get; set; }

        public int RepoId { get; set; }

        public int ContainerId { get; set; }

        public int ParentId { get; set; }
        public int Counter { get; set; }
        public int Version { get; set; }
        public int RemoteId { get; set; }
        public int Permissions { get; set; }
        public byte SchemaType { get; set; }
        public ContainerSchema Schema { get; set; }
        public Metadata Metadata { get; set; }
    }
}
