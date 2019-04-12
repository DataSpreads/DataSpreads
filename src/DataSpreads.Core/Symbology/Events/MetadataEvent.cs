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
using Spreads.Serialization;

namespace DataSpreads.Symbology.Events
{
    /// <summary>
    ///
    /// </summary>
    internal struct MetadataEvent
    {
        public enum EventType : byte
        {
            /// <summary>
            /// Requires <see cref="MetadataEvent.DataPath"/>, <see cref="MetadataEvent.Metadata"/>.
            /// For streams requires <see cref="MetadataEvent.SchemaType"/> and <see cref="MetadataEvent.Schema"/>.
            /// </summary>
            Create = 1,
            Rename = 2,
            Move = 3,
            UpdateMetadata = 4,
            Delete = 5,

            AddPermissions = 6
        }

        /// <summary>
        /// Absolute path.
        /// </summary>
        [DataMember(Name = "dp")]
        public string DataPath { get; set; }

        [DataMember(Name = "repoid")]
        public int RepoId { get; set; }

        [DataMember(Name = "repoid")]
        public int StreamId { get; set; }

        [DataMember(Name = "remoteid")]
        public int RemoteId { get; set; }

        [DataMember(Name = "meta")]
        public Metadata Metadata { get; set; }

        [DataMember(Name = "schematype")]
        public byte SchemaType { get; set; }

        [DataMember(Name = "schema")]
        public ContainerSchema Schema { get; set; }
    }
}