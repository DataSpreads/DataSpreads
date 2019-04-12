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
using DataSpreads.Config;
using Spreads;
using Spreads.Serialization.Utf8Json;
using Spreads.Serialization.Utf8Json.Resolvers;

namespace DataSpreads.Symbology
{
    // TODO this is internal, merge with DTO and expose just RO reference value.

    [DataContract]
    public class DataStreamMetadataOld
    {
        // We need shorter names because this type is stored uncompressed and often in memory

        [DataMember(Name = "sid")]
        public long StreamId { get; set; }

        [DataMember(Name = "gsid")]
        public long GlobalStreamId { get; set; }

        [DataMember(Name = "tid")]
        public string TextId { get; set; }

        [DataMember(Name = "uat")]
        public long UpdatedAt { get; set; } = (long)ProcessConfig.CurrentTime;

        /// <summary>
        /// Value of DataStreamType enum
        /// </summary>
        [DataMember(Name = "sty")]
        public int DataStreamType { get; set; }

        [DataMember(Name = "fmt")]
        public int SerializationFormat { get; set; }

        [DataMember(Name = "ktc")]
        public int KeyTypeCode { get; set; }

        [DataMember(Name = "kts")]
        public int KeyTypeSize { get; set; }

        [DataMember(Name = "ktn")]
        public string KeyTypeName { get; set; }

        [DataMember(Name = "ktsch")]
        public string KeyTypeSchema { get; set; }

        [DataMember(Name = "vtc")]
        public int ValueTypeCode { get; set; }

        [DataMember(Name = "vts")]
        public int ValueTypeSize { get; set; }

        [DataMember(Name = "vtn")]
        public string ValueTypeName { get; set; }

        [DataMember(Name = "vtsch")]
        public string ValueTypeSchema { get; set; }

        [DataMember(Name = "vstyc")]
        public int? ValueSubtypeCode { get; set; }

        [DataMember(Name = "per")]
        public int? UnitPeriod { get; set; }

        [DataMember(Name = "perc")]
        public int? PeriodCount { get; set; }

        [DataMember(Name = "tz")]
        public string TimeZone { get; set; }

        /// <summary>
        /// Null means not chunked. Should not use small numbers, but 1 is a valid chunk size.
        /// </summary>
        [DataMember(Name = "serchs")]
        public int? SeriesChunkSize { get; set; }

        [DataMember(Name = "rth")]
        public int? RateHint { get; set; }

        [DataMember(Name = "info")]
        public string Description { get; set; }

        [DataMember(Name = "dynmd")]
        public string JsonDynamicMetadata { get; set; }
    }

    
    internal class DataStreamMetadataDto
    {
        private DataStreamMetadataOld _dataStreamMetadataOld;

        public DataStreamMetadataDto()
        { }

        public DataStreamMetadataDto(DataStreamMetadataOld dataStreamMetadataOld)
        {
            _dataStreamMetadataOld = dataStreamMetadataOld;
            if (dataStreamMetadataOld.GlobalStreamId != 0)
            {
                ThrowHelper.ThrowInvalidOperationException("GlobalStreamId must be set by remote storage.");
            }
            SerializedMetadata = JsonSerializer.ToJsonString(dataStreamMetadataOld, StandardResolver.ExcludeNull);
            TextId = dataStreamMetadataOld.TextId;
            StreamId = dataStreamMetadataOld.StreamId;
            // GlobalStreamId = dataStreamMetadata.GlobalStreamId == 0 ? null : (long?)dataStreamMetadata.GlobalStreamId;
            UpdatedAt = dataStreamMetadataOld.UpdatedAt;
        }

        // Hashkey in DynamoDB
        public string TextId { get; set; }

        // In the (long long) future we will support aliasing from sid to gsid

        // Global index in DynamoDB
        public long StreamId { get; set; }

        // When upstream is present
        public long? GlobalStreamId { get; set; }

        public long UpdatedAt { get; set; }

        public string SerializedMetadata { get; set; }

        // DTO is not supposed to live longer than reads from db and getting this once
        
        public DataStreamMetadataOld DataStreamMetadataOld =>
            _dataStreamMetadataOld ?? (_dataStreamMetadataOld = JsonSerializer.Deserialize<DataStreamMetadataOld>(SerializedMetadata, StandardResolver.ExcludeNull));
    }
}
