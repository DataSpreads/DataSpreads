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
using System.Collections.Generic;
using System.Runtime.Serialization;
using DataSpreads.Config;

namespace DataSpreads
{
    /// <summary>
    /// Static metadata. If some fields need to change over time
    /// it indicates that those fields must be stored is a
    /// separate stream and are not a part of static metadata.
    /// </summary>
    [DataContract]
    public class Metadata
    {
        private Metadata()
        {
        }

        public Metadata(string description)
        {
            Description = description;
        }

        [DataMember(Name = "createdat")]
        public long CreatedAt { get; internal set; } = (long)ProcessConfig.CurrentTime;

        [DataMember(Name = "description")]
        public string Description { get; internal set; }

        [DataMember(Name = "tags")]
        public Dictionary<string, string> Tags { get; internal set; }

        public Metadata New(string description)
        {
            return new Metadata().WithDescription(description);
        }

        public Metadata WithDescription(string description)
        {
            if (Description == null)
            {
                Description = description;
                return this;
            }
            throw new InvalidOperationException("Metadata description is already set.");
        }

        public Metadata WithTimezone(string timezone)
        {
            // TODO Validate Timezone
            if (Tags == null)
            {
                Tags = new Dictionary<string, string>();
            }
            Tags.Add("timezone", timezone);
            return this;
        }

        // TODO etc. With pattern for known tags
        // Will need to validate known tags on save.
        // Or add item getter, make Tags internal and
        // add WithTag(), check for known types inside it
    }
}
