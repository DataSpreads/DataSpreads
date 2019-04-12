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
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using Spreads.Serialization.Utf8Json;

namespace DataSpreads.Events
{
    /// <summary>
    /// When an event
    /// </summary>
    [Obsolete]
    [JsonFormatter(typeof(Formatter))]
    internal struct RemoteEventResponse
    {
        // TODO custom formatter
        // Just zero on success, or [rc, "err"] on failure.

        /// <summary>
        /// Response code. 0 means success. Non zero error code depends on request context.
        /// </summary>
        [DataMember(Name = "rc")]
        public int ResultCode { get; set; }

        /// <summary>
        /// Human readable error description.
        /// </summary>
        [DataMember(Name = "err")]
        public string Err { get; set; }

        public class Formatter : IJsonFormatter<RemoteEventResponse>
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Serialize(ref JsonWriter writer, RemoteEventResponse value, IJsonFormatterResolver formatterResolver)
            {
                if (value.ResultCode == 0)
                {
                    writer.WriteRaw((byte)'0');
                }
                else
                {
                    writer.WriteBeginArray();

                    writer.WriteInt32(value.ResultCode);
                    writer.WriteValueSeparator();

                    writer.WriteString(value.Err);

                    writer.WriteEndArray();
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public RemoteEventResponse Deserialize(ref JsonReader reader, IJsonFormatterResolver formatterResolver)
            {
                if (reader.ReadIsBeginArray())
                {
                    var rc = reader.ReadInt32();
                    reader.ReadIsValueSeparatorWithVerify();
                    var err = reader.ReadString();
                    reader.ReadIsEndArrayWithVerify();
                    return new RemoteEventResponse
                    {
                        ResultCode = rc,
                        Err = err
                    };
                }

                var zero = reader.ReadInt32();
                if (zero != 0)
                {
                    throw new InvalidOperationException("Bad RemoteEventResponse format");
                }

                return default;
            }
        }
    }
}