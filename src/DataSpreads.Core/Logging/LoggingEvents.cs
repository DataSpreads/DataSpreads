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

using System.Diagnostics.Tracing;

namespace DataSpreads.Logging
{

    // TODO We should log into StreamLogs.
    // The Maoni's post is nice, but we should write GC events to internal streams
    // via some background monitoring service.
    // However having data in PerfView is also nice.

    [EventSource(Name = "DataSpreadsEvents", Guid = "{FCCE9DBC-6668-4248-9581-3F5D4FC7A71B}")]
    public class LoggingEvents : EventSource
    {
        public new static readonly LoggingEvents Write = new LoggingEvents();

        public enum Event
        {
            StreamBlockRotated = 1
        }

        [Event(1, Level = EventLevel.Verbose)]
        public void StreamBlockRotated(int repoId, int streamId, int newBlockSize)
        {
            if (IsEnabled(EventLevel.Informational, EventKeywords.None))
            {
                WriteEvent((int)Event.StreamBlockRotated, repoId, streamId, newBlockSize);
            }
        }
    }
}
