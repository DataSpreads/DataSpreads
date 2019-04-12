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
using DataSpreads.StreamLogs;
using NUnit.Framework;
using Spreads.Serialization;

namespace DataSpreads.Tests.StreamLogs
{
    [TestFixture]
    public class StreamLogManagerTests
    {
        [Test]
        public void CouldOpenCloseSlm()
        {
            var path = TestUtils.GetPath();
            var pc = new ProcessConfig(path);
            var slm = new StreamLogManager(pc, "test");
            var slid = new StreamLogId(-1, 1);

            var inited = slm.InitStreamLog(slid, 0, StreamLogFlags.IsBinary);

            Assert.IsTrue(inited);
            
            var sl = slm.OpenStreamLog(slid);

            var mem = sl.ClaimRestOfBlockMemory(1);
            var freeSpace = mem.Length;
            mem.Span[0] = 123;
            sl.Commit(1);

            var mem1 = sl.ClaimRestOfBlockMemory(1);
            Assert.AreEqual(mem1.Length, freeSpace - 1 - 4);

            sl.Commit(1);

            var db = sl.DangerousGetUnpacked(1);
            Assert.AreEqual(123, db.ReadByte(0));

            sl.Dispose();

            slm.Dispose();
        }
    }
}
