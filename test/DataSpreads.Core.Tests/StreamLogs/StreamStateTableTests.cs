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
using System.Threading;
using DataSpreads.Config;
using DataSpreads.StreamLogs;
using NUnit.Framework;

namespace DataSpreads.Tests.StreamLogs
{
    // [Category("CI")]
    [TestFixture]
    public class StreamStateTableTests
    {
        [Test]
        public void CouldOpenStateTable()
        {
            var path = TestUtils.GetPath();
            var stateTable = new StreamLogStateStorage(path);
            stateTable.Dispose();
        }

        [Test]
        public void CouldOpenSharedState()
        {
            var path = TestUtils.GetPath();

            var stateTable = new StreamLogStateStorage(path);

            stateTable.SharedState.SetWalPosition(123);

            var position = stateTable.SharedState.GetWalPosition();

            Assert.AreEqual(123, position);

            stateTable.Dispose();
        }

        [Test]
        public void CouldCreateStreamStateView()
        {
            var path = TestUtils.GetPath();

            var stateTable = new StreamLogStateStorage(path);
            Console.WriteLine($"Used size before 1: {stateTable.UsedSize}");
            var view0 = stateTable.GetState((StreamLogId)1);
            Console.WriteLine($"Used size after 1: {stateTable.UsedSize}");

            var view1 = stateTable.GetState((StreamLogId)2);
            Console.WriteLine($"Used size after 2: {stateTable.UsedSize}");

            Console.WriteLine($"0 - 1 Pointer diff: {view1.StatePointer.ToInt64() - view0.StatePointer.ToInt64()}");

            var view1023 = stateTable.GetState((StreamLogId)1023);
            Console.WriteLine($"Used size after 1023: {stateTable.UsedSize}");

            Console.WriteLine($"1 - 1024 Pointer diff: {view1023.StatePointer.ToInt64() - view0.StatePointer.ToInt64()}");

            var view1024 = stateTable.GetState((StreamLogId)1024);
            Console.WriteLine($"Used size after 1024: {stateTable.UsedSize}");

            Console.WriteLine($"1 - 1024 Pointer diff: {view1024.StatePointer.ToInt64() - view0.StatePointer.ToInt64()}");

            var viewN1 = stateTable.GetState((StreamLogId)(-1));
            Console.WriteLine($"Used size after -1: {stateTable.UsedSize}");

            var viewN2 = stateTable.GetState((StreamLogId)(-2));
            Console.WriteLine($"Used size after -2: {stateTable.UsedSize}");

            Console.WriteLine($"-2 - -1 Pointer diff: {viewN2.StatePointer.ToInt64() - viewN1.StatePointer.ToInt64()}");

            stateTable.Dispose();
        }

        [Test]
        public void CouldAcquirePackerLock()
        {
            var path = TestUtils.GetPath();
            ProcessConfig.Init(path);
            var streamId = 42;

            StartupConfig.PackerTimeoutSeconds = 0.1;

            var stateTable = new StreamLogStateStorage(path);
            var state = stateTable.GetState((StreamLogId)streamId);

            var pl = state.AcquirePackerLock();
            var pl1 = state.AcquirePackerLock();
            Assert.IsTrue(pl > 0);
            Assert.IsTrue(pl1 == 0);

            var pl2 = state.AcquirePackerLock(pl);
            Assert.IsTrue(pl2 > pl);

            Thread.Sleep(200);

            var pl3 = state.AcquirePackerLock();
            var pl4 = state.AcquirePackerLock();
            Assert.IsTrue(pl3 > 0);
            Assert.IsTrue(pl4 == 0);

            stateTable.Dispose();
        }
    }
}
