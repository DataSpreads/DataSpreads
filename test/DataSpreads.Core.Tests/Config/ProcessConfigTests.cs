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
using NUnit.Framework;
using Spreads.Threading;
using Spreads.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DataSpreads.Tests.Config
{
    [Category("CI")]
    [TestFixture, SingleThreaded]
    public unsafe class ProcessConfigTests
    {
        [OneTimeSetUp]
        public void Clear()
        {
            StartupConfig.AppDataPath = Path.Combine(TestUtils.BaseDataPath, "DataConfigTests");
            // TestUtils.ClearAll();
        }

        [Test]
        public void CouldOpenProcessConfigStorage()
        {
            var path = TestUtils.GetPath(clear: true);
            var pcs = new ProcessConfig.ProcessConfigStorage(path);

            var value = TimeService.Default.CurrentTime;

            pcs.SharedRecord._processBuffer.WriteInt64(0, value.Nanos);
            Assert.IsTrue(pcs.SharedRecord._processBuffer.Data != null);

            pcs.Dispose();
            pcs = new ProcessConfig.ProcessConfigStorage(path);

            Assert.AreEqual(value.Nanos, pcs.SharedRecord._processBuffer.ReadInt64(0));

            pcs.Dispose();
        }

        [Test]
        public void CouldCreateGetDeleteProcessConfigRecord()
        {
            var path = TestUtils.GetPath(clear: true);

            ProcessConfig.Init(path);

            var ct = ProcessConfig.CurrentTime;

            var pcs = ProcessConfig.Storage;

            var pcr = pcs.CreateNew();

            var pcr1 = pcs.GetRecord(pcr.Wpid);

            Assert.AreEqual(pcr.Wpid, pcr1.Wpid);
            Assert.IsTrue(pcr._processBuffer.Data == pcr1._processBuffer.Data);

            var wpid = pcr1.Wpid;

            pcs.Delete(pcr1);

            var pcr2 = pcs.GetRecord(wpid);

            Assert.IsFalse(pcr2._processBuffer.IsValid);

            pcs.Dispose();
        }

        [Test, Explicit("Has side effects for parallel execution")]
        public void CouldOpenProcessConfig()
        {
            //Settings.EnableChaosMonkey = true;
            //ChaosMonkey.Force = true;
            StartupConfig.AssumeDeadAfterSeconds = 1;
            var path = TestUtils.GetPath();
            // ProcessConfig.Init(path);

            var pc1 = new ProcessConfig(path);
            var pc2 = new ProcessConfig(path);

            var wpid1 = pc1.Wpid;
            var wpid2 = pc2.Wpid;

            Console.WriteLine(ProcessConfig.IsPidAlive(1234567));
            Console.WriteLine(ProcessConfig.IsPidAlive(System.Diagnostics.Process.GetCurrentProcess().Id));

            Console.WriteLine("1 ------");
            Console.WriteLine(ProcessConfig.IsWpidAlive(wpid2));
            Console.WriteLine(ProcessConfig.IsWpidAlive(wpid1));

            pc2.TogglePaused();

            Thread.Sleep((StartupConfig.AssumeDeadAfterSeconds + 1) * 1000);

            Console.WriteLine("2 ------");
            Console.WriteLine(ProcessConfig.IsWpidAlive(wpid2));
            Console.WriteLine(ProcessConfig.IsWpidAlive(wpid1));

            pc2.Suicide();

            pc1.Suicide();
            Console.WriteLine("3 ------");
            Console.WriteLine(ProcessConfig.IsWpidAlive(wpid2));
            Console.WriteLine(ProcessConfig.IsWpidAlive(wpid1));

            Console.WriteLine("4 ------");
            Console.WriteLine(ProcessConfig.IsWpidAlive(wpid2));
            Console.WriteLine(ProcessConfig.IsWpidAlive(wpid1));

            foreach (var deadWpid in ProcessConfig.DeadWpids)
            {
                Console.WriteLine($"Known dead wpid: " + deadWpid);
            }

            pc1.Dispose();
            pc2.Dispose();
        }

        [Test, Explicit("Has side effects for parallel execution")]
        public void CouldOpenCloseManyProcessConfigs()
        {
            StartupConfig.AssumeDeadAfterSeconds = 1;
            var path = TestUtils.GetPath();

            // ProcessConfig.Init(path);

            var tasks = new List<Task>();
            var count = 10;
            using (Benchmark.Run("Many DC", count * 1000))
            {
                for (int i = 0; i < count; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        var pc1 = new ProcessConfig(path);
                        //pc1.Dispose();
                    }));
                }

                Task.WaitAll(tasks.ToArray());
            }

            Console.WriteLine("Used space: " + ProcessConfig.Storage.Environment.UsedSize);
            Console.WriteLine("Last inatnce id: " + ProcessConfig.LastInstanceId);
        }

        [Test, Explicit("Has side effects for parallel execution")]
        public void CouldWriteDataStoreConfig()
        {
            var path = TestUtils.GetPath();
            var pc = new ProcessConfig(path);

            var init = pc.SaveDataStoreConfig(new DataStoreConfig()
            {
                DataStoreDirectory = "G:/DataSpreads"
            });
            Assert.IsTrue(init);

            init = pc.SaveDataStoreConfig(new DataStoreConfig()
            {
                DataStoreDirectory = "G:/DataSpreads"
            });
            Assert.IsFalse(init);
        }
    }
}
