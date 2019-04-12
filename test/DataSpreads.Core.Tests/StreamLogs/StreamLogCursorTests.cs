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

using DataSpreads.StreamLogs;
using NUnit.Framework;
using Spreads;
using Spreads.LMDB;
using Spreads.Serialization;
using Spreads.Utils;
using System;
using System.Threading.Tasks;
using DataSpreads.Config;

namespace DataSpreads.Tests.StreamLogs
{
    [Category("CI")]
    [TestFixture]
    public class StreamLogCursorTests
    {
        [Test]
        public void StreamLogCursorWorks()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteToWAlFromStream";
            var processConfig = new ProcessConfig(path);

            var slm = new StreamLogManager(processConfig, repoName, null, 1024, true, true);
            var streamId = (StreamLogId)42L;
            short valueSize = 8;
            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, valueSize, SerializationFormat.Binary);
            var sl = new StreamLog(slm, state, 10_000_000, "test_stream");

            var c = sl.GetCursor();

            Assert.IsFalse(c.MoveFirst());
            Assert.IsFalse(c.MoveLast());
            Assert.IsFalse(c.MoveNext());
            Assert.IsFalse(c.MovePrevious());

            Assert.IsFalse(c.MoveAt(0, Lookup.LT));
            Assert.IsFalse(c.MoveAt(0, Lookup.LE));
            Assert.IsFalse(c.MoveAt(0, Lookup.EQ));
            Assert.IsFalse(c.MoveAt(0, Lookup.GE));
            Assert.IsFalse(c.MoveAt(0, Lookup.GT));

            Assert.IsFalse(c.MoveAt(ulong.MaxValue, Lookup.LT));
            Assert.IsFalse(c.MoveAt(ulong.MaxValue, Lookup.LE));
            Assert.IsFalse(c.MoveAt(ulong.MaxValue, Lookup.EQ));
            Assert.IsFalse(c.MoveAt(ulong.MaxValue, Lookup.GE));
            Assert.IsFalse(c.MoveAt(ulong.MaxValue, Lookup.GT));

            Assert.IsFalse(c.TryGetValue(123, out _));

            AddToSl();

            Assert.IsTrue(c.MoveFirst());
            Assert.IsTrue(c.MoveLast());
            Assert.IsFalse(c.MoveNext());
            Assert.IsFalse(c.MovePrevious());

            Assert.IsFalse(c.MoveAt(0, Lookup.LT));
            Assert.IsFalse(c.MoveAt(0, Lookup.LE));
            Assert.IsFalse(c.MoveAt(0, Lookup.EQ));
            Assert.IsTrue(c.MoveAt(0, Lookup.GE));
            Assert.IsTrue(c.MoveAt(0, Lookup.GT));

            Assert.IsTrue(c.MoveAt(ulong.MaxValue, Lookup.LT));
            Assert.IsTrue(c.MoveAt(ulong.MaxValue, Lookup.LE));
            Assert.IsFalse(c.MoveAt(ulong.MaxValue, Lookup.EQ));
            Assert.IsFalse(c.MoveAt(ulong.MaxValue, Lookup.GE));
            Assert.IsFalse(c.MoveAt(ulong.MaxValue, Lookup.GT));

            Assert.IsFalse(c.TryGetValue(123, out _));

            void AddToSl()
            {
                var claim = sl.Claim(0, 8);

                if (claim.Length != 8)
                {
                    Assert.Fail();
                }
                claim.Write<long>(0, 123456);

                sl.Commit();
            }

            sl.Dispose();

            // Benchmark.Dump();

            slm.Dispose();
        }

        [Test]
        public void CouldGetVersionFromNewllyCreatedSl()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteToWAlFromStream";
            var processConfig = new ProcessConfig(path);

            var slm = new StreamLogManager(processConfig, repoName, null, 1024, true, true);
            var streamId = (StreamLogId)42L;
            short valueSize = 8;
            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, valueSize, SerializationFormat.Binary);
            var sl = new StreamLog(slm, state, 10_000_000, "test_stream");

            Assert.IsFalse(sl.IsCompleted);

            Assert.AreEqual(0, sl.CurrentVersion);
            Assert.AreEqual(sl.Count, sl.CurrentVersion);

            sl.Dispose();

            slm.Dispose();
        }

        [Test]
        public void CouldReadWhileWriting()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteToWAlFromStream";
            var processConfig = new ProcessConfig(path);

            var slm = new StreamLogManager(processConfig, repoName, null, 1024, true, true);
            var streamId = (StreamLogId)42L;
            short valueSize = 8;
            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, valueSize, SerializationFormat.Binary);
            var sl = new StreamLog(slm, state, 10_000_000, "test_stream");

            var count = 1_000_000;

            var rt = Task.Run(() =>
            {
                var cnt = 0L;
                var c = sl.GetCursor();
                while (cnt < count)
                {
                    if (c.MoveNext())
                    {
                        if (c.CurrentValue.Read<long>(0) != cnt)
                        {
                            Assert.Fail($"c.CurrentValue.Read<long>(0) {c.CurrentValue.Read<long>(0)} != cnt {cnt}");
                        }
                        cnt++;
                    }
                    else
                    {
                        // Console.WriteLine("false");
                    }
                }

                Console.WriteLine("Reader finished: " + cnt);
                c.Dispose();
            });

            for (int i = 0; i < count; i++)
            {
                var claim = sl.Claim(0, 8);

                if (claim.Length != 8)
                {
                    Assert.Fail();
                }
                claim.Write<long>(0, i);

                sl.Commit();
            }

            Console.WriteLine("Writer finished");

            rt.Wait();

            sl.Dispose();

            slm.Dispose();
        }

        [Test]
        public void CouldReadAfterWriting()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteToWAlFromStream";
            var processConfig = new ProcessConfig(path);

            var slm = new StreamLogManager(processConfig, repoName, null, 1024, true, true);
            var streamId = (StreamLogId)42L;
            short valueSize = 8;
            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, valueSize, StreamLogFlags.IsBinary | StreamLogFlags.NoPacking);
            // sl.DisablePacking = true;

            var sl = new StreamLog(slm, state, 100_000, "test_stream");

            var count = TestUtils.GetBenchCount(1_000_000, 1000);

            var cnt = 0L;
            var c = sl.GetCursor();

            for (int i = 0; i < count; i++)
            {
                var claim = sl.Claim(0, valueSize);

                if (claim.Length != valueSize)
                {
                    Assert.Fail("claim.Length != 8");
                }
                claim.Write<long>(0, i);

                var version = sl.Commit();

                if (version != (ulong)i + 1)
                {
                    Assert.Fail("version != (ulong) i + 1");
                }

                if (i == 16360)
                {
                    Console.WriteLine("stop");
                }

                if (c.MoveNext())
                {
                    if (c.CurrentValue.Read<long>(0) != cnt)
                    {
                        Assert.Fail($"c.CurrentValue.Read<long>(0) {c.CurrentValue.Read<long>(0)} != cnt {cnt}");
                    }
                    cnt++;
                }
                else
                {
                    Assert.Fail("Cannot move next after write: " + i);
                }
            }

            Console.WriteLine("Writer finished");

            c.Dispose();

            sl.Dispose();

            slm.Dispose();
        }

        [Test]
        public void CouldMoveAtAfterWriting()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteToWAlFromStream";
            var processConfig = new ProcessConfig(path);

            var slm = new StreamLogManager(processConfig, repoName, null, 1024, true, disablePacker: true);
            var streamId = (StreamLogId)42L;
            short valueSize = 8;
            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, valueSize, StreamLogFlags.IsBinary | StreamLogFlags.NoPacking);
            // sl.DisablePacking = true;

            var sl = new StreamLog(slm, state, 1_000, "test_stream");

            ulong count = (ulong)TestUtils.GetBenchCount(500_000, 500);

            var cnt = 0L;
            var c = sl.GetCursor();

            using (Benchmark.Run("Write-MoveAt", (long)count))
            {
                for (ulong i = 0; i < count; i++)
                {
                    var claim = sl.Claim(0, valueSize);

                    if (claim.Length != valueSize)
                    {
                        Assert.Fail("claim.Length != 8");
                    }

                    claim.Write<long>(0, (long)i);

                    var version = sl.Commit();

                    if (version != (ulong)i + 1)
                    {
                        Assert.Fail("version != (ulong) i + 1");
                    }

                    if (c.MoveNext())
                    {
                        if (c.CurrentValue.Read<long>(0) != cnt)
                        {
                            Assert.Fail($"c.CurrentValue.Read<long>(0) {c.CurrentValue.Read<long>(0)} != cnt {cnt}");
                        }

                        cnt++;

                        var position = c.CurrentKey;

                        if (i > 1)
                        {
                            if (!c.MoveAt(i, Lookup.LT))
                            {
                                Assert.Fail($"!c.MoveAt(i {i}, Lookup.LT)");
                            }

                            if (c.CurrentKey != i - 1)
                            {
                                Assert.Fail($"c.CurrentKey {c.CurrentKey} != i - 1 {i - 1}");
                            }

                            if (!c.MoveAt(i - 1, Lookup.LE))
                            {
                                Assert.Fail($"!c.MoveAt(i - 1 {i - 1}, Lookup.LE)");
                            }

                            if (c.CurrentKey != i - 1)
                            {
                                Assert.Fail($"c.CurrentKey {c.CurrentKey} != i - 1 {i - 1}");
                            }
                        }

                        if (i > 2 && i < count - 1)
                        {
                            if (!c.MoveAt(i - 1, Lookup.GT))
                            {
                                Assert.Fail($"X: !c.MoveAt(i {i}, Lookup.GT)");
                            }

                            if (c.CurrentKey != i)
                            {
                                Assert.Fail($"c.CurrentKey {c.CurrentKey} != i + 1 {i + 1}");
                            }

                            //if (i == 481)
                            //{
                            //    Console.WriteLine("fix me");
                            //}

                            if (!c.MoveAt(i - 1, Lookup.GE))
                            {
                                Assert.Fail($"!c.MoveAt(i - 1 {i}, Lookup.GE)");
                            }

                            if (c.CurrentKey != i - 1)
                            {
                                Assert.Fail($"c.CurrentKey {c.CurrentKey} != i + 1 {i + 1}");
                            }
                        }

                        if (!c.MoveAt(position, Lookup.EQ))
                        {
                            Assert.Fail("Cannot move to position: !c.MoveAt(position, Lookup.EQ)");
                        }
                    }
                    else
                    {
                        Assert.Fail("Cannot move next after write: " + i);
                    }
                }
            }

            using (Benchmark.Run("MoveAt", (long)count))
            {
                for (ulong i = 2; i < count - 1; i++)
                {
                    var position = i;

                    if (i > 1)
                    {
                        if (!c.MoveAt(i, Lookup.LT))
                        {
                            Assert.Fail($"!c.MoveAt(i {i}, Lookup.LT)");
                        }

                        if (c.CurrentKey != i - 1)
                        {
                            Assert.Fail($"c.CurrentKey {c.CurrentKey} != i - 1 {i - 1}");
                        }

                        if (!c.MoveAt(i - 1, Lookup.LE))
                        {
                            Assert.Fail($"!c.MoveAt(i - 1 {i - 1}, Lookup.LE)");
                        }

                        if (c.CurrentKey != i - 1)
                        {
                            Assert.Fail($"c.CurrentKey {c.CurrentKey} != i - 1 {i - 1}");
                        }
                    }

                    if (i < count - 1)
                    {
                        if (!c.MoveAt(i, Lookup.GT))
                        {
                            Assert.Fail($"!c.MoveAt(i {i}, Lookup.GT)");
                        }

                        if (c.CurrentKey != i + 1)
                        {
                            Assert.Fail($"c.CurrentKey {c.CurrentKey} != i + 1 {i + 1}");
                        }

                        if (!c.MoveAt(i + 1, Lookup.GE))
                        {
                            Assert.Fail($"!c.MoveAt(i + 1 {i}, Lookup.GE)");
                        }

                        if (c.CurrentKey != i + 1)
                        {
                            Assert.Fail($"c.CurrentKey {c.CurrentKey} != i + 1 {i + 1}");
                        }
                    }

                    if (!c.MoveAt(position, Lookup.EQ))
                    {
                        Assert.Fail("Cannot move to position: !c.MoveAt(position, Lookup.EQ)");
                    }
                }
            }

            Benchmark.Dump();

            c.Dispose();

            sl.Dispose();

            slm.Dispose();
        }
    }
}
