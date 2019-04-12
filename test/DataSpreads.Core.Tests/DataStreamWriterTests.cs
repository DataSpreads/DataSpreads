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
using Spreads;
using Spreads.DataTypes;
using Spreads.LMDB;
using Spreads.Utils;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataSpreads.Tests
{
    [TestFixture]
    public class DataStreamWriterTests
    {
        [Test, Explicit("long running, changes static setting")]
        public async Task CouldWriteUsingDswAsync()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618

            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 20 * 1024, disableNotificationLog: false,
                disablePacker: false);

            var streamId = (StreamLogId)42L;

            var count = TestUtils.GetBenchCount(50_000_000, 1_000);
            var countSlice = TestUtils.GetBenchCount(10_000_000, 100);

            short valueSize = (short)(Timestamp.Size + Unsafe.SizeOf<long>());

            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, valueSize, StreamLogFlags.IsBinary);

            var sl = new StreamLog(slm, state, 8_000_000, "test_stream");

            Marshal.WriteInt64(sl.State.StatePointer + StreamLogState.StreamLogStateRecord.LockerOffset, 0);

            slm.OpenStreams.TryAdd(42, sl);

            var writer = new DataStreamWriter<long>(sl, KeySorting.NotEnforced, WriteMode.BatchWrite);

            try
            {
                var bench = Benchmark.Run("Write", countSlice);
                for (long i = 0; i < count; i++)
                {
                    var addedVersion = await writer.TryAppend(100000L + i);
                    if (addedVersion == 0)
                    {
                        Assert.Fail();
                    }

                    if (i > 0 && i % countSlice == 0)
                    {
                        bench.Dispose();
                        bench = Benchmark.Run("Write", countSlice);
                    }
                }

                bench.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"EXCEPTION DURING WRITE: " + ex);
                throw;
            }

            for (int r = 0; r < 10; r++)
            {
                using (Benchmark.Run("Read", count))
                {
                    var slc = sl.GetCursor();
                    var dsc = new DataStreamCursor<long>(slc);
                    var ds = dsc.Source;
                    var c = ds.GetCursor();

                    var cnt = 0L;
                    while (c.MoveNext())
                    {
                        var cv = c.CurrentValue;
                        var ts = cv.Timestamp;
                        if (ts == default)
                        {
                            Assert.Fail("ts == default");
                        }

                        var value = cv.Value;

                        if (value != 100000L + cnt)
                        {
                            Assert.Fail($"value {value} != cnt {100000L + cnt}");
                        }
                        cnt++;
                    }
                    c.Dispose();
                    if (cnt != count)
                    {
                        Assert.Fail($"cnt {cnt } != count {count}");
                    }
                }
            }

            slm.BufferPool.PrintBuffersAfterPoolDispose = true;

            Thread.Sleep(500);

            // TODO (!) Fix disposal

            // sl.Dispose();

            // slm.Dispose();

            Benchmark.Dump();
        }
    }
}
