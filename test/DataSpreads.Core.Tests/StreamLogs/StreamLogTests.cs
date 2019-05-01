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

using DataSpreads.Buffers;
using DataSpreads.Config;
using DataSpreads.StreamLogs;
using HdrHistogram;
using NUnit.Framework;
using ObjectLayoutInspector;
using Spreads;
using Spreads.DataTypes;
using Spreads.LMDB;
using Spreads.Serialization;
using Spreads.Utils;
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Spreads.Buffers;

#pragma warning disable 618

namespace DataSpreads.Tests.StreamLogs
{
    [Category("CI")]
    [TestFixture, SingleThreaded]
    public class StreamLogTests
    {
        [Test]
        public void StreamLogSize()
        {
            TypeLayout.PrintLayout<StreamLog>();
        }

        [Test]
        public void StreamBlockProxySize()
        {
            TypeLayout.PrintLayout<ReaderBlockCache.StreamBlockProxy>();
        }

        [Test]
        public void StreamLogCursorSize()
        {
            TypeLayout.PrintLayout<StreamLog.StreamLogCursor>();
        }

        [Test]
        public void CouldInitStreamLog()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadToLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.MapAsync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.MapAsync;
            var slm = new StreamLogManager(processConfig, repoName, null, 512, true, true);

            var streamId = (StreamLogId)42L;

            short valueSize = 8;

            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, 8, SerializationFormat.Binary);

            var sl = new StreamLog(slm, state, 100000, textId: "test_stream");

            Assert.IsFalse(sl.Init());

            sl.RotateActiveBlock(1, default, 8);

            sl.ActiveBlock.Claim(1, 8);
            sl.ActiveBlock.Commit();

            Assert.AreEqual(sl.ActiveBlock.FirstVersion, 1);

            sl.ActiveBlock.DisposeFree();

            sl.ActiveBlock = default;

            sl.Init();

            Assert.AreEqual(sl.ActiveBlock.FirstVersion, 1);

            sl.ActiveBlock.Claim(1, 8);
            sl.ActiveBlock.Commit();

            sl.ActiveBlock.Claim(2, 8);
            sl.ActiveBlock.Commit();

            sl.ActiveBlock.DisposeFree();
            sl.ActiveBlock = default;

            sl.Init();

            Assert.AreEqual(sl.ActiveBlock.FirstVersion, 1);
            Assert.AreEqual(sl.ActiveBlock.CountVolatile, 3);

            sl.ActiveBlock.DisposeFree();
            sl.ActiveBlock = default;

            sl.Init();

            Assert.AreEqual(sl.ActiveBlock.FirstVersion, 1);
            Assert.AreEqual(sl.ActiveBlock.CountVolatile, 3);

            sl.ActiveBlock.Complete();

            sl.ActiveBlock.DisposeFree();
            sl.ActiveBlock = default;

            sl.Init();

            Assert.AreEqual(sl.ActiveBlock.FirstVersion, 1);
            Assert.AreEqual(sl.ActiveBlock.CountVolatile, 3);
            Assert.IsTrue(sl.ActiveBlock.IsCompleted);

            // TODO Init after rotate

            // sl.Dispose does this: SharedMemory.Free(sl._activeBuffer);
            sl.Dispose();

            slm.Dispose();
        }

        [Test]
        public void CouldCommitToStreamLog()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldCommitToStreamLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.MapAsync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.MapAsync;
            var slm = new StreamLogManager(processConfig, repoName, null, 512, true, true);

            var streamId = (StreamLogId)42L;

            short valueSize = 8;

            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, 8, SerializationFormat.Binary);

            var sl = new StreamLog(slm, state, textId: "test_stream");

            sl.RotateActiveBlock(0, default, default);

            Assert.AreEqual(sl.ActiveBlock.FirstVersion, 1);

            var version = 1UL;
            while ((sl.ActiveBlock.Claim(version, 8)).IsValid)
            {
                sl.ActiveBlock.Commit();
                version++;
            }
            sl.ActiveBlock.Complete();

            SharedMemory.Free(sl.ActiveBuffer);
            sl.ActiveBlock = default;

            sl.Init();

            Assert.AreEqual(sl.ActiveBlock.FirstVersion, 1);
            Assert.IsTrue(sl.ActiveBlock.CountVolatile > 100);
            Assert.IsTrue(sl.ActiveBlock.IsCompleted);

            sl.Dispose();

            slm.Dispose();
        }

        [Test]
        public void CouldRotateStreamLog()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldRotateStreamLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.MapAsync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.MapAsync;
            var slm = new StreamLogManager(processConfig, repoName, null, 512, true, true);

            var streamId = (StreamLogId)42L;

            short valueSize = 8;

            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, 8, SerializationFormat.Binary);

            var sl = new StreamLog(slm, state, textId: "test_stream");

            Assert.IsFalse(sl.Init());

            sl.RotateActiveBlock(1, default, default);

            Assert.AreEqual(sl.ActiveBlock.FirstVersion, 1);

            var version = 1UL;
            while ((sl.ActiveBlock.Claim(version, 8)).IsValid)
            {
                sl.ActiveBlock.Commit();
                version++;
            }

            sl.RotateActiveBlock(sl.ActiveBlock.NextVersion, default, 8);

            while ((sl.ActiveBlock.Claim(version, 8)).IsValid)
            {
                sl.ActiveBlock.Commit();
                version++;
            }

            sl.Dispose();

            slm.Dispose();
        }

        [Test
#if !DEBUG
         , Explicit("long running, changes static setting")
#endif
        ]
        public unsafe void CouldWriteAndReadLog()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync; //  | LMDBEnvironmentFlags.MapAsync; // | LMDBEnvironmentFlags.MapAsync; //
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync; // | LMDBEnvironmentFlags.MapAsync; // | LMDBEnvironmentFlags.MapAsync; // |  LMDBEnvironmentFlags.MapAsync; //

            var slm = new StreamLogManager(processConfig, repoName, null, 20 * 1024, disableNotificationLog: false, disablePacker: false);

            var streamId = (StreamLogId)42L;

            var count = TestUtils.GetBenchCount(10_000_000, 100);

            short valueSize = 12;

            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, valueSize, StreamLogFlags.None);
            state.SetWriteMode(WriteMode.BatchWrite);

            var sl = new StreamLog(slm, state, 8_000_000, textId: "test_stream");

            slm.OpenStreams.TryAdd(42, sl);

            // *(long*)(sl.State.StatePointer + StreamLogState.StreamLogStateRecord.LockerOffset) = 0;
            var acquired = sl.TryAcquireExlusiveLock(out var tt);
            Assert.AreEqual(0, (long)acquired);

            // using (Benchmark.Run("Write", count))
            {
                try
                {
                    var bench = Benchmark.Run("Write 10M", count);
                    for (long i = 0; i < count; i++)
                    {
                        //ts = ProcessConfig.CurrentTime;
                        var size = BinarySerializer.SizeOf(0L, out var payload, SerializationFormat.Binary); // valueSize; //
                        var expectedVersion = (ulong)(i + 1);
                        var claim = sl.LockAndClaim(expectedVersion, valueSize, tt);

                        if (claim.Length != valueSize)
                        {
                            Assert.Fail();
                        }

                        // claim.Write(4, 100000L + i);

                        BinarySerializer.Write(100000L + i, claim, payload, SerializationFormat.Binary);

                        var version = sl.CommitAndRelease(tt);
                        if (version != expectedVersion)
                        {
                            Assert.Fail();
                        }

                        // Thread.SpinWait(2);
                        if (i > 0 && i % 10_000_000 == 0)
                        {
                            bench.Dispose();
                            bench = Benchmark.Run("Write 10M", 10_000_000);
                        }
                    }
                    bench.Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"EXCEPTION DURING WRITE: " + ex);
                    throw;
                }
            }

            var released = sl.TryReleaseExclusiveLock();
            Assert.AreEqual(0, (long)released);

            for (int r = 0; r < 5; r++)
            {
                using (Benchmark.Run("Read", count))
                {
                    var c = sl.GetCursor();
                    var cnt = 0L;
                    while (c.MoveNext())
                    {
                        BinarySerializer.Read(c.CurrentValue, out long value, skipTypeInfoValidation: true);
                        // var value = c.CurrentValue.Read<long>(4);
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

            sl.Dispose();

            slm.Dispose();

            Benchmark.Dump();
        }

        [Test
#if !DEBUG
         , Explicit("long running, changes static setting")
#endif
        ]
        public unsafe void CouldGetByVersionFromUnpacked()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync; //  | LMDBEnvironmentFlags.MapAsync; // | LMDBEnvironmentFlags.MapAsync; //
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync; // | LMDBEnvironmentFlags.MapAsync; // | LMDBEnvironmentFlags.MapAsync; // |  LMDBEnvironmentFlags.MapAsync; //

            var slm = new StreamLogManager(processConfig, repoName, null, 20 * 1024, disableNotificationLog: false, disablePacker: true);

            var streamId = (StreamLogId)42L;

            var count = TestUtils.GetBenchCount(1_000_000, 1000);

            short valueSize = 12;

            var state = slm.StateStorage.GetState(streamId);

            state.CheckInit(streamId, valueSize, StreamLogFlags.None);
            // sl._disablePacking = true;

            var sl = new StreamLog(slm, state, 8_000_000, textId: "test_stream");

            // TODO check value
            sl.TryAcquireExlusiveLock(out _);

            sl.State.SetWriteMode(WriteMode.BatchWrite);
            // sl.DisableNotifications = true;

            slm.OpenStreams.TryAdd(42, sl);

            *(long*)(sl.State.StatePointer + StreamLogState.StreamLogStateRecord.LockerOffset) = 0;

            var ts = ProcessConfig.CurrentTime;
            using (Benchmark.Run("Write", count))
            {
                try
                {
                    for (long i = 0; i < count; i++)
                    {
                        var size = BinarySerializer.SizeOf(0L, out var payload, SerializationFormat.Binary); // valueSize; //
                        var expectedVersion = (ulong)(i + 1);
                        var claim = sl.Claim(expectedVersion, size);

                        if (claim.Length != valueSize)
                        {
                            Assert.Fail();
                        }

                        BinarySerializer.Write(100000L + i, claim, payload, SerializationFormat.Binary);

                        var version = sl.Commit();
                        if (version != expectedVersion)
                        {
                            Assert.Fail();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"EXCEPTION DURING WRITE: " + ex);
                    throw;
                }
            }

            var mult = 2;

            for (int r = 0; r < 50; r++)
            {
                using (Benchmark.Run("Get", count * mult))
                {
                    for (int i = 0; i < count * mult; i++)
                    {
                        ulong version = (ulong)(1 + i % count);
                        var db = sl.DangerousGetUnpacked(version);
                        if (db.Length != valueSize)
                        {
                            Assert.Fail();
                        }
                    }
                }
            }

            slm.BufferPool.PrintBuffersAfterPoolDispose = true;

            Thread.Sleep(500);

            sl.Dispose();

            slm.Dispose();

            Benchmark.Dump();
        }

        [Test
#if !DEBUG
         , Explicit("long running, changes static setting")
#endif
        ]
        public async Task CouldWriteAndReadLogWithWalAckAsync()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.None;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.None;

            var slm = new StreamLogManager(processConfig, repoName, null, 20 * 1024, disableNotificationLog: false, disablePacker: true);

            var streamId = (StreamLogId)42L;

            var count = TestUtils.GetBenchCount(1000_000, 1000);

            short valueSize = 16;

            var state = slm.StateStorage.GetState(streamId);

            state.CheckInit(streamId, valueSize, StreamLogFlags.IsBinary);
            // sl._disablePacking = true;

            var sl = new StreamLog(slm, state, textId: "test_stream");

            slm.OpenStreams.TryAdd(42, sl);

            var dataStreamWriter = new DataStreamWriter<long>(sl, KeySorting.NotEnforced, WriteMode.LocalSync);

            var ts = ProcessConfig.CurrentTime;

            using (Benchmark.Run("Write (KOPS)", count * 1000))
            {
                for (int i = 0; i < count; i++)
                {
                    var expectedVersion = (ulong)(i + 1);
                    var added = await dataStreamWriter.TryAppend(expectedVersion, 100000 + i).ConfigureAwait(false);
                    if (!added)
                    {
                        Assert.Fail("!added");
                    }

                    //ts = ProcessConfig.CurrentTime;
                    //var size = BinarySerializer.SizeOf(0, out var segment, SerializationFormat.Binary, ts);

                    //var claim = sl.Claim(expectedVersion, size);

                    //if (claim.Length != valueSize)
                    //{
                    //    Assert.Fail();
                    //}

                    //BinarySerializer.Write(100000 + i, ref claim, segment, SerializationFormat.Binary, ts);

                    //var version = sl.Commit();

                    //if (version != expectedVersion)
                    //{
                    //    Assert.Fail();
                    //}

                    //var spinner = new SpinWait();
                    //while (!sl.IsVersionInWal(version))
                    //{
                    //    spinner.SpinOnce();
                    //    if (spinner.NextSpinWillYield)
                    //    {
                    //        spinner.Reset();
                    //    }
                    //}

                    // await sl.WaitForAck(version).ConfigureAwait(false);

                    // Thread.SpinWait(2);
                    if (i % (count / 10) == 0)
                    {
                        Console.WriteLine($"Written: {i:N0}");
                    }
                }
            }

            Console.WriteLine("FINISHED WRITING");
            //for (int r = 0; r < 5; r++)
            //{
            //    using (Benchmark.Run("Read", count))
            //    {
            //        var c = sl.GetEnumerator();
            //        var cnt = 0L;
            //        while (c.MoveNext())
            //        {
            //            var value = c.CurrentValue.Read<long>(4 + 8);
            //            if (value != 100000L + cnt)
            //            {
            //                Assert.Fail($"value {value} != cnt {100000L + cnt}");
            //            }
            //            cnt++;
            //        }
            //        c.Dispose();
            //        if (cnt != count)
            //        {
            //            Assert.Fail($"cnt {cnt } != count {count}");
            //        }
            //    }
            //}

            slm.BufferPool.PrintBuffersAfterPoolDispose = true;

            Thread.Sleep(500);

            dataStreamWriter.Dispose();
            Console.WriteLine("sl.Dispose()");
            sl.Dispose();
            Console.WriteLine("slm.Dispose()");
            slm.Dispose();
            Console.WriteLine("Benchmark.Dump();");
            Benchmark.Dump();
        }

        [Test
#if !DEBUG
         , Explicit("long running, changes static setting")
#endif
        ]
        public async Task CouldWriteAndReadLogWithWalAckParalell()
        {
            // TODO LocalAck is disabled in this test

            Console.WriteLine("IS SERVER GC: " + GCSettings.IsServerGC);
            GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;

            Settings.DoAdditionalCorrectnessChecks = false;
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 20 * 1024, disableNotificationLog: false, disablePacker: true);

            var taskCount = TestUtils.GetBenchCount(512, 16);
            var countPerTask = TestUtils.GetBenchCount(10_000L, 10);

            var skipInitial = taskCount * countPerTask / 4;

            short valueSize = 16;

            var tasks = new Task[taskCount];
            var logs = new StreamLog[taskCount];

            long totalCount = 0;

            //var tsPtr = Marshal.AllocHGlobal(8);
            //var ts = new TimeService(tsPtr);
            //var cts = new CancellationTokenSource();
            //ts.StartSpinUpdate(cts.Token);
            var hdr = new LongHistogram(TimeStamp.Seconds(1), 5);

            var timings = new long[taskCount * countPerTask - skipInitial];

            using (Benchmark.Run("Write (KOPS)", taskCount * countPerTask * 1000))
            {
                for (int t = 1; t <= taskCount; t++)
                {
                    var t1 = t;
                    async Task localFunc()
                    {
                        try
                        {
                            var streamId = (StreamLogId)t1;

                            var state = slm.StateStorage.GetState(streamId);
                            state.CheckInit(streamId, valueSize, StreamLogFlags.NoPacking | StreamLogFlags.IsBinary);

                            var sl = new StreamLog(slm, state, textId: "test_stream_" + t1);

                            slm.OpenStreams.TryAdd((long)streamId, sl);
                            using (var mds = new DataStreamWriter<long>(sl, KeySorting.NotEnforced, WriteMode.LocalSync))
                            {
                                logs[t1 - 1] = sl;

                                int i = 0;
                                while (true)
                                {
                                    // Console.WriteLine($"Writing [{t1}]: {i}");
                                    var expectedVersion = (ulong)(i + 1);
                                    var before = Stopwatch.GetTimestamp();
                                    var added = await mds.TryAppend(100000 + i).ConfigureAwait(false);
                                    var after = Stopwatch.GetTimestamp();
                                    var timing = after - before;

                                    if (expectedVersion != added)
                                    {
                                        Assert.Fail("!added");
                                    }
                                    else
                                    {
                                        // Console.WriteLine($"Added [{t1}]: {i}");
                                    }

                                    var tc = Interlocked.Increment(ref totalCount);
                                    if (tc % (taskCount * countPerTask / 10) == 0)
                                    {
                                        Console.WriteLine($"Total written: {tc:N0}");
                                    }

                                    if (tc > skipInitial)
                                    {
                                        timings[tc - skipInitial - 1] = timing;
                                    }

                                    //if (i % (countPerTask / 100) == 0)
                                    //{
                                    //    Console.WriteLine($"[{t1}]Written: {i:N0}");
                                    //}

                                    i++;
                                    if (i == countPerTask)
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Exception in [{t1}]: {ex}");
                        }
                    }

                    tasks[t - 1] = localFunc();
                }

                await Task.WhenAll(tasks);
            }

            // slm.BufferPool.PrintBuffersAfterPoolDispose = true;

            foreach (var timing in timings)
            {
                hdr.RecordValue(timing);
            }

            var hdrWriter = new StringWriter();
            var scalingRatio = OutputScalingFactor.TimeStampToMicroseconds;
            hdr.OutputPercentileDistribution(hdrWriter, outputValueUnitScalingRatio: scalingRatio);

            Console.WriteLine(hdrWriter.ToString());

            using (var writer = new StreamWriter(Path.Combine(path, "WriteWithWalAckParalell.hgrm")))
            {
                hdr.OutputPercentileDistribution(writer, outputValueUnitScalingRatio: scalingRatio);
            }

            Thread.Sleep(500);

            foreach (var streamLog in logs)
            {
                streamLog.Dispose();
            }

            slm.Dispose();

            Benchmark.Dump();
        }

        [Test]
        public void CouldWriteLargeValues()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = true;
#pragma warning restore 618

            var path = TestUtils.GetPath();
            var repoName = "CouldWriteLargeValues";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.MapAsync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.MapAsync;

            var slm = new StreamLogManager(processConfig, repoName, null, 1024, false, true);

            var streamId = (StreamLogId)42L;

            var count = 1000;

            var streamLogTable = slm.BlockIndex;

            short valueSize = -1;

            var state = slm.StateStorage.GetState(streamId);

            state.CheckInit(streamId, valueSize, StreamLogFlags.IsBinary);

            var sl = new StreamLog(slm, state, 1000, textId: "test_stream");

            using (Benchmark.Run("CouldWriteLargeValues", count))
            {
                var version = 0;
                for (int i = 0; i < count; i++)
                {
                    version++;

                    var size = i + 4096; // i % 10 == 0 ? i + 4096 * 10 : 8;
                    if (i == 31)
                    {
                        Console.WriteLine("");
                    }
                    var claim = sl.Claim((ulong)(version), size);

                    if (claim.Length != size)
                    {
                        Assert.Fail();
                    }

                    claim.Write<long>(0, 100000 + i);

                    sl.Commit();
                }
            }

            sl.Dispose();

            Benchmark.Dump();

            // slm.BufferPool.PrintBuffers();
            slm.BufferPool.PrintBuffersAfterPoolDispose = true;

            // TODO print free lists

            slm.Dispose();
        }

        [Test
#if !DEBUG
         , Explicit("long running")
#endif
        ]
        public void ClaimPerformance()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldCommitToStreamLog";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.MapAsync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.MapAsync;
            var slm = new StreamLogManager(processConfig, repoName, null, 512, true, true);

            var streamId = (StreamLogId)42L;

            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, 8, StreamLogFlags.IsBinary | StreamLogFlags.NoPacking);

            var sl = new StreamLog(slm, state, textId: "test_stream");

            var count = TestUtils.GetBenchCount(100_000_000L, 1000);

            var db = sl.Claim(0, 8, default);

            for (int i = 0; i < 10; i++)
            {
                ClaimPerformance_Loop(count, sl);
            }
            
            Benchmark.Dump();

            sl.Dispose();
            slm.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.NoInlining)]
        private static void ClaimPerformance_Loop(long count, StreamLog sl)
        {
            DirectBuffer db;
            using (Benchmark.Run("Claim", count))
            {
                for (int i = 0; i < count; i++)
                {
                    db = sl.Claim(0, 8, default);
                }
            }
        }

        //[Test]
        //public void CouldListenToRotateLog()
        //{
        //    var path = Utils.GetPath("StreamLogTests/CouldListenToRotateLog");
        //    var repo = new Repo("CouldListenToRotateLog", SyncMode.LocalRealTime, path, 256);

        //    var streamId = 42L;

        //    var rounds = 1;
        //    var count = 10000;

        //    var slm = repo._streamLogManager;

        //    while (slm.RotateLog == null)
        //    {
        //        Thread.Sleep(50);
        //    }

        //    for (int i = 0; i < 10000; i++)
        //    {
        //        Thread.Sleep(250);
        //        slm.RotateLog.TryAddLast(new TaggedKeyValue<long, ulong>(streamId, (ulong)i, 0));
        //    }

        //    Thread.Sleep(5000);

        //    // slm.Dispose();
        //}

        //[Test]
        //public void CouldListenToZeroLog()
        //{
        //    var path = Utils.GetPath("StreamLogTests/CouldListenToZeroLog");
        //    var repo = new Repo("CouldListenToZeroLog", SyncMode.LocalRealTime, path, 10 * 1024);

        //    var sId = 42L;

        //    var rounds = 1;
        //    var count = 2_000_000;

        //    var slm = repo._streamLogManager;
        //    var streamLogTable = slm._chunksTable;
        //    streamLogTable.Truncate().Wait();
        //    var series = new StreamLogChunkSeries(sId, streamLogTable);
        //    var sl = new StreamLog(slm, series, "test", 8, 2000);
        //    slm._openStreams.TryAdd(sId, sl);

        //    sl.Init().AsTask().Wait();

        //    using (Benchmark.Run("CouldWriteAndReadToTerm", count * rounds))
        //    {
        //        var task = Task.Run(() =>
        //        {
        //            try
        //            {
        //                Thread.CurrentThread.Name = "ZC";
        //                var zc = slm.ZeroLog.GetContainerCursor();
        //                var cnt = 0;
        //                var sw = new SpinWait();
        //                var previousStreamId = 0L;
        //                WeakReference<StreamLog> previousStream = new WeakReference<StreamLog>(null);

        //                while (cnt < count)
        //                {
        //                    if (zc.MoveNext())
        //                    {
        //                        cnt++;
        //                        var streamId = zc.CurrentValue.ReadInt64(0);
        //                        if (previousStreamId == streamId && previousStream.TryGetTarget(out var cached))
        //                        {
        //                            //if (cached.StreamId != streamId)
        //                            //{
        //                            //    Environment.FailFast("");
        //                            //}
        //                            cached.NotifyUpdate(false);
        //                        }
        //                        else
        //                        if (slm._openStreams.TryGetValue(streamId, out var streamLog))
        //                        {
        //                            // previousStreamId = streamId;
        //                            previousStream.SetTarget(streamLog);
        //                            // NB this completes any waiting async cursors on a non-allocating thread pool
        //                            streamLog.NotifyUpdate(false);
        //                        }
        //                        sw.Reset();
        //                    }
        //                    else
        //                    {
        //                        sw.SpinOnce();
        //                        //if (sw.NextSpinWillYield)
        //                        //{
        //                        //    // Console.WriteLine($"Yielding on cnt {cnt}");
        //                        //    sw.Reset();
        //                        //}
        //                    }
        //                }

        //                Console.WriteLine("Last key: " + zc.CurrentKey);
        //                zc.Dispose();
        //            }
        //            catch (Exception ex)
        //            {
        //                Console.WriteLine(ex);
        //                Environment.FailFast(ex.Message);
        //            }
        //        });

        //        for (int r = 0; r < rounds; r++)
        //        {
        //            for (int i = 0; i < count; i++)
        //            {
        //                var claim = sl.Claim((ulong)(i + 1), 8);
        //                // Assert.AreEqual(claim.Length, 8);
        //                claim.Write<long>(0, 100000 + i);
        //                sl.Commit();
        //            }
        //        }

        //        task.Wait();
        //    }

        //    Benchmark.Dump();
        //    //sl.Dispose();
        //    repo.Close();
        //}

        [StructLayout(LayoutKind.Sequential, Size = 64)]
        [BinarySerialization(64)]
        public struct StreamValue
        {
            public long Item1;
            public SmallDecimal Item2;
            public long Item3;
            public SmallDecimal Item4;
            public long Item5;
            public SmallDecimal Item6;
            public long Item7;
            public SmallDecimal Item8;
        }

        //        [Test]
        //        public Task CouldListenToZeroLogWithConcurrentWrites()
        //        {
        //            return CouldListenToZeroLogWithConcurrentWrites(null);
        //        }

        //        public async Task CouldListenToZeroLogWithConcurrentWrites(string id)
        //        {
        //            uint repoSizeMb = 20 * 1024;
        //#pragma warning disable HAA0302 // Display class allocation to capture closure
        //            var count = 2_000_000L;
        //#pragma warning restore HAA0302 // Display class allocation to capture closure
        //            var estimatedSize = count * 4 * (8 + 8 + 8);
        //            var printCount = Math.Min(1_000_000, count / 2);

        //            TaskScheduler.UnobservedTaskException += (sender, args) =>
        //            {
        //                Console.WriteLine("Unobserved task exception: " + sender + " - " + args);
        //            };

        //            AppDomain.CurrentDomain.UnhandledException += (sender, args) =>
        //            {
        //                Console.WriteLine("Unobserved domain exception: " + sender + " - " + args);
        //            };

        //            List<Task> tasks = new List<Task>();

        //            var path = TestUtils.GetPath("StreamLogTests/CouldListenToZeroLogWithConcurrentWrites");
        //            Repo repo1 = null;
        //            Repo repo2 = null;

        //            MutableDataStream<StreamValue> s11 = null;
        //            MutableDataStream<StreamValue> s12 = default;

        //            MutableDataStream<StreamValue> s21 = null;
        //            MutableDataStream<StreamValue> s22 = default;

        //            var isBinary = false;

        //            using (Benchmark.Run("DS Write/Read 4 threads", (int)((count * 4))))
        //            {
        //                if (id != "2")
        //                {
        //                    repo1 = new Repo("CouldListenToZeroLogWithConcurrentWrites", SyncMode.LocalRealTime, path, path, repoSizeMb);
        //                    // Thread.Sleep(500);
        //                    Console.WriteLine("Opened repo");

        //                    var created1 = await repo1.CreateDataStream<StreamValue>("stream1", isBinaryStream: isBinary, rateHint: 1024 * 1024);
        //                    Assert.IsTrue(created1 != null);
        //                    var created2 = await repo1.CreateDataStream<StreamValue>("stream2", isBinaryStream: isBinary, rateHint: 1024 * 1024);
        //                    Assert.IsTrue(created2 != null);

        //                    Console.WriteLine("Created streams");

        //                    s11 = await (await repo1.OpenDataStream<StreamValue>("stream1")).AsMutable();
        //                    s12 = await (await repo1.OpenDataStream<StreamValue>("stream2")).AsMutable();
        //                    Console.WriteLine("Opened streams");

        //                    var t11w = Task.Run(async () =>
        //                    {
        //                        try
        //                        {
        //                            var missedCount = 0;
        //                            for (int i = 0; i < count; i++)
        //                            {
        //                                var added = await s11.TryAddLast(new StreamValue { Item1 = i });
        //                                if (added == 0)
        //                                {
        //                                    missedCount++;
        //                                    //if (missedCount % (printCount/100) == 0)
        //                                    {
        //                                        Console.WriteLine("Not added s11");
        //                                    }
        //                                }
        //                                else if ((long)added % printCount == 0)
        //                                {
        //                                    Console.WriteLine($"Added s11: {added}");
        //                                    //Thread.Sleep(1);
        //                                }

        //                                // Thread.SpinWait(50);
        //                            }

        //                            // await s11.Complete();
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Environment.FailFast(ex.Message, ex);
        //                        }
        //                    });
        //                    tasks.Add(t11w);

        //                    var t12w = Task.Run(async () =>
        //                    {
        //                        try
        //                        {
        //                            var missedCount = 0;
        //                            for (int i = 0; i < count; i++)
        //                            {
        //                                var added = await s12.TryAddLast(new StreamValue { Item1 = i });
        //                                if (added == 0)
        //                                {
        //                                    missedCount++;
        //                                    //if (missedCount % (printCount / 100) == 0)
        //                                    {
        //                                        Console.WriteLine("Not added s12");
        //                                    }
        //                                }
        //                                else if ((long)added % printCount == 0)
        //                                {
        //                                    Console.WriteLine($"Added s12: {added}");
        //                                    //Thread.Sleep(1);
        //                                }

        //                                //Thread.SpinWait(50);
        //                            }

        //                            // await s11.Complete();
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Environment.FailFast(ex.Message, ex);
        //                        }
        //                    });
        //                    tasks.Add(t12w);

        //                    var t11r = Task.Run(async () =>
        //                    {
        //                        try
        //                        {
        //                            var cnt11 = 0L;
        //                            var c = s11.GetCursor();
        //                            while (await c.MoveNextAsync())
        //                            {
        //                                var value = c.CurrentValue;
        //                                if (value.Item1 < 0)
        //                                {
        //                                    throw new ApplicationException("value.Item1 < 0");
        //                                }

        //                                cnt11++;
        //                                if (cnt11 % printCount == 0)
        //                                {
        //                                    Console.WriteLine($"Read11: {cnt11}");
        //                                }
        //                                if (cnt11 == count * 2)
        //                                {
        //                                    break;
        //                                }
        //                            }
        //                            Console.WriteLine("Reader finished 11: " + cnt11.ToString("N"));
        //                            c.Dispose();
        //                            //Assert.AreEqual(count * 2, cnt11);
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Console.WriteLine(ex);
        //                            throw;
        //                        }
        //                    });

        //                    tasks.Add(t11r);

        //                    var t12r = Task.Run(async () =>
        //                    {
        //                        try
        //                        {
        //                            var cnt12 = 0L;
        //                            var c = s12.GetCursor();
        //                            while (await c.MoveNextAsync())
        //                            {
        //                                var value = c.CurrentValue;
        //                                if (value.Item1 < 0)
        //                                {
        //                                    throw new ApplicationException("value.Item1 < 0");
        //                                }

        //                                cnt12++;
        //                                if (cnt12 % printCount == 0)
        //                                {
        //                                    Console.WriteLine($"Read12: {cnt12}");
        //                                }
        //                                if (cnt12 == count * 2)
        //                                {
        //                                    break;
        //                                }
        //                            }
        //                            Console.WriteLine("Reader finished 12: " + cnt12.ToString("N"));
        //                            c.Dispose();
        //                            //Assert.AreEqual(count * 2, cnt12);
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Console.WriteLine(ex);
        //                            throw;
        //                        }
        //                    });
        //                    tasks.Add(t12r);
        //                }

        //                if (id != "1")
        //                {
        //                    repo2 = new Repo("CouldListenToZeroLogWithConcurrentWrites", SyncMode.LocalRealTime, path, path, repoSizeMb);
        //                    // Thread.Sleep(500);

        //                    var created1 = await repo2.CreateDataStream<StreamValue>("stream1", isBinaryStream: isBinary, rateHint: 1024 * 1024);
        //                    Assert.IsTrue(created1 != null);

        //                    var created2 = await repo2.CreateDataStream<StreamValue>("stream2", isBinaryStream: isBinary, rateHint: 1024 * 1024);
        //                    Assert.IsTrue(created2 != null);

        //                    s21 = await (await repo2.OpenDataStream<StreamValue>("stream1")).AsMutable();
        //                    s22 = await (await repo2.OpenDataStream<StreamValue>("stream2")).AsMutable();

        //                    var t21w = Task.Run(async () =>
        //                    {
        //                        try
        //                        {
        //                            var missedCount = 0;
        //                            for (int i = 0; i < count; i++)
        //                            {
        //                                var added = await s21.TryAddLast(new StreamValue { Item1 = i });
        //                                if (added == 0)
        //                                {
        //                                    missedCount++;
        //                                    //if (missedCount % (printCount / 100) == 0)
        //                                    {
        //                                        Console.WriteLine("Not added s21");
        //                                    }
        //                                }
        //                                else if ((long)added % printCount == 0)
        //                                {
        //                                    Console.WriteLine($"Added s21: {added}");
        //                                    //Thread.Sleep(1);
        //                                }

        //                                //Thread.SpinWait(50);
        //                            }

        //                            // await s21.Complete();
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Environment.FailFast(ex.Message, ex);
        //                        }
        //                    });
        //                    tasks.Add(t21w);

        //                    var t22w = Task.Run(async () =>
        //                    {
        //                        try
        //                        {
        //                            var missedCount = 0;
        //                            for (int i = 0; i < count; i++)
        //                            {
        //                                var added = await s22.TryAddLast(new StreamValue { Item1 = i });
        //                                if (added == 0)
        //                                {
        //                                    missedCount++;
        //                                    //if (missedCount % (printCount / 100) == 0)
        //                                    {
        //                                        Console.WriteLine("Not added s22");
        //                                    }
        //                                }
        //                                else if ((long)added % printCount == 0)
        //                                {
        //                                    Console.WriteLine($"Added s22: {added}");
        //                                    //Thread.Sleep(1);
        //                                }

        //                                //Thread.SpinWait(50);
        //                            }

        //                            // await s21.Complete();
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Environment.FailFast(ex.Message, ex);
        //                        }
        //                    });
        //                    tasks.Add(t22w);

        //                    var t21r = Task.Run(async () =>
        //                    {
        //                        try
        //                        {
        //                            var cnt21 = 0L;
        //                            var c = s21.GetCursor();
        //                            while (await c.MoveNextAsync())
        //                            {
        //                                var value = c.CurrentValue;
        //                                if (value.Item1 < 0)
        //                                {
        //                                    throw new ApplicationException("value.Item1 < 0");
        //                                }

        //                                cnt21++;
        //                                if (cnt21 % printCount == 0)
        //                                {
        //                                    Console.WriteLine($"Read21: {cnt21}");
        //                                }
        //                                if (cnt21 == count * 2)
        //                                {
        //                                    break;
        //                                }
        //                            }

        //                            Console.WriteLine("Reader finished 21: " + cnt21.ToString("N"));
        //                            c.Dispose();
        //                            //Assert.AreEqual(count * 2, cnt21);
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Console.WriteLine(ex);
        //                            throw;
        //                        }
        //                    });
        //                    tasks.Add(t21r);

        //                    var t22r = Task.Run(async () =>
        //                    {
        //                        try
        //                        {
        //                            var cnt22 = 0L;
        //                            var c = s22.GetCursor();
        //                            while (await c.MoveNextAsync())
        //                            {
        //                                var value = c.CurrentValue;
        //                                if (value.Item1 < 0)
        //                                {
        //                                    throw new ApplicationException("value.Item1 < 0");
        //                                }

        //                                cnt22++;
        //                                if (cnt22 % printCount == 0)
        //                                {
        //                                    Console.WriteLine($"Read22: {cnt22}");
        //                                }
        //                                if (cnt22 == count * 2)
        //                                {
        //                                    break;
        //                                }
        //                            }
        //                            Console.WriteLine("Reader finished 22: " + cnt22.ToString("N"));
        //                            c.Dispose();
        //                            //Assert.AreEqual(count * 2, cnt22);
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            Console.WriteLine(ex);
        //                            throw;
        //                        }
        //                    });
        //                    tasks.Add(t22r);
        //                }

        //                await Task.WhenAll(tasks);
        //            }

        //            //GC.KeepAlive(s11);
        //            //GC.KeepAlive(s12);
        //            //GC.KeepAlive(s21);
        //            //GC.KeepAlive(s22);

        //            Console.WriteLine("Finished");
        //            Benchmark.Dump();
        //            repo1?.Close();
        //            repo2?.Close();
        //            // Thread.Sleep(1000);
        //        }

        //[Test, Explicit("Benchmark")]
        //public void CouldWriteAndReadToTermBenchmark()
        //{
        //    var env = LMDBEnvironment.Create("./TempData/StreamLogTests",
        //        LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
        //    env.MapSize = 512 * 1024 * 1024;
        //    env.Open();

        //    var provider = new SQLiteStorageProvider(@"Filename=./TempData/StreamLogTests.db", "StreamLogTests");

        //    var streamId = 42L;

        //    var rounds = 1; // TODO
        //    var count = 10_000_000;

        //    var streamLogTable = new StreamLogChunkTable(env);
        //    streamLogTable.Truncate().Wait();
        //    var series = new StreamLogChunkSeries(streamId, streamLogTable);

        //    var slm = new StreamLogManager(env, provider, disableZeroLogThread: true);

        //    var sl = new StreamLog(slm, series, "test", 8, 2000);

        //    sl.Init().AsTask().Wait();

        //    using (Benchmark.Run("CouldWriteAndReadToTerm", count * rounds))
        //    {
        //        for (int r = 0; r < rounds; r++)
        //        {
        //            for (int i = 0; i < count; i++)
        //            {
        //                var claim = sl.Claim((ulong)(i + 1), 8);

        //                // Assert.AreEqual(claim.Length, 8);
        //                claim.Write<long>(0, 100000 + i);

        //                sl.Commit();

        //                //var readDb = slTerm.GetUnchecked(i);

        //                //Assert.AreEqual(readDb.Length, 16);

        //                //readDb.Read<long>(0, out var written);

        //                //Assert.AreEqual(written, 100000 + i);
        //            }

        //            streamLogTable.Truncate().Wait();
        //        }
        //    }

        //    Benchmark.Dump();

        //    env.Close().Wait();
        //}

        //[Test]
        //public void CouldWriteAndReadVarSizeToTerm()
        //{
        //    var env = LMDBEnvironment.Create("./TempData",
        //        LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync);
        //    env.MapSize = 256 * 1024 * 1024;

        //    env.Open();
        //    var rounds = 2;
        //    var count = 10000;

        //    var pool = new MemoryBufferPool(env);

        //    var term = pool.Acquire(count * 100 + StreamLogChunk.DataOffset);

        //    var values = new byte[1000];
        //    for (int i = 0; i < values.Length; i++)
        //    {
        //        values[i] = (byte)(i % 255);
        //    }

        //    using (Benchmark.Run("CouldWriteAndReadVarSizeToTerm", count * rounds))
        //    {
        //        for (int r = 0; r < rounds; r++)
        //        {
        //            var slTerm = new StreamLogChunk(term, 1, -1, 0);

        //            for (int i = 0; i < count; i++)
        //            {
        //                var itemLength = i % 100;

        //                var claim = slTerm.ClaimUnchecked(itemLength);

        //                Assert.AreEqual(claim.Buffer.Length, itemLength);

        //                claim.Buffer.CopyFrom(0, values, itemLength);

        //                claim.Commit();

        //                var readDb = slTerm.GetUnchecked(i);

        //                Assert.AreEqual(readDb.Length, itemLength);

        //                Assert.IsTrue(readDb.Span.SequenceEqual(values.AsSpan(0, itemLength)));
        //            }

        //            term.GetSpan().Clear();
        //        }
        //    }

        //    Benchmark.Dump();

        //    pool.Release(term);

        //    env.Close().Wait();
        //}

        //[Test]
        //public async Task CouldProduceDtosFromStream()
        //{
        //    uint repoSizeMb = 2 * 1024;
        //    var count = 10_000_000L;
        //    var printCount = Math.Min(1_000_000, count / 2);

        //    var path = TestUtils.GetPath("StreamLogTests/CouldListenToZeroLogWithConcurrentWrites");
        //    Repo repo1 = null;

        //    MutableDataStream<StreamValue> s11 = null;

        //    var isBinary = true;

        //    using (Benchmark.Run("Write", (int)count))
        //    {
        //        repo1 = new Repo("CouldProduceDtosFromStream", SyncMode.LocalRealTime, path, path, repoSizeMb);

        //        var created1 =
        //            await repo1.CreateDataStream<StreamValue>("stream1", isBinaryStream: isBinary,
        //                rateHint: 1024 * 1024);
        //        Assert.IsTrue(created1 != null);

        //        Console.WriteLine("Created streams");

        //        s11 = await (await repo1.OpenDataStream<StreamValue>("stream1")).AsMutable();

        //        Console.WriteLine("Opened streams");

        //        try
        //        {
        //            var missedCount = 0;
        //            for (int i = 0; i < count; i++)
        //            {
        //                var added = await s11.TryAddLast(new StreamValue { Item1 = i });
        //                if (added == 0)
        //                {
        //                    missedCount++;
        //                    //if (missedCount % (printCount/100) == 0)
        //                    {
        //                        Console.WriteLine("Not added s11");
        //                    }
        //                }
        //                else if ((long)added % printCount == 0)
        //                {
        //                    Console.WriteLine($"Added s11: {added}");
        //                }
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            Environment.FailFast(ex.Message, ex);
        //        }
        //    }

        //    using (Benchmark.Run("Read dtos", (int)count))
        //    {
        //        try
        //        {
        //            var c = s11._inner.GetContainerCursor(false);
        //            var len = 0;
        //            var chunkCount = 0;
        //            var itemCount = 0;
        //            while (c.MoveNextDto(10 * 680, out var dto))
        //            {
        //                len += dto.DataItems.Span.Length;
        //                chunkCount++;
        //                itemCount += dto.Count;

        //                // (new DirectBuffer(dto.DataItems.Span)).Read<DataTypeHeader>(0, out var header);
        //                // Console.WriteLine(header.VersionAndFlags.IsBinary);

        //                // Console.WriteLine($"DTO: first version {dto.Version}; length: {dto.DataItems.Length}");
        //                //if (dto.Version == 41)
        //                //{
        //                //    Console.WriteLine("Catch me");
        //                //}
        //            }
        //            Console.WriteLine("Reader finished, len: " + len.ToString("N") + "; item count: " + itemCount + ", chunk count: " + chunkCount);
        //            c.Dispose();
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine(ex);
        //            throw;
        //        }
        //    }

        //    Console.WriteLine("Finished");
        //    Benchmark.Dump();
        //    repo1?.Close();
        //}
    }
}
