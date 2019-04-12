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
using Spreads.Threading;
using Spreads.Utils;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DataSpreads.Config;
using NUnit.Framework.Constraints;

namespace DataSpreads.Tests.StreamLogs
{
    //[Category("CI")]
    [TestFixture, SingleThreaded]
    public unsafe class NotificationLogTests
    {
        public static long MaxStall;

        [Test]
        public void CouldInitNotificationLog()
        {
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog0";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 10 * 1024, false, true);

            slm.Dispose();
        }

        [Test]
        public void CouldWriteReadLog0MoveGT()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog0";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 10 * 1024, true, true);

            var log0 = new NotificationLog(slm);

            // will disable packing
            log0.ActiveBuffer.Increment();

            var count = 3_000_000;

            using (Benchmark.Run("Log0.Append", count))
            {
                for (long i = 1; i <= count; i++)
                {
                    log0.Append((StreamLogNotification)(ulong)i);
                }
            }

            using (Benchmark.Run("Log0 MoveGT", count))
            {
                using (var reader = new NotificationLog.Reader(log0, CancellationToken.None, false))
                {
                    for (long i = 0; i < count; i++)
                    {
                        if (reader.MoveGT((ulong)i))
                        {
                            if (reader.CurrentVersion != (ulong)(i + 1))
                            {
                                Assert.Fail($"reader.CurrentVersion {reader.CurrentVersion} != [(ulong) (i + 1)] {i + 1}");
                            }
                        }
                    }
                }
            }

            // readerTask.Wait();

            Benchmark.Dump();

            slm.BufferPool.PrintBuffersAfterPoolDispose = true;
            log0.Dispose();
            slm.Dispose();
        }

        [Test]
        public void CouldWriteInParallelAllValuesNonZero()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog0";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 10 * 1024, true, true);

            var log0 = new NotificationLog(slm);

            // will disable packing
            log0.ActiveBuffer.Increment();

            var count = 2 * 1024 * 1024;

            List<Task> tasks = new List<Task>();
            var taskCount = 4;

            for (int j = 0; j < taskCount; j++)
            {
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        using (Benchmark.Run("Log0.Append", count * taskCount))
                        {
                            for (long i = 1; i <= count; i++)
                            {
                                log0.Append((StreamLogNotification)(ulong)i);
                                // Thread.Yield();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("EX: " + ex);
                    }
                }));
            }

            Task.WhenAll(tasks).Wait();

            Benchmark.Dump();

            using (Benchmark.Run("Log0 MoveNext", count * taskCount))
            {
                var chunkCursor = log0.BlockIndex.GetBlockRecordCursor(StreamLogId.Log0Id);
                var readCount = 0;
                while (chunkCursor.MoveNext())
                {
                    var record = chunkCursor.Current.Value;
                    if ((long)record.Version >= count * taskCount)
                    {
                        break;
                    }

                    if (record.IsPacked)
                    {
                        break;
                    }

                    var chunk = log0.RentChunkFromRecord(record);

                    for (ulong i = 0; i < 1024 * 1024; i++)
                    {
                        var position = NotificationLog.Log0ItemPosition(chunk.Pointer, (long)(chunk.FirstVersion + i), out _);
                        var value = *(ulong*)position;
                        if (value == 0)
                        {
                            Assert.Fail("Zero value at: " + (chunk.FirstVersion + i));
                        }

                        readCount++;
                    }

                    chunk.DisposeFree();
                }

                //var c = log0.GetContainerCursor(false);

                //while (c.MoveNext())
                //{
                //    if (c.CurrentValue.ReadUInt64(0) == 0)
                //    {
                //        // Console.WriteLine($"c.CurrentValue == 0 at {c.CurrentKey}");
                //        Assert.Fail($"c.CurrentValue == 0 at {c.CurrentKey}");
                //    }

                //    readCount++;
                //}

                //c.Dispose();
                // Assert.AreEqual(count * taskCount, readCount);
                Console.WriteLine("READ COUNT M: " + readCount * 1.0 / (1024 * 1024));
            }

            Console.WriteLine("OLD LOOKUP COUNT: " + log0.OldPositionLookupCount);

            // readerTask.Wait();

            slm.BufferPool.PrintBuffersAfterPoolDispose = true;
            log0.Dispose();
            slm.Dispose();
        }

        [Test]
        public void CouldRotateNotificationLog()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath();
            var repoName = "CouldWriteAndReadLog0";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 10 * 1024, true, true);

            var log0 = new NotificationLog(slm);

            // will disable packing
            log0.ActiveBuffer.Increment();

            var count = 3_000_000;

            log0.Rotate();

            using (Benchmark.Run("Log0.Append", count))
            {
                for (long i = 1; i <= count; i++)
                {
                    log0.Append((StreamLogNotification)(ulong)i);
                }
            }

            Benchmark.Dump();

            slm.BufferPool.PrintBuffersAfterPoolDispose = true;
            log0.Dispose();
            slm.Dispose();
        }


        private volatile bool _couldWriteReadLog0Finished = false;

        [Test]
        public void CouldWriteReadLog0()
        {
            CouldWriteReadLog0(true);
        }

        public void CouldWriteReadLog0(bool delete)
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath(clear: delete);
            var repoName = "CouldWriteAndReadLog0";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 10 * 1024, disableNotificationLog: false, disablePacker:false);

            // var log0 = new NotificationLog(slm);

            // will disable packing: log0._activeBuffer.Increment();

            var count = TestUtils.GetBenchCount(100_000_000, 1000);

            var readerTask = Task.Run(() =>
            {
                try
                {
                    var step = 10_000_000;
                    var stat = Benchmark.Run("Read", step);
                    {
                        using (var reader = new NotificationLog.Reader(slm.Log0, CancellationToken.None, false))
                        {
                            var previous = 0UL;

                            while (reader.MoveNext())
                            {
                                var current = (ulong)reader.Current;
                                //if (current != previous + 1)
                                //{
                                //    Assert.Fail($"current {current} != [previous + 1] {previous + 1}");
                                //}

                                previous = current;
                                if (_couldWriteReadLog0Finished)
                                {
                                    break;
                                }

                                if ((reader.CurrentVersion % (ulong)(step)) == 0)
                                {
                                    stat.Dispose();
                                    stat = Benchmark.Run("Read", step);
                                    Console.WriteLine($"Running total: {reader.CurrentVersion:N0}");
                                }
                            }

                            Console.WriteLine("Reader done");
                            //if (previous != (ulong)count)
                            //{
                            //    Assert.Fail($"previous {previous} != count {count}");
                            //}
                        }
                    }
                    stat.Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("READER EX: " + ex);
                }
            });

            var writerTask = Task.Run(() =>
            {
                try
                {
                    var stat = Benchmark.Run("Write to Log0", count);
                    {
                        for (long i = 1; i <= count; i++)
                        {
                            if (_couldWriteReadLog0Finished)
                            {
                                break;
                            }
                            slm.Log0.Append((StreamLogNotification)(ulong)i);
                            //if (i % 10_000_000 == 0)
                            //{
                            //    stat.Dispose();
                            //    stat = Benchmark.Run("Write to Log0", 10_000_000);
                            //}
                        }

                        Console.WriteLine("W1 Done");
                        _couldWriteReadLog0Finished = true;
                    }
                    stat.Dispose();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("EX: " + ex);
                }
            });

            var writerTask2 = Task.Run(() =>
            {
                try
                {
                    using (Benchmark.Run("Write to Log0", count))
                    {
                        for (long i = 1; i <= count; i++)
                        {
                            if (_couldWriteReadLog0Finished)
                            {
                                break;
                            }
                            slm.Log0.Append((StreamLogNotification)(ulong)i);
                        }

                        Console.WriteLine("W2 Done");
                        _couldWriteReadLog0Finished = true;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("EX: " + ex);
                }
            });
            writerTask2.Wait();

            readerTask.Wait();
            writerTask.Wait();

            Benchmark.Dump();

            slm.BufferPool.PrintBuffersAfterPoolDispose = true;
            // log0.Dispose();
            slm.Dispose();
        }

        [Test
#if !DEBUG
         , Explicit("long running")
#endif
        ]
        public void CouldWriteReadLog0ManyTimes()
        {
            for (int r = 0; r < 10; r++)
            {
                CouldWriteReadLog0(false);
            }

            // Console.WriteLine("Max stall after many iterations: " + MaxStall.ToString("N"));
        }

        [Test
#if !DEBUG
         , Explicit("long running")
#endif
        ]
        public void CouldReadWriteZeroLog()
        {
            Console.WriteLine("Starting test");
            Console.Out.Flush();
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath(clear: true);
            var repoName = "CouldWriteAndReadLog0";
            var processConfig = new ProcessConfig(path);
            var processConfig2 = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 20 * 1024, disableNotificationLog: false, true);
            var slm2 = new StreamLogManager(processConfig2, repoName, null, 20 * 1024, disableNotificationLog: false, true);

            slm.Log0.ActiveBlock.SharedMemory.Increment();
            slm2.Log0.ActiveBlock.SharedMemory.Increment();

            if (slm.Log0.State.StreamLogId != slm2.Log0.State.StreamLogId)
            {
                Assert.Fail("slm.Log0.State.StreamLogId != slm2.Log0.State.StreamLogId");
            }

            var count = TestUtils.GetBenchCount(200 * 1024 * 1024L, 1000);
            // must be pow2 because we divide count by it
            var taskPerProcessCount = 1;

            //var tasks1 = new List<Task>();
            //var tasks2 = new List<Task>();

            var threads1 = new List<Thread>();
            var threads2 = new List<Thread>();

            for (int t = 0; t < taskPerProcessCount; t++)
            {
                var x = t;

                threads1.Add(new Thread(() =>
                {
                    Thread.CurrentThread.Name = "W1_" + x;
                    using (Benchmark.Run("ZL Write", count / (2 * taskPerProcessCount), false))
                    {
                        for (long i = 1; i <= count / (2 * taskPerProcessCount); i++)
                        {
                            try
                            {
                                {
                                    if (slm.Log0.Append((StreamLogNotification)(ulong)i) == 0)
                                    {
                                        Assert.Fail("Cannot append");
                                    }
                                    // Thread.SpinWait(5);
                                    //if (i % 50 == 0)
                                    //{
                                    //    if (!Thread.Yield())
                                    //    {
                                    //        Thread.Sleep(0);
                                    //    }
                                    //}
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine(ex);
                            }
                        }

                        Console.WriteLine("W1 done");
                    }
                }));
            }

            for (int t = 0; t < taskPerProcessCount; t++)
            {
                var x = t;
                threads2.Add(new Thread(() =>
                {
                    try
                    {
                        Thread.CurrentThread.Name = "W2_" + x;
                        using (Benchmark.Run("ZL Write", count / (2 * taskPerProcessCount), false))
                        {
                            for (long i = 1; i <= count / (2 * taskPerProcessCount); i++)
                            {
                                try
                                {
                                    {
                                        if (slm2.Log0.Append((StreamLogNotification)(ulong)i) == 0)
                                        {
                                            Assert.Fail("Cannot Append: " + i);
                                        };
                                        // Thread.SpinWait(5);
                                        //if (i % 50 == 0)
                                        //{
                                        //    if (!Thread.Yield())
                                        //    {
                                        //        Thread.Sleep(0);
                                        //    }
                                        //}
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine(ex);
                                }
                            }
                            Console.WriteLine("W2 done");
                            //slm2.Log0.Dispose();
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                    }
                }));
            }

            foreach (var thread in threads1)
            {
                thread.Priority = ThreadPriority.AboveNormal;
                thread.Start();
            }
            foreach (var thread in threads2)
            {
                thread.Priority = ThreadPriority.AboveNormal;
                thread.Start();
            }

            var reader = new Thread(() =>

            {
                try
                {
                    var c = 0L;
                    using (Benchmark.Run("ZL Read", count, true))
                    {
                        var zlr = new NotificationLog.Reader(slm.Log0, CancellationToken.None);
                        var sw = new SpinWait();
                        while (zlr.MoveNext())
                        {
                            // if (zlr.MoveNext())
                            {
                                // Thread.SpinWait(1);
                                c++;
                                if (c % 10_000_000 == 0)
                                {
                                    Console.WriteLine($"{c:N0} |  MQL {zlr.MissedQueueLength}");
                                }

                                if (c >= count - 2 * taskPerProcessCount - 1000)
                                {
                                    if (zlr.MissedQueueLength > 0)
                                    {
                                        Console.WriteLine("Missed queue non empty");
                                        continue;
                                    }

                                    Console.WriteLine(
                                        $"Finished at CurrentVersion: " + zlr.CurrentVersion.ToString("N"));
                                    break;
                                }
                            }
                            //else
                            //{
                            //    sw.SpinOnce();
                            //    if (sw.NextSpinWillYield)
                            //    {
                            //        sw.Reset();
                            //        if (zlr.CurrentVersion > slm.Log0.State.GetZeroLogVersion())
                            //        {
                            //            Console.WriteLine("Reached the end");
                            //            // break;
                            //        }
                            //        // Console.WriteLine($"Spinning in ZLMN: " + zlr.CurrentVersion.ToString("N"));
                            //    }
                            //}
                        }

                        if (zlr.MaxStall > MaxStall)
                        {
                            MaxStall = zlr.MaxStall;
                        }

                        Console.WriteLine("STALLS: " + zlr.StallCount);
                        Console.WriteLine("-------------------------");
                        Console.WriteLine("MAX STALL: " + zlr.MaxStall.ToString("N"));
                        Console.WriteLine("GLOBAL MAX STALL: " + MaxStall.ToString("N"));
                        Console.WriteLine("-------------------------");
                        zlr.Dispose();
                    }

                    Console.WriteLine("Read count: " + c.ToString("N"));
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Reader exception: " + ex);
                }
            });

            reader.Priority = ThreadPriority.Highest;
            reader.Start();

            foreach (var thread in threads1)
            {
                thread.Join();
            }
            foreach (var thread in threads2)
            {
                thread.Join();
            }

            reader.Join();

            //using (Benchmark.Run("Log0 MoveNext", count))
            //{
            //    var readCount = 0;
            //    try
            //    {
            //        var chunkCursor =
            //            slm.Log0.State.StreamLogManager.LogChunkIndex.GetChunkRecordCursor(StreamLogId.Log0Id);

            //        using (var txn = slm.Log0.State.StreamLogManager.LogChunkIndex._env.BeginReadOnlyTransaction())
            //        {
            //            var chunks = slm.Log0.State.StreamLogManager.LogChunkIndex._chunksDb
            //                .AsEnumerable<long, StreamLogChunkRecord>(txn, -1).ToArray();
            //            var previous = 0UL;
            //            foreach (var r in chunks)
            //            {
            //                var chunk = slm.Log0.RentChunkFromRecord(r);

            //                if (chunk.IsValid)
            //                {
            //                    Console.WriteLine(
            //                        $"SLCR: version {r.Version} - chunk first version: {chunk.FirstVersion}");
            //                    if (r.Version != chunk.FirstVersion &&
            //                        r.Version != StreamLogChunkIndex.ReadyChunkVersion)
            //                    {
            //                        Assert.Fail("Bad versions");
            //                    }

            //                    chunk.DisposeFree();

            //                    if (previous != 0 && previous == r.Version)
            //                    {
            //                        Assert.Fail("Duplicate SLCR");
            //                    }

            //                    previous = r.Version;
            //                }
            //            }
            //        }

            //        slm.BufferPool.PrintBuffers();

            //        Console.WriteLine("Move next");
            //        while (chunkCursor.MoveNext())
            //        {
            //            var record = chunkCursor.Current.Value;
            //            Console.WriteLine("Record version: " + record.Version);

            //            if ((long)record.Version >= count)
            //            {
            //                Console.WriteLine("Break");
            //                break;
            //            }

            //            var chunk = slm.Log0.RentChunkFromRecord(record);

            //            Console.WriteLine("Chunk first version: " + chunk.FirstVersion);

            //            for (ulong i = 0; i < 1024 * 1024; i++)
            //            {
            //                var position = NotificationLog.Log0ItemPosition(chunk._pointer,
            //                    (long)(chunk.FirstVersion + i), out _);
            //                var value = *(ulong*)position;
            //                if (value == 0)
            //                {
            //                    Assert.Fail("Zero value at: " + (chunk.FirstVersion + i));
            //                    // Console.WriteLine("Zero value at: " + (chunk.FirstVersion + i));
            //                }

            //                // Console.WriteLine("OK");
            //                readCount++;
            //            }

            //            chunk.DisposeFree();
            //        }

            //        //var c = log0.GetContainerCursor(false);

            //        //while (c.MoveNext())
            //        //{
            //        //    if (c.CurrentValue.ReadUInt64(0) == 0)
            //        //    {
            //        //        // Console.WriteLine($"c.CurrentValue == 0 at {c.CurrentKey}");
            //        //        Assert.Fail($"c.CurrentValue == 0 at {c.CurrentKey}");
            //        //    }

            //        //    readCount++;
            //        //}

            //        //c.Dispose();
            //        // Assert.AreEqual(count, readCount);
            //    }
            //    catch (Exception ex)
            //    {
            //        Console.WriteLine("EX: " + ex);
            //    }

            //    Console.WriteLine("READ COUNT M: " + readCount * 1.0 / (1024 * 1024));
            //}

            //Console.ReadLine();

            //slm.Log0.Dispose();
            //slm2.Log0.Dispose();

            //slm.Dispose();
            //slm2.Dispose();

            Benchmark.Dump();

            Thread.Sleep(100);

            Console.WriteLine("PREVIOUS: " + (slm.Log0.OldPositionLookupCount + slm2.Log0.OldPositionLookupCount));
            Console.WriteLine("TABLE: " + (slm.Log0.ChunkTableLookupCount + slm2.Log0.ChunkTableLookupCount));
            Console.WriteLine("ROTATE CNT: " + (slm.Log0.RotateCount + slm2.Log0.RotateCount));

            slm.Dispose();
            slm2.Dispose();
            GC.KeepAlive(slm);
            GC.KeepAlive(slm2);
        }

        [Test
#if !DEBUG
         , Explicit("long running")
#endif
        ]
        public void CouldReadWriteZeroLogViaThreadPool()
        {
            Console.WriteLine("Starting test");
            Console.Out.Flush();
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath(clear: true);
            var repoName = "CouldWriteAndReadLog0";
            var processConfig = new ProcessConfig(path);

            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;

            var slm = new StreamLogManager(processConfig, repoName, null, 20 * 1024, false, true);

            var count = TestUtils.GetBenchCount(200 * 1024 * 1024L, 1000);

            var tp = new SpreadsThreadPool(new ThreadPoolSettings(6, ThreadType.Foreground, "Log0Pool",
                ApartmentState.Unknown, null, 0, ThreadPriority.AboveNormal));

            var writer = new Thread(() =>
            {
                for (int i = 1; i <= count; i++)
                {
                    var x = i;

                    ThreadPool.UnsafeQueueUserWorkItem(o =>
                        {
                            (o as NotificationLog).Append((StreamLogNotification)(ulong)x);
                        }
                        , slm.Log0);
                }
            });

            writer.Priority = ThreadPriority.Highest;
            writer.Start();

            var reader = new Thread(() =>

            {
                try
                {
                    var c = 0L;
                    using (Benchmark.Run("ZL Read", count, true))
                    {
                        var zlr = new NotificationLog.Reader(slm.Log0, CancellationToken.None);
                        var sw = new SpinWait();
                        while (zlr.MoveNext())
                        {
                            // if (zlr.MoveNext())
                            {
                                // Thread.SpinWait(1);
                                c++;
                                if (c % 10_000_000 == 0)
                                {
                                    Console.WriteLine($"{c:N0} |  MQL {zlr.MissedQueueLength}");
                                }

                                if (c >= count - 1000)
                                {
                                    if (zlr.MissedQueueLength > 0)
                                    {
                                        Console.WriteLine("Missed queue non empty");
                                        continue;
                                    }

                                    Console.WriteLine(
                                        $"Finished at CurrentVersion: " + zlr.CurrentVersion.ToString("N"));
                                    break;
                                }
                            }
                            //else
                            //{
                            //    sw.SpinOnce();
                            //    if (sw.NextSpinWillYield)
                            //    {
                            //        sw.Reset();
                            //        if (zlr.CurrentVersion > slm.Log0.State.GetZeroLogVersion())
                            //        {
                            //            Console.WriteLine("Reached the end");
                            //            // break;
                            //        }
                            //        // Console.WriteLine($"Spinning in ZLMN: " + zlr.CurrentVersion.ToString("N"));
                            //    }
                            //}
                        }

                        if (zlr.MaxStall > MaxStall)
                        {
                            MaxStall = zlr.MaxStall;
                        }

                        Console.WriteLine("STALLS: " + zlr.StallCount);
                        Console.WriteLine("-------------------------");
                        Console.WriteLine("MAX STALL: " + zlr.MaxStall.ToString("N"));
                        Console.WriteLine("GLOBAL MAX STALL: " + MaxStall.ToString("N"));
                        Console.WriteLine("-------------------------");
                        zlr.Dispose();
                    }

                    Console.WriteLine("Read count: " + c.ToString("N"));
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Reader exception: " + ex);
                }
            });

            reader.Priority = ThreadPriority.Highest;
            reader.Start();

            reader.Join();

            tp.Dispose();
            tp.WaitForThreadsExit();

            Benchmark.Dump();

            Thread.Sleep(100);

            Console.WriteLine("PREVIOUS: " + (slm.Log0.OldPositionLookupCount + slm.Log0.OldPositionLookupCount));
            Console.WriteLine("TABLE: " + (slm.Log0.ChunkTableLookupCount + slm.Log0.ChunkTableLookupCount));
            Console.WriteLine("ROTATE CNT: " + (slm.Log0.RotateCount + slm.Log0.RotateCount));

            //slm.Dispose();
            //slm2.Dispose();
            GC.KeepAlive(slm); GC.KeepAlive(slm);
            slm.Dispose();

            Thread.Sleep(1000);
        }
    }
}
