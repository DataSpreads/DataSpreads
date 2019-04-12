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
using System.IO;
using System.Threading.Tasks;
using DataSpreads.Config;
using DataSpreads.Storage;
using DataSpreads.StreamLogs;
using NUnit.Framework;
using Spreads;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.Utils;

namespace DataSpreads.Tests.Storage
{
    [Category("CI")]
    [TestFixture, SingleThreaded]
    public class SqLiteBlockStorageTests
    {
        [Test]
        public void CouldOpenCloseStorage()
        {
            var path = TestUtils.GetPath(clear: true);
            var storage = new SQLiteStorage($@"Filename={Path.Combine(path, "blockstorage.db")}");
            var blobLen = 16; //_000;
            var rm = BufferPool.Retain(blobLen, true);

            var count = 1_000;

            var tasksCount = 1;

            Task[] tasks = new Task[tasksCount];

            using (Benchmark.Run("TestInsert", count * tasksCount * 1000))
            {
                for (int t = 1; t <= tasksCount; t++)
                {
                    var t1 = t;
                    tasks[t - 1] = Task.Run(() =>
                    {
                        try
                        {
                            for (int i = 1; i <= count; i++)
                            {
                                bool done;
                                long rowid;
                                (done, rowid) = storage.InsertTestQuery(t1, i, rm);
                                if (!done)
                                {
                                    Console.WriteLine($"Done: {done}, rowid: {rowid}");
                                }

                                //var rc = 0;
                                //while (true)
                                //{
                                //    // var txn = storage.BeginConcurrent();
                                //    storage.InsertTestQuery(t1, i, rm.Slice(0)) // , txn);
                                //    rc = txn.RawCommit();
                                //    txn.Dispose();
                                //    if (rc == Spreads.SQLite.Interop.Constants.SQLITE_BUSY_SNAPSHOT || rc == Spreads.SQLite.Interop.Constants.SQLITE_BUSY)
                                //    {
                                //        Console.WriteLine("SQLITE_BUSY_SNAPSHOT: " + rc);
                                //    }
                                //    else if (rc == Spreads.SQLite.Interop.Constants.SQLITE_DONE)
                                //    {
                                //        break;
                                //    }
                                //    else
                                //    {
                                //        Console.WriteLine("RC: " + rc);
                                //    }
                                //}
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                        }
                    });
                }

                Task.WaitAll(tasks);
            }

            using (Benchmark.Run("TestSelect", count * tasksCount * 1000))
            {
                for (int t = 1; t <= tasksCount; t++)
                {
                    var t1 = t;
                    tasks[t - 1] = Task.Run(() =>
                    {
                        try
                        {
                            for (int i = 1; i <= count; i++)
                            {
                                var (tx, ix) = storage.SelectTestQuery(t1, i);
                                if (tx != t1 || ix != i)
                                {
                                    Assert.Fail();
                                }
                                //if (rmSelect.Length != blobLen)
                                //{
                                //    Assert.Fail();
                                //}
                                //// else
                                //{
                                //    rmSelect.Dispose();
                                //}
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                        }
                    });
                }

                Task.WaitAll(tasks);
            }

            Benchmark.Dump();

            //var txn = storage.BeginConcurrent();
            //var txn2 = storage.BeginConcurrent();
            //txn.Commit();
            //txn2.Commit();

            // storage.Checkpoint();

            rm.Dispose();
            storage.Dispose();
        }

        [Test]
        public void CouldWriteManyFinalizedChunksToTable()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
            Settings.DoDetectBufferLeaks = true;
#pragma warning restore 618
            ProcessConfig.InitDefault();
            var path = TestUtils.GetPath();

            ulong count = 100_000;

            var rng = new Random(42);

            var values = new SmallDecimal[count + 1];
            for (ulong i = 1; i <= count; i++)
            {
                values[(int)i] = new SmallDecimal(10 + Math.Round(rng.NextDouble() * 2, 4), 4);
            }

            var storage = new SQLiteStorage($@"Filename={Path.Combine(path, "blockstorage.db")}");

            var totalPayload = 0;
            var totalCapacity = 0L;

            //Settings.ZstdCompressionLevel = 1;
            //Settings.LZ4CompressionLevel = 1;
            //Settings.ZlibCompressionLevel = 5;

            using (Benchmark.Run("Chunks", (int)(count)))
            {
                var chunkSize = 4032;

                var rm = BufferPool.Retain(chunkSize, true);
                rm.Span.Clear();

                StreamBlock.TryInitialize(new DirectBuffer(rm), (StreamLogId)1, 8, 1);

                var block = new StreamBlock(rm, (StreamLogId)1, 8, 1);

                var chunkCount = 1;
                for (ulong i = 1; i <= count; i++)
                {
                    var claim = block.Claim(i, 8);
                    if (!claim.IsValid)
                    {
                        Assert.IsTrue(block.IsCompleted);

                        var (inserted, rowid) = storage.InsertBlock(block);

                        //var couldRead = file.TryReadChunk(chunk.FirstVersion, out var readChunk);
                        //Assert.IsTrue(couldRead);
                        //Assert.AreEqual(chunk.WriteEnd, readChunk.WriteEnd);

                        //var fv = readChunk.FirstVersion;
                        //for (int j = 0; j < readChunk.Count; j++)
                        //{
                        //    var ii = fv + (ulong)j;
                        //    var readValue = readChunk[j].Read<SmallDecimal>(0);
                        //    if (readValue != values[ii])
                        //    {
                        //        Assert.Fail();
                        //    }
                        //}

                        //readChunk.Dispose();

                        block.DisposeFree();
                        chunkCount++;

                        if (chunkCount % 100 == 0)
                        {
                            storage.Checkpoint(true);
                        }

                        //if (chunkCount % 50 == 0)
                        //{
                        //    file.Complete();
                        //    totalPayload += file.PayloadSize;
                        //    totalCapacity += file.FileCapacity;
                        //    fileCount++;
                        //    file.Dispose();
                        //    filePath = Path.Combine(path, $"{fileCount}.slc");
                        //    file = new StreamLogChunkFile(filePath, true);
                        //}

                        rm = BufferPool.Retain(chunkSize, true);
                        rm.Span.Clear();

                        StreamBlock.TryInitialize(new DirectBuffer(rm), (StreamLogId)1, 8, i);
                        block = new StreamBlock(rm, (StreamLogId)1, 8, i);
                        claim = block.Claim(i, 8);
                    }

                    claim.Write(0, values[i]);
                    block.Commit();
                }

                var (inserted1, rowid1) = storage.InsertBlock(block);
                Assert.IsTrue(inserted1);
                var blockR = storage.TryGetStreamBlock(block.StreamLogIdLong, block.FirstVersion);

                var lastVersion = blockR.CurrentVersion;
                Assert.AreEqual(block.CurrentVersion, lastVersion);

                blockR.DisposeFree();

                block.Complete();
                (inserted1, rowid1) = storage.InsertBlock(block);
                Assert.IsTrue(inserted1);
                Assert.AreEqual(block.CurrentVersion, lastVersion);

                (inserted1, rowid1) = storage.InsertBlock(block);
                Assert.IsFalse(inserted1);
                //var couldRead1 = file.TryReadChunk(chunk.FirstVersion, out var readChunk1);
                //Assert.IsTrue(couldRead1);
                //Assert.AreEqual(chunk.WriteStart, readChunk1.WriteStart, "WriteStart");
                //Assert.AreEqual(chunk.WriteEnd, readChunk1.WriteEnd, "WriteEnd");
                //readChunk1.Dispose();
                block.DisposeFree();

                //totalPayload += file.PayloadSize;
                //totalCapacity += file.FileCapacity;

                Console.WriteLine("Chunk count: " + chunkCount);
                //Console.WriteLine("Payload: " + totalPayload);
                //Console.WriteLine("File size: " + totalCapacity);
                Console.WriteLine("Useful size: " + count * 8);
                //Console.WriteLine("Effective Compression: " + (totalPayload / (1.0 * count * 8)));

                storage.Dispose();
            }
        }
    }
}
