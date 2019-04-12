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
using DataSpreads.StreamLogs;
using NUnit.Framework;
using Spreads;
using Spreads.LMDB;
using Spreads.Serialization;
using Spreads.Utils;
using System;
using System.Diagnostics;
using System.Linq;
using DataSpreads.Config;
using Spreads.DataTypes;

#pragma warning disable 618
// ReSharper disable AccessToDisposedClosure
// ReSharper disable AccessToModifiedClosure

namespace DataSpreads.Tests.StreamLogs
{
    [Category("CI")]
    [TestFixture, SingleThreaded]
    public class StreamBlockIndexTests
    {
        [Test]
        public void CouldAddGetRecord()
        {
            var path = TestUtils.GetPath();

            var table = new StreamBlockIndex(path, 16, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, null);

            var streamId = (StreamLogId)1;

            var lr1 = new StreamBlockRecord(BufferRef.Create(0, 1)) { Version = 12 };

            Assert.IsTrue(table.TryAddBlockRecord(streamId, lr1));

            Assert.AreEqual(1, table.GetBlockCount((StreamLogId)1));

            Assert.IsTrue(table.TryGetLast(streamId, false, out var last));

            Assert.AreEqual(last.Version, 12);
            Assert.AreEqual(last.BufferRef, lr1.BufferRef);

            table.Dispose();
        }

        [Test]
        public void CouldWriteAndReadFromStreamLogAsSeries()
        {
            var path = TestUtils.GetPath();

            var table = new StreamBlockIndex(path, 16, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, null);

            var streamId = (StreamLogId)10000;

            var series = table.GetBlockRecordSeries(streamId);

            var count = 1000;

            for (int i = 0; i < count; i++)
            {
                var lr1 = new StreamBlockRecord(BufferRef.Create(0, i + 1)) { Version = (ulong)i + 1, Timestamp = new Timestamp(i + 1) };

                Assert.IsTrue(table.TryAddBlockRecord(streamId, lr1));
                Assert.AreEqual(i + 1, table.GetBlockCount(streamId));
                Assert.AreEqual(i + 1, series.Count());

                var first = series.First;
                Assert.IsTrue(first.IsPresent);

                Assert.IsTrue(table.TryGetLast(streamId, true, out var lst));
                Assert.AreEqual((ulong)i + 1, lst.Version);

                var last = series.Last;
                Assert.IsTrue(last.IsPresent);

                Assert.AreEqual(last.Present.Key, i + 1);
                Assert.AreEqual(last.Present.Value.BufferRef, lr1.BufferRef);
            }

            var j = 1;

            foreach (var kvp in series)
            {
                Assert.AreEqual(kvp.Key, j);
                Assert.AreEqual(kvp.Value.Version, j);
                Assert.AreEqual(kvp.Value.BufferRef, BufferRef.Create(0, j));
                j++;
            }

            Assert.IsTrue(series.First.IsPresent);
            Assert.IsTrue(series.First.Present.Key == 1);

            Assert.IsTrue(series.Last.IsPresent);
            Assert.IsTrue(series.Last.Present.Key == (ulong)count);

            var c = series.GetEnumerator();

            Assert.IsTrue(c.MoveFirst());
            Assert.IsTrue(c.CurrentKey == 1);
            Assert.IsFalse(c.MovePrevious());

            Assert.IsTrue(c.MoveNext());
            Assert.IsTrue(c.CurrentKey == 2);

            Assert.IsTrue(c.MoveLast());
            Assert.IsTrue(c.CurrentKey == (ulong)count);

            Assert.IsFalse(c.MoveNext());

            Assert.IsTrue(c.MovePrevious());
            Assert.IsTrue(c.CurrentKey == (ulong)count - 1);

            Assert.IsTrue(c.MoveAt(50, Lookup.EQ));
            Assert.IsTrue(c.CurrentKey == 50);

            Assert.IsTrue(c.MoveAt((ulong)count + 10, Lookup.LE));
            Assert.IsTrue(c.CurrentKey == (ulong)count);

            Assert.IsTrue(c.MoveAt(0, Lookup.GE));
            Assert.IsTrue(c.CurrentKey == 1);

            Assert.IsTrue(c.MoveAt(50, Lookup.LE));
            Assert.IsTrue(c.CurrentKey == 50);

            Assert.IsTrue(c.MoveAt(50, Lookup.LT));
            Assert.IsTrue(c.CurrentKey == 49);

            Assert.IsTrue(c.MoveAt(50, Lookup.GE));
            Assert.IsTrue(c.CurrentKey == 50);

            Assert.IsTrue(c.MoveAt(50, Lookup.GT));
            Assert.IsTrue(c.CurrentKey == 51);

            var chunkRecord = c.CurrentValue;

            chunkRecord.BufferRef = default;

            var _ = table.UpdateBlockRecord(streamId, chunkRecord);

            // add more after update
            for (int i = count; i < count + 100; i++)
            {
                var lr1 = new StreamBlockRecord(BufferRef.Create(0, i + 1)) { Version = (ulong)i + 1 };

                Assert.IsTrue(table.TryAddBlockRecord(streamId, lr1));
                Assert.AreEqual(i + 1, table.GetBlockCount(streamId));
                Assert.AreEqual(i + 1, series.Count());

                var last = series.Last;

                Assert.IsTrue(last.IsPresent);

                Assert.AreEqual(last.Present.Key, i + 1);
                Assert.AreEqual(last.Present.Value.BufferRef, lr1.BufferRef);
            }

            Assert.IsTrue(c.MoveAt(chunkRecord.Version, Lookup.GE));
            Assert.IsTrue(c.CurrentValue.BufferRef == default);

            // ready chunk test

            var lastVersion = series.Last.Present.Key;

            var readyChunk = new StreamBlockRecord() { Version = StreamBlockIndex.ReadyBlockVersion };

            table.TryAddBlockRecord(streamId, readyChunk);

            // not visible before update
            Assert.AreEqual(lastVersion, series.Last.Present.Value.Version);

            table.TryGetLast(streamId, false, out var lastIsReady);

            Assert.AreEqual(StreamBlockIndex.ReadyBlockVersion, lastIsReady.Version);

            //var newVersion = lastVersion + 1;

            //Assert.IsTrue(table.UpdateReadyChunkVersion(streamId, newVersion));

            //Assert.AreEqual(newVersion, series.Last.Present.Value.Version);

            c.Dispose();
            table.Dispose();
        }

        [Test]
        public void CouldWriteAndReadFromStreamLogAsTimeSeries()
        {
            var path = TestUtils.GetPath();

            var table = new StreamBlockIndex(path, 16, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, null);

            var streamId = (StreamLogId)10000;

            var series = table.GetBlockRecordTimeSeries(streamId);

            var count = 1000;

            for (int i = 0; i < count; i++)
            {
                var lr1 = new StreamBlockRecord(BufferRef.Create(0, i + 1)) { Version = (ulong)i + 1, Timestamp = new Timestamp(i + 1) };

                Assert.IsTrue(table.TryAddBlockRecord(streamId, lr1));
                Assert.AreEqual(i + 1, table.GetBlockCount(streamId));
                Assert.AreEqual(i + 1, series.Count());

                var first = series.First;
                Assert.IsTrue(first.IsPresent);

                Assert.IsTrue(table.TryGetLast(streamId, true, out var lst));
                Assert.AreEqual((ulong)i + 1, lst.Version);

                var last = series.Last;
                Assert.IsTrue(last.IsPresent);

                Assert.AreEqual(last.Present.Key.Nanos, i + 1);
                Assert.AreEqual(last.Present.Value.BufferRef, lr1.BufferRef);
            }

            var j = 1;

            foreach (var kvp in series)
            {
                Assert.AreEqual(kvp.Key.Nanos, j);
                Assert.AreEqual(kvp.Value.Timestamp.Nanos, j);
                Assert.AreEqual(kvp.Value.BufferRef, BufferRef.Create(0, j));
                j++;
            }

            Assert.IsTrue(series.First.IsPresent);
            Assert.IsTrue(series.First.Present.Key == new Timestamp(1));

            Assert.IsTrue(series.Last.IsPresent);
            Assert.IsTrue(series.Last.Present.Key == new Timestamp(count));

            var c = series.GetEnumerator();

            Assert.IsTrue(c.MoveFirst());
            Assert.IsTrue(c.CurrentKey == new Timestamp(1));
            Assert.IsFalse(c.MovePrevious());

            Assert.IsTrue(c.MoveNext());
            Assert.IsTrue(c.CurrentKey == new Timestamp(2));

            Assert.IsTrue(c.MoveLast());
            Assert.IsTrue(c.CurrentKey == new Timestamp(count));

            Assert.IsFalse(c.MoveNext());

            Assert.IsTrue(c.MovePrevious());
            Assert.IsTrue(c.CurrentKey == new Timestamp(count - 1));

            Assert.IsTrue(c.MoveAt(new Timestamp(50), Lookup.EQ));
            Assert.IsTrue(c.CurrentKey == new Timestamp(50));

            Assert.IsTrue(c.MoveAt(new Timestamp(count + 10), Lookup.LE));
            Assert.IsTrue(c.CurrentKey == new Timestamp(count));

            Assert.IsTrue(c.MoveAt(new Timestamp(0), Lookup.GE));
            Assert.IsTrue(c.CurrentKey == new Timestamp(1));

            Assert.IsTrue(c.MoveAt(new Timestamp(50), Lookup.LE));
            Assert.IsTrue(c.CurrentKey == new Timestamp(50));

            Assert.IsTrue(c.MoveAt(new Timestamp(50), Lookup.LT));
            Assert.IsTrue(c.CurrentKey == new Timestamp(49));

            Assert.IsTrue(c.MoveAt(new Timestamp(50), Lookup.GE));
            Assert.IsTrue(c.CurrentKey == new Timestamp(50));

            Assert.IsTrue(c.MoveAt(new Timestamp(50), Lookup.GT));
            Assert.IsTrue(c.CurrentKey == new Timestamp(51));

            var chunkRecord = c.CurrentValue;

            chunkRecord.BufferRef = default;

            var _ = table.UpdateBlockRecord(streamId, chunkRecord);

            var count1 = series.Count();

            // add more after update
            for (int i = count; i < count + 100; i++)
            {
                var lr1 = new StreamBlockRecord(BufferRef.Create(0, i + 1)) { Version = (ulong)i + 1, Timestamp = new Timestamp(i + 1) };

                Assert.IsTrue(table.TryAddBlockRecord(streamId, lr1));
                Assert.AreEqual(i + 1, table.GetBlockCount(streamId));
                Assert.AreEqual(i + 1, series.Count());

                var last = series.Last;

                Assert.IsTrue(last.IsPresent);

                Assert.AreEqual(last.Present.Key.Nanos, i + 1);
                Assert.AreEqual(last.Present.Value.BufferRef, lr1.BufferRef);
            }

            Assert.IsTrue(c.MoveAt(chunkRecord.Timestamp, Lookup.GE));
            Assert.IsTrue(c.CurrentValue.BufferRef == default);

            // ready chunk test

            var lastTimestamp = series.Last.Present.Key.Nanos;

            var readyChunk = new StreamBlockRecord() { Version = StreamBlockIndex.ReadyBlockVersion };

            table.TryAddBlockRecord(streamId, readyChunk);

            // not visible before update
            Assert.AreEqual(lastTimestamp, series.Last.Present.Value.Timestamp.Nanos);

            table.TryGetLast(streamId, false, out var lastIsReady);

            Assert.AreEqual(StreamBlockIndex.ReadyBlockVersion, lastIsReady.Version);

            //var newVersion = lastVersion + 1;

            //Assert.IsTrue(table.UpdateReadyChunkVersion(streamId, newVersion));

            //Assert.AreEqual(newVersion, series.Last.Present.Value.Version);

            c.Dispose();
            table.Dispose();
        }

        [Test]
        public void CouldWriteManyValuesToSeries()
        {
            var path = TestUtils.GetPath();

            var table = new StreamBlockIndex(path, 512, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, null);

            var streamId = (StreamLogId)10000;

            var series = table.GetBlockRecordSeries(streamId);

            var count = 50_000;

            using (Benchmark.Run("Writes", count))
            {
                for (int i = 0; i < count; i++)
                {
                    var lr1 = new StreamBlockRecord(BufferRef.Create(0, i + 1)) { Version = (ulong)i + 1 };

                    if (!table.TryAddBlockRecord(streamId, lr1))

                    {
                        Assert.Fail();
                    }

                    if (i + 1 != table.GetBlockCount(streamId))
                    {
                        Assert.Fail();
                    }

                    var last = series.Last;

                    if (!last.IsPresent)
                    {
                        Assert.Fail();
                    }

                    if (last.Present.Key != (ulong)i + 1)
                    {
                        Assert.Fail();
                    }

                    if (last.Present.Value.BufferRef != lr1.BufferRef)
                    {
                        Assert.Fail();
                    }
                }
            }
            Benchmark.Dump();

            table.Dispose();
        }

        [Test]
        public void CouldUpdateChunk()
        {
            var path = TestUtils.GetPath();

            var table = new StreamBlockIndex(path, 512, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, null);

            var streamId = (StreamLogId)10000;

            var series = table.GetBlockRecordSeries(streamId);

            var count = 1000;

            using (Benchmark.Run("Writes", count))
            {
                for (int i = 0; i < count; i++)
                {
                    var version = (ulong)i + 1;

                    var chunk = new StreamBlockRecord(BufferRef.Create(0, i + 1)) { Version = version };

                    if (!table.TryAddBlockRecord(streamId, chunk))
                    {
                        Assert.Fail("!table.TryAddChunkRecord(streamId, chunk)");
                    }

                    if (!table.TryGetLast(streamId, false, out var lastReady))
                    {
                        Assert.Fail("!table.TryGetLast(streamId, false, out var lastReady)");
                    }

                    if (version != lastReady.Version)
                    {
                        Assert.Fail("version != lastReady.Version");
                    }

                    chunk.BufferRef = default;

                    if (!table.UpdateBlockRecord(streamId, chunk))
                    {
                        Assert.Fail("!table.UpdateChunk(streamId, chunk)");
                    }

                    if (i + 1 != table.GetBlockCount(streamId))
                    {
                        Assert.Fail("i + 1 != table.GetChunkCount(streamId)");
                    }

                    var last = series.Last;

                    if (!last.IsPresent)
                    {
                        Assert.Fail("!last.IsPresent");
                    }

                    if (last.Present.Key != (ulong)i + 1)
                    {
                        Assert.Fail("last.Present.Key != (ulong)i + 1");
                    }

                    if (last.Present.Value.BufferRef != chunk.BufferRef)
                    {
                        Assert.Fail("last.Present.Value.BufferRef != chunk.BufferRef");
                    }

                    if (last.Present.Value.BufferRef != default)
                    {
                        Assert.Fail("last.Present.Value.BufferRef != default");
                    }
                }
            }
            Benchmark.Dump();

            table.Dispose();
        }

        [Test]
        public void CouldUpdateNextBlock()
        {
            var path = TestUtils.GetPath();

            var table = new StreamBlockIndex(path, 512, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, null);

            var streamId = (StreamLogId)10000;

            var series = table.GetBlockRecordSeries(streamId);

            var count = 1000;

            using (Benchmark.Run("Writes", count))
            {
                for (int i = 0; i < count; i++)
                {
                    var version = (ulong)i + 1;

                    var chunk = new StreamBlockRecord(BufferRef.Create(0, i + 1)) { Version = StreamBlockIndex.ReadyBlockVersion };

                    if (!table.TryAddBlockRecord(streamId, chunk))
                    {
                        Assert.Fail();
                    }

                    if (!table.TryGetLast(streamId, false, out var lastReady))
                    {
                        Assert.Fail();
                    }

                    if (StreamBlockIndex.ReadyBlockVersion != lastReady.Version)
                    {
                        Assert.Fail();
                    }

                    if (i == 254)
                    {
                        Console.WriteLine("stop");
                    }

                    if (!table.UpdateReadyBlockVersion(streamId, version))
                    {
                        Assert.Fail();
                    }

                    if (i + 1 != table.GetBlockCount(streamId))
                    {
                        Assert.Fail();
                    }

                    var last = series.Last;

                    if (!last.IsPresent)
                    {
                        Assert.Fail();
                    }

                    if (last.Present.Key != (ulong)i + 1)
                    {
                        Assert.Fail();
                    }

                    if (last.Present.Value.BufferRef != chunk.BufferRef)
                    {
                        Assert.Fail();
                    }
                }
            }
            Benchmark.Dump();
            Console.WriteLine("Finished");
            table.Dispose();
        }

        [Test]
        public void CouldAddNextChunkAfterCompleted()
        {
            Settings.DoAdditionalCorrectnessChecks = false;

            var path = TestUtils.GetPath();
            var processConfig = new ProcessConfig(path);
            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync; // LMDBEnvironmentFlags.MapAsync | LMDBEnvironmentFlags.NoMetaSync | LMDBEnvironmentFlags.WriteMap;
            var slm = new StreamLogManager(processConfig, "CouldAddNextChunk", null, 512, true, true);

            var bufferPool = slm.BufferPool;

            var blockIndex = slm.BlockIndex;

            var streamId = (StreamLogId)42;

            short valueSize = 8;

            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, 8, SerializationFormat.Binary);

            var sl = new StreamLog(slm, state, textId: "test_stream");

            var count = 100;

            SharedMemory currentBuffer = null;

            using (Benchmark.Run("AddNext (KOPS)", count * 1000))
            {
                var version = 1UL;
                var block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, version, default);
                Assert.AreEqual(2, block.SharedMemory.ReferenceCount);
                // var firstBufferRef = block.SharedMemory.BufferRef;

                for (int i = 0; i < count; i++)
                {
                    version = (ulong)i + 1;

                    if (!block.Claim(version, 8).IsValid)
                    {
                        Assert.Fail("!slc.Claim(version, 8).IsValid");
                    }
                    block.Commit();
                    block.Complete();

                    var bufferRef = block.SharedMemory.BufferRef;

                    block.DisposeFree();
                    block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, version + 1, default);

                    var bufferRef1 = block.SharedMemory.BufferRef;
                    if (bufferRef1 == bufferRef)
                    {
                        Assert.Fail("bufferRef1 == bufferRef");
                    }
                    // As if someone has created the next buffer before the second call, forget the first call
                    var prev = block;
                    // block.DisposeFree();
                    block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, version + 1, default);
                    prev.DisposeFree();

                    var bufferRef2 = block.SharedMemory.BufferRef;

                    if (block.SharedMemory.ReferenceCount != 2)
                    {
                        Assert.Fail($"block.SharedMemory.RefCount {block.SharedMemory.ReferenceCount} != 1");
                    }

                    if (bufferRef1 != bufferRef2)
                    {
                        Assert.Fail($"bufferRef1 {bufferRef1} != bufferRef2 {bufferRef2}");
                    }

                    var nextVersion = block.NextVersion;
                    block.DisposeFree();

                    // block = new StreamBlock(block.SharedMemory.RetainBlockMemory(false), streamId, valueSize, nextVersion);
                    block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, nextVersion, default);
                }

                // SharedMemory.Free(buffer);
                block.DisposeFree();

                block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, version + 1, default);
                block = new StreamBlock(block.SharedMemory.RetainBlockMemory(false), streamId, valueSize, version + 1);

                // SharedMemory.Free(buffer);
                block.DisposeFree();
            }

            bufferPool.PrintBuffers();
            bufferPool.PrintBuffersAfterPoolDispose = true;

            Benchmark.Dump();
            Console.WriteLine("Finished");
            sl.Dispose();
            slm.Dispose();
        }

        [Test]
        public void CouldAddNextChunkBeforeCompleted()
        {
            var path = TestUtils.GetPath();
            var processConfig = new ProcessConfig(path);
            StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
            StartupConfig.StreamBlockIndexFlags = LMDBEnvironmentFlags.NoSync;
            var slm = new StreamLogManager(processConfig, "CouldAddNextChunk", null, 512, true, true);

            var bufferPool = slm.BufferPool;

            var blockIndex = slm.BlockIndex;

            var streamId = (StreamLogId)42;

            short valueSize = 8;

            var state = slm.StateStorage.GetState(streamId);
            state.CheckInit(streamId, 8, SerializationFormat.Binary);

            var sl = new StreamLog(slm, state, textId: "test_stream");

            var count = 100;

            SharedMemory currentBuffer = null;

            using (Benchmark.Run("AddNext (KOPS)", count * 1000))
            {
                var version = 1UL;
                var block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, version, default);
                // var slc = new StreamBlock(buffer.RetainChunkMemory(false), streamId, valueSize, 1);

                var firstBufferRef = block.SharedMemory.BufferRef;

                for (int i = 0; i < count; i++)
                {
                    version = (ulong)i + 1;

                    if (!block.Claim(version, 8).IsValid)
                    {
                        Assert.Fail("!slc.Claim(version, 8).IsValid");
                    }
                    block.Commit();

                    var bufferRef = block.SharedMemory.BufferRef;

                    blockIndex.PrepareNextWritableStreamBlock(sl, length: 1);
                    blockIndex.PrepareNextWritableStreamBlock(sl, length: 1);
                    block.Complete();

                    block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, version + 1, default);
                    var bufferRef1 = block.SharedMemory.BufferRef;
                    // As if someone has created the next buffer before the second call, forget the first call

                    block.DisposeFree();
                    block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, version + 1, default);
                    var bufferRef2 = block.SharedMemory.BufferRef;

                    if (block.SharedMemory.ReferenceCount != 2)
                    {
                        Assert.Fail($"buffer.RefCount {block.SharedMemory.ReferenceCount} != 1");
                    }

                    if (bufferRef1 != bufferRef2)
                    {
                        Assert.Fail($"bufferRef1 {bufferRef1} != bufferRef2 {bufferRef2}");
                    }

                    block.Complete();

                    var nextVersion = block.NextVersion;
                    block.DisposeFree();

                    blockIndex.PrepareNextWritableStreamBlock(sl, length: 1);
                    blockIndex.PrepareNextWritableStreamBlock(sl, length: 1);

                    // block = new StreamBlock(block.SharedMemory.RetainBlockMemory(false), streamId, valueSize, nextVersion);
                    block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, nextVersion, default);
                }

                if (!block.Claim(version + 1, 8).IsValid)
                {
                    Assert.Fail("!slc.Claim(version, 8).IsValid");
                }
                block.Commit();
                block.Complete();

                block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, block.NextVersion, default);

                Assert.AreEqual(2, block.SharedMemory.ReferenceCount);

                block.DisposeFree();
                block = blockIndex.RentNextWritableBlock(sl, minimumLength: 1, version + 2, default);

                Assert.IsFalse(block.IsCompleted);

                // TODO test replace

                //// replace existing
                //var toReplaceVersion = slc.FirstVersion;
                //var toReplaceRef = buffer.BufferRef;

                //slc.Complete();
                //// SharedMemory.Free(buffer);
                //// slc.DisposeFree();

                //buffer = chunkIndex.GetOrCreateNextWritableChunkBuffer(sl, toReplaceVersion, toReplaceRef, 1);
                //slc = new StreamLogChunk(buffer.RetainChunkMemory(false), streamId, valueSize, version + 2);
                //Assert.AreNotEqual(toReplaceRef, buffer.BufferRef);

                // SharedMemory.Free(buffer);
                block.DisposeFree();
            }

            //bufferPool.PrintBuffers();

            //bufferPool.PrintBuffersAfterPoolDispose = true;

            Benchmark.Dump();
            Console.WriteLine("Finished");
            sl.Dispose();
            slm.Dispose();
        }

        //[Test]
        //public void CouldGetBufferOfUnexpectedSize()
        //{
        //    var path = TestUtils.GetPath();
        //    var processConfig = new DataConfig(path);
        //    StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync;
        //    StartupConfig.StreamLogChunksIndexFlags = LMDBEnvironmentFlags.NoSync;
        //    var slm = new StreamLogManager(processConfig, "CouldAddNextChunk", null, 10 * 1024, true, true);

        //    var bufferPool = slm.BufferPool;

        //    var chunkIndex = slm.BlockIndex;

        //    var streamId = (StreamLogId)42;

        //    short valueSize = -1;

        //    var state = slm.StateTable.GetState(streamId);
        //    state.CheckInit(streamId, valueSize, SerializationFormat.Binary);

        //    var sl = new StreamLog("test_stream", state, valueSize);

        //    var count = 100;

        //    SharedMemoryOld currentBuffer = null;

        //    using (Benchmark.Run("AddNext (KOPS)", count * 1000))
        //    {
        //        var version = 1UL;
        //        var buffer = chunkIndex.GetOrCreateNextWritableChunkBuffer(sl, length: 1, version);
        //        var slc = new StreamBlock(buffer.RetainChunkMemory(false), streamId, valueSize, 1);

        //        var firstBufferRef = buffer.BufferRef;

        //        for (int i = 0; i < count; i++)
        //        {
        //            version = (ulong)i + 1;

        //            var size = i % 2 == 0 ? 8 : (4096 + i % 4096);
        //            chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //            if (!slc.Claim(version, size).IsValid)
        //            {
        //                chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //                Assert.IsTrue(slc.IsCompleted);
        //                slc.DisposeFree();
        //                chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //                buffer = chunkIndex.GetOrCreateNextWritableChunkBuffer(sl, length: size * 4, version);
        //                Trace.TraceWarning("Replacing existing chunk " + i);
        //                chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //                slc = new StreamBlock(buffer.RetainChunkMemory(false), streamId, valueSize, version);
        //                chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //                if (!slc.Claim(version, size).IsValid)
        //                {
        //                    Assert.Fail("!slc.Claim(version, 8).IsValid");
        //                }
        //            }
        //            chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //            slc.Commit();
        //            chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //            var bufferRef = buffer.BufferRef;

        //            chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //            chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);

        //            buffer = chunkIndex.GetOrCreateNextWritableChunkBuffer(sl, length: 1, version + 1);
        //            var bufferRef1 = buffer.BufferRef;
        //            // As if someone has created the next buffer before the second call, forget the first call

        //            SharedMemoryOld.Free(buffer);
        //            buffer = chunkIndex.GetOrCreateNextWritableChunkBuffer(sl, length: 1, version + 1);
        //            var bufferRef2 = buffer.BufferRef;

        //            if (buffer.ReferenceCount != 1)
        //            {
        //                Assert.Fail($"buffer.RefCount {buffer.ReferenceCount} != 1");
        //            }

        //            if (bufferRef1 != bufferRef2)
        //            {
        //                Assert.Fail($"bufferRef1 {bufferRef1} != bufferRef2 {bufferRef2}");
        //            }

        //            slc.Complete();

        //            var nextVersion = slc.NextVersion;
        //            slc.DisposeFree();

        //            chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);
        //            chunkIndex.PrepareNextWritableChunkBuffer(sl, length: 1);

        //            slc = new StreamBlock(buffer.RetainChunkMemory(false), streamId, valueSize, nextVersion);
        //        }

        //        if (!slc.Claim(version + 1, 8).IsValid)
        //        {
        //            Assert.Fail("!slc.Claim(version, 8).IsValid");
        //        }
        //        slc.Commit();
        //        slc.Complete();

        //        buffer = chunkIndex.GetOrCreateNextWritableChunkBuffer(sl, length: 1, slc.NextVersion);

        //        Assert.AreEqual(1, buffer.ReferenceCount);

        //        slc.DisposeFree();
        //        slc = new StreamBlock(buffer.RetainChunkMemory(false), streamId, valueSize, version + 2);

        //        Assert.IsFalse(slc.IsCompleted);

        //        // TODO test replace

        //        //// replace existing
        //        //var toReplaceVersion = slc.FirstVersion;
        //        //var toReplaceRef = buffer.BufferRef;

        //        //slc.Complete();
        //        //// SharedMemory.Free(buffer);
        //        //// slc.DisposeFree();

        //        //buffer = chunkIndex.GetOrCreateNextWritableChunkBuffer(sl, toReplaceVersion, toReplaceRef, 1);
        //        //slc = new StreamLogChunk(buffer.RetainChunkMemory(false), streamId, valueSize, version + 2);
        //        //Assert.AreNotEqual(toReplaceRef, buffer.BufferRef);

        //        // SharedMemory.Free(buffer);
        //        slc.DisposeFree();
        //    }

        //    //bufferPool.PrintBuffers();

        //    //bufferPool.PrintBuffersAfterPoolDispose = true;

        //    Benchmark.Dump();
        //    Console.WriteLine("Finished");
        //    sl.Dispose();
        //    slm.Dispose();
        //}
    }
}
