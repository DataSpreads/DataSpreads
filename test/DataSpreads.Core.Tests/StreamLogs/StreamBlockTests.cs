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
using Spreads.Algorithms.Hash;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.LMDB;
using Spreads.Serialization;
using Spreads.Threading;
using Spreads.Utils;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DataSpreads.Security.Crypto;

#pragma warning disable 618

namespace DataSpreads.Tests.StreamLogs
{
    [Category("CI")]
    [TestFixture, SingleThreaded]
    public class StreamBlockTests
    {
        [Test]
        public void SizeOfStreamBlock()
        {
            var size = Unsafe.SizeOf<StreamBlock>();
            // perf drops a lot when > cache line
            Console.WriteLine(size);
            Assert.IsTrue(size <= 16);
        }

        [Test]
        public unsafe void StreamBlockHeaderLayout()
        {
            var ty = typeof(StreamBlock.StreamBlockHeader);
            var rm = BufferPool.Retain(4096, true);


            rm.Span.Clear();

            StreamBlock.TryInitialize(new DirectBuffer(rm), (StreamLogId)1L, 1, 1);

            var sb = new StreamBlock(rm, (StreamLogId)1L);

            Assert.AreEqual(0, StreamBlock.StreamBlockHeader.InitTimestampOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.InitTimestampOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.InitTimestamp)));
            Assert.AreEqual(typeof(Timestamp), default(StreamBlock.StreamBlockHeader).InitTimestamp.GetType());

            Assert.AreEqual(64, StreamBlock.StreamBlockHeader.VersionAndFlagsOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.VersionAndFlagsOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.VersionAndFlags)));
            Assert.AreEqual(typeof(VersionAndFlags), default(StreamBlock.StreamBlockHeader).VersionAndFlags.GetType());

            Assert.AreEqual(65, StreamBlock.StreamBlockHeader.AdditionalFlagsOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.AdditionalFlagsOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.AdditionalFlags)));
            Assert.AreEqual(typeof(byte), default(StreamBlock.StreamBlockHeader).AdditionalFlags.GetType());

            Assert.AreEqual(66, StreamBlock.StreamBlockHeader.ItemFixedSizeOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.ItemFixedSizeOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.ItemFixedSize)));
            Assert.AreEqual(typeof(short), default(StreamBlock.StreamBlockHeader).ItemFixedSize.GetType());

            Assert.AreEqual(68, StreamBlock.StreamBlockHeader.PayloadLengthOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.PayloadLengthOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.PayloadLength)));
            Assert.AreEqual(typeof(int), default(StreamBlock.StreamBlockHeader).PayloadLength.GetType());

            Assert.AreEqual(72, StreamBlock.StreamBlockHeader.StreamLogIdOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.StreamLogIdOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.StreamLogId)));
            Assert.AreEqual(typeof(StreamLogId), default(StreamBlock.StreamBlockHeader).StreamLogId.GetType());
            Assert.AreEqual(typeof(StreamLogId), sb.StreamLogId.GetType());
            Assert.AreEqual(typeof(long), sb.StreamLogIdLong.GetType());


            Assert.AreEqual(80, StreamBlock.StreamBlockHeader.FirstVersionOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.FirstVersionOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.FirstVersion)));
            Assert.AreEqual(typeof(ulong), default(StreamBlock.StreamBlockHeader).FirstVersion.GetType());
            Assert.AreEqual(typeof(ulong), sb.FirstVersion.GetType());
            Assert.AreEqual(1, sb.FirstVersion);

            Assert.AreEqual(88, StreamBlock.StreamBlockHeader.CountOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.CountOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.Count)));
            Assert.AreEqual(typeof(int), default(StreamBlock.StreamBlockHeader).Count.GetType());
            Assert.AreEqual(typeof(int), sb.CountVolatile.GetType());


            Assert.AreEqual(92, StreamBlock.StreamBlockHeader.ChecksumOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.ChecksumOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.Checksum)));
            Assert.AreEqual(typeof(uint), default(StreamBlock.StreamBlockHeader).Checksum.GetType());
            Assert.AreEqual(typeof(uint), sb.Checksum.GetType());


            Assert.AreEqual(96, StreamBlock.StreamBlockHeader.WriteEndTimestampOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.WriteEndTimestampOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.WriteEnd)));
            Assert.AreEqual(typeof(Timestamp), default(StreamBlock.StreamBlockHeader).WriteEnd.GetType());
            Assert.AreEqual(typeof(Timestamp), sb.WriteEnd.GetType());

            Assert.AreEqual(104, StreamBlock.StreamBlockHeader.HashOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.HashOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.Hash)));
            Assert.AreEqual(typeof(Hash16), default(StreamBlock.StreamBlockHeader).Hash.GetType());
            Assert.AreEqual(typeof(Hash16), sb.Hash.GetType());

            Assert.AreEqual(120, StreamBlock.StreamBlockHeader.PkContextIdOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.PkContextIdOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.PkContextId)));
            Assert.AreEqual(typeof(uint), default(StreamBlock.StreamBlockHeader).PkContextId.GetType());
            Assert.AreEqual(typeof(uint), sb.PkContextId.GetType());

            Assert.AreEqual(124, StreamBlock.StreamBlockHeader.ItemDthOffset);
            Assert.AreEqual(StreamBlock.StreamBlockHeader.ItemDthOffset,
                (int)Marshal.OffsetOf(ty, nameof(StreamBlock.StreamBlockHeader.ItemDth)));
            Assert.AreEqual(typeof(DataTypeHeader), default(StreamBlock.StreamBlockHeader).ItemDth.GetType());
            Assert.AreEqual(typeof(DataTypeHeader), sb.ItemDth.GetType());


            rm.Dispose();
        }

        [Test]
        public void CouldCreateUninitializedBlock()
        {
            var rm = BufferPool.Retain(1024, true);
            Assert.Throws<InvalidOperationException>(() =>
            {
                var _ = new StreamBlock(rm, default);
            });

            var sb = new StreamBlock(new DirectBuffer(rm));

            Assert.IsTrue(sb.IsValid);
            Assert.IsFalse(sb.IsRented);
            Assert.IsFalse(sb.IsInitialized);

            rm.Dispose();
        }

        [Test]
        public unsafe void CountAndChecksum()
        {
            var path = TestUtils.GetPath();

            var rounds = 10;
            var count = 100_000;

            var bufferSize = count * 16 + StreamBlock.DataOffset;

            var pool = new BlockMemoryPool(path, 64, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, (Wpid)123,
                maxBufferLength: BitUtil.FindNextPositivePowerOfTwo(bufferSize));

            // TODO step on what happens when buffer is too large
            var buffer = pool.RentMemory(bufferSize);

            var rm = buffer.RetainBlockMemory(false);

            var rm2 = rm.Clone();

            var seed = 2u;

            StreamBlock.TryInitialize(new DirectBuffer(rm2), (StreamLogId)1, default, 16, 1, seed, default);
#pragma warning restore 618
            var chunk = new StreamBlock(rm2, (StreamLogId)1, 16, 1);

            chunk.Claim(0, 16);
            chunk.Commit();

            Assert.AreEqual(1, chunk.CountVolatile);

            var ptr = (byte*)buffer.Pointer + 1000;
            var updatedChecksum = (ulong)Crc32C.CalculateCrc32C(ptr, 16, seed);
            Console.WriteLine(updatedChecksum);
            Assert.AreEqual(updatedChecksum, chunk.Checksum);

            Assert.IsTrue(chunk.ValidateChecksum());

            chunk.Dispose();

            // pool.Release(buffer, true);
            rm.Dispose();
            pool.Dispose();
        }

        [Test]
        public void CouldWriteAndReadToChunk()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618
            var path = TestUtils.GetPath();

            var rounds = 10;
            var count = 100_000;

            var bufferSize = count * 16 + StreamBlock.DataOffset;

            var pool = new BlockMemoryPool(path, 64, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, (Wpid)123,
                maxBufferLength: BitUtil.FindNextPositivePowerOfTwo(bufferSize));

            // TODO step on what happens when buffer is too large
            var buffer = pool.RentMemory(bufferSize);

            var rm = buffer.RetainBlockMemory(false);

            using (Benchmark.Run("CouldWriteAndReadToBlock", count * rounds))
            {
                for (int r = 0; r < rounds; r++)
                {
                    var rm2 = rm.Clone();
                    StreamBlock.TryInitialize(new DirectBuffer(rm2), (StreamLogId)1, 16, 1);
                    var chunk = new StreamBlock(rm2, (StreamLogId)1, 16, 1);

                    for (int i = 1; i <= count; i++)
                    {
                        var claim = chunk.Claim((ulong)i, 16);

                        if (claim.Length != 16)
                        {
                            Assert.Fail();
                        }
                        claim.Write<long>(0, 100000 + i);

                        chunk.Commit();

                        var readDb = chunk.DangerousGet(i - 1);

                        if (readDb.Length != 16)
                        {
                            Assert.Fail();
                        }

                        var written = readDb.Read<long>(0);

                        if (written != 100000 + i)
                        {
                            Assert.Fail();
                        }
                    }

                    buffer.GetSpan().Clear();
                    chunk.Dispose();
                }
            }

            Benchmark.Dump();

            // pool.Release(buffer, true);
            rm.Dispose();
            pool.Dispose();
        }

        [Test]
        public void CouldWriteAndReadVarSizeToChunk()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = true;
#pragma warning restore 618
            var path = TestUtils.GetPath();

            var rounds = 10;
            var count = 1_00_000;

            var bufferSize = count * 100 + StreamBlock.DataOffset;

            var pool = new BlockMemoryPool(path, 64, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, (Wpid)123,
                BitUtil.FindNextPositivePowerOfTwo(bufferSize));

            var buffer = pool.RentMemory(bufferSize);

            var rm = buffer.Retain();

            var values = new byte[1000];
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = (byte)(i % 255);
            }

            using (Benchmark.Run("CouldWriteAndReadVarSizeToTerm", count * rounds))
            {
                for (int r = 0; r < rounds; r++)
                {
                    var crm = buffer.RetainBlockMemory(false);
                    StreamBlock.TryInitialize(new DirectBuffer(crm), (StreamLogId)1, -1, 1);
                    var chunk = new StreamBlock(crm, (StreamLogId)1, -1, 1);

                    for (int i = 1; i <= count; i++)
                    {
                        var itemLength = i % 100 + 1;

                        var claim = chunk.Claim((ulong)i, itemLength);

                        if (claim.Length != itemLength)
                        {
                            Assert.Fail();
                        }

                        ((Span<byte>)values).Slice(0, itemLength).CopyTo(claim.Span);

                        chunk.Commit();

                        var readDb = chunk.DangerousGet(i - 1);

                        if (readDb.Length != itemLength)
                        {
                            Assert.Fail();
                        }

                        if (!readDb.Span.SequenceEqual(values.AsSpan(0, itemLength)))
                        {
                            Assert.Fail();
                        }
                    }

                    buffer.GetSpan().Clear();
                    chunk.Dispose();
                }
            }

            Benchmark.Dump();
            rm.Dispose();
            pool.Dispose();
        }

        [Test, Explicit("Benchmark")]
        // [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public void CouldWriteAndReadToChunkBenchmark()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618

            var rounds = 100;
            var count = 8_000_000;

            var path = TestUtils.GetPath();

            var bufferSize = count * 8 + StreamBlock.DataOffset;

            var pool = new BlockMemoryPool(path, 4 * 256, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, (Wpid)1,
                BitUtil.FindNextPositivePowerOfTwo(bufferSize));

            var buffer = pool.RentMemory(bufferSize);

            var rm = buffer.Retain();

            for (int r = 0; r < rounds; r++)
            {
                var crm = buffer.RetainBlockMemory(false);
                StreamBlock.TryInitialize(new DirectBuffer(crm), (StreamLogId)10, 8, 1);
                using (Benchmark.Run("CouldWriteAndReadToTerm", count))
                {
                    using (var logChunk = new StreamBlock(crm, (StreamLogId)10, 8, 1))
                    {
                        for (int i = 0; i < count; i++)
                        {
                            var claim = logChunk.Claim(logChunk.NextVersion, 8);

                            //if (claim.Length != 8)
                            //{
                            //    Assert.Fail();
                            //}
                            claim.Write<long>(0, 100000 + i);

                            logChunk.Commit();

                            //logChunk.GetUnchecked(i).Read<long>(0, out var written);

                            //if (written != 100000 + i)
                            //{
                            //    ThrowHelper.ThrowInvalidOperationException(String.Empty);
                            //    // throw new Exception();
                            //}
                        }
                    }
                }
                buffer.GetSpan().Clear();
            }

            Benchmark.Dump();

            rm.Dispose();
            pool.Dispose();
        }

        [Test, Explicit("Benchmark")]
        public void CouldWriteAndReadVarSizeToChunkBenchmark()
        {
#pragma warning disable 618
            Settings.DoAdditionalCorrectnessChecks = false;
#pragma warning restore 618

            var path = TestUtils.GetPath();

            var rounds = 100;
            var count = 500_000;

            var bufferSize = count * 108 + StreamBlock.DataOffset;

            var pool = new BlockMemoryPool(path, 2 * 256, LMDBEnvironmentFlags.WriteMap | LMDBEnvironmentFlags.NoSync, (Wpid)1,
                BitUtil.FindNextPositivePowerOfTwo(bufferSize));

            var buffer = pool.RentMemory(bufferSize);
            var rm = buffer.Retain();

            var values = new byte[1000];
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = (byte)(i % 255);
            }

            for (int r = 0; r < rounds; r++)
            {
                var crm = buffer.RetainBlockMemory(false);
                using (Benchmark.Run("CouldWriteAndReadVarSizeToTerm", count))
                {
                    StreamBlock.TryInitialize(new DirectBuffer(crm), (StreamLogId)1, -1, 1);
                    using (var logChunk = new StreamBlock(crm, (StreamLogId)1, -1, 1))
                    {
                        for (int i = 1; i <= count; i++)
                        {
                            var itemLength = i % 100 + 1;

                            var claim = logChunk.Claim(logChunk.NextVersion, itemLength);

                            // Assert.AreEqual(claim.Buffer.Length, itemLength);
                            ((Span<byte>)values).Slice(0, itemLength).CopyTo(claim.Span);

                            logChunk.Commit();

                            var readDb = logChunk.DangerousGet(i - 1);

                            if (readDb.Length != itemLength)
                            {
                                Assert.Fail();
                            }

                            if (!readDb.Span.SequenceEqual(values.AsSpan(0, itemLength)))
                            {
                                Assert.Fail();
                            }
                        }
                    }
                }
                crm.Span.Clear();
            }

            Benchmark.Dump();

            rm.Dispose();
            pool.Dispose();
        }
    }
}
