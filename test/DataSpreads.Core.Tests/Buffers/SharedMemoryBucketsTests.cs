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
using NUnit.Framework;
using Spreads.Utils;
using System;
using System.Runtime.CompilerServices;
using Spreads;

#pragma warning disable 618

namespace DataSpreads.Tests.Buffers
{
    [Category("CI")]
    [TestFixture]
    public class SharedMemoryBucketsTests
    {
        [Test]
        public void CouldOpenBucketsAndGetBuffers()
        {
            var path = TestUtils.GetPath();

            var buckets = new SharedMemoryBuckets(path);

            var available = buckets.GetAvailableFreeSpace();
            Console.WriteLine("AVAILABLE: " + available);

            for (int i = 0; i <= BufferRef.MaxBucketIdx; i++)
            {
                var br = BufferRef.Create(i, 1);
                var buffer = buckets[br];
                var bufferSize = SharedMemoryPool.BucketIndexToBufferSize(i, 4096);

                buffer.Span.Fill(0);

                Assert.AreEqual(bufferSize, buffer.Length);
            }

            var sndSmall = BufferRef.Create(0, 40000);
            var buffer1 = buckets[sndSmall];
            buffer1.Span.Fill(0);
            Assert.AreEqual(4096, buffer1.Length);

            buckets.Dispose();
        }

        [Test]
        public void CouldUseSmallPages()
        {
            var path = TestUtils.GetPath();

            var buckets = new SharedMemoryBuckets(path, pageSize: 32, 0);

            var tooBigBucket = BufferRef.Create(1, 1);

            Assert.Throws<ArgumentException>(() =>
            {
                var _ = buckets[tooBigBucket];
            });

            var br = BufferRef.Create(0, 40000);
            var buffer1 = buckets[br];
            buffer1.Span.Fill(0);
            Assert.AreEqual(32, buffer1.Length);

            buckets.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        [Test, Explicit("bench")]
        public unsafe void BufferAccessBench()
        {
            Settings.DoAdditionalCorrectnessChecks = false;

            var path = TestUtils.GetPath();

            var buckets = new SharedMemoryBuckets(path);

            var bufferCount = 100_000; // c.2800 = 12 MB (L3 cache on i7-8700)

            using (Benchmark.Run("Init (K)", bufferCount * 1000))
            {
                for (int i = 0; i < bufferCount; i++)
                {
                    var bi = i + 1;
                    var br = BufferRef.Create(0, bi);
                    var buffer = buckets[br];
                    buffer.WriteInt32(0, bi);
                }
            }

            var count = 10_000_000;

            for (int r = 0; r < 10; r++)
            {
                using (Benchmark.Run("Access Unsafe", count))
                {
                    for (int i = 0; i < count; i++)
                    {
                        var bi = 1 + (i % bufferCount);
                        var br = BufferRef.Create(0, bi);
                        var buffer = buckets.DangerousGet(br);
                        if (buffer.Length != 4096)
                        {
                            Assert.Fail();
                        }
                        if (bi != *(int*)buffer.Data)
                        {
                            // Assert.Fail($"bi [{bi}] != buffer.ReadInt32(0) [{buffer.ReadInt32(0)}]");
                        }
                    }
                }

                using (Benchmark.Run("Access Safe", count))
                {
                    for (int i = 0; i < count; i++)
                    {
                        var bi = 1 + (i % bufferCount);
                        var br = BufferRef.Create(0, bi);
                        var buffer = buckets[br];
                        if (buffer.Length != 4096)
                        {
                            Assert.Fail();
                        }
                        if (bi != *(int*)buffer.Data)
                        {
                            // Assert.Fail($"bi [{bi}] != buffer.ReadInt32(0) [{buffer.ReadInt32(0)}]");
                        }
                    }
                }
            }
            Benchmark.Dump();
            buckets.Dispose();
        }
    }
}
