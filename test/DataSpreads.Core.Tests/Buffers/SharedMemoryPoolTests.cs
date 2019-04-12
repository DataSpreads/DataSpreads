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
using Spreads.Threading;
using Spreads.Utils;
using System;
using System.Threading;

#pragma warning disable 618

namespace DataSpreads.Tests.Buffers
{
    [Category("CI")]
    [TestFixture, SingleThreaded]
    public unsafe class SharedMemoryPoolTests
    {
        [Test, Explicit("output")]
        public void PrintPow2Sizes()
        {
            var path = TestUtils.GetPath();
            var count = 23;
            var pool = new SharedMemoryPool(path, 10 * 1024, (Wpid)1, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            using (Benchmark.Run("Rent/Release Buffers", count))
            {
                for (int i = 1; i <= count; i++)
                {
                    var len = 1 << i;
                    var buffer = pool.RentMemory(len);

                    Console.WriteLine($"{i} - {len} - {buffer.Length} - { Math.Round(100.0 - len * 100.0 / buffer.Length, 2)} - BR: [{buffer.BufferRef}] RefCount: [{buffer.ReferenceCount}]");

                    pool.Return(buffer, false);
                }
            }

            pool.Dispose();
        }

        [Test]
        public void CouldRentAndReturnBuffers()
        {
            var path = TestUtils.GetPath();
            var pool = new SharedMemoryPool(path, 126, (Wpid)123, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            for (int i = 1000; i < 1024 * 1024; i++)
            {
                var mem = pool.RentMemory(i);
                mem.InvariantCheck();

                Assert.IsTrue(mem.Length >= i);
                if (i > SharedMemoryPool.Pow2ModeSizeLimit)
                {
                    Assert.IsTrue(mem.LengthPow2 >= i);
                    Assert.IsTrue(mem.Length - mem.LengthPow2 == 4088);
                    // Assert.AreEqual(pool.DataSizeToBucketSize(mem.Length), mem.LengthPow2);
                }

                Assert.IsTrue(mem.Length > mem.LengthPow2);

                var lenX = (long)mem.PointerPow2 + mem.LengthPow2 - (long)mem.Pointer;
                Assert.AreEqual(mem.Length, lenX);

                if (i > 4096)
                {
                    Assert.IsTrue(BitUtil.IsAligned((long)mem.PointerPow2, 4096));
                    Assert.IsTrue(BitUtil.IsPowerOfTwo(mem.LengthPow2));
                }
                else
                {
                    Assert.IsTrue(BitUtil.IsAligned((long)mem.PointerPow2, 2048));
                }

                if (i % 1000 == 0)
                {
                    Console.WriteLine($"Requested: {i}, RM pool: {mem.LengthPow2}, data: {mem.Length}, pow2/avail: {Math.Round(mem.LengthPow2 * 100.0 / mem.Length, 1)}%");
                }

                pool.Return(mem, false);
            }
        }

        [Test, Explicit("long running")]
        public void CouldAcquireAndReleaseBuffers()
        {
            var path = TestUtils.GetPath();
#pragma warning disable 219
            var count = 1000;
#pragma warning restore 219
            var pool = new SharedMemoryPool(path, 126, (Wpid)123, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            for (int i = 1100; i < 1000000; i++)
            {
                var mem = pool.RentMemory(i);
                mem.InvariantCheck();
                //Assert.GreaterOrEqual(mem.Length, i);

                //Assert.IsTrue(mem.Length >= i);
                //Assert.IsTrue(mem.Length >= mem.Length);
                //Assert.AreEqual(pool.DataSizeToBucketSize(mem.Length), mem.Length);
                //Assert.IsTrue(pool.BucketSizeToDataSize(mem.Length) == mem.Length);

                //Assert.IsTrue(BitUtil.IsAligned((long)mem.Pointer, 2048));

                if (i % 1000 == 0)
                {
                    Console.WriteLine($"Requested data: {i}, pool: {mem.LengthPow2}, data: {mem.Length}, data/avail: {(i * 100.0 / mem.Length):0.00}%");
                }

                pool.Return(mem);
            }

            //using (Benchmark.Run("SharedMemoryBuffer acquire/release", count))
            //{
            //    for (int i = 0; i < count; i++)
            //    {
            //        var buffer = pool.Acquire(2000);

            //        var h = buffer.Pin();
            //        h.Dispose();

            //        var buffer2 = pool.Acquire(2000);
            //        buffer2.Pin();
            //        buffer2.Unpin();
            //    }
            //}

            // Benchmark.Dump();

            // pool.Dispose();
        }

        [Test]
        public void CouldRentAndReturnBuffers2()
        {
            var path = TestUtils.GetPath();
            var count = 10_000;

            var pool = new SharedMemoryPool(path, 126, (Wpid)123, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            using (Benchmark.Run("SharedMemoryBuffer acquire/release", count))
            {
                for (int i = 1; i < count; i++)
                {
                    // Rent returns a buffer with zero refcount
                    // Return expects zero refcount
                    // With positive RefCount we do not return,
                    // first we unpin according to app logic, e.g. archive a chunk
                    // and only then return

                    var buffer = (SharedMemory)pool.Rent(i);
                    var buffer2 = (SharedMemory)pool.Rent(i);

                    // var existing = pool.UnsafeGetBuffer(buffer.BufferRef);

                    // switching to refcount mode, decrement to zero causes return to pool of finalize
                    var h = buffer.Pin();
                    h.Dispose();

                    if (!buffer._isPooled)
                    {
                        Assert.Fail("!buffer._isPooled");
                    }

                    if (!pool.Return(buffer2, true))
                    {
                        Assert.Fail();
                    }
                }
            }

            Benchmark.Dump();

            pool.Dispose();
        }

        [Test] // TODO review
        public void CouldRentAndGetExisting()
        {
            var path = TestUtils.GetPath();
            var count = 100;

            var pool = new SharedMemoryPool(path, 126, (Wpid)123, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            using (Benchmark.Run("SharedMemoryBuffer acquire/release", count))
            {
                for (int i = 1; i < count; i++)
                {
                    var buffer = pool.RentMemory(i);

                    ThreadPool.UnsafeQueueUserWorkItem((o) =>
                    {
                        if (buffer.ReferenceCount != 0)
                        {
                            Assert.Fail("buffer.Counter.Count != 0");
                        }

                        // ReSharper disable once AccessToDisposedClosure
                        // ReSharper disable once PossibleNullReferenceException
                        var existing = pool.Buckets[((o as SharedMemory).BufferRef)];

                        if (buffer.HeaderPointer != existing.Data)
                        {
                            Assert.Fail("buffer.HeaderPointer != existing.Data");
                        }

                        //if (*(int*)existing.Data != 0)
                        //{
                        //    Assert.Fail($"*(int*)existing.Data {*(int*)existing.Data} != 0");
                        //}

                        buffer.DisposeFinalize();
                    }, buffer);

                    //Task.Run(() =>
                    //{
                    //    var existing = pool.GetExisting(buffer.BufferRef);
                    //    if (existing.BufferRef != buffer.BufferRef)
                    //    {
                    //        Assert.Fail();
                    //    }
                    //}).Wait();

                    //if (!pool.Release(buffer))
                    //{
                    //    Assert.Fail();
                    //}
                }
            }

            Benchmark.Dump();
            Console.WriteLine("Disposing pool");
            pool.Dispose();
        }
    }
}
