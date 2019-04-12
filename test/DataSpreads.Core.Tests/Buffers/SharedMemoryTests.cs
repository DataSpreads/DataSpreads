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
using ObjectLayoutInspector;
using Spreads.Buffers;
using Spreads.Threading;
using Spreads.Utils;
using System;

#pragma warning disable 618

namespace DataSpreads.Tests.Buffers
{
    [Category("CI")]
    [TestFixture]
    public class SharedMemoryTests
    {
        [Test, Explicit("not disposing")]
        public void ArrayMemorySize()
        {
            TypeLayout.PrintLayout<ArrayMemory<byte>>();
        }

        [Test, Explicit("not disposing")]
        public void SharedMemorySize()
        {
            TypeLayout.PrintLayout<SharedMemory>();
        }

        [Test]
        public void InFreeListHeaderEqualsToAtomicCounterCountMask()
        {
            Assert.AreEqual((int)SharedMemory.BufferHeader.HeaderFlags.IsDisposed, AtomicCounter.CountMask);
        }

        [Test]
        public void CouldRetain()
        {
            var path = TestUtils.GetPath();
            var pool = new SharedMemoryPool(path, 10 * 1024, (Wpid)1, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            var lens = new[]
            {
                15, 16, 17, 127, 128, 129, 1023, 1024, 1025, 4095, 4096, 4097, 8191, 8192, 8193, 31 * 1024,
                32 * 1024, 45 * 1024
            };

            foreach (var len in lens)
            {
                var memory = pool.RentMemory(len);

                Assert.IsTrue(memory.IsPoolable);
                Assert.IsFalse(memory.IsPooled);
                Assert.IsFalse(memory.IsRetained);
                Assert.IsFalse(memory.IsDisposed);
                Assert.AreEqual(pool, memory.Pool);
                Assert.AreEqual(memory.Vec.Length, memory.Length);

                Assert.AreEqual(pool.PoolIdx, memory._poolIdx);

                Assert.GreaterOrEqual(memory.Length, len, "memory.Length, len");
                var pow2Len = BitUtil.IsPowerOfTwo(memory.Length) ? memory.Length : (BitUtil.FindNextPositivePowerOfTwo(memory.Length) / 2);
                Assert.AreEqual(pow2Len, memory.LengthPow2, "BitUtil.FindNextPositivePowerOfTwo(len) / 2, memory.LengthPow2");

                var rm = memory.Retain(0, len);
                Assert.AreEqual(1, memory.ReferenceCount, "1, memory.ReferenceCount");
                Assert.IsTrue(memory.IsRetained);

                Assert.AreEqual(len, rm.Length, "len, rm.Length");

                var rm1 = memory.Retain(len / 2, len / 4);
                Assert.AreEqual(2, memory.ReferenceCount);
                Assert.AreEqual(len / 4, rm1.Length);

                rm.Dispose();
                Assert.AreEqual(1, memory.ReferenceCount);

                rm1.Dispose();
                Assert.IsTrue(memory.IsDisposed);
            }

            pool.Dispose();
        }

        [Test]
        public void CannotDisposeRetained()
        {
            var path = TestUtils.GetPath();
            var pool = new SharedMemoryPool(path, 10 * 1024, (Wpid)1, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            var memory = pool.RentMemory(32 * 1024);
            var rm = memory.Retain();
            Assert.Throws<InvalidOperationException>(() => { ((IDisposable)memory).Dispose(); });
            rm.Dispose();

            pool.Dispose();
        }

        [Test]
        public void CannotDoubleDispose()
        {
            var path = TestUtils.GetPath();
            var pool = new SharedMemoryPool(path, 10 * 1024, (Wpid)1, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            var memory = pool.RentMemory(32 * 1024);
            ((IDisposable)memory).Dispose();
            Assert.Throws<ObjectDisposedException>(() => { ((IDisposable)memory).Dispose(); });

            pool.Dispose();
        }

        [Test]
        public void CannotRetainDisposed()
        {
            var path = TestUtils.GetPath();
            var pool = new SharedMemoryPool(path, 10 * 1024, (Wpid)1, rmMaxBufferLength: SharedMemoryPool.RmMaxPoolBufferLength);

            var memory = pool.RentMemory(32 * 1024);
            ((IDisposable)memory).Dispose();

            Assert.Throws<ObjectDisposedException>(() => { var _ = memory.Retain(); });
            Assert.Throws<ObjectDisposedException>(() => { var _ = new RetainedMemory<byte>(memory, 0, memory.Length, false); });

            pool.Dispose();
        }
    }
}
