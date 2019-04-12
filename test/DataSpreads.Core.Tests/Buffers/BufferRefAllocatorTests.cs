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
using System;
using System.Collections.Generic;
using Spreads.LMDB;

#pragma warning disable 618

namespace DataSpreads.Tests.Buffers
{
    [Category("CI")]
    [TestFixture]
    public class BufferRefAllocatorTests
    {
        [Test]
        public void CouldAllocateFreePrint()
        {
            var path = TestUtils.GetPath();
            var wpid = (Wpid)1;

            var pageSize = 1024;

            var bra = new BufferRefAllocator(path, LMDBEnvironmentFlags.NoSync, wpid, pageSize, maxTotalSize: 1024 * 1024);

            var br0 = bra.Allocate(0, out var fromFreeList);
            Assert.IsFalse(fromFreeList);
            Assert.IsFalse(bra.IsInFreeList(br0));

            //Assert.AreEqual(wpid.InstanceId, bra.GetAllocatorInstanceId(br0));
            //Assert.AreEqual(-1, bra.GetAllocatorInstanceId(BufferRef.Create(0, 10)));

            var totalAllocated = bra.GetTotalAllocated();
            Assert.AreEqual(pageSize, totalAllocated);

            totalAllocated = bra.GetTotalAllocated(0);
            Assert.AreEqual(pageSize, totalAllocated);

            totalAllocated = bra.GetTotalAllocated(1);
            Assert.AreEqual(0, totalAllocated);

            var br1 = bra.Allocate(1, out fromFreeList);
            Assert.IsFalse(fromFreeList);
            var br2 = bra.Allocate(2, out fromFreeList);
            Assert.IsFalse(fromFreeList);

            var br0_1 = bra.Allocate(0, out fromFreeList);
            Assert.IsFalse(fromFreeList);
            var br0_2 = bra.Allocate(0, out fromFreeList);
            Assert.IsFalse(fromFreeList);
            Assert.AreEqual(0, br0_2.BucketIndex);
            Assert.AreEqual(3, br0_2.BufferIndex);
            Assert.IsFalse(br0_2.Flag);

            bra.PrintAllocated(printBuffers: true, additionalInfo: (x) => $"alloc info {x.ToString()}");
            bra.PrintAllocated(0, printBuffers: true, additionalInfo: (x) => $"alloc info {x.ToString()}");
            bra.PrintAllocated(1, printBuffers: true, additionalInfo: (x) => $"alloc info {x.ToString()}");

            bra.Free(br0_1);
            Assert.IsTrue(bra.IsInFreeList(br0_1));

            bra.PrintAllocated(0, printBuffers: true, additionalInfo: (x) => $"alloc info {x.ToString()}");

            bra.Free(br0_2);

            for (int i = 0; i < 10; i++)
            {
                var br0_x = bra.Allocate(0, out fromFreeList);
                bra.Free(br0_x);
            }

            var list = new List<BufferRef>();
            for (int _ = 0; _ < 5; _++)
            {
                for (int i = 0; i < 10; i++)
                {
                    list.Add(bra.Allocate(0, out fromFreeList));
                }

                foreach (var bufferRef in list)
                {
                    bra.Free(bufferRef);
                }
                list.Clear();
            }

            bra.Free(br0);
            Assert.Throws<KeyNotFoundException>(() => { bra.Free(br0); });
            br0 = bra.Allocate(0, out fromFreeList);
            Assert.IsTrue(fromFreeList);
            bra.Free(br0);

            bra.Free(br1);

            // Assert.Throws<InvalidOperationException>(() => { bra.Free(br2, (Wpid)2); });
            bra.Free(br2);

            totalAllocated = bra.GetTotalAllocated(0);
            Assert.AreEqual(0, totalAllocated);

            bra.PrintFree(printBuffers: true, additionalInfo: (x) => $"free info {x.ToString()}");
            bra.PrintFree(1, printBuffers: true, additionalInfo: (x) => $"free info {x.ToString()}");
            bra.PrintFree(10, printBuffers: true, additionalInfo: (x) => $"free info {x.ToString()}");

            Assert.Throws<NotEnoughSpaceException>(() => { bra.Allocate(15, out _); });
            Assert.AreEqual(0, bra.GetTotalAllocated(0));

            bra.PrintAllocated(0, printBuffers: true, additionalInfo: (x) => $"alloc info {x.ToString()}");

            bra.Dispose();
        }
    }
}
