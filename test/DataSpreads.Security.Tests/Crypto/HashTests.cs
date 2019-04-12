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

using DataSpreads.Security.Crypto;
using NUnit.Framework;
using Spreads.Buffers;
using Spreads.Utils;
using System;
using System.Runtime.CompilerServices;

namespace DataSpreads.Tests.Crypto
{
    [TestFixture]
    public class HashTests
    {
        [Test]
        public void CouldRoundtripToString()
        {
            var h1 = Hash16.Random();
            var s1 = h1.ToString();
            Console.WriteLine(s1);
            var h2 = new Hash16(s1);
            var s2 = h2.ToString();
            Assert.AreEqual(s1, s2, "strings equal");
            Assert.AreEqual(h1, h2, "hashes equal");
            Assert.AreEqual(h1.ToString(), s2);
        }

        [Test]
        public unsafe void CouldChainHashes()
        {
            Console.WriteLine("Default: " + default(Hash16).ToString());

            var h1 = Hash16.Random();
            var h2 = Hash16.Random();

            var rm = BufferPool.Retain(Hash16.Size * 2, true);
            var ptr = (byte*)rm.Pointer;
            *(Hash16*)ptr = h1;
            *(Hash16*)(ptr + Hash16.Size) = h2;
            var db = new DirectBuffer(Hash16.Size * 2, ptr);

            var chainStr = Hash16.Chain(h1, h2.AsSpan()).ToString();
            Console.WriteLine(chainStr);

            var chainStr2 = Hash16.Chain2(h1, h2.AsSpan()).ToString();
            Console.WriteLine(chainStr2);

            Assert.AreEqual(chainStr, chainStr2);

            Span<byte> output = stackalloc byte[Hash16.Size];

            //SauceControl.Blake2Fast.Blake2b.ComputeAndWriteHash(Hash32.Size,
            //    h1.AsSpan(), h2.AsSpan(), output);

            //var reference = Unsafe.As<byte, Hash32>(ref output[0]);
            //Assert.AreEqual(chained, reference);

            //var incrHash = SauceControl.Blake2Fast.Blake2b.CreateIncrementalHasher(Hash32.Size);

            //incrHash.Update(h1.AsSpan());
            //incrHash.Update(default(Hash32).AsSpan());
            //incrHash.Update(default(Hash32).AsSpan());
            //incrHash.Update(default(Hash32).AsSpan());
            //incrHash.Update(h2.AsSpan());
            //incrHash.TryFinish(output, out _);
            //var incremental = Unsafe.As<byte, Hash32>(ref output[0]);
            //Console.WriteLine(incremental);

            Spreads.Algorithms.Hash.Blake2b.ComputeAndWriteHash(Hash16.Size,
                db, output);

            var manual = Unsafe.As<byte, Hash16>(ref output[0]);
            // Assert.AreEqual(chained, manual);
        }

        [Test, Explicit("bench")]
        public unsafe void CouldChainHashesBench()
        {
            var h = Hash16.Random();

            var count = 1_000_000;
            var rounds = 10;

            var h1 = Hash16.Chain(h, h.AsSpan());
            var h2 = Hash16.Chain2(h, h.AsSpan());

            for (int _ = 0; _ < rounds; _++)
            {
                h1 = CouldChainHashesBench_ChainSimd(count, h);
                h2 = CouldChainHashesBench_ChainLibsodium(count, h);

                Assert.AreEqual(h1.ToString(), h2.ToString());
                Console.WriteLine(h1.ToString());
                h = h1;
            }

            Benchmark.Dump();
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.NoInlining)]
        private static Hash16 CouldChainHashesBench_ChainSimd(int count, Hash16 h1)
        {
            Hash16 h = default;
            
            using (Benchmark.Run("Chain SIMD", count * 32))
            {
                for (int i = 0; i < count; i++)
                {
                    h = Hash16.Chain(in h1, h1.AsSpan());
                }
            }

            return h;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.NoInlining)]
        private static Hash16 CouldChainHashesBench_ChainLibsodium(int count, Hash16 h1)
        {
            Hash16 h = default;
            using (Benchmark.Run("Chain Libsodium", count * 32))
            {
                for (int i = 0; i < count; i++)
                {
                    h = Hash16.Chain2(in h1, h1.AsSpan());
                }
            }

            return h;
        }
    }
}
