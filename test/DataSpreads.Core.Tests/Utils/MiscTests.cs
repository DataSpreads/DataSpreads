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

using NUnit.Framework;
using Spreads.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using DataSpreads.Buffers;
using Spreads.Collections.Generic;
using ThrowHelper = Spreads.ThrowHelper;

namespace DataSpreads.Tests.Utlis
{
    [TestFixture]
    public class MiscTests
    {
        [Test]
        public void TaskDelayAllocations()
        {
            var count = 100;
            List<Task> _tasks = new List<Task>();
            for (int i = 0; i < count; i++)
            {
                _tasks.Add(Task.Run(async () =>
                {
                    for (int j = 0; j < 1000; j++)
                    {
                        await Task.Delay(1);
                    }
                }));
            }
            using (Benchmark.Run("Task.Delay", 100 * 1000))
            {
                Task.WhenAll(_tasks).Wait();
            }
            Benchmark.Dump();
            Console.WriteLine(_tasks.Count);
        }

        [Test]
        public void OpenHugeDirectFile()
        {
            var path = TestUtils.GetPath();
            var filePath = Path.Combine(path, "test.tmp");

            var df = new DirectFile(filePath, 50 * 1024 * 1024 * 1024L, true);
            var span = df.Buffer.Span;
            span[1] = 1;

            df.Dispose();
        }

        [Test]
        public void ImmutableDictionaryEnumeration()
        {
            var count = 1_000_000;
            var rounds = 10;

            for (int r = 0; r < rounds; r++)
            {
                var imm = System.Collections.Immutable.ImmutableDictionary<long, long>.Empty;
                var dic = new Dictionary<long, long>();

                using (Benchmark.Run("Imm Add", count, true))
                {
                    for (int i = 0; i < count; i++)
                    {
                        imm = imm.Add(i, i);
                    }
                }

                using (Benchmark.Run("Dic Add", count, true))
                {
                    for (int i = 0; i < count; i++)
                    {
                        dic.Add(i, i);
                    }
                }

                using (Benchmark.Run("Imm Enum", count, true))
                {
                    var sum = 0L;
                    foreach (var kvp in imm)
                    {
                        sum += kvp.Value;
                    }

                    if (sum < 1)
                    {
                        ThrowHelper.FailFast("");
                    }
                }

                using (Benchmark.Run("Dic Enum", count, true))
                {
                    var sum = 0L;
                    foreach (var kvp in dic)
                    {
                        sum += kvp.Value;
                    }

                    if (sum < 1)
                    {
                        ThrowHelper.FailFast("");
                    }
                }
            }
            Benchmark.Dump();
        }

        [Test]
        public void WeakReferenceLookup()
        {
            // How bad is WR lookup

            var d = new FastDictionary<long, object>();
            var lockedWeakDictionary = new LockedWeakDictionary<long>();
            var cd = new ConcurrentDictionary<long, object>();
            var wcd = new ConcurrentDictionary<long, WeakReference<object>>();
            //var wcd2 = new ConcurrentDictionary<long, WeakReference>();
            var wcd3 = new ConcurrentDictionary<long, GCHandle>();

            var locker = 0L;

            var count = 1_000;

            for (int i = 0; i < count; i++)
            {
                var obj = (object)i;
                d.Add(i, obj);
                cd.TryAdd(i, obj);
                wcd.TryAdd(i, new WeakReference<object>(obj));
                //wcd2.TryAdd(i, new WeakReference(obj));

                var h = GCHandle.Alloc(obj, GCHandleType.Weak);
                wcd3.TryAdd(i, h);
                lockedWeakDictionary.TryAdd(i, obj);
            }

            var mult = 100_000;

            var sum1 = 0.0;
            using (Benchmark.Run("CD", count * mult))
            {
                for (int i = 0; i < count * mult; i++)
                {
                    if (cd.TryGetValue(i / mult, out var obj))
                    {
                        sum1 += (int)obj;
                    }
                    else
                    {
                        Assert.Fail();
                    }
                }
            }

            var sum2 = 0.0;
            using (Benchmark.Run("WCD", count * mult))
            {
                // Not so bad, this just needs to save something more in other places

                for (int i = 0; i < count * mult; i++)
                {
                    wcd.TryGetValue(i / mult, out var wr);
                    if (wr.TryGetTarget(out var tgt))
                    {
                        sum2 += (int)tgt;
                    }
                    else
                    {
                        Assert.Fail();
                    }
                }
            }
            Assert.AreEqual(sum1, sum2);

            //var sum4 = 0.0;
            //using (Benchmark.Run("WCD2", count * mult))
            //{
            //    // Not so bad, this just needs to save something more in other places

            //    for (int i = 0; i < count * mult; i++)
            //    {
            //        wcd2.TryGetValue(i / mult, out var wr2);
            //        if (wr2.Target != null)
            //        {
            //            sum4 += (int)wr2.Target;
            //        }
            //        else
            //        {
            //            Assert.Fail();
            //        }
            //    }
            //}
            //Assert.AreEqual(sum1, sum4);

            var sum5 = 0.0;
            using (Benchmark.Run("WCD H", count * mult))
            {
                // Not so bad, this just needs to save something more in other places

                for (int i = 0; i < count * mult; i++)
                {
                    wcd3.TryGetValue(i / mult, out var wr2);

                    if (wr2.Target is int val)
                    {
                        sum5 += val;
                    }
                    //else
                    //{
                    //    Assert.Fail();
                    //}
                }
            }
            Assert.AreEqual(sum1, sum5);

            //var sum3 = 0.0;
            //using (Benchmark.Run("LD", count * mult))
            //{
            //    for (int i = 0; i < count * mult; i++)
            //    {
            //        lock (d)
            //        {
            //            if (d.TryGetValue(i / mult, out var obj))
            //            {
            //                sum3 += (int)obj;
            //            }
            //            else
            //            {
            //                Assert.Fail();
            //            }
            //        }
            //    }
            //}

            var sum6 = 0.0;
            using (Benchmark.Run("LWD", count * mult))
            {
                for (int i = 0; i < count * mult; i++)
                {
                    if (lockedWeakDictionary.TryGetValue(i / mult, out var val))
                    {
                        sum6 += (int)val;
                    }
                }
            }

            Benchmark.Dump();

            Console.WriteLine(d.Count);
        }
    }

    public sealed class LockedWeakDictionary<TKey>
    {
#pragma warning disable CS0618 // Type or member is obsolete
        private readonly FastDictionary<TKey, GCHandle> _inner = new FastDictionary<TKey, GCHandle>();
#pragma warning restore CS0618 // Type or member is obsolete
        private long _locker;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAdd(TKey key, object obj)
        {
            // prefer reader

            //var incr = Interlocked.Increment(ref _locker);
            //if (incr != 1L)
            {
                var sw = new SpinWait();
                while (true)
                {
                    var existing = Interlocked.CompareExchange(ref _locker, 1, 0);
                    if (existing == 0)
                    {
                        break;
                    }
                    sw.SpinOnce();
                    //if (sw.NextSpinWillYield)
                    //{
                    //    sw.Reset();
                    //}
                }
            }

            var h = GCHandle.Alloc(obj, GCHandleType.Weak);
            var added = _inner.TryAdd(key, h);
            if (!added)
            {
                h.Free();
            }

            Volatile.Write(ref _locker, 0L);
            return added;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(TKey key, out object value)
        {
            var incr = Interlocked.Increment(ref _locker);
            if (incr != 1L)
            {
                NewMethod();
            }

            var found = _inner.TryGetValue(key, out var h);

            if (found) // TODO what is GC between the lines?
            {
                if (h.IsAllocated)
                {
                    value = h.Target;
                }
                else
                {
                    _inner.Remove(key);
                    h.Free();
                    value = null;
                    found = false;
                }
            }
            else
            {
                value = null;
            }

            Volatile.Write(ref _locker, 0L);

            return found;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void NewMethod()
        {
            var sw = new SpinWait();
            while (true)
            {
                var existing = Interlocked.CompareExchange(ref _locker, 1, 0);
                if (existing == 0)
                {
                    break;
                }

                sw.SpinOnce();
                if (sw.NextSpinWillYield)
                {
                    sw.Reset();
                }
            }
        }
    }
}
