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

using Spreads.Buffers;

// ReSharper disable once RedundantUsingDirective
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DataSpreads.Buffers
{
    internal static class BufferWriterExtensions
    {
        /// <summary>
        /// Consumes the <paramref name="stream"/> to the end into a <see cref="BufferWriter"/> and disposes the stream.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public static ValueTask<BufferWriter> Consume(this Stream stream, CancellationToken ct = default) // TODO dispose stream?
        {
            const int minReadCapacity = 1024;

            var knownSize = -1;
            if (stream.CanSeek)
            {
                knownSize = checked((int)stream.Length);
            }

            var bw = BufferWriter.Create(knownSize);

            // ReSharper disable RedundantAssignment
            ValueTask<int> t = default;
            bool finishSync = false;
            // ReSharper restore RedundantAssignment
#if NETCOREAPP

            while (true)
            {
                bw.EnsureCapacity(minReadCapacity);

                t = stream.ReadAsync(bw.FreeMemory, ct);
                if (t.IsCompletedSuccessfully)
                {
                    bw.Advance(t.Result);
                    if (t.Result == 0 || (knownSize >= 0 && t.Result == knownSize))
                    {
                        stream.Dispose();
                        return new ValueTask<BufferWriter>(bw);
                    }
                    // Continue, we do not know the size or have read partially. Cannot tell that at the end of stream until ReadAsync returns 0.
                }
                else
                {
                    finishSync = true;
                    break;
                }
            }
#endif

            return ConsumeAsync();

            async ValueTask<BufferWriter> ConsumeAsync()
            {
                byte[] tempBuff = null;
                try
                {
                    do
                    {
                        if (finishSync)
                        {
#if NETCOREAPP
                            await t;
#endif
                            // we have written to free memory but not advanced yet
                            bw.Advance(t.Result);

                            if (t.Result == 0 || (knownSize >= 0 && t.Result == knownSize))
                            {
                                return bw;
                            }
                        }

                        bw.EnsureCapacity(minReadCapacity);
#if NETCOREAPP

                        t = stream.ReadAsync(bw.FreeMemory, ct);
#else
                        if (tempBuff == null)
                        {
                            tempBuff = BufferPool<byte>.Rent(bw.FreeCapacity);
                        }
                        var bytesRead = await stream.ReadAsync(tempBuff, 0, tempBuff.Length, ct);
                        if (bytesRead > 0)
                        {
                            tempBuff.AsSpan().Slice(0, bytesRead).CopyTo(bw.FreeSpan);
                        }
                        t = new ValueTask<int>(bytesRead);
#endif
                        finishSync = true;
                    } while (true);
                }
                finally
                {
                    // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                    if (tempBuff != null)
                    {
                        BufferPool<byte>.Return(tempBuff);
                    }
                    stream.Dispose();
                }
            }
        }
    }
}
