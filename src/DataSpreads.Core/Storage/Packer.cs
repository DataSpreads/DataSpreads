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
using Spreads;
using Spreads.Collections.Concurrent;
using Spreads.Threading;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace DataSpreads.Storage
{
    /// <summary>
    /// Packer compresses completed blocks and writes them to <see cref="IStreamBlockStorage"/>.
    /// Packer runs or a thread with <see cref="ThreadPriority.BelowNormal"/>,
    /// which should minimize it's impact on main activity.
    /// </summary>
    /// <remarks>
    /// Packer service could run in a separate process with given CPU affinity. (TODO ds-cli command)
    /// We do not try to play with threads affinity inside a process because it's complicated
    /// and dotnetcore recommends isolation at process level (dropped AppDomain). Also Packer
    /// is not essential for correct operations so we avoid risks of some unhandled exception
    /// to kill the main process.
    /// </remarks>
    internal class Packer : IDisposable, ISpreadsThreadPoolWorkItem
    {
        public enum PackAction
        {
            ///// <summary>
            ///// Release memory if a block does not have active users (<see cref="SharedMemory.BufferHeader.HeaderFlags.Count"/>> = 1).
            ///// </summary>
            //ReleaseMemoryOnly = 0,

            /// <summary>
            /// Compress and persist blocks. Update <see cref="StreamBlockIndex"/> bye removing <see cref="BufferRef"/> of packed blocks.
            /// Return unused blocks to <see cref="BlockMemoryPool"/>.
            /// </summary>
            Pack = 1,

            /// <summary>
            /// Remove unused blocks from <see cref="StreamBlockIndex"/> and release buffers to <see cref="BlockMemoryPool"/>.
            /// </summary>
            Delete = 2,
        }

        private readonly StreamLogManager _streamLogManager;
        private readonly StreamLogStateStorage _stateStorage;
        private readonly bool _disablePacking;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();

        private readonly LockedDictionary<StreamLogId, byte> _jobs = new LockedDictionary<StreamLogId, byte>();

        /// <summary>
        /// Background thread pool.
        /// </summary>
        public SpreadsThreadPool Pool { [DebuggerStepThrough][MethodImpl(MethodImplOptions.AggressiveInlining)] get; }

        /// <summary>
        /// If <paramref name="disablePacking"/> is set to true Packer only releases
        /// unused memory pages back to OS and does not perform compression.
        /// TODO This setting per stream
        /// </summary>
        /// <param name="streamLogManager"></param>
        /// <param name="stateStorage"></param>
        /// <param name="disablePacking"></param>
        public Packer(StreamLogManager streamLogManager, StreamLogStateStorage stateStorage, bool disablePacking = false)
        {
            _streamLogManager = streamLogManager;
            _stateStorage = stateStorage;
            _disablePacking = disablePacking;
            Pool = new SpreadsThreadPool(
                new ThreadPoolSettings(1 + Math.Max(Environment.ProcessorCount / 2 - 1, 0),
                    ThreadType.Background,
                    "Packer_pool",
                    ApartmentState.Unknown, ex =>
                    {
                        ThrowHelper.FailFast("Unhandled exception in Packer thread pool: \n" + ex);
                    }, 0, ThreadPriority.BelowNormal));
        }

        /// <summary>
        /// Called from ZeroLog listener thread.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(StreamLogId id)
        {
            _jobs.Set(id, 0);
            Pool.UnsafeQueueCompletableItem(this, false);
        }

        public void Execute()
        {
            // first try NotificationLog, it has priority so we do not leak it's large buffers
            if (_jobs.TryRemove(StreamLogId.Log0Id, out _))
            {
                TryPackBlocks(StreamLogId.Log0Id);
            }
            // note no else
            if (_jobs.TryPop(out var pair))
            {
                TryPackBlocks(pair.Key);
            }
            else
            {
                // this could happen if a job was removed right after _jobs.Set in Add
            }
        }

        internal bool TryPackBlocks(StreamLogId slid)
        {
            var state = _stateStorage.GetState(slid);
            long packerLock;

            if (state.IsValid && (packerLock = state.AcquirePackerLock()) > 0)
            {
                try
                {
                    var isPackable = !_disablePacking && !state.StreamLogFlags.NoPacking();
                    var couldHaveMore = isPackable;
                    while (couldHaveMore)
                    {
                        var packAction =
                            state.StreamLogFlags.DropPacked()
                                ? PackAction.Delete
                                : PackAction.Pack;

                        couldHaveMore = _streamLogManager.BlockIndex.PackBlocks(state, packAction);

                        if (couldHaveMore)
                        {
                            // extend lock to avoid timeout
                            packerLock = state.AcquirePackerLock(packerLock);
                        }
                    }
                }
                finally
                {
                    if (!state.ReleasePackerLock(packerLock))
                    {
                        ThrowHelper.FailFast("Cannot release owned packer lock");
                    }
                }

                return true;
            }

            return false;
        }

        public void Dispose()
        {
            _cts.Cancel();

            Pool.Dispose();
            Pool.WaitForThreadsExit(TimeSpan.FromSeconds(10));
        }
    }
}
