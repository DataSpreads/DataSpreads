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

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks.Sources;

namespace DataSpreads.StreamLogs
{
    internal class AckRequest : IValueTaskSource<ulong>
    {
        private long _streamVersion;
        private readonly StreamLog _streamLog;
        private object _cont;
        private object _state;

        public AckRequest(StreamLog sl, ulong version)
        {
            _streamLog = sl;
            _streamVersion = checked((long)version);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryComplete()
        {
            var isCompleted = IsCompleted || _streamLog.IsVersionInWal((ulong)Math.Abs(_streamVersion));
            if (isCompleted)
            {
                lock (this)
                {
                    if (_streamVersion > 0)
                    {
                        _streamVersion = -_streamVersion;
                    }

                    if (_cont is Action<object> single)
                    {
                        // TODO use WorkItem interface
                        ThreadPool.UnsafeQueueUserWorkItem(o => single(o), _state);
                    }
                    else if (_cont is List<Action<object>> multi)
                    {
                        for (int i = 0; i < multi.Count; i++)
                        {
                            var c = multi[i];
                            // ReSharper disable once PossibleNullReferenceException
                            var st = (_state as List<object>)[i];
                            // TODO use WorkItem interface
                            ThreadPool.UnsafeQueueUserWorkItem(o => c(o), st);
                        }
                        multi.Clear();
                    }
                    _cont = null;
                }
                return true;
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            lock (this)
            {
                if (IsCompleted)
                {
                    continuation(state);
                }

                if (_cont == null)
                {
                    _cont = continuation;
                    _state = state;
                }
                else
                {
                    if (_cont is Action<object> single)
                    {
                        _cont = new List<Action<object>>(4) { single, continuation };
                        _state = new List<object>(4) { _state, state };
                    }
                    else if (_cont is List<Action<object>> multi)
                    {
                        multi.Add(continuation);
                        // ReSharper disable once PossibleNullReferenceException
                        (_state as List<object>).Add(state);
                    }
                }
            }
        }

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public void UnsafeOnCompleted(Action continuation)
        //{
        //    OnCompleted(continuation);
        //}

        public bool IsCompleted
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _streamVersion < 0;
        }

        //public void GetResult()
        //{
        //}

        //[MethodImpl(MethodImplOptions.AggressiveInlining)]
        //public AckRequest GetAwaiter()
        //{
        //    return this;
        //}

        public ValueTaskSourceStatus GetStatus(short token)
        {
            if (IsCompleted)
            {
                return ValueTaskSourceStatus.Succeeded;
            }

            return ValueTaskSourceStatus.Pending;

            //var isCompleted = _streamLog.IsVersionInWal((ulong)Math.Abs(_streamVersion));
            //if (isCompleted)
            //{
            //    lock (this)
            //    {
            //        if (!IsCompleted)
            //        {
            //            _streamVersion = -_streamVersion;
            //        }
            //    }
            //    return ValueTaskSourceStatus.Succeeded;
            //}
            //else
            //{
            //    return ValueTaskSourceStatus.Pending;
            //}
        }

        public ulong GetResult(short token)
        {
            return checked((ulong)(Math.Abs(_streamVersion)));
        }
    }
}
