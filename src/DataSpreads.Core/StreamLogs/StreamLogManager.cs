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
using DataSpreads.Config;
using DataSpreads.Storage;
using DataSpreads.Symbology;
using Spreads;
using Spreads.Collections.Concurrent;
using Spreads.Serialization.Utf8Json;
using Spreads.Threading;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace DataSpreads.StreamLogs
{
    // TODO
    // * Review & cleanup the loop
    // * (low) Fast open stream lookup by repo id & smaller stream key
    // * Streams disposal. And what happens when a stream is GCed and
    //   we receive a notification for it. Review this point.

    /// <summary>
    /// Manages StreamLogs.
    /// </summary>
    internal class StreamLogManager : IDisposable
    {
        /// <summary>
        /// This is used as max block size and Notification log block size.
        /// Max single item size is defined in <see cref="StreamLog.MaxItemSize"/>
        /// </summary>
        public const int MaxBufferSize = 8 * 1024 * 1024;

        protected readonly bool DisableNotificationLog;
        protected readonly bool DisablePacker;

        internal readonly IndexedLockedWeakDictionary<long, StreamLog> OpenStreams = new IndexedLockedWeakDictionary<long, StreamLog>();
        internal readonly ConcurrentQueue<AckRequest> AckCompletions = new ConcurrentQueue<AckRequest>();

        public static Action<object> CompleteTcs = tcs =>
        {
            try
            {
                ((TaskCompletionSource<byte>)tcs).SetResult(1);
            }
            catch (Exception ex)
            {
                ThrowHelper.FailFast(ex.ToString());
            }
        };

        internal ProcessConfig ProcessConfig { [DebuggerStepThrough][MethodImpl(MethodImplOptions.AggressiveInlining)] get; }
        internal BlockMemoryPool BufferPool { [DebuggerStepThrough][MethodImpl(MethodImplOptions.AggressiveInlining)] get; }
        internal StreamBlockIndex BlockIndex { [DebuggerStepThrough][MethodImpl(MethodImplOptions.AggressiveInlining)] get; }
        internal StreamLogStateStorage StateStorage { [DebuggerStepThrough][MethodImpl(MethodImplOptions.AggressiveInlining)] get; }
        internal Packer Packer { [DebuggerStepThrough][MethodImpl(MethodImplOptions.AggressiveInlining)] get; }

        protected readonly CancellationTokenSource Cts = new CancellationTokenSource();

        private Thread _log0Thread;

        private int _disposed;

        internal StreamLogState Log0State;
        internal NotificationLog Log0;
        protected readonly NotificationLog.Reader Log0Reader;

        protected readonly string DataStorePath;
        private readonly Wpid _wpidValue;

        public int Log0ThreadId;

        public StreamLogManager(ProcessConfig processConfig,
            string dataStoreName,
            string dataStorePath = null,
            uint maxLogSizeMb = 1024,
            bool disableNotificationLog = false,
            bool disablePacker = false,
            IStreamBlockStorage blockStorage = null)
        {
            if (LeaksDetection.Enabled)
            {
                Spreads.Buffers.BufferPool.PinnedArrayMemoryPool.AddStackTraceOnRent = true;
            }

            DataStorePath = dataStorePath ?? Path.Combine(processConfig.DataRootPath, dataStoreName);

            ProcessConfig = processConfig;
            _wpidValue = ProcessConfig.Wpid;

            DisableNotificationLog = disableNotificationLog;
            DisablePacker = disablePacker;

            var bufferPoolPath = Path.Combine(DataStorePath, "log", "logbuffer");
            var bufferPoolFlags = StartupConfig.StreamLogBufferPoolFlags;

            BufferPool = new BlockMemoryPool(bufferPoolPath, maxLogSizeMb, bufferPoolFlags, processConfig.Wpid,
                maxBufferLength: MaxBufferSize,
                maxBuffersPerBucket: Environment.ProcessorCount * 4);

            if (blockStorage == null)
            {
                var dataStoragePath = Path.Combine(DataStorePath, "storage");
                Directory.CreateDirectory(dataStoragePath);
                var path = Path.GetFullPath(Path.Combine(dataStoragePath, "data.db"));
                var uri = new Uri(path);
                var absoluteUri = uri.AbsoluteUri;

                blockStorage = new SQLiteStorage($@"Data Source={absoluteUri}"); // ?cache=shared
            }

            var blockIndexPath = Path.Combine(DataStorePath, "log", "blockindex");
            var blockIndexFlags = StartupConfig.StreamBlockIndexFlags;
            var blockIndexSizeMb = Math.Max(StartupConfig.StreamBlockTableMaxSizeMb, 128);
            BlockIndex = new StreamBlockIndex(blockIndexPath, blockIndexSizeMb, blockIndexFlags, BufferPool, blockStorage);

            var logStateStoragePath = Path.Combine(DataStorePath, "log", "logstate");

            StateStorage = new StreamLogStateStorage(logStateStoragePath);

            // For Log0 tests we need state but we removed it from Log0 ctor, so always init it.
            // In real code _disableNotificationLog is always false.
            Log0State = StateStorage.GetState(StreamLogId.Log0Id);
            Log0State.CheckInit(StreamLogId.Log0Id, StreamLogNotification.Size, StreamLogFlags.IsBinary | StreamLogFlags.NoTimestamp | StreamLogFlags.DropPacked | StreamLogFlags.Pow2Payload);

            Log0State.HintRatePerMinute(MaxBufferSize);

            if (!DisableNotificationLog)
            {
                Log0 = new NotificationLog(this);
                OpenStreams.TryAdd((long)StreamLogId.Log0Id, Log0);
                Log0Reader = new NotificationLog.Reader(Log0, Cts.Token);
                var lastVersion = (ulong)Log0.CurrentVersion;
                if (lastVersion > 1)
                {
                    Log0Reader.MoveGT(lastVersion - 1);
                }
            }

            Packer = new Packer(this, StateStorage, disablePacker);

            StartLog0(dataStoreName);

            // TODO we need more general unlocking locking that detects missed updates
            // and works for all waiters, not only for ack. See Log0.Reader comments.
            //_unlockTimer = new Timer(o =>
            //{
            //    TryCompleteAckRequests();
            //}, null, 0, 1000);
        }

        public Wpid Wpid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _wpidValue;
        }

        public bool IsPackerEnabled
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => !DisablePacker;
        }

        public bool IsDisposed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref _disposed) != 0;
        }

        public string RepoPath
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => DataStorePath;
        }

        //
        [Obsolete("MD to SLid should happen outside SLM, here we should only work with SLids")]
        public Opt<StreamLog> GetStreamLogAsync(DataStreamMetadataOld metadataOld)
        {
            if (metadataOld == null)
            {
                ThrowHelper.ThrowArgumentNullException(nameof(metadataOld));
                return default;
            }

            var streamId = metadataOld.StreamId;
            if (streamId <= 0)
            {
                Console.WriteLine(JsonSerializer.ToJsonString(metadataOld));
                ThrowHelper.ThrowArgumentException($"Cannot open control streams directly {streamId}");
            }

            Opt<StreamLog> result;
            if (OpenStreams.TryGetValue(streamId, out var stream))
            {
                result = Opt.Present(stream);
            }
            else
            {
                throw new NotImplementedException();
                //var slid = (StreamLogId)metadataOld.StreamId;
                //var state = StateStorage.GetState(slid);
                //state.CheckInit(slid, checked((short)metadataOld.ValueTypeSize),
                //    (SerializationFormat)checked((byte)metadataOld.SerializationFormat));
                //var streamLog = new StreamLog(metadataOld.TextId, this, state, checked((short)metadataOld.ValueTypeSize),
                //    metadataOld.RateHint ?? 0);
                //// await streamLog.Init();
                //_openStreams.TryAdd(metadataOld.StreamId, streamLog);
                //result = Opt.Present(streamLog);
            }

            return result;
        }

        public bool InitStreamLog(StreamLogId slid, short valueSize, StreamLogFlags streamLogFlags)
        {
            var state = StateStorage.GetState(slid);
            return state.CheckInit(slid, valueSize, streamLogFlags);
        }

        public StreamLog OpenStreamLog(StreamLogId slid, int rateHintPerMinute = 0, string textId = default)
        {
            if (!OpenStreams.TryGetValue((long)slid, out var sl))
            {
                // State must be initialized by caller of this method. We need metadata
                // & flags that depend on types and user input and are not SLM concern.
                var state = StateStorage.GetState(slid);
                sl = new StreamLog(this, state, rateHintPerMinute, textId);
                // Do not init, it's done on first claim: sl.Init();
                OpenStreams.TryAdd((long)slid, sl);
            }
            sl.State.SetRatePerMinute(rateHintPerMinute);
            return sl;
        }

        protected void StartLog0(string threadId)
        {
            _log0Thread = new Thread(() =>
            {
                if (DisableNotificationLog)
                {
                    return;
                }

                try
                {
                    Trace.TraceInformation($"[{Wpid}]: Started listening to NotificationLog from version: " + Log0Reader.CurrentVersion);
                    Log0ThreadId = Thread.CurrentThread.ManagedThreadId;
                    Log0Loop();
                    Log0Reader.Dispose();
                }
                catch (Exception ex)
                {
                    if (!IsDisposed)
                    {
                        ThrowHelper.FailFast("Error in Log0: " + ex);
                    }
                    Trace.TraceError("Error during Log0 dispose: " + ex);
                }
            });

            _log0Thread.IsBackground = true;
            _log0Thread.Name = "NotificationLogListenerThread_" + threadId;
            _log0Thread.Priority = GetSpinnerThreadPriority();
            _log0Thread.Start();
        }

        /// <summary>
        /// Returns priority for Log0 loop and Syncer (WAL) loop. We use AboveNormal priority if we have enough resources (CPU count > 8).
        /// </summary>
        internal static ThreadPriority GetSpinnerThreadPriority()
        {
            // Need UDP notifications & blocking wait on machines with low CPU count
            if (Environment.ProcessorCount > 8)
            {
                return ThreadPriority.AboveNormal;
            }
            if (Environment.ProcessorCount > 4)
            {
                return ThreadPriority.Normal;
            }

            return ThreadPriority.Normal;
        }

        // NB: we do not store a reference because they are weak in the storage, but save storage index for fast lookup
        private StreamLogId _previousSlid; // cast from 0 throws (StreamLogId)0;

        protected int PreviousStreamStorageIndex = -1;

        /// <summary>
        /// Returns null if a stream is not currently open with this manager.
        /// </summary>
        [Obsolete("Must be used ONLY FROM Log0 loop.")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected StreamLog TryFindOpenStreamLogFromLoop0(StreamLogId slid)
        {
            // TODO Lookup table by RepoId (up to c.1024 local ids)
            // Then _previousStreamStorageIndex per repo (4 bytes stream id ony)
            // Then weak dictionary per repo with int32 key.

            StreamLog current;
            if (slid == _previousSlid && OpenStreams.TryGetByIndex(PreviousStreamStorageIndex, out var current1))
            {
                if (current1.StorageIndex != PreviousStreamStorageIndex)
                {
                    FailBadStorageId();
                }
                current = current1;
            }
            else if (OpenStreams.TryGetValue((long)slid, out current) && current != null)
            {
                _previousSlid = slid;
                PreviousStreamStorageIndex = current.StorageIndex;
            }

            return current;
        }

#if NETCOREAPP
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
#endif

        protected virtual void Log0Loop()
        {
            while (Log0Reader.MoveNext())
            {
                var currentNotification = Log0Reader.Current;

                if (currentNotification.IsWalPosition)
                {
                    TryCompleteAckRequests();
                    continue;
                }

                if ((ulong)currentNotification == 0)
                {
                    if (Cts.IsCancellationRequested)
                    {
                        break;
                    }
                    else
                    {
                        FailEmptyLog0Payload();
                    }
                }

                var slid = currentNotification.StreamLogId;

#pragma warning disable 618
                var currentStreamLog = TryFindOpenStreamLogFromLoop0(slid);
#pragma warning restore 618

                if (currentStreamLog is null)
                {
                    continue;
                }

                if (currentNotification.IsRotated)
                {
                    Packer.Add(currentNotification.StreamLogId);
                    continue;
                }

                // notify readers only if we are reading data from another process, this process has already done so
                if (!currentStreamLog.CurrentWpidIsWriter)
                {
                    if (currentNotification.IsCompleted)
                    {
                        // Trace.TraceInformation($"Received completed message for {slid}");
                        // completion is reflected in shared-memory state, but in-memory container
                        // needs to update its status and call proper notification
                        currentStreamLog.Complete();
                    }
                    else
                    {
                        currentStreamLog.NotifyCurrentProcess(false);
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TryCompleteAckRequests()
        {
            AckRequest firstUnprocessed = default;

            while (true)
            {
                if (!AckCompletions.TryDequeue(out var ackRequest))
                {
                    break;
                }

                if (!ackRequest.TryComplete())
                {
                    if (firstUnprocessed is null)
                    {
                        firstUnprocessed = ackRequest;
                    }

                    AckCompletions.Enqueue(ackRequest);

                    if (ReferenceEquals(firstUnprocessed, ackRequest))
                    {
                        break;
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailBadStorageId()
        {
            ThrowHelper.FailFast("current1.StorageIndex != previousStreamStorageIndex");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        protected static void FailEmptyLog0Payload()
        {
            ThrowHelper.FailFast("Log0Payload == 0");
        }

        // ReSharper disable once UnusedParameter.Local
        protected virtual void Dispose(bool disposing)
        {
            Interlocked.Exchange(ref _disposed, 1);
            Cts.Cancel();
            if (!DisableNotificationLog)
            {
                _log0Thread.Join();
                Trace.TraceInformation($"[{Wpid}] Disposed Log0");
            }
            Log0?.Dispose();
            Packer?.Dispose();

            for (int i = OpenStreams.Count - 1; i >= 0; i--)
            {
                if (OpenStreams.TryGetByIndex(i, out var sl))
                {
                    sl?.Dispose();
                }
            }

            BufferPool.Dispose();
            StateStorage.Dispose();
            BlockIndex.Dispose();

            if (ProcessConfig != ProcessConfig.Default)
            {
                ProcessConfig.Dispose();
            }
        }

        public void Dispose()
        {
            // TODO Stream disposal
            //foreach (var openStream in OpenStreams)
            //{
            //    openStream.Value.Dispose();
            //}
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        ~StreamLogManager()
        {
            Dispose(false);
        }
    }
}
