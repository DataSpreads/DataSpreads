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

using Spreads;
using Spreads.DataTypes;
using Spreads.LMDB;
using Spreads.Serialization.Utf8Json;
using Spreads.Threading;
using Spreads.Utils;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Text;
using System.Threading;

namespace DataSpreads.Config
{
    // TODO this semi-lazy semi-static init is quite complicated.
    // Need single but configurable instance in prod but multiple
    // instances in testing. Should simplify setting things up.

    /// <summary>
    /// Abstract a process. Allows testing multiple DS instances in the same process or to work with Docker without pid==host (which by default is disabled).
    /// </summary>
    internal partial class ProcessConfig : CriticalFinalizerObject, IWpidHelper, IDisposable
    {
        private const string ConfigFolderName = ".config";

        #region Static setup

        private static ProcessConfig _default;

        public static ProcessConfig Default
        {
            get
            {
                if (_default == null)
                {
                    _default = new ProcessConfig();
                }
                return _default;
            }
        }

        internal static string ProcessConfigPath;

        internal static ProcessConfigStorage Storage;

        private static ProcessConfigRecord SharedRecord
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Storage.SharedRecord;
        }

        private static TimeService _sharedTs = TimeService.Default;

        private static int _heartbeatIntervalMillisec = 100;

        private static readonly TimeSpan AssumedDeadAfter = TimeSpan.FromSeconds(StartupConfig.AssumeDeadAfterSeconds);

        internal static readonly ConcurrentDictionary<Wpid, bool> DeadWpids = new ConcurrentDictionary<Wpid, bool>();

        internal static volatile bool Exiting;

        internal static event EventHandler OnExit;

        private static readonly object StaticSyncRoot = new object();

        internal static bool IsInitialized;

        internal static DataStoreConfig DataStoreConfig;

        internal static void InitDefault()
        {
            lock (StaticSyncRoot)
            {
                var processPath = Path.Combine(StartupConfig.AppDataPath, ConfigFolderName);
                Directory.CreateDirectory(processPath);

                Init(processPath);
            }
        }

        internal static void Init(string processPath)
        {
            lock (StaticSyncRoot)
            {
                if (IsInitialized)
                {
                    Trace.TraceWarning("Process config already initialized.");
                    return;
                }
                AppDomain.CurrentDomain.DomainUnload += (sender, args) =>
                {
                    Trace.TraceInformation("App domain unload");
                    Exiting = true;
                    OnExit?.Invoke(null, default);
                };
                AppDomain.CurrentDomain.ProcessExit += (sender, args) =>
                {
                    Trace.TraceInformation("App domain exit");
                    Exiting = true;
                    OnExit?.Invoke(null, default);
                };

                var isLiteClient = StartupConfig.IsLiteClient || IntPtr.Size == 4;

                if (!isLiteClient)
                {
                    // LCs should throw NRE if wrong code and they try to access the location.
                    ProcessConfigPath = processPath;

                    Storage = new ProcessConfigStorage(ProcessConfigPath);

                    OnExit += (sender, e) =>
                    {
                        DataStore._default?.Dispose();
                        Storage.Dispose();
                    };

                    _sharedTs = new TimeService(Storage.SharedRecord.TimeStampPointer, 1);
                    _sharedTs.UpdateTime();

                    TimeService.Default = _sharedTs;

                    var datastoreConfigPath = Path.Combine(processPath, "datastore.json");
                    if (File.Exists(datastoreConfigPath))
                    {
                        try
                        {
                            var json = File.ReadAllText(datastoreConfigPath, Encoding.UTF8);
                            DataStoreConfig = JsonSerializer.Deserialize<DataStoreConfig>(json);
                        }
                        catch
                        {
                            Trace.TraceWarning("Cannot read data store config from existing file.");
                        }
                    }
                }

                IsInitialized = true;
            }
        }

        #endregion Static setup

        /// <summary>
        /// DataStreams and other containers are stored in memory
        /// without depending on LMDB/SQLite access and communicate with server only via network.
        /// </summary>
        public readonly bool IsLiteClient;

        public readonly string DataRootPath;
        public const string DefaultDataStoreName = "default";

        private ProcessConfigRecord _processRecord;

        private readonly Timer _heartBeatTimer;

        /// <summary>
        ///
        /// </summary>
        /// <param name="dataRootPathOverride">Override data root path. For tests only.</param>
        /// <param name="serverPort"></param>
        /// <param name="isLightClient"></param>
        public ProcessConfig(string dataRootPathOverride = null, uint serverPort = 0, bool isLightClient = false)
        {
            if (dataRootPathOverride == null)
            {
                dataRootPathOverride = Path.Combine(StartupConfig.AppDataPath, "datastores");
                InitDefault();
            }
            else
            {
                var path = Path.Combine(dataRootPathOverride, ".processconfig");
                Init(path);
            }

            DataRootPath = dataRootPathOverride;

            if (isLightClient || StartupConfig.IsLiteClient || IntPtr.Size == 4)
            {
                IsLiteClient = true;
            }

            if (!IsLiteClient)
            {
                _processRecord = Storage.CreateNew();
                _processRecord.Timestamp = CurrentTime;

                // Without WR timer callback captures this, this holds timer and timer holds the callback,
                // so the object is never finalized unless disposed properly. But we need finalizer.
                // See CouldOpenCloseManyDataConfigs test.
                var wr = new WeakReference<ProcessConfig>(this);
                _heartBeatTimer = new Timer(TimerCallback, wr, 0, _heartbeatIntervalMillisec);

                UnlockDeadWpids();

                if (serverPort > 0)
                {
                    throw new NotImplementedException();
                    //var existingServer = SharedBuffer.InterlockedCompareExchangeInt64(
                    //    SharedProcessConfigRecord.ServerWpidOffset,
                    //    Wpid, 0L);
                    //if (0L != existingServer)
                    //{
                    //    if (IsWpidAlive(existingServer))
                    //    {
                    //        ThrowHelper.ThrowInvalidOperationException("Server already exists");
                    //    }
                    //    else
                    //    {
                    //        if (existingServer != SharedBuffer.InterlockedCompareExchangeInt64(
                    //                SharedProcessConfigRecord.ServerWpidOffset,
                    //                Wpid, existingServer))
                    //        {
                    //            ThrowHelper.FailFast("Cannot acquire server role. Won't handle this race condition.");
                    //        }
                    //    }
                    //}
                }
            }

            if (_default == null)
            {
                _default = this;
            }
        }

        internal bool SaveDataStoreConfig(DataStoreConfig dataStoreConfig)
        {
            try
            {
                var datastoreConfigPath = Path.Combine(ProcessConfigPath, "datastore.json");
                if (!File.Exists(datastoreConfigPath))
                {
                    if (dataStoreConfig.DataStoreDirectory == null)
                    {
                        dataStoreConfig.DataStoreDirectory = Path.Combine(DataRootPath, "DefaultDataStoreName");
                    }

                    var json = JsonSerializer.ToJsonString(dataStoreConfig);
                    File.WriteAllText(datastoreConfigPath, json, Encoding.UTF8);
                    if (DataStoreConfig == null)
                    {
                        DataStoreConfig = dataStoreConfig;
                    }
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                Trace.TraceError("Cannot save data store config: " + ex);
                return false;
            }
        }

        private static void TimerCallback(object o)
        {
            if (o is WeakReference<ProcessConfig> wr && wr.TryGetTarget(out var dc))
            {
                dc.Heartbeat();
            }
            UpdateTime();
        }

        public Wpid Wpid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _processRecord.Wpid;
        }

        public bool IsServer => false; // SharedBuffer.IsValid && SharedBuffer.Read<long>(SharedProcessConfigRecord.ServerWpidOffset) == _wpidValue && _wpidValue > 0;

        #region Simulate process hang

        internal bool Paused;

        internal void TogglePaused()
        {
            Paused = !Paused;
        }

        #endregion Simulate process hang

        private void Heartbeat()
        {
            if (!_processRecord.IsValid || !SharedRecord.IsValid)
            {
                return;
            }
#if DEBUG
            if (!Paused)
            {
#endif

                // before checking AssumedDead so that we could detect that the process was alive at least
                // it killed itself when checked AssumedDead
                _processRecord.Timestamp = CurrentTime;
#if DEBUG
            }
#endif
            if (AssumedDead != default && Wpid != default)
            {
                Suicide();
            }
            else if (Wpid == default)
            {
                _heartBeatTimer.Dispose();
            }
            else
            {
                if (ChaosMonkey.Enabled)
                {
                    try
                    {
                        // TODO Either remove CM completely or do scenario that depends on this instance id.
                        // Failing by probability is inconvenient in tests.
                        ChaosMonkey.Exception(0.001);
                    }
                    catch (ChaosMonkeyException)
                    {
                        Trace.WriteLine($"Heartattack during Heartbeat in wpid {Wpid}");
                        Suicide();
                    }
                }
            }

            // could add counter and start some background work every N heartbeats
        }

        public void Suicide()
        {
            var pcr = _processRecord;
            var wpid = pcr.Wpid;
            var alreadyDead = pcr.CasAssumedDead(CurrentTime, default) != default;
            Trace.WriteLine($"Performed suicide: wpid = {wpid}, was already dead: {alreadyDead}");
        }

        public void OnForceUnlock(Wpid wpid)
        {
            Trace.TraceWarning($"Force unlocked wpid: {wpid}");
        }

        Wpid IWpidHelper.MyWpid => Wpid;

        public Timestamp LastHeartbeat
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _processRecord.Timestamp;
        }

        /// <summary>
        /// Someone kicked us out. We do not use Process.Kill e.g. in Docker because do not know how it will work.
        /// Dead pages from perished processes will be cleaned at some point, but if an assumed-dead process is alive it must kill itself.
        /// </summary>
        public Timestamp AssumedDead
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _processRecord.AssumedDead;
        }

        #region Dispose

        // ReSharper disable once UnusedParameter.Local
        private void Dispose(bool disposing)
        {
            if (_default == this)
            {
                _default = null;
            }

            if (_heartBeatTimer != null)
            {
                var wh = new EventWaitHandle(false, EventResetMode.ManualReset);
                _heartBeatTimer?.Dispose(wh);
                wh.WaitOne(_heartbeatIntervalMillisec * 2);
            }

            var wpid = Wpid;
            lock (StaticSyncRoot)
            {
                var deleted = Storage.Delete(_processRecord);
                if (deleted)
                {
                    Trace.TraceInformation($"[{wpid}] {(disposing ? "Disposed" : "Finalized")} ProcessConfig.");
                }
                _processRecord = default;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~ProcessConfig()
        {
            Dispose(false);
        }

        #endregion Dispose

        #region Static members

        private static ProcessConfigRecord GetRecord(Wpid wpid)
        {
            return Storage.GetRecord(wpid);
        }

        /// <summary>
        /// Cross-process synchronized time updated in background via a shared <see cref="TimeService"/>.
        /// Accessing this property guarantees that the returned value is unique and increasing across
        /// processes and stays close enough to current time. If exact current time is important call
        /// <see cref="UpdateTime"/> method instead. That call will also update the underlying shared
        /// value and improve time precision for subsequent getters of this property.
        /// </summary>
        public static Timestamp CurrentTime
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _sharedTs.CurrentTime;
        }

        public static uint LastInstanceId
        {
            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            get => SharedRecord._processBuffer.Read<uint>(ProcessConfigRecord.SharedInstanceIdCounterOffset);
        }

        /// <summary>
        /// Updates shared <see cref="TimeService"/> time and returns system time with max available precision.
        /// If this method is called very often from many threads then a better solution would be to start
        /// a spinning update thread via <see cref="TimeService.StartSpinUpdate"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Timestamp UpdateTime()
        {
            if (SharedRecord.IsValid && _sharedTs != null)
            {
                return (Timestamp)_sharedTs.UpdateTime();
            }
            return DateTime.UtcNow;
        }

        private static void ScanDeadWpids()
        {
            lock (StaticSyncRoot)
            {
                var deadWpidCandidates = Storage.GetAllProcesses();

                foreach (var wpid in deadWpidCandidates)
                {
                    if (!IsWpidAlive(GetRecord(wpid)))
                    {
                        Trace.TraceWarning($"Detected dead wpid {wpid}.");
                        DeadWpids.TryAdd(wpid, false);
                    }
                }

                deadWpidCandidates.Clear();
            }
        }

        private static void UnlockDeadWpids()
        {
            ScanDeadWpids();
            lock (StaticSyncRoot)
            {
                foreach (var deadWpid in DeadWpids)
                {
                    if (deadWpid.Key == 0)
                    {
                        ThrowHelper.FailFast("Zero wpid is shared and should not be marked as dead.");
                    }

                    try
                    {
                        var deleted = Storage.Delete(GetRecord(deadWpid.Key));
                        if (deleted)
                        {
                            Trace.TraceWarning($"Removed dead wpid {deadWpid.Key}.");
                        }
                    }
                    catch (LMDBException)
                    {
                        // ignore
                    }
                }
            }
        }

        internal static bool IsPidAlive(int pid)
        {
            try
            {
                // ReSharper disable once UnusedVariable
                var process = Process.GetProcessById(pid);
                return true;
            }
            catch (ArgumentException)
            {
                // Pid is dead
                return false;
            }
        }

        // wpid helper interface
        bool IWpidHelper.IsWpidAlive(Wpid wpid)
        {
            return IsWpidAlive(GetRecord(wpid));
        }

        public static bool IsWpidAlive(Wpid wpid)
        {
            return IsWpidAlive(GetRecord(wpid));
        }

        public static bool IsWpidAlive(ProcessConfigRecord record)
        {
            if (!record.IsValid)
            {
                return false;
            }
            var wpid = record.Wpid;
            if (wpid == 0)
            {
                return true;
            }

            if (DeadWpids.ContainsKey(wpid))
            {
                Trace.WriteLine($"Wpid {wpid} is dead and is already in the dead list");
                return false;
            }

            //if (wpid == _wpidValue)
            //{
            //    if (!(AssumedDead == default))
            //    {
            //        AddWpidToDeadListAndUnlock(wpid);

            //        return false;
            //    }

            //    return true;
            //}

            var pid = wpid.Pid;
            if (IsPidAlive(pid))
            {
                // Docker pid = 1 is PITA (no --pid=host support on ECS yet), need to check wpid

                if (!record.IsValid)
                {
                    Console.WriteLine($"Config buffer for Wpid {wpid} is missing, assuming dead");
                    AddWpidToDeadListAndUnlock(wpid);
                    return false;
                }

                var assumedDead = record.AssumedDead;

                if (assumedDead != default)
                {
                    Console.WriteLine($"Wpid {wpid} is already assumed dead: {assumedDead}");
                    AddWpidToDeadListAndUnlock(wpid);
                    return false;
                }

                var lastTs = record.Timestamp;

                if (lastTs == default)
                {
                    Thread.Sleep(_heartbeatIntervalMillisec * 2);
                    lastTs = record.Timestamp;
                }

                var timeSpan = (CurrentTime - lastTs).TimeSpan;

                if (timeSpan > AssumedDeadAfter)
                {
                    var alreadyDead = record.CasAssumedDead(CurrentTime, default) != default;
                    if (alreadyDead)
                    {
                        Console.WriteLine($"Wpid {wpid} is already assumed dead");
                    }
                    else
                    {
                        Console.WriteLine($"Assuming {wpid} dead after timeout");
                    }
                    AddWpidToDeadListAndUnlock(wpid);

                    return false;
                }

                return true;
            }
            return false;
        }

        private static void AddWpidToDeadListAndUnlock(Wpid wpid)
        {
            DeadWpids.TryAdd(wpid, false);
            UnlockDeadWpids();
        }

        #endregion Static members
    }
}
