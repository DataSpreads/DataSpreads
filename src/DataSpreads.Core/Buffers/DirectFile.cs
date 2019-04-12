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

using Microsoft.Win32.SafeHandles;
using Spreads;
using Spreads.Buffers;
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace DataSpreads.Buffers
{
    // NB Rule of thumb: always work with file sizes of 4kb multiples
    //    Do not open partial views unless proven effective, OS manages
    //    pages well and will cache only accessed ones. Use sane file
    //    sizes for simple shared storage files. Use LMDB/MemoryPool
    //    for complex and/or granular data access.

    internal unsafe class DirectFile : IDisposable
    {
        private readonly string _filePath;
        private readonly bool _isWritable;
        private readonly FileOptions _fileOptions;
        private FileStream _fileStream;
        // ReSharper disable once InconsistentNaming
        internal MemoryMappedFile _mmf;
        // ReSharper disable once InconsistentNaming
        internal MemoryMappedViewAccessor _va;
        private SafeMemoryMappedViewHandle _vaHandle;

        private long _capacity;
        private byte* _pointer;

        public DirectFile(string filePath, long minCapacity = 0, bool isWritable = false, FileOptions fileOptions = FileOptions.None, bool sparse = false)
        {
            if (!isWritable && !File.Exists(filePath))
            {
                ThrowHelper.ThrowArgumentException($"File {filePath} does not exists.");
            }
            _filePath = filePath;
            _isWritable = isWritable;
            _fileOptions = fileOptions;
            _mmf = null;
            _va = null;
            _fileStream = null;
            _vaHandle = default;
            // ReSharper disable once ExpressionIsAlwaysNull
            Grow(minCapacity, sparse);
        }

        internal void Grow(long minCapacity, bool sparse = false)
        {
            if (minCapacity < 0)
            {
                ThrowHelper.ThrowArgumentOutOfRangeException(nameof(minCapacity));
            }

            if (_isWritable)
            {
                minCapacity = ((minCapacity - 1) / 4096 + 1) * 4096;
            }

            if (_fileStream == null)
            {
                // NB No need to recreate file
                // If capacity is larger than the size of the file on disk, the file on disk is
                // increased to match the specified capacity even if no data is written to the
                // memory-mapped file. To prevent this from occurring, specify 0 (zero) for the
                // default capacity, which will internally set capacity to the size of the file on disk.

                //var handle = Mono.Posix.Syscall.open(_filePath, (Mono.Posix.OpenFlags) (0)); // _isWritable
                //    //? Mono.Posix.OpenFlags.O_RDWR //| Mono.Posix.OpenFlags.O_CREAT
                //    //: Mono.Posix.OpenFlags.O_RDONLY); //, FileMode.S_IWUSR | FileMode.S_IRUSR | FileMode.S_IROTH | FileMode.S_IWOTH);
                //var sfh = new SafeFileHandle((IntPtr)handle, true);

                //_fileStream = new FileStream(sfh, _isWritable ? FileAccess.ReadWrite : FileAccess.Read, 1, false);

                _fileStream = new FileStream(_filePath, _isWritable ? FileMode.OpenOrCreate : FileMode.Open,
                    _isWritable ? FileAccess.ReadWrite : FileAccess.Read,
                    FileShare.ReadWrite, 1,
                    _fileOptions);

                if (sparse)
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        if (!MarkAsSparseFile(_fileStream))
                        {
                            throw new Win32Exception("Cannot make a sparse file.");
                        }
                    }
                    else
                    {
                        // We allocate in chunks of 64-128 MB, not a big deal for servers
                        // Need this for MacOS, it's not nice to have pre-allocated 1 GB
                        // just in case.
                        Trace.TraceWarning("Sparse files not implemented on non-Windows");
                    }
                }

                if (!_isWritable && _fileStream.Length == 0)
                {
                    _fileStream.Dispose();
                    _fileStream = null;
                    ThrowHelper.ThrowInvalidOperationException($"File {_filePath} is empty. Cannot open DirectFile in read-only mode.");
                    return;
                }
            }
            else if (_isWritable)
            {
                // _va.Dispose does this
                // Flush(flushToDisk);
            }

            if (!_isWritable && minCapacity > _fileStream.Length)
            {
                ThrowHelper.ThrowArgumentException("minCapacity > _fileStream.Length");
            }

            // NB another thread could have increased the map size and _capacity could be stale
            var bytesCapacity = Math.Max(_fileStream.Length, minCapacity);

            _va?.Flush();
            _vaHandle?.ReleasePointer();
            _vaHandle?.Dispose();
            _va?.Dispose();
            _mmf?.Dispose();

            // var unique = ((long)Process.GetCurrentProcess().Id << 32) | _counter++;
            // NB: map name must be unique
            var mmf = MemoryMappedFile.CreateFromFile(_fileStream,
                null,  // $@"{Path.GetFileName(_filePath)}.{unique}"
                bytesCapacity,
                _isWritable ? MemoryMappedFileAccess.ReadWrite : MemoryMappedFileAccess.Read, HandleInheritability.None,
                true);
            _mmf = mmf;

            byte* ptr = (byte*)0;
            _va = _mmf.CreateViewAccessor(0, bytesCapacity, _isWritable ? MemoryMappedFileAccess.ReadWrite : MemoryMappedFileAccess.Read);
            _vaHandle = _va.SafeMemoryMappedViewHandle;
            _vaHandle.AcquirePointer(ref ptr);

            _pointer = ptr;
            _capacity = bytesCapacity;
        }

        public DirectBuffer DirectBuffer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new DirectBuffer(_capacity, _pointer);
        }

        public DirectBuffer Buffer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new DirectBuffer(_capacity, _pointer);
        }

        public long Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _capacity;
        }

        internal void Trim(long trimSize, bool reopen = false)
        {
            if (_isWritable) { Flush(false); } // TODO(!) true

            _va?.Flush();
            _vaHandle?.ReleasePointer();
            _vaHandle?.Dispose();
            _va?.Dispose();
            _mmf?.Dispose();

            _va = default;
            _vaHandle = default;
            _mmf = default;

            _fileStream.SetLength(trimSize);
            if (reopen)
            {
                Grow(1);
            }
            else
            {
                _fileStream?.Dispose();
            }
        }

        protected void Dispose(bool disposing)
        {
            if (_isWritable)
            {
                Flush(true);
            }

            _vaHandle?.ReleasePointer();
            _va?.Dispose();
            _mmf?.Dispose();
            _fileStream?.Dispose();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public void Flush(bool flushToDisk = false)
        {
            _va.Flush();
            if (flushToDisk)
            {
                _fileStream.Flush(true);
            }
        }

        public string FilePath
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _filePath;
        }

        public bool IsWriteable
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _isWritable;
        }

        /// <summary>
        /// Releases pages from the process's working set. The region of affected pages includes all pages containing one or more bytes in the <paramref name="buffer"/>.
        /// </summary>
        /// <param name="buffer"></param>
        public static void ReleaseMemory(DirectBuffer buffer)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                // ignore return value, it is false for this use case but the error is ERROR_NOT_LOCKED and it is expected.
                VirtualUnlock((IntPtr)buffer.Data, (UIntPtr)buffer.Length);
            }
            else
            {
#if !NET461
                // Mono.Posix.Syscall.

#endif
                // TODO
                // Trace.TraceWarning($"ReleasePages is only implemented on Windows");
            }
        }

        public static IntPtr CurrentProcessPtr = IntPtr.Zero;

        public static void PrefetchMemory(DirectBuffer buffer)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (CurrentProcessPtr == IntPtr.Zero)
                {
                    CurrentProcessPtr = GetCurrentProcess();
                }

                var entry = new WIN32_MEMORY_RANGE_ENTRY
                {
                    VirtualAddress = buffer.Data,
                    NumberOfBytes = (IntPtr)buffer.Length
                };

                if (!PrefetchVirtualMemory(CurrentProcessPtr, (UIntPtr)1, &entry, 0))
                {
                    Console.WriteLine("FAILED TO PREFETCH");
                }
            }
            else
            {
                // TODO
                // Trace.TraceWarning($"ReleasePages is only implemented on Windows");
            }
        }

        // ReSharper disable once InconsistentNaming
        private struct WIN32_MEMORY_RANGE_ENTRY
        {
            // ReSharper disable once NotAccessedField.Local
            public void* VirtualAddress;

            // ReSharper disable once NotAccessedField.Local
            public IntPtr NumberOfBytes;
        }

        // https://docs.microsoft.com/en-us/windows/desktop/api/memoryapi/nf-memoryapi-virtualunlock
        // Calling VirtualUnlock on a range of memory that is not locked releases the pages from the process's working set.
        [DllImport("kernel32.dll", SetLastError = true, ExactSpelling = true, CallingConvention = CallingConvention.Winapi)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool VirtualUnlock(IntPtr lpAddress, UIntPtr dwSize);

        [DllImport("kernel32.dll", SetLastError = true, ExactSpelling = true, CallingConvention = CallingConvention.Winapi)]
        public static extern IntPtr GetCurrentProcess();

        [DllImport("kernel32.dll", SetLastError = true, ExactSpelling = true, CallingConvention = CallingConvention.Winapi)]
        private static extern bool PrefetchVirtualMemory(IntPtr hProcess, UIntPtr numberOfEntries, WIN32_MEMORY_RANGE_ENTRY* virtualAddresses, ulong flags);

        [DllImport("Kernel32.dll", SetLastError = true, ExactSpelling = true, CallingConvention = CallingConvention.Winapi)]
        private static extern bool DeviceIoControl(
            SafeFileHandle hDevice,
            int dwIoControlCode,
            IntPtr inBuffer,
            int nInBufferSize,
            IntPtr outBuffer,
            int nOutBufferSize,
            ref int pBytesReturned,
            [In] ref NativeOverlapped lpOverlapped
        );

        public static bool MarkAsSparseFile(FileStream fileStream)
        {
            int bytesReturned = 0;
            var lpOverlapped = new NativeOverlapped();
            return DeviceIoControl(
                fileStream.SafeFileHandle,
                590020, //FSCTL_SET_SPARSE,
                IntPtr.Zero,
                0,
                IntPtr.Zero,
                0,
                ref bytesReturned,
                ref lpOverlapped);
        }
    }
}
