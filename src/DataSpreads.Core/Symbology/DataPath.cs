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
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;

// ReSharper disable PrivateFieldCanBeConvertedToLocalVariable

namespace DataSpreads.Symbology
{
    // Note: this is after user/org

    // Runtime representation of DataPath. We store Utf8 on stack instead of strings to avoid allocations.
    // DataPath has 254 bytes limit

    // Last byte is Node count so that by reading from beginning we know when we started to read asset part
    // Otherwise it is the same as Symbol254. Initially byte 254 should be zero to simplify things.

    [DebuggerDisplay("{" + nameof(ToString) + "()}")]
    [StructLayout(LayoutKind.Sequential)]
    internal readonly struct DataPath : IEquatable<DataPath>
    {
        public const char NodeSeparatorChar = '/';
        public const string NodeSeparator = "/";
        public const char AccountSeparatorChar = '@';
        public const string AccountSeparator = "@";
        public const string CurrentAccountPrefix = "~/";
        public const string CurrentAccountAlias = "@my/";

        public const string LocalPrefix = "_/";
       

        private static readonly Regex ValidPathRegex =
            new Regex(@"^(@|~/)?((([a-z0-9_]+)((\.|\-)[a-z0-9_]+)*)[/])*(([a-z0-9_]+)((\.|\-)[a-z0-9_]+)*)?$",
                RegexOptions.Compiled);

        // Canonical path:
        // @user/repo[/sub1[/sub2[...]]][:branch]/data_source.with_dots[:meta]
        //
        // Repo path could be at the end:
        // sub1/data_source@user/repo
        // This works by just rewriting: part starting with @... is placed at the
        // beginning to get canonical path. Simple rewriting implies that
        // repos could be in any part:
        // repo/sub1/data_source@user
        // data_source@user/repo/sub1
        // @user/repo/sub1/data_source
        // This helps in Excel that does not accept strings starting with @.
        // Also this helps with scoping, when part @.. is stored is a variable
        // or an Excel cell. From code we could open Repo at any path and
        // it will automatically add prefixes.

        // Input strings from users could have leading or trailing /,
        // they do not have any meaning and we resolve path depending
        // on a context.

        private readonly string _path;

        internal DataPath(string rawPath)
        {
            _path = rawPath;
        }

        public DataPath(string path, bool isRepo)
        {
            if (!TryValidate(path, out var normalized, out var error))
            {
                throw new ArgumentException(error);
            }
            _path = isRepo ? normalized + NodeSeparatorChar : normalized;
        }

        public bool IsRepo => _path.EndsWith(NodeSeparator);

        public bool IsRooted => IsCurrentAccount || IsExternalAccount || IsLocal;

        public bool IsLocal => _path.StartsWith(LocalPrefix);

        public bool IsCurrentAccount => _path.StartsWith(CurrentAccountPrefix);

        public bool IsExternalAccount => _path.StartsWith(AccountSeparator);

        public bool IsRoot
        {
            get
            {
                var idx = _path.AsSpan(0, IsRepo ? _path.Length - 1 : _path.Length).LastIndexOf(NodeSeparatorChar); ;
                return idx < 0;
            }
        }

        public DataPath Parent
        {
            get
            {
                var idx = _path.AsSpan(0, IsRepo ? _path.Length - 1 : _path.Length).LastIndexOf(NodeSeparatorChar); ;
                if (idx >= 0)
                {
                    var parent = _path.Substring(0, idx + 1);
                    return new DataPath(parent);
                }
                else
                {
                    return new DataPath(string.Empty);
                }
            }
        }

        public DataPath Root
        {
            get
            {
                if (!IsRooted)
                {
                    return new DataPath(string.Empty);
                }

                if (IsRoot)
                {
                    return this;
                }

                var idx = _path.IndexOf(NodeSeparatorChar);
                if (idx >= 0)
                {
                    var parent = _path.Substring(0, idx + 1);
                    return new DataPath(parent);
                }

                return new DataPath(string.Empty);
            }
        }

        public string Name
        {
            get
            {
                var lenExSeparator = IsRepo ? _path.Length - 1 : _path.Length;
                var idx = _path.AsSpan(0, lenExSeparator).LastIndexOf(NodeSeparatorChar); ;
                if (idx >= 0)
                {
                    var name = _path.Substring(idx + 1, lenExSeparator - (idx + 1));
                    return name;
                }
                else
                {
                    return _path;
                }
            }
        }

        [Pure]
        public DataPath Combine(string dataPath, bool isRepo)
        {
            var right = new DataPath(dataPath);
            if (right.IsRooted)
            {
                return right;
            }

            if (!IsRepo)
            {
                throw new InvalidOperationException("Laft part is not repo.");
            }
            return new DataPath(_path + right._path + (isRepo ? NodeSeparator : string.Empty));
        }

        public bool Equals(DataPath other)
        {
            return _path.Equals(other._path);
        }

        public override string ToString()
        {
            return _path;
        }

        public static bool TryValidate(string input, out string normalized, out string error)
        {
            // we do not require /s so remove them and add when needed
            input = input.Trim('/');
            input = input.ToLowerInvariant();

            if (false)
            {
                input = input.Replace('!', '_');
                input = input.Replace('#', '_');
                input = input.Replace('$', '_');
                input = input.Replace('%', '_');
                input = input.Replace('^', '_');
                input = input.Replace('&', '_');
                input = input.Replace('*', '_');
                input = input.Replace('(', '_');
                input = input.Replace(')', '_');
                input = input.Replace('=', '_');
                input = input.Replace(' ', '_');
                input = input.Replace('[', '_');
                input = input.Replace(']', '_');
                input = input.Replace('{', '_');
                input = input.Replace('}', '_');
                input = input.Replace(':', '_');
                input = input.Replace(';', '_');
                input = input.Replace('.', '_');
                input = input.Replace('\'', '_');
                input = input.Replace('"', '_');
                input = input.Replace('|', '_');
                input = input.Replace('\\', '_');
                input = input.Replace('`', '_');
            }
            // TODO Special symbols. Optional to replace them with - or _ instead of throwing

            int idx = -1;
            for (int i = 0; i < input.Length; i++)
            {
                if (input[i] == AccountSeparatorChar)
                {
                    if (idx >= 0)
                    {
                        error = "Data path can have only one @ symbol.";
                        normalized = null;
                        return false;
                    }

                    idx = i;
                }
            }

            if (idx > 0)
            {
                input = input.Substring(idx, input.Length - idx)
                        + NodeSeparatorChar
                        + input.Substring(0, idx);
                input = input.Trim('/');
            }

            Debug.Assert(idx < 0 || input.StartsWith(AccountSeparator));

            if (input.StartsWith(CurrentAccountAlias))
            {
                input = input.Replace(CurrentAccountAlias, CurrentAccountPrefix);
            }

            if (!ValidPathRegex.IsMatch(input))
            {
                normalized = null;
                error = "Invalid data path (RegEx mismatch)";
                return false;
            }

            normalized = input;
            error = null;
            return true;
        }
    }
}
