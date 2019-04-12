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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;

namespace DataSpreads.Tests
{
    public static class TestUtils
    {
        public static bool InDocker = Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") == "true";

        public static string BaseDataPath => InDocker ? "/TestData" : "G:/localdata/tmp/TestData";

        public static void ClearAll([CallerFilePath]string groupPath = null)
        {
            var group = Path.GetFileNameWithoutExtension(groupPath);
            var path = Path.Combine(BaseDataPath, group);
            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
        }

        public static string GetPath(
            [CallerMemberName]string testPath = null,
            [CallerFilePath]string groupPath = null,
            bool clear = true)
        {
            var group =
                InDocker
                ? groupPath.Split("\\").Last().Replace(".cs", "")
                : Path.GetFileName(Path.GetFileNameWithoutExtension(groupPath));
            var path = Path.Combine(BaseDataPath, group, testPath);
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
            else if (clear)
            {
                var di = new DirectoryInfo(path);

                foreach (FileInfo file in di.GetFiles())
                {
                    file.Delete();
                }
                foreach (DirectoryInfo dir in di.GetDirectories())
                {
                    dir.Delete(true);
                }
            }

            return path;
        }

        public static long GetBenchCount(long count = 1_000_000, long debugCount = -1)
        {
#if DEBUG
            if (debugCount <= 0)
            {
                return 100;
            }

            return debugCount;
#else
            return count;
#endif
        }
    }
}
