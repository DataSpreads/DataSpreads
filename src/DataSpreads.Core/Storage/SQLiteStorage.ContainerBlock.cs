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

using Spreads.SQLite;

namespace DataSpreads.Storage
{
    // ReSharper disable once InconsistentNaming
    internal partial class SQLiteStorage
    {
        private const string ContainerDataBlockTable = "container_data_blocks";

        private static void CreateContainerBlockTable(SqliteConnection connection)
        {
            string commandText;
            SqliteCommand command;
            commandText =
#pragma warning disable HAA0201 // Implicit string concatenation allocation
                $"CREATE TABLE IF NOT EXISTS `{ContainerDataBlockTable}` (\n" +
                "  `StreamId`        INTEGER NOT NULL,\n" +
                "  `BlockKey`        INTEGER NOT NULL,\n" +
                "  `ColumnId`        INTEGER DEFAULT 0,\n" + // the only addition to stream block table
                "  `Timestamp`       INTEGER NOT NULL,\n" +
                "  `Count`           INTEGER NOT NULL,\n" +
                "  `UncompressedLen` INTEGER NOT NULL,\n" +
                "  `WriteEnd`        INTEGER DEFAULT 0,\n" + // TODO review/delete
                "  `BlobCrc32`       INTEGER NOT NULL,\n" +
                "  `RawData`         BLOB,\n" +
                "  PRIMARY KEY (`StreamId`,`BlockKey`,`ColumnId`));\n" +
                $" CREATE INDEX IF NOT EXISTS stream_id_timestamp_index ON `{ContainerDataBlockTable}` (`StreamId`, `Timestamp`);";
#pragma warning restore HAA0201 // Implicit string concatenation allocation

            command = connection.CreateCommand();
            command.CommandText = commandText;
            command.ExecuteNonQuery();
            command.Dispose();
        }

        public partial class StorageQueries
        {
            private void CreateContainerBlockQueries(SqliteConnection connection, ConnectionPool pool)
            {
            }
        }
    }
}
