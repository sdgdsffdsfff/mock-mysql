/**
 *    Copyright 2019 MetaRing s.r.l.
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.metaring.mock.jdbc.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class MySQLMockUtilities {

    public static final Connection getConfigurationConnection() throws SQLException {
        return Driver.getConfigurationConnection();
    }

    public static final void truncateDatabase(Connection connection) throws SQLException {
        truncateDatabase(connection, null);
    }

    public static final void truncateDatabase(Connection connection, List<String> afterTruncateQueries) throws SQLException {
        String schemaName = connection.getMetaData().getURL();
        schemaName = schemaName.substring(schemaName.lastIndexOf("/") + 1);
        Statement statement = connection.createStatement();
        statement.executeUpdate("USE " + schemaName);
        statement.executeUpdate("SET FOREIGN_KEY_CHECKS=0");
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet resultSet = databaseMetaData.getTables(connection.getCatalog(), null, "%", new String[] {"TABLE"});
        while (resultSet.next()) {
            statement.executeUpdate(String.format("TRUNCATE TABLE %s.%s", schemaName, resultSet.getString(3)));
        }
        statement.executeUpdate("SET FOREIGN_KEY_CHECKS=1");
        resultSet.close();

        if (afterTruncateQueries != null && afterTruncateQueries.size() > 0) {
            for (String afterTruncateQuery : afterTruncateQueries) {
                statement.executeUpdate(afterTruncateQuery.trim());
            }
        }
        statement.close();
    }
}
