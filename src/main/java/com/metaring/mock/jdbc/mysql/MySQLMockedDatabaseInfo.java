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

import java.util.Properties;

import com.metaring.java.process.fork.JavaProcessFork;

class MySQLMockedDatabaseInfo {

    Properties mockProperties;

    String mockedUrl;

    String dbName;

    String[] dumpDBCommands;

    String dumpDB;

    JavaProcessFork tempInstance;

    MySQLMockedDatabaseInfo(Properties mockProperties) {
        this.mockProperties = mockProperties;
        this.dbName = mockProperties.getProperty(MySQLMockPropertyEnum.NAME.getPropertyName());
        this.mockedUrl = mockUrl();
    }

    private final String mockUrl() {
        int port = -1;
        try {
            port = Integer.parseInt(mockProperties.get(MySQLMockPropertyEnum.MOCK_PORT.getPropertyName()).toString());
        }
        catch(Exception e) {}
        String portString = port == -1 ? "" : ":" + port;
        return String.format("jdbc:mysql://localhost%s/%s", portString, dbName);
    }
}
