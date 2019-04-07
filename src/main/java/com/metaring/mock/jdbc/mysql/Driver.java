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

import java.net.InetAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import com.metaring.framework.Core;
import com.metaring.framework.type.DataRepresentation;
import com.metaring.framework.util.CryptoUtil;
import com.metaring.framework.util.StringUtil;

public class Driver implements java.sql.Driver {

    private static final String DEFAULT_MYSQL_ORIGINAL_DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

    static final java.sql.Driver MYSQL_ORIGINAL_DRIVER;

    final Map<String, MySQLMockedDatabaseInfo> alreadyMockedDatabases = new HashMap<>();

    static {
        MYSQL_ORIGINAL_DRIVER = getMySQLOriginalDriver();
        try {
            DriverManager.registerDriver(new Driver());
        }
        catch (SQLException ex) {
            throw new RuntimeException("Can't register driver", ex);
        }
    }

    public Driver() {
    }

    @SuppressWarnings("unchecked")
    private static final java.sql.Driver getMySQLOriginalDriver() {
        String mySQLOriginalDriverClassName = DEFAULT_MYSQL_ORIGINAL_DRIVER_CLASS_NAME;
        try {
            mySQLOriginalDriverClassName = Core.SYSKB.getText(MySQLMockPropertyEnum.CFG_MYSQL_ORIGNAL_DRIVER_CLASS_NAME);
        }
        catch (Exception e) {
        }
        if(mySQLOriginalDriverClassName == null) {
            mySQLOriginalDriverClassName = DEFAULT_MYSQL_ORIGINAL_DRIVER_CLASS_NAME;
        }
        java.sql.Driver mySQLOriginalDriver = null;
        Class<? extends java.sql.Driver> mySQLOriginalDriverClass = null;

        try {
            mySQLOriginalDriverClass = (Class<? extends java.sql.Driver>) Class.forName(mySQLOriginalDriverClassName);
            if (mySQLOriginalDriverClass == null) {
                throw new RuntimeException(String.format("Error while loading MySQL Original Driver class name, '%s'.", mySQLOriginalDriverClassName));
            }
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(String.format("MySQL Original Driver class name, '%s', not found.", mySQLOriginalDriverClassName), e);
        }
        try {
            Enumeration<java.sql.Driver> driverManagerDrivers = DriverManager.getDrivers();
            while (driverManagerDrivers.hasMoreElements()) {
                java.sql.Driver driverManagerDriver = driverManagerDrivers.nextElement();
                if (driverManagerDriver.getClass().equals(mySQLOriginalDriverClass)) {
                    mySQLOriginalDriver = driverManagerDriver;
                    break;
                }
            }
            if (mySQLOriginalDriver == null) {
                mySQLOriginalDriver = mySQLOriginalDriverClass.newInstance();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(String.format("Error while instantiating MySQL orignal Driver class, '%s'.", mySQLOriginalDriverClassName), e);
        }
        return mySQLOriginalDriver;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Properties resolvedProperties = resolveProperties(url, info);
        if (resolvedProperties == null) {
            return null;
        }
        MySQLMockedDatabaseInfo mockedDatabaseInfo = alreadyMockedDatabases.get(resolvedProperties.getProperty(MySQLMockPropertyEnum.MOCK_KEY.getPropertyName()));
        if (mockedDatabaseInfo == null) {
            mockedDatabaseInfo = MySQLMockManager.mock(resolvedProperties);
            alreadyMockedDatabases.put(resolvedProperties.getProperty(MySQLMockPropertyEnum.MOCK_KEY.getPropertyName()), mockedDatabaseInfo);
        }

        return MYSQL_ORIGINAL_DRIVER.connect(mockedDatabaseInfo.mockedUrl, cleanProperties(resolvedProperties));
    }

    private final Properties resolveProperties(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        Properties resolvedProperties = new Properties();
        if (info != null) {
            resolvedProperties.putAll(info);
        }
        try {

            URI uri = new URI(url.replace("jdbc:mysql:mock://", "http://"));

            String host = uri.getHost();
            if (StringUtil.isNullOrEmpty(host)) {
                host = "localhost";
            }
            resolvedProperties.putIfAbsent(MySQLMockPropertyEnum.ORIGINAL_HOST.getPropertyName(), host);

            String path = uri.getPath();
            if (!StringUtil.isNullOrEmpty(path)) {
                path = path.substring(1);
            }
            else {
                path = "";
            }
            resolvedProperties.putIfAbsent(MySQLMockPropertyEnum.NAME.getPropertyName(), path);
            try {
                String queryParamsLocationString = "/" + path + "?";
                int queryParamsLocation = url.indexOf(queryParamsLocationString) + queryParamsLocationString.length();
                String[] queryParams = url.substring(queryParamsLocation).split("&");
                for (String queryParam : queryParams) {
                    try {
                        String[] queryParamSplit = queryParam.split("=");
                        String name = queryParamSplit[0];
                        String value = queryParamSplit[1];
                        resolvedProperties.putIfAbsent(name, URLDecoder.decode(value, CryptoUtil.CHARSET_UTF_8.name()));
                    }
                    catch (Exception e) {
                    }
                }
            }
            catch (Exception e) {
            }

            int port = uri.getPort();
            if (port != -1) {
                resolvedProperties.put(MySQLMockPropertyEnum.ORIGINAL_PORT.getPropertyName(), port);
            }

            path = resolvedProperties.getProperty(MySQLMockPropertyEnum.NAME.getPropertyName());

            resolvedProperties.put(MySQLMockPropertyEnum.MOCK_KEY.getPropertyName(), String.format("%s:%d:%s", InetAddress.getByName(host).getHostAddress(), Integer.parseInt(resolvedProperties.get(MySQLMockPropertyEnum.MOCK_PORT.getPropertyName()).toString()), path));

            return resolvedProperties;
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private final Properties cleanProperties(Properties resolvedProperties) {
        Properties cleanProperties = new Properties();
        if (resolvedProperties != null) {
            cleanProperties.putAll(resolvedProperties);
        }
        for (MySQLMockPropertyEnum mockPropertyEnum : MySQLMockPropertyEnum.values()) {
            cleanProperties.remove(mockPropertyEnum.getPropertyName());
        }
        cleanProperties.put(MySQLMockPropertyEnum.USER.getPropertyName(), "root");
        return cleanProperties;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return !StringUtil.isNullOrEmpty(url) && url.startsWith("jdbc:mysql:mock://") && !url.equalsIgnoreCase("jdbc:mysql:mock://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return MYSQL_ORIGINAL_DRIVER.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return MYSQL_ORIGINAL_DRIVER.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return MYSQL_ORIGINAL_DRIVER.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return MYSQL_ORIGINAL_DRIVER.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return MYSQL_ORIGINAL_DRIVER.getParentLogger();
    }

    static final Connection getConfigurationConnection() throws SQLException {
        Properties properties = new Properties();
        DataRepresentation persistenceConfiguration = Core.SYSKB.get("persistence");
        for (String key : persistenceConfiguration.getProperties()) {
            properties.put(key, persistenceConfiguration.getText(key));
        }
        if (persistenceConfiguration.hasProperty(MySQLMockPropertyEnum.CFG_MOCK_PORT)) {
            properties.put(MySQLMockPropertyEnum.MOCK_PORT.getPropertyName(), persistenceConfiguration.getDigit(MySQLMockPropertyEnum.CFG_MOCK_PORT));
        }
        return DriverManager.getConnection(properties.getProperty(MySQLMockPropertyEnum.URL.getPropertyName()), properties);
    }
}
