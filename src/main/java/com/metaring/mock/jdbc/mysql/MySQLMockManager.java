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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.metaring.framework.util.StringUtil;
import com.metaring.java.process.fork.JavaChildProcess;
import com.metaring.java.process.fork.JavaProcessFork;

class MySQLMockManager {

    private static String LOCAL_MYSQL_BIN_LOCATION;

    static MySQLMockedDatabaseInfo mock(Properties properties) throws SQLException {

        MySQLMockedDatabaseInfo mockedDatabaseInfo = new MySQLMockedDatabaseInfo(properties);

        String dbName = properties.getProperty(MySQLMockPropertyEnum.NAME.getPropertyName());

        if (StringUtil.isNullOrEmpty(dbName)) {
            throw new SQLException(String.format("Missing mandatory configuration parameter '%s'.", MySQLMockPropertyEnum.NAME.getPropertyName()));
        }

        int mockPort = -1;
        try {
            mockPort = Integer.parseInt(properties.get(MySQLMockPropertyEnum.MOCK_PORT.getPropertyName()).toString());
        }
        catch (Exception e) {
        }

        if (mockPort == -1) {
            throw new SQLException(String.format("Missing mandatory configuration parameter '%s'.", MySQLMockPropertyEnum.MOCK_PORT.getPropertyName()));
        }

        int originalPort = -1;
        try {
            originalPort = Integer.parseInt(properties.get(MySQLMockPropertyEnum.ORIGINAL_PORT.getPropertyName()).toString());
        }
        catch (Exception e) {
        }

        if (mockPort == originalPort) {
            throw new SQLException(String.format("Mock port %d cannot be the same as the original port.", mockPort));
        }

        String localMySqlBinLocation = properties.getProperty(MySQLMockPropertyEnum.BIN_LOCATION.getPropertyName());

        if (StringUtil.isNullOrEmpty(localMySqlBinLocation)) {
            localMySqlBinLocation = LOCAL_MYSQL_BIN_LOCATION;
        }

        if (StringUtil.isNullOrEmpty(localMySqlBinLocation)) {
            throw new SQLException(String.format("Missing mandatory configuration parameter '%s'.", MySQLMockPropertyEnum.BIN_LOCATION.getPropertyName()));
        }

        String binLocation = localMySqlBinLocation.replace("\\", "/");

        if (binLocation.startsWith("\"")) {
            binLocation = binLocation.substring(1);
        }
        if (binLocation.endsWith("\"")) {
            binLocation = binLocation.substring(0, binLocation.length() - 1);
        }

        if (!binLocation.endsWith("/")) {
            binLocation += "/";
        }

        if (!new File(binLocation + "mysqldump").exists() && !new File(binLocation + "mysqldump.exe").exists() && !new File(binLocation + "mysqldump.sh").exists()) {
            throw new RuntimeException("Missing mysqldump in " + localMySqlBinLocation);
        }

        if (!new File(binLocation + "mysql").exists() && !new File(binLocation + "mysql.exe").exists() && !new File(binLocation + "mysql.sh").exists()) {
            throw new RuntimeException("Missing mysql in " + localMySqlBinLocation);
        }

        if (!new File(binLocation + "mysqld").exists() && !new File(binLocation + "mysqld.exe").exists() && !new File(binLocation + "mysqld.sh").exists()) {
            String linux = "";
            if (!JavaChildProcess.IS_WINDOWS) {
                linux = "\n(In Linux distros you can try to solve with command:\n\nsudo ln -s /usr/sbin/mysqld /usr/bin/mysqld\n\n)";
            }
            throw new RuntimeException("Missing mysqld in " + localMySqlBinLocation + linux);
        }

        if (!new File(binLocation + "mysql_upgrade").exists() && !new File(binLocation + "mysql_upgrade.exe").exists() && !new File(binLocation + "mysql_upgrade.sh").exists()) {
            throw new RuntimeException("Missing mysql_upgrade in " + localMySqlBinLocation);
        }

        if (StringUtil.isNullOrEmpty(LOCAL_MYSQL_BIN_LOCATION)) {
            LOCAL_MYSQL_BIN_LOCATION = localMySqlBinLocation;
        }

        String orignalTemporaryFolder = null;
        try {
            orignalTemporaryFolder = properties.getProperty(MySQLMockPropertyEnum.TEMP_FOLDER.getPropertyName(), Files.createTempDirectory("mysqlmock").toFile().getAbsolutePath());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        String temporaryFolder = orignalTemporaryFolder.replace("\\", "/");

        if (temporaryFolder.contains(" ")) {
            throw new RuntimeException(String.format("Use of temporary directory path with spaces ('%s') is discouraged in MySQL Mock.\nPlease use the '%s' property or the '%s' Core property to specify an empty temporary path without spaces.", orignalTemporaryFolder, MySQLMockPropertyEnum.TEMP_FOLDER.getPropertyName(), MySQLMockPropertyEnum.CFG_TEMP_FOLDER));
        }

        if (!temporaryFolder.endsWith("/")) {
            temporaryFolder += "/";
        }

        File temporaryFile = new File(temporaryFolder);

        if (temporaryFile.exists()) {
            if (!temporaryFile.isDirectory()) {
                throw new RuntimeException(String.format("Temporary path %s does not represent a directory.", orignalTemporaryFolder));
            }
            if (temporaryFile.listFiles().length > 0) {
                throw new RuntimeException(String.format("Temporary directory %s is not empty.", orignalTemporaryFolder));
            }
        }
        temporaryFile.mkdirs();

        mockedDatabaseInfo.dumpDBCommands = dumpDBCommands(temporaryFile, properties, binLocation);

        mockedDatabaseInfo.dumpDB = dumpDB(mockedDatabaseInfo.dumpDBCommands, mockedDatabaseInfo.dbName);

        mockedDatabaseInfo.tempInstance = createTempInstance(temporaryFile, binLocation, mockPort);

        loadDB(temporaryFile, binLocation, mockPort, mockedDatabaseInfo.dumpDB, properties);

        return mockedDatabaseInfo;
    }

    private static final String[] dumpDBCommands(File rootFolder, Properties properties, String binLocation) {

        LinkedList<String> arguments = new LinkedList<>();

        arguments.add(JavaChildProcess.QUOTES + binLocation + "mysqldump" + JavaChildProcess.QUOTES);

        if (!StringUtil.isNullOrEmpty(properties.getProperty(MySQLMockPropertyEnum.ORIGINAL_HOST.getPropertyName(), ""))) {
            arguments.add("-h");
            arguments.add(properties.getProperty(MySQLMockPropertyEnum.ORIGINAL_HOST.getPropertyName()));
        }

        if (!StringUtil.isNullOrEmpty(properties.get(MySQLMockPropertyEnum.ORIGINAL_PORT.getPropertyName()))) {
            arguments.add("-P");
            arguments.add(properties.get(MySQLMockPropertyEnum.ORIGINAL_PORT.getPropertyName()).toString());
        }

        if (!StringUtil.isNullOrEmpty(properties.getProperty(MySQLMockPropertyEnum.USER.getPropertyName(), ""))) {
            arguments.add("-u");
            arguments.add(properties.getProperty(MySQLMockPropertyEnum.USER.getPropertyName()));
        }

        if (!StringUtil.isNullOrEmpty(properties.get(MySQLMockPropertyEnum.PASSWORD.getPropertyName()))) {
            arguments.add("-p" + properties.getProperty(MySQLMockPropertyEnum.PASSWORD.getPropertyName()));
        }

        arguments.add("--routines=true");
        arguments.add("--events");
        arguments.add("-d");
        arguments.add(properties.getProperty(MySQLMockPropertyEnum.NAME.getPropertyName()));

        return arguments.toArray(new String[arguments.size()]);
    }

    private static final String dumpDB(String[] dumpDBCommand, String dbName) {

        String output;
        boolean error = false;

        try {
            Process process = Runtime.getRuntime().exec(dumpDBCommand);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String dump = null;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("DROP DATABASE IF EXISTS ").append(dbName).append(";\n\n");
            stringBuilder.append("CREATE DATABASE ").append(dbName).append(";\n\n");
            stringBuilder.append("USE ").append(dbName).append(";\n\n");
            while ((dump = bufferedReader.readLine()) != null) {
                stringBuilder.append(dump);
                stringBuilder.append("\n");
            }
            StringBuilder errorBuilder = new StringBuilder();
            if (process.getErrorStream().available() > 0) {
                try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));) {
                    while ((dump = errorReader.readLine()) != null) {
                        if (!dump.contains("Using a password")) {
                            error = true;
                            errorBuilder.append(dump);
                            errorBuilder.append("\n");
                        }
                        if (!dump.contains("Using a password")) {
                            System.err.println(dump);
                        }
                    }
                }
            }
            process.destroy();
            output = error ? errorBuilder.toString() : stringBuilder.toString().replaceAll("DEFINER=[a-zA-Z0-9._%-`]+@[`a-zA-Z0-9.-_%]+", "DEFINER=`root`@`localhost`").trim();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (error) {
            throw new RuntimeException("Error from mysqldump procedure:\n\n" + output);
        }
        return output;
    }

    private static final JavaProcessFork createTempInstance(File rootFolder, String binLocation, int port) throws SQLException {

        JavaProcessFork javaProcessFork = null;
        try {
            Path rootPath = rootFolder.toPath();
            String rootPathString = rootFolder.getAbsolutePath().replace("\\", "/");
            if (!rootPathString.endsWith("/")) {
                rootPathString += "/";
            }

            Path dataPath = rootPath.resolve("data");
            File dataFolder = dataPath.toFile();
            dataFolder.mkdirs();
            String dataPathString = dataFolder.getAbsolutePath().replace("\\", "/");
            if (!dataPathString.endsWith("/")) {
                dataPathString += "/";
            }

            File socketFile = rootPath.resolve("mysql.sock").toFile();
            String socketPathString = socketFile.getAbsolutePath().replace("\\", "/");

            File pidFile = rootPath.resolve("mysql.pid").toFile();
            String pidPathString = pidFile.getAbsolutePath().replace("\\", "/");

            pidFile.deleteOnExit();
            socketFile.deleteOnExit();
            dataFolder.deleteOnExit();
            rootFolder.deleteOnExit();

            try (ZipInputStream zIS = new ZipInputStream(MySQLMockManager.class.getClassLoader().getResourceAsStream("mysql.zip"))) {
                ZipEntry zipEntry = null;
                while ((zipEntry = zIS.getNextEntry()) != null) {
                    Path zipEntryPath = dataPath.resolve(zipEntry.getName());
                    if (zipEntry.isDirectory()) {
                        zipEntryPath.toFile().mkdirs();
                    }
                    else {
                        Files.copy(zIS, zipEntryPath);
                    }
                }
            }

            LinkedList<String> arguments = new LinkedList<>();

            arguments.add(JavaChildProcess.QUOTES + binLocation + "mysqld" + JavaChildProcess.QUOTES);

            arguments.add("-h");
            arguments.add(JavaChildProcess.QUOTES + dataPathString + JavaChildProcess.QUOTES);

            arguments.add("--socket=" + JavaChildProcess.QUOTES + socketPathString + JavaChildProcess.QUOTES);

            arguments.add("--pid-file=" + JavaChildProcess.QUOTES + pidPathString + JavaChildProcess.QUOTES);

            arguments.add("-P");
            arguments.add("" + port);

            arguments.add("--explicit_defaults_for_timestamp");

            Process process = Runtime.getRuntime().exec(arguments.toArray(new String[arguments.size()]));

            try {
                Thread.sleep(2500);
            }
            catch (Exception e) {
            }

            arguments.clear();

            arguments.add(JavaChildProcess.QUOTES + binLocation + "mysql_upgrade" + JavaChildProcess.QUOTES);

            arguments.add("-u");
            arguments.add("root");

            arguments.add("--socket=" + JavaChildProcess.QUOTES + socketPathString + JavaChildProcess.QUOTES);

            arguments.add("-P");
            arguments.add("" + port);

            arguments.add("--force");

            Runtime.getRuntime().exec(arguments.toArray(new String[arguments.size()])).waitFor();

            try {
                process.destroy();
            }
            catch (Exception e) {
            }

            socketFile.delete();

            pidFile.delete();

            arguments.clear();

            arguments.add(JavaChildProcess.QUOTES + binLocation + "mysqld" + JavaChildProcess.QUOTES);

            arguments.add("-h");
            arguments.add(JavaChildProcess.QUOTES + dataPathString + JavaChildProcess.QUOTES);

            arguments.add("--socket=" + JavaChildProcess.QUOTES + socketPathString + JavaChildProcess.QUOTES);

            arguments.add("--pid-file=" + JavaChildProcess.QUOTES + pidPathString + JavaChildProcess.QUOTES);

            arguments.add("-P");
            arguments.add("" + port);

            arguments.add("--explicit_defaults_for_timestamp");

            javaProcessFork = JavaProcessFork.fork(arguments.toArray(new String[arguments.size()]), rootFolder);

            if (!JavaChildProcess.IS_WINDOWS) {
                try {
                    System.err.println("UNIX Systems need a preventive wait before MySQL Mock procedure end. Sleeping 3.5 secs...");
                    Thread.sleep(3500);
                }
                catch (Exception e) {
                }
            }
        }
        catch (Exception e) {
            throw new SQLException(e);
        }
        return javaProcessFork;
    }

    private static final void loadDB(File rootFolder, String binLocation, int port, String dumpDB, Properties properties) {

        Path rootPath = rootFolder.toPath();

        Path sqlPath = rootPath.resolve("dump.sql");

        try {

            Files.write(sqlPath, dumpDB.getBytes());

            File socketFile = rootPath.resolve("mysql.sock").toFile();
            String socketPathString = socketFile.getAbsolutePath().replace("\\", "/");

            List<String> arguments = new ArrayList<>();

            arguments.add(JavaChildProcess.QUOTES + binLocation + "mysql" + JavaChildProcess.QUOTES);

            arguments.add("-u");
            arguments.add("root");

            arguments.add("--socket=" + JavaChildProcess.QUOTES + socketPathString + JavaChildProcess.QUOTES);

            arguments.add("-P");
            arguments.add("" + port);

            ProcessBuilder processBuilder = new ProcessBuilder(arguments.toArray(new String[arguments.size()]));
            processBuilder.redirectInput(ProcessBuilder.Redirect.from(sqlPath.toFile()));
            Process process = processBuilder.start();
            process.waitFor();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
