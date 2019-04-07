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

import com.metaring.framework.util.StringUtil;

enum MySQLMockPropertyEnum {

    URL("url"),

    MOCK_PORT("mockPort"),

    BIN_LOCATION("binLocation"),

    ORIGINAL_HOST("originalHost"),

    ORIGINAL_PORT("port"),

    USER("user"),

    PASSWORD("password"),

    NAME("name"),

    TEMP_FOLDER("tempFolder"),

    MOCK_KEY("mockKey");

    private String propertyName;

    private MySQLMockPropertyEnum(String propertyName) {
        this.propertyName = propertyName;
    }

    public String getPropertyName() {
        return this.propertyName;
    }

    public static MySQLMockPropertyEnum getByPropertyName(String propertyName) {
        if (StringUtil.isNullOrEmpty(propertyName)) {
            return null;
        }
        for (MySQLMockPropertyEnum mySQLMockPropertyEnum : MySQLMockPropertyEnum.values()) {
            if (mySQLMockPropertyEnum.getPropertyName().equalsIgnoreCase(propertyName)) {
                return mySQLMockPropertyEnum;
            }
        }
        return null;
    }

    static final String CFG_MYSQL_ORIGNAL_DRIVER_CLASS_NAME = MySQLMockPropertyEnum.class.getPackage().getName() + ".original.driver_class";
    static final String CFG_MOCK_PORT = MySQLMockPropertyEnum.class.getPackage().getName() + "." + MOCK_PORT.getPropertyName();
    static final String CFG_BIN_LOCATION = MySQLMockPropertyEnum.class.getPackage().getName() + "." + BIN_LOCATION.getPropertyName();
    static final String CFG_TEMP_FOLDER = MySQLMockPropertyEnum.class.getPackage().getName() + "." + TEMP_FOLDER.getPropertyName();
}
