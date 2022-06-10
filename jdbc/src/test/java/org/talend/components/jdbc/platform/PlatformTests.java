/*
 * Copyright (C) 2006-2022 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.platform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.configuration.JdbcConfiguration.Defaults;
import org.talend.components.jdbc.configuration.JdbcConfiguration.Driver;
import org.talend.components.jdbc.configuration.JdbcConfiguration.KeyVal;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.DeltaLakePlatform;
import org.talend.components.jdbc.output.platforms.DerbyPlatform;
import org.talend.components.jdbc.output.platforms.MSSQLPlatform;
import org.talend.components.jdbc.output.platforms.MariaDbPlatform;
import org.talend.components.jdbc.output.platforms.OraclePlatform;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.SnowflakePlatform;
import org.talend.components.jdbc.service.I18nMessage;

import lombok.extern.slf4j.Slf4j;

public class PlatformTests {

    private Driver driver;

    private JdbcConnection connection;

    @BeforeEach
    public void before() {
        JdbcConfiguration.Driver driver = new Driver();
        driver.setId("test");
        driver.setClassName("a.fake.ClassName");
        driver.setPaths(Collections.emptyList());
        driver.setProtocol("jdbc:test");

        Defaults defaults = new JdbcConfiguration.Defaults();
        defaults.setHost("talend.local");
        defaults.setPort(1234);
        defaults.setDatabase("mydatbase");
        defaults.setParameters(Collections.emptyList());
        driver.setDefaults(defaults);

        this.driver = driver;

        connection = new JdbcConnection();
        connection.setDatabase("connection.database");
        connection.setHost("connection.talend.local");
        connection.setPort(21);

        List<KeyVal> params = new ArrayList<>();
        params.add(new KeyVal("param1", "aaa aaaa"));
        params.add(new KeyVal("param2", "bbbb"));
        params.add(new KeyVal("param3", "cccc"));
        connection.setParameters(params);
    }

    @Test
    public void buildUrlDefaultForceProtocol() {
        Platform platform = new MariaDbPlatform(null, driver);

        connection.setSetRawUrl(false);
        connection.setDefineProtocol(true);
        connection.setProtocol("jdbc:forced");
        final String urlForced = platform.buildUrl(connection);
        Assertions.assertEquals(
                "jdbc:forced://connection.talend.local:21/connection.database?param1=aaa aaaa&param2=bbbb&param3=cccc",
                urlForced);

        connection.setSetRawUrl(false);
        connection.setDefineProtocol(false);
        connection.setProtocol("jdbc:forced");
        final String url = platform.buildUrl(connection);
        Assertions.assertEquals(
                "jdbc:test://connection.talend.local:21/connection.database?param1=aaa aaaa&param2=bbbb&param3=cccc",
                url);
    }

    @Test
    public void buildUrlDefault() {
        Platform platform = new MariaDbPlatform(null, driver);
        final String url = platform.buildUrl(connection);
        Assertions.assertEquals(
                "jdbc:test://connection.talend.local:21/connection.database?param1=aaa aaaa&param2=bbbb&param3=cccc",
                url);

        // No param
        connection.setParameters(Collections.emptyList());
        final String urlNoParam = platform.buildUrl(connection);
        Assertions.assertEquals("jdbc:test://connection.talend.local:21/connection.database", urlNoParam);
    }

    @Test
    public void buildUrlSnowflake() {
        Platform platform = new SnowflakePlatform(null, driver);
        final String url = platform.buildUrl(connection);
        Assertions.assertEquals(
                "jdbc:test://connection.talend.local:21/?db=connection.database&param1=aaa aaaa&param2=bbbb&param3=cccc",
                url);

        // No param
        connection.setParameters(Collections.emptyList());
        final String urlNoParam = platform.buildUrl(connection);
        Assertions.assertEquals("jdbc:test://connection.talend.local:21/?db=connection.database", urlNoParam);
    }

    @Test
    public void buildUrlOracleTest() {
        Platform platform = new OraclePlatform(null, driver);
        final String url = platform.buildUrl(connection);
        Assertions.assertEquals(
                "jdbc:test:@connection.talend.local:21:connection.database?param1=aaa aaaa&param2=bbbb&param3=cccc",
                url);

        // No param
        connection.setParameters(Collections.emptyList());
        final String urlNoParam = platform.buildUrl(connection);
        Assertions.assertEquals("jdbc:test:@connection.talend.local:21:connection.database", urlNoParam);
    }

    @Test
    public void buildUrlMSSQLTest() {
        Platform platform = new MSSQLPlatform(null, driver);
        final String url = platform.buildUrl(connection);
        Assertions.assertEquals(
                "jdbc:test://connection.talend.local:21;databaseName=connection.database;param1=aaa aaaa;param2=bbbb;param3=cccc",
                url);

        // No param
        connection.setParameters(Collections.emptyList());
        final String urlNoParam = platform.buildUrl(connection);
        Assertions.assertEquals("jdbc:test://connection.talend.local:21;databaseName=connection.database", urlNoParam);
    }

    @Test
    public void buildUrlDeltalakeTest() {
        Platform platform = new DeltaLakePlatform(null, driver);
        final String url = platform.buildUrl(connection);
        Assertions.assertEquals(
                "jdbc:test://connection.talend.local:21/connection.database;param1=aaa aaaa;param2=bbbb;param3=cccc",
                url);

        // No param
        connection.setParameters(Collections.emptyList());
        final String urlNoParam = platform.buildUrl(connection);
        Assertions.assertEquals("jdbc:test://connection.talend.local:21/connection.database", urlNoParam);
    }

    @Test
    public void buildUrlDerbyTest() {
        Platform platform = new DerbyPlatform(null, driver);
        final String url = platform.buildUrl(connection);
        Assertions.assertEquals(
                "jdbc:test://connection.talend.local:21/connection.database;param1=aaa aaaa;param2=bbbb;param3=cccc",
                url);

        // No Param
        connection.setParameters(Collections.emptyList());
        final String urlNoParam = platform.buildUrl(connection);
        Assertions.assertEquals("jdbc:test://connection.talend.local:21/connection.database", urlNoParam);
    }

}
