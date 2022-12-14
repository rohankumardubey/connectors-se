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
package org.talend.components.jdbc.containers;

import lombok.experimental.Delegate;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

public class MySQLTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class,
            ContainerState.class }, excludes = AutoCloseable.class)
    private final MySQLContainer container = (MySQLContainer) new MySQLContainer("mysql:8.0.13")
            // https://github.com/testcontainers/testcontainers-java/issues/736
            .withCommand("--default-authentication-plugin=mysql_native_password");

    @Override
    public String getDatabaseType() {
        return "MySQL";
    }

    @Override
    public void close() {
        this.container.close();
    }
}
