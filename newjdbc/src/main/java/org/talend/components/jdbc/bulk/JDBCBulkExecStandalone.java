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
package org.talend.components.jdbc.bulk;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.service.JDBCService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.standalone.DriverRunner;
import org.talend.sdk.component.api.standalone.RunAtDriver;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.SQLException;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "datastore-connector")
@DriverRunner(name = "BulkExec")
@Documentation("JDBC Bulk Exec component.")
public class JDBCBulkExecStandalone implements Serializable {

    private static final long serialVersionUID = 1;

    private final JDBCBulkExecConfig configuration;

    private final JDBCService service;

    private final RecordBuilderFactory recordBuilderFactory;

    private transient JDBCBulkExecRuntime runtime;

    @Connection
    private transient java.sql.Connection connection;

    public JDBCBulkExecStandalone(@Option("configuration") final JDBCBulkExecConfig configuration,
            final JDBCService service, final RecordBuilderFactory recordBuilderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @PostConstruct
    public void init() {
        boolean useExistedConnection = false;

        if (connection == null) {
            try {
                connection = service.createConnection(configuration.getDataSet().getDataStore());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            useExistedConnection = true;
        }

        runtime = new JDBCBulkExecRuntime(configuration.getDataSet(), configuration.getBulkCommonConfig(),
                useExistedConnection, connection, recordBuilderFactory);
    }

    @RunAtDriver
    public void run() throws SQLException {
        runtime.runDriver();
    }

    @PreDestroy
    public void release() {
    }

}