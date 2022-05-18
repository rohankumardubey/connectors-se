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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.service.JDBCService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;

@Slf4j
@Getter
@Version(1)
@Processor(name = "OutputBulkExec")
@Icon(value = Icon.IconType.CUSTOM, custom = "datastore-connector")
@Documentation("JDBC Output Bulk Exec component")
// TODO this class is a compose component in studio, but now we no need the compose way, we reuse model/runtime here, is
// good too
public class JDBCOutputBulkExecProcessor implements Serializable {

    private final JDBCOutputBulkExecConfig configuration;

    private final JDBCService jdbcService;

    private transient RecordBuilderFactory recordBuilderFactory;

    private transient JDBCBulkFileWriter writer;

    private transient JDBCBulkExecRuntime runtime;

    @Connection
    private transient java.sql.Connection connection;

    // private final I18nMessage i18n;

    // private transient boolean init;

    public JDBCOutputBulkExecProcessor(@Option("configuration") final JDBCOutputBulkExecConfig configuration,
            final JDBCService jdbcService, RecordBuilderFactory recordBuilderFactory) {
        this.configuration = configuration;
        this.jdbcService = jdbcService;
        // this.i18n = i18nMessage;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @BeforeGroup
    public void beforeGroup() {

    }

    @ElementListener
    public void elementListener(@Input final Record record)
            throws IOException {
        writer.write(record);
    }

    @AfterGroup
    public void afterGroup() throws SQLException {
    }

    @PostConstruct
    public void init() throws IOException {
        boolean useExistedConnection = false;

        if (connection == null) {
            try {
                connection = jdbcService.createConnection(configuration.getDataSet().getDataStore());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else {
            useExistedConnection = true;
        }

        writer = new JDBCBulkFileWriter(configuration.getDataSet().getSchema(), configuration.getBulkCommonConfig(),
                configuration.isAppend(),
                recordBuilderFactory);
        writer.open();

        runtime = new JDBCBulkExecRuntime(configuration.getDataSet(), configuration.getBulkCommonConfig(),
                useExistedConnection, connection, recordBuilderFactory);
    }

    @PreDestroy
    public void close() throws IOException, SQLException {
        // we import bulk file here to database by sql commmand/or database cmd
        writer.close();

        runtime.runDriver();
    }

}
