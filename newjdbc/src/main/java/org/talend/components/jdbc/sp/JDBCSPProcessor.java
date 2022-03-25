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
package org.talend.components.jdbc.sp;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.row.JDBCRowConfig;
import org.talend.components.jdbc.service.JDBCService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.standalone.DriverRunner;
import org.talend.sdk.component.api.standalone.RunAtDriver;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.SQLException;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "datastore-connector")
@Processor(name = "SP")
@Documentation("JDBC SP component.")
public class JDBCSPProcessor implements Serializable {

    private static final long serialVersionUID = 1;

    private final JDBCSPConfig configuration;

    private final JDBCService service;

    private boolean reuseConnection;

    @Connection
    private transient java.sql.Connection connection;

    public JDBCSPProcessor(@Option("configuration") final JDBCSPConfig configuration,
            final JDBCService service) {
        this.configuration = configuration;
        this.service = service;
    }

    @PostConstruct
    public void init() {

    }

    @ElementListener
    public void elementListener(@Input final Record record, @Output final OutputEmitter<Record> success)
            throws SQLException {

    }

    @PreDestroy
    public void release() {
    }

}