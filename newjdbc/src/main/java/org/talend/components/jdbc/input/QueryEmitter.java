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
package org.talend.components.jdbc.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.service.JDBCService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.IntStream;

import static org.talend.sdk.component.api.record.Schema.Type.RECORD;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "datastore-connector")
@Emitter(name = "Input")
@Documentation("JDBC query input")
public class QueryEmitter implements Serializable {

    private static final long serialVersionUID = 1;

    // TODO how to make it both works well for cloud and studio?
    private final JDBCInputConfig configuration;

    private final RecordBuilderFactory recordBuilderFactory;

    private final JDBCService jdbcService;

    @Connection
    private transient java.sql.Connection connection;

    private transient ResultSet resultSet;

    // private final I18nMessage i18n;

    private transient JDBCService.ColumnInfo[] columnInfoList;

    private transient Schema schema;

    public QueryEmitter(@Option("configuration") final JDBCInputConfig configuration, final JDBCService jdbcService,
            final RecordBuilderFactory recordBuilderFactory/* .final I18nMessage i18nMessage */) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jdbcService = jdbcService;
        // this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        if (connection == null) {
            try {
                connection = jdbcService.createConnection(configuration.getDataSet().getDataStore());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        try {
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery(configuration.getDataSet().getSqlQuery());

            ResultSetMetaData metaData = resultSet.getMetaData();
            Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(RECORD);
            columnInfoList = IntStream.rangeClosed(1, metaData.getColumnCount())
                    .mapToObj(index -> jdbcService.addField(schemaBuilder, metaData, index))
                    .toArray(JDBCService.ColumnInfo[]::new);
            schema = schemaBuilder.build();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Producer
    public Record next() throws SQLException {
        if (!resultSet.next()) {
            return null;
        }

        final Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(schema);
        for (int index = 0; index < columnInfoList.length; index++) {
            jdbcService.addColumn(recordBuilder, columnInfoList[index], resultSet.getObject(index + 1));
        }
        return recordBuilder.build();
    }

    @PreDestroy
    public void release() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
