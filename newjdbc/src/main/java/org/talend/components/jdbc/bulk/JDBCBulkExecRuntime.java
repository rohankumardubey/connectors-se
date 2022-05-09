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
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * JDBC bulk exec runtime execution object
 *
 */
@Slf4j
public class JDBCBulkExecRuntime {

    public JDBCBulkExecConfig config;

    private boolean useExistedConnection;

    private Connection conn;

    private RecordBuilderFactory recordBuilderFactory;

    // TODO how to get it from model? here no input record
    private Schema designSchema;

    public JDBCBulkExecRuntime(JDBCBulkExecConfig config, boolean useExistedConnection, Connection conn,
            RecordBuilderFactory recordBuilderFactory) {
        // log.debug("Parameters: [{}]", "");//TODO
        this.config = config;
        this.useExistedConnection = useExistedConnection;
        this.conn = conn;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    private String createBulkSQL() {
        StringBuilder sb = new StringBuilder();

        sb.append("LOAD DATA LOCAL INFILE '")
                .append(config.getBulkCommonConfig().getBulkFile())
                .append("' INTO TABLE ")
                .append(config.getDataSet().getTableName())
                .append(" FIELDS TERMINATED BY '")
                .append(config.getBulkCommonConfig().getFieldSeparator())
                .append("' ");
        if (config.getBulkCommonConfig().isSetTextEnclosure()) {
            sb.append("OPTIONALLY ENCLOSED BY '").append(config.getBulkCommonConfig().getTextEnclosure()).append("' ");
        }
        sb.append("LINES TERMINATED BY '").append(config.getBulkCommonConfig().getRowSeparator()).append("' ");
        if (config.getBulkCommonConfig().isSetNullValue()) {
            sb.append("NULL DEFINED BY '").append(config.getBulkCommonConfig().getNullValue()).append("' ");
        }

        // if design schema is empty, no need to fill column settings, TODO now can't get design schema
        if (designSchema == null) {
            return sb.toString();
        }

        List<Schema.Entry> fields = designSchema.getEntries();

        if (fields == null || fields.isEmpty()) {
            return sb.toString();
        }

        // TODO support dynamic
        sb.append('(');
        for (int i = 0; i < fields.size(); i++) {
            Schema.Entry field = fields.get(i);
            String originName = field.getRawName();
            String headerName = (originName == null || "".equals(originName)) ? field.getName() : originName;
            sb.append('`').append(headerName).append('`');
            if (i != fields.size() - 1) {
                sb.append(',');
            }
        }
        sb.append(')');
        return sb.toString();
    }

    public void runDriver() throws SQLException {
        try {
            try (Statement stmt = conn.createStatement()) {
                String bulkSql = createBulkSQL();
                log.debug("Executing the query: '{}'", bulkSql);
                stmt.execute(bulkSql);
            }
        } catch (Exception ex) {
            // TODO
        } finally {
            if (!useExistedConnection) {
                try {
                    log.debug("Closing connection");
                    conn.close();
                } catch (SQLException e) {
                    throw e;
                }
            }
        }
    }

}
