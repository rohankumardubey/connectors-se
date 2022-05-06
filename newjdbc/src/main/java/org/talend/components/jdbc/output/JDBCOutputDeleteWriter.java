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
package org.talend.components.jdbc.output;

import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class JDBCOutputDeleteWriter extends JDBCOutputWriter {

    private String sql;

    public JDBCOutputDeleteWriter(JDBCOutputConfig config, boolean useExistedConnection, Connection conn,
            RecordBuilderFactory recordBuilderFactory) {
        super(config, useExistedConnection, conn, recordBuilderFactory);
    }

    @Override
    public void open() throws SQLException {
        super.open();
        try {
            if (!isDynamic) {
                sql = JDBCSQLBuilder.getInstance().generateSQL4Delete(config.getDataSet().getTableName(), columnList);
                statement = conn.prepareStatement(sql);
            }
        } catch (SQLException e) {
            throw e;
        }

    }

    private RowWriter rowWriter = null;

    private void initRowWriterIfNot(Schema inputSchema) throws SQLException {
        if (rowWriter == null) {
            Schema currentSchema = componentSchema;
            if (isDynamic) {
                try {
                    // TODO currentSchema = CommonUtils.mergeRuntimeSchema2DesignSchema4Dynamic(componentSchema,
                    // inputSchema);
                    currentSchema = inputSchema;
                    columnList = JDBCSQLBuilder.getInstance().createColumnList(config, currentSchema);
                    sql = JDBCSQLBuilder.getInstance()
                            .generateSQL4Delete(config.getDataSet().getTableName(), columnList);
                    statement = conn.prepareStatement(sql);
                } catch (SQLException e) {
                    throw e;
                }
            }

            List<JDBCSQLBuilder.Column> columnList4Statement = new ArrayList<>();
            for (JDBCSQLBuilder.Column column : columnList) {
                if (column.addCol || (column.isReplaced())) {
                    continue;
                }

                if (column.deletionKey) {
                    columnList4Statement.add(column);
                }
            }

            rowWriter = new RowWriter(columnList4Statement, inputSchema, currentSchema, statement,
                    config.isDebugQuery(), sql);
        }
    }

    @Override
    public void write(Record input) throws SQLException {
        super.write(input);

        Schema inputSchema = input.getSchema();

        initRowWriterIfNot(inputSchema);

        try {
            String sql_fact = rowWriter.write(input);
            if (sql_fact != null) {
                // runtime.setComponentData(runtime.getCurrentComponentId(), QUERY_KEY, sql_fact);
            }
            if (config.isDebugQuery()) {
                log.debug("'" + sql_fact.trim() + "'.");
            }
        } catch (SQLException e) {
            throw e;
        }

        try {
            deleteCount += execute(input, statement);
        } catch (SQLException e) {
            if (dieOnError) {
                throw e;
            } else {
                totalCount++;

                System.err.println(e.getMessage());
                log.warn(e.getMessage());
            }

            handleReject(input, e);
        }

        try {
            deleteCount += executeCommit(statement);
        } catch (SQLException e) {
            if (dieOnError) {
                throw e;
            } else {
                log.warn(e.getMessage());
            }
        }
    }

    @Override
    public void close() throws SQLException {
        deleteCount += executeBatchAtLast();

        closeStatementQuietly(statement);
        statement = null;

        commitAndCloseAtLast();

        constructResult();
    }

}
