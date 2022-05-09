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
package org.talend.components.jdbc.row;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.input.JDBCRuntimeUtils;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * JDBC reader for JDBC row
 *
 */
//TODO remove it, as not necessary
@Slf4j
public class JDBCRowReader {

    protected Connection conn;

    protected ResultSet resultSet;

    private PreparedStatement prepared_statement;

    private Statement statement;

    private boolean useExistedConnection;

    private JDBCRowConfig config;

    private boolean useCommit;

    private int commitEvery;

    private Schema outSchema;

    private Schema rejectSchema;

    private Boolean useQueryTimeout;

    private Integer queryTimeout;

    protected final RecordBuilderFactory recordBuilderFactory;

    private int totalCount;

    public JDBCRowReader(JDBCRowConfig config, boolean useExistedConnection, Connection conn,
            RecordBuilderFactory recordBuilderFactory) {
        this.config = config;
        this.useExistedConnection = useExistedConnection;
        this.conn = conn;
        this.recordBuilderFactory = recordBuilderFactory;

        commitEvery = config.getCommitEvery();
        useCommit = !useExistedConnection && commitEvery != 0;

        // TODO now can't get them
        // not like tjdbcinput which can provide runtime schema/output which can get schema from input as a workaround,
        // jdbcrow must have user design schema
        outSchema = null;
        rejectSchema = null;

        useQueryTimeout = config.isUseQueryTimeout();
        queryTimeout = config.getQueryTimeout();
    }

    public void open() throws SQLException {
        log.debug("JDBCRowReader start.");

        // TOOD pass query
    }

    public boolean advance() {
        return false;// only one row
    }

    public Record getCurrent() throws NoSuchElementException, SQLException {
        try {
            boolean usePreparedStatement = config.isUsePreparedStatement();
            String sql = config.getDataSet().getSqlQuery();
            boolean propagateQueryResultSet = config.isPropagateRecordSet();

            if (usePreparedStatement) {
                log.debug("Prepared statement: " + sql);
                prepared_statement = conn.prepareStatement(sql);

                if (usePreparedStatement) {
                    prepared_statement.setQueryTimeout(queryTimeout);
                }

                // TODO correct it
                JDBCRuntimeUtils.setPreparedStatement(prepared_statement, config.getPreparedStatementParameters());

                if (propagateQueryResultSet) {
                    resultSet = prepared_statement.executeQuery();
                } else {
                    prepared_statement.execute();
                }
            } else {
                statement = conn.createStatement();

                if (usePreparedStatement) {
                    statement.setQueryTimeout(queryTimeout);
                }

                log.debug("Executing the query: '{}'", sql);
                if (propagateQueryResultSet) {
                    resultSet = statement.executeQuery(sql);
                } else {
                    statement.execute(sql);
                }
            }

            Record output = handleSuccess(propagateQueryResultSet);

            if (useCommit) {
                log.debug("Committing the transaction.");
                conn.commit();
            }

            return output;
        } catch (SQLException e) {
            if (config.isDieOnError()) {
                throw e;
            } else {
                // no need to print it as we will print the error message in component_begin.javajet for the reader if
                // no reject
                // line
            }

            handleReject(e);
        }
        return null;
    }

    private Record handleSuccess(boolean propagateQueryResultSet) {
        Record.Builder builder = recordBuilderFactory.newRecordBuilder(outSchema);

        if (propagateQueryResultSet) {
            String columnName = config.getRecordSetColumn();
            for (Schema.Entry outField : outSchema.getEntries()) {
                if (outField.getName().equals(columnName)) {
                    builder.with(outField, resultSet);
                }
            }
        }

        return builder.build();
    }

    private void handleReject(SQLException e) {
        Record.Builder builder = recordBuilderFactory.newRecordBuilder(rejectSchema);

        for (Schema.Entry outField : rejectSchema.getEntries()) {
            Object outValue = null;

            if ("errorCode".equals(outField.getName())) {
                outValue = e.getSQLState();
            } else if ("errorMessage".equals(outField.getName())) {
                outValue = e.getMessage();
            }

            builder.with(outField, outValue);
        }

        Record reject = builder.build();

        Map<String, Object> resultMessage = new HashMap<String, Object>();
        resultMessage.put("error", e.getMessage());
        resultMessage.put("errorCode", e.getSQLState());
        resultMessage.put("errorMessage", e.getMessage() + " - Line: " + totalCount);
        resultMessage.put("talend_record", reject);

        // TODO
        // throw new DataRejectException(resultMessage);
    }

    public void close() throws SQLException {
        try {
            if (prepared_statement != null) {
                prepared_statement.close();
                prepared_statement = null;
            }

            if (statement != null) {
                statement.close();
                statement = null;
            }

            if (!useExistedConnection && conn != null) {
                // need to call the commit before close for some database when do some read action like reading the
                // resultset
                if (useCommit) {
                    log.debug("Committing the transaction.");
                    conn.commit();
                }
                log.debug("Closing connection");
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            throw e;
        }
    }

}
