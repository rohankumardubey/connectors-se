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
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC row runtime execution object
 *
 */
@Slf4j
public class JDBCRowStandalone {

    private static final long serialVersionUID = 1L;

    protected JDBCRowConfig config;

    protected Connection conn;

    private boolean useExistedConnection;

    private boolean useCommit;

    private Integer commitEvery;

    private Boolean useQueryTimeout;

    private Integer queryTimeout;

    protected final RecordBuilderFactory recordBuilderFactory;

    public JDBCRowStandalone(JDBCRowConfig config, boolean useExistedConnection, Connection conn,
            RecordBuilderFactory recordBuilderFactory) {
        // log.debug("Parameters: [{}]",getLogString(properties));
        this.config = config;
        this.useExistedConnection = useExistedConnection;
        this.conn = conn;
        this.recordBuilderFactory = recordBuilderFactory;

        commitEvery = config.getCommitEvery();
        useCommit = !useExistedConnection && commitEvery != 0;

        useQueryTimeout = config.isUseQueryTimeout();
        if (useQueryTimeout) {
            queryTimeout = config.getQueryTimeout();
        }
    }

    public void runDriver() throws SQLException {
        // TODO pass query

        String sql = config.getDataSet().getSqlQuery();
        boolean usePreparedStatement = config.isUsePreparedStatement();
        boolean dieOnError = config.isDieOnError();
        boolean detectErrorOnMultipleSQL = config.isDetectErrorWhenMultiStatements();

        Connection conn = null;
        // TODO move forward
        log.debug("Connection attempt to '{}' with the username '{}'", config.getDataSet().getDataStore().getJdbcUrl(),
                config.getDataSet().getDataStore().getUserId());

        try {
            if (usePreparedStatement) {
                log.debug("Prepared statement: " + sql);
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    if (useQueryTimeout) {
                        pstmt.setQueryTimeout(queryTimeout);
                    }
                    JDBCRuntimeUtils.setPreparedStatement(pstmt, config.getPreparedStatementParameters());
                    pstmt.execute();
                    // In order to retrieve all the error messages, the method 'getMoreResults' needs to be called in
                    // loop.
                    // https://docs.oracle.com/en/java/javase/11/docs/api/java.sql/java/sql/Statement.html#getMoreResults()
                    if (detectErrorOnMultipleSQL) {
                        while (pstmt.getMoreResults() || pstmt.getLargeUpdateCount() != -1)
                            ;
                    }
                }
            } else {
                try (Statement stmt = conn.createStatement()) {
                    if (useQueryTimeout) {
                        stmt.setQueryTimeout(queryTimeout);
                    }
                    log.debug("Executing the query: '{}'", sql);
                    stmt.execute(sql);
                    if (detectErrorOnMultipleSQL) {
                        while (stmt.getMoreResults() || stmt.getLargeUpdateCount() != -1)
                            ;
                    }
                }
            }

            if (useCommit) {
                log.debug("Committing the transaction.");
                conn.commit();
            }
        } catch (Exception ex) {
            if (dieOnError) {
                // TODO
            } else {
                // TODO
            }
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
