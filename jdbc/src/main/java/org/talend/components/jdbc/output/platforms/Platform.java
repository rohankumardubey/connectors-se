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
package org.talend.components.jdbc.output.platforms;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.configuration.RedshiftSortStrategy;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.zaxxer.hikari.HikariDataSource;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.talend.components.jdbc.ErrorFactory.toIllegalStateException;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

@Slf4j
@Getter
public abstract class Platform implements Serializable {

    public static final String APPLICATION = "Talend";

    private final I18nMessage i18n;

    private final JdbcConfiguration.Driver driver;

    protected Platform(final I18nMessage i18n, final JdbcConfiguration.Driver driver) {
        this.driver = driver;
        this.i18n = i18n;
    }

    abstract public String name();

    abstract protected String delimiterToken();

    protected abstract String buildQuery(final Connection connection, final Table table,
            final boolean useOriginColumnName) throws SQLException;

    /**
     * @param e if the exception if a table already exist ignore it. otherwise re throw e
     * @return true if the error is because the table already exist
     */
    protected abstract boolean isTableExistsCreationError(final Throwable e);

    protected String buildUrlFromPattern(final String protocol, final String host, final int port,
            final String database,
            String params) {

        if (!"".equals(params.trim())) {
            params = "?" + params;
        }
        return String.format("%s://%s:%s/%s%s", protocol, host, port, database, params);
    }

    public String buildUrl(final JdbcConnection connection) {

        // final JdbcConnection config = specializeConfiguration(connection);

        // @Datastore/Dataset migration handlers are not called at runtime
        // This is a workaround to detect if we have to get jdbcRul or build it from other fields.
        String host = Optional.ofNullable(connection.getHost()).orElse("");
        String url = Optional.ofNullable(connection.getJdbcUrl()).orElse("");
        if (!connection.getSetRawUrl() && "".equals(host.trim()) && !"".equals(url.trim())) {
            // If true that it's mean that this configuration has been created before v3
            // org.talend.components.jdbc.datastore.JdbcConnection.VERSION < 3
            connection.setSetRawUrl(true);
        }

        if (connection.getSetRawUrl()) {
            return connection.getJdbcUrl();
        }

        final String params = Optional.ofNullable(connection.getParameters())
                .orElse(new ArrayList<>())
                .stream()
                .map(p -> p.getKey() + "=" + p.getValue())
                .collect(Collectors.joining("&"));
        final String protocol =
                connection.getDefineProtocol() ? connection.getProtocol() : this.getDriver().getProtocol();
        final String builtURL = buildUrlFromPattern(protocol, connection.getHost(), connection.getPort(),
                connection.getDatabase(), params);

        return builtURL;
    }

    public void createTableIfNotExist(final Connection connection, final String name, final List<String> keys,
            final RedshiftSortStrategy sortStrategy, final List<String> sortKeys,
            final DistributionStrategy distributionStrategy,
            final List<String> distributionKeys, final int varcharLength, final boolean useOriginColumnName,
            final List<Record> records)
            throws SQLException {
        if (records.isEmpty()) {
            return;
        }
        final Table table =
                getTableModel(connection, name, keys, sortStrategy, sortKeys, distributionStrategy, distributionKeys,
                        varcharLength, records);
        final String sql = buildQuery(connection, table, useOriginColumnName);
        try (final Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (final Throwable e) {
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
            if (!isTableExistsCreationError(e)) {
                log.error("Create Table error for '" + sql + "'", e);
                throw toIllegalStateException(e);
            }

            log
                    .trace("create table issue was ignored. The table and it's name space has been created by an other worker",
                            e);
        }
    }

    public String identifier(final String name) {
        return name == null || name.isEmpty() ? name : delimiterToken() + name + delimiterToken();
    }

    String createPKs(DatabaseMetaData metaData, final String table, final List<Column> primaryKeys)
            throws SQLException {
        return primaryKeys == null || primaryKeys.isEmpty() ? ""
                : ", CONSTRAINT " + pkConstraintName(metaData, table, primaryKeys) + " PRIMARY KEY "
                        + primaryKeys
                                .stream()
                                .map(Column::getOriginalFieldName)
                                .map(this::identifier)
                                .collect(joining(",", "(", ")"));
    }

    protected String pkConstraintName(DatabaseMetaData metaData, String table, List<Column> primaryKeys)
            throws SQLException {
        final String uuid = UUID.randomUUID().toString();
        final int nameLength = metaData.getMaxColumnNameLength();
        String constraint =
                "pk_" + table + "_" + primaryKeys.stream().map(Column::getOriginalFieldName).collect(joining("_")) + "_"
                        + uuid.substring(0, Math.min(4, uuid.length()));
        if (nameLength > 0 && constraint.length() > nameLength) {
            constraint = "pk_" + uuid.replace('-', '_');
            if (constraint.length() > nameLength) {
                constraint = constraint.substring(0, nameLength);
            }
        }
        return constraint;
    }

    protected String isRequired(final Column column) {
        return column.isNullable() && !column.isPrimaryKey() ? "NULL" : "NOT NULL";
    }

    protected Table getTableModel(final Connection connection, final String name, final List<String> keys,
            final RedshiftSortStrategy sortStrategy, final List<String> sortKeys,
            DistributionStrategy distributionStrategy,
            final List<String> distributionKeys, final int varcharLength, final List<Record> records) {
        final Table.TableBuilder builder = Table
                .builder()
                .name(name)
                .distributionStrategy(distributionStrategy)
                .sortStrategy(sortStrategy);
        try {
            builder.catalog(connection.getCatalog()).schema(JdbcService.getSchema(connection));
        } catch (final SQLException e) {
            log.warn("can't get database catalog or schema", e);
        }
        log.debug("Schema getRawName: " + records.get(0)
                .getSchema()
                .getEntries()
                .stream()
                .map(Schema.Entry::getRawName)
                .collect(Collectors.joining(",")));
        final List<Schema.Entry> entries = records
                .stream()
                .flatMap(record -> record.getSchema().getEntries().stream())
                .distinct()
                .collect(toList());
        log.debug("Schema Entries: " + entries);
        return builder
                .columns(entries
                        .stream()
                        .map(entry -> Column
                                .builder()
                                .entry(entry)
                                .primaryKey(keys.contains(entry.getOriginalFieldName()))
                                .sortKey(sortKeys.contains(entry.getOriginalFieldName()))
                                .distributionKey(distributionKeys.contains(entry.getOriginalFieldName()))
                                .size(STRING == entry.getType() ? varcharLength : null)
                                .build())
                        .collect(toList()))
                .build();
    }

    /**
     * Add platform related properties to jdbc connections
     *
     * @param dataSource the data source object to be configured
     */
    public void addDataSourceProperties(final HikariDataSource dataSource) {
        // to be override by impl
    }
}
