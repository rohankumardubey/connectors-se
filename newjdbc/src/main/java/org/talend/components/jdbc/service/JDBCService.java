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
package org.talend.components.jdbc.service;

import com.zaxxer.hikari.HikariDataSource;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.dataset.JDBCQueryDataSet;
import org.talend.components.jdbc.datastore.JDBCDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.connection.CloseConnection;
import org.talend.sdk.component.api.service.connection.CloseConnectionObject;
import org.talend.sdk.component.api.service.connection.CreateConnection;
import org.talend.sdk.component.api.service.dependency.Resolver;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.talend.sdk.component.api.record.Schema.Type.*;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

@Slf4j
@Service
public class JDBCService implements Serializable {

    private static final long serialVersionUID = 1;

    // TODO get the classloader tool to use maven gav pathes to load the jdbc driver jars classes dynamiclly
    @Service
    private transient Resolver resolver;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Suggestions("GUESS_DRIVER_CLASS")
    public SuggestionValues loadRecordTypes(@Option final List<String> driverJars) throws Exception {
        final List<SuggestionValues.Item> items = new ArrayList<>();

        // items.add(new SuggestionValues.Item("com.mysql.cj.jdbc.Driver", "com.mysql.cj.jdbc.Driver"));

        getDriverClasses(driverJars).stream().forEach(driverClass -> {
            items.add(new SuggestionValues.Item(driverClass, driverClass));
        });

        return new SuggestionValues(true, items);
    }

    private List<String> getDriverClasses(List<String> driverJars) throws IOException {
        // TODO check it if right
        List<String> driverClasses = new ArrayList<>();

        try {
            List<URL> urls = new ArrayList<>();
            for (String maven_path : driverJars) {
                URL url = new URL(removeQuote(maven_path));
                urls.add(url);
            }

            // TODO before this, should register mvn protocol for : new URL("mvn:foo/bar");
            // tck should already support that and provide some way to do that
            // but if not, we can use tcompv0 way
            URLClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[0]), this.getClass().getClassLoader());

            for (URL jarUrl : urls) {
                try (JarInputStream jarInputStream = new JarInputStream(jarUrl.openStream())) {
                    JarEntry nextJarEntry = jarInputStream.getNextJarEntry();
                    while (nextJarEntry != null) {
                        boolean isFile = !nextJarEntry.isDirectory();
                        if (isFile) {
                            String name = nextJarEntry.getName();
                            if (name != null && name.toLowerCase().endsWith(".class")) {
                                String className = changeFileNameToClassName(name);
                                try {
                                    Class clazz = classLoader.loadClass(className);
                                    if (Driver.class.isAssignableFrom(clazz)) {
                                        driverClasses.add(clazz.getName());
                                    }
                                } catch (Throwable th) {
                                    // ignore all the exceptions, especially the class not found exception when look up
                                    // a class
                                    // outside the jar
                                }
                            }
                        }

                        nextJarEntry = jarInputStream.getNextJarEntry();
                    }
                }
            }
        } catch (IOException ex) {
            // TODO process
            throw ex;
        }

        if (driverClasses.isEmpty()) {
            // TODO process
            throw new RuntimeException("");
        }

        return driverClasses;
    }

    private String changeFileNameToClassName(String name) {
        name = name.replace('/', '.');
        name = name.replace('\\', '.');
        name = name.substring(0, name.length() - 6);
        return name;
    }

    private String removeQuote(String content) {
        if (content.startsWith("\"") && content.endsWith("\"")) {
            return content.substring(1, content.length() - 1);
        }

        return content;
    }

    @HealthCheck("CheckConnection")
    public HealthCheckStatus validateBasicDataStore(@Option final JDBCDataStore dataStore) {
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, "success message, TODO, i18n");
    }

    @CreateConnection
    public Connection createConnection(@Option("configuration") final JDBCDataStore dataStore) throws SQLException {
        return createJDBCConnection(dataStore).getConnection();
    }

    @CloseConnection
    public CloseConnectionObject closeConnection() {
        // TODO create jdbc connection
        return new CloseConnectionObject() {

            public boolean close() {
                // TODO close connection here
                Optional.ofNullable(this.getConnection())
                        .map(Connection.class::cast)
                        .ifPresent(conn -> {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                // TODO
                            }
                        });
                return true;
            }

        };
    }

    @Suggestions("FETCH_TABLES")
    public SuggestionValues fetchTables(@Option final JDBCDataStore dataStore) throws SQLException {
        final List<SuggestionValues.Item> items = new ArrayList<>();

        getSchemaNames(dataStore).stream().forEach(tableName -> {
            items.add(new SuggestionValues.Item(tableName, tableName));
        });

        return new SuggestionValues(true, items);
    }

    public List<String> getSchemaNames(final JDBCDataStore dataStore) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Connection conn = createConnection(dataStore)) {
            DatabaseMetaData dbMetaData = conn.getMetaData();

            Set<String> tableTypes = getAvailableTableTypes(dbMetaData);

            String database_schema = getDatabaseSchema(dataStore);

            try (ResultSet resultset =
                    dbMetaData.getTables(null, database_schema, null, tableTypes.toArray(new String[0]))) {
                while (resultset.next()) {
                    String tableName = resultset.getString("TABLE_NAME");
                    if (tableName == null) {
                        tableName = resultset.getString("SYNONYM_NAME");
                    }
                    result.add(tableName);
                }
            }
        } catch (SQLException e) {
            // TODO process it
            throw e;
        }
        return result;
    }

    /**
     * get database schema for database special
     * 
     * @return
     */
    private String getDatabaseSchema(final JDBCDataStore dataStore) {
        // TODO fetch it from dataStore
        String jdbc_url = "";
        String username = "";
        if (jdbc_url != null && username != null && jdbc_url.contains("oracle")) {
            return username.toUpperCase();
        }
        return null;
    }

    private Set<String> getAvailableTableTypes(DatabaseMetaData dbMetaData) throws SQLException {
        Set<String> availableTableTypes = new HashSet<String>();
        List<String> neededTableTypes = Arrays.asList("TABLE", "VIEW", "SYNONYM");

        try (ResultSet rsTableTypes = dbMetaData.getTableTypes()) {
            while (rsTableTypes.next()) {
                String currentTableType = rsTableTypes.getString("TABLE_TYPE");
                if (currentTableType == null) {
                    currentTableType = "";
                }
                currentTableType = currentTableType.trim();
                if ("BASE TABLE".equalsIgnoreCase(currentTableType)) {
                    currentTableType = "TABLE";
                }
                if (neededTableTypes.contains(currentTableType)) {
                    availableTableTypes.add(currentTableType);
                }
            }
        }

        return availableTableTypes;
    }

    public JDBCDataSource createJDBCConnection(final JDBCDataStore dataStore) throws SQLException {
        return new JDBCDataSource(this.resolver, dataStore);
    }

    // copy from tck jdbc connector for cloud, TODO now for fast development, will unify them to one
    public static class JDBCDataSource implements AutoCloseable {

        private final Resolver.ClassLoaderDescriptor classLoaderDescriptor;

        private final HikariDataSource dataSource;

        public JDBCDataSource(final Resolver resolver,
                final JDBCDataStore dataStore) {
            final Thread thread = Thread.currentThread();
            final ClassLoader prev = thread.getContextClassLoader();

            List<org.talend.components.jdbc.common.Driver> drivers = dataStore.getJdbcDriver();
            List<String> paths = Optional.ofNullable(drivers)
                    .orElse(Collections.emptyList())
                    .stream()
                    .map(driver -> driver.getPath())
                    .collect(Collectors.toList());

            classLoaderDescriptor = resolver.mapDescriptorToClassLoader(paths);

            try {
                thread.setContextClassLoader(classLoaderDescriptor.asClassLoader());
                dataSource = new HikariDataSource();
                dataSource.setJdbcUrl(dataStore.getJdbcUrl());
                dataSource.setDriverClassName(dataStore.getJdbcClass());

                // TODO consider no auth case
                dataSource.setUsername(dataStore.getUserId());
                dataSource.setPassword(dataStore.getPassword());

                dataSource.setAutoCommit(dataStore.isUseAutoCommit() && dataStore.isAutoCommit());

                dataSource.setMaximumPoolSize(1);

                // mysql special property?
                dataSource.addDataSourceProperty("rewriteBatchedStatements", "true");
                // Security Issues with LOAD DATA LOCAL https://jira.talendforge.org/browse/TDI-42001
                dataSource.addDataSourceProperty("allowLoadLocalInfile", "false"); // MySQL
                dataSource.addDataSourceProperty("allowLocalInfile", "false"); // MariaDB
            } finally {
                thread.setContextClassLoader(prev);
            }
        }

        public Connection getConnection() throws SQLException {
            final Thread thread = Thread.currentThread();
            final ClassLoader prev = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(classLoaderDescriptor.asClassLoader());
                return wrap(classLoaderDescriptor.asClassLoader(), dataSource.getConnection(), Connection.class);
            } finally {
                thread.setContextClassLoader(prev);
            }
        }

        @Override
        public void close() {
            final Thread thread = Thread.currentThread();
            final ClassLoader prev = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(classLoaderDescriptor.asClassLoader());
                dataSource.close();
            } finally {
                thread.setContextClassLoader(prev);
                try {
                    classLoaderDescriptor.close();
                } catch (final Exception e) {
                    log.error("can't close driver classloader properly", e);
                }
            }
        }

        private static <T> T wrap(final ClassLoader classLoader, final Object delegate, final Class<T> api) {
            return api
                    .cast(
                            Proxy
                                    .newProxyInstance(classLoader, new Class<?>[] { api },
                                            new ContextualDelegate(delegate, classLoader)));
        }

        @AllArgsConstructor
        private static class ContextualDelegate implements InvocationHandler {

            private final Object delegate;

            private final ClassLoader classLoader;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                final Thread thread = Thread.currentThread();
                final ClassLoader prev = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);
                try {
                    final Object invoked = method.invoke(delegate, args);
                    if (method.getReturnType().getName().startsWith("java.sql.")
                            && method.getReturnType().isInterface()) {
                        return wrap(classLoader, invoked, method.getReturnType());
                    }
                    return invoked;
                } catch (final InvocationTargetException ite) {
                    throw ite.getTargetException();
                } finally {
                    thread.setContextClassLoader(prev);
                }
            }
        }
    }

    @DiscoverSchema(value = "JDBCQueryDataSet")
    public Schema guessSchema(@Option final JDBCQueryDataSet dataSet) throws SQLException {
        try (JDBCDataSource dataSource = this.createJDBCConnection(dataSet.getDataStore())) {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(dataSet.getSqlQuery());

            ResultSetMetaData metaData = resultSet.getMetaData();
            Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(RECORD);
            IntStream.rangeClosed(1, metaData.getColumnCount())
                    .mapToObj(index -> addField(schemaBuilder, metaData, index))
                    .toArray(JDBCService.ColumnInfo[]::new);
            Schema schema = schemaBuilder.build();
            return schema;
        }
    }

    @Suggestions("FETCH_COLUMN_NAMES")
    public SuggestionValues fetchColumnNames(JDBCDataStore dataStore, String tableName) {
        if (true)
            throw new RuntimeException("i am running");
        return null;
    }

    @Getter
    @AllArgsConstructor
    public static class ColumnInfo {

        private final String columnName;

        private final int sqlType;

        private final boolean isNullable;
    }

    // TODO use studio db mapping files to decide the type convert
    public ColumnInfo addField(final Schema.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final int sqlType = metaData.getColumnType(columnIndex);
            final String columnName = metaData.getColumnLabel(columnIndex);// select field as f1, 1 as f2, here return
                                                                           // f1 and f2

            final String dbColumnName = metaData.getColumnName(columnIndex);// select field as f1 from t1, here return
                                                                            // "field"?
            // TODO use talend db mapping files to do type convert to talend type
            final String columnTypeName = metaData.getColumnTypeName(columnIndex).toUpperCase();
            final int size = metaData.getPrecision(columnIndex);
            final int scale = metaData.getScale(columnIndex);

            final boolean ignorePrecision = false;
            final boolean ignoreScale = false;

            final boolean isNullable = metaData.isNullable(columnIndex) != ResultSetMetaData.columnNoNulls;
            final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder()
                    .withName(columnName)
                    // .withRawName(columnName)//no need this as withName will do it
                    .withNullable(isNullable)
                    .withProp("talend.studio.key", "false")// as metadata from sql query, not table metadata, so no key
                                                           // info or no need
            ;
            switch (sqlType) {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
                if (javaType.equals(Integer.class.getName()) || Short.class.getName().equals(javaType)) {
                    entryBuilder.withType(INT).withProp("talend.studio.type", "id_Integer");
                    withPrecision(entryBuilder, ignorePrecision, size);
                } else {
                    entryBuilder.withType(LONG).withProp("talend.studio.type", "id_Long");
                    withPrecision(entryBuilder, ignorePrecision, size);
                }

                break;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                entryBuilder.withType(FLOAT).withProp("talend.studio.type", "id_Float");
                withPrecision(entryBuilder, ignorePrecision, size);
                withScale(entryBuilder, ignoreScale, scale);
                break;
            case java.sql.Types.DOUBLE:
                entryBuilder.withType(DOUBLE).withProp("talend.studio.type", "id_Double");
                withPrecision(entryBuilder, ignorePrecision, size);
                withScale(entryBuilder, ignoreScale, scale);
                break;
            case java.sql.Types.BOOLEAN:
                entryBuilder.withType(BOOLEAN).withProp("talend.studio.type", "id_Boolean");
                break;
            case java.sql.Types.TIME:
                entryBuilder.withType(DATETIME)
                        .withProp("talend.studio.type", "id_Date")
                        .withProp("talend.studio.pattern", "HH:mm:ss");
                withPrecision(entryBuilder, ignorePrecision, size);
                withScale(entryBuilder, ignoreScale, scale);
                break;
            case java.sql.Types.DATE:
                entryBuilder.withType(DATETIME)
                        .withProp("talend.studio.type", "id_Date")
                        .withProp("talend.studio.pattern", "yyyy-MM-dd");
                withPrecision(entryBuilder, ignorePrecision, size);
                withScale(entryBuilder, ignoreScale, scale);
                break;
            case java.sql.Types.TIMESTAMP:
                entryBuilder.withType(DATETIME)
                        .withProp("talend.studio.type", "id_Date")
                        .withProp("talend.studio.pattern", "yyyy-MM-dd HH:mm:ss");
                withPrecision(entryBuilder, ignorePrecision, size);
                withScale(entryBuilder, ignoreScale, scale);
                break;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                entryBuilder.withType(BYTES).withProp("talend.studio.type", "id_byte[]");
                withPrecision(entryBuilder, ignorePrecision, size);
                break;
            case java.sql.Types.BIGINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                entryBuilder.withType(STRING).withProp("talend.studio.type", "id_BigDecimal");
                withPrecision(entryBuilder, ignorePrecision, size);
                withScale(entryBuilder, ignoreScale, scale);
                break;
            case java.sql.Types.CHAR:
                entryBuilder.withType(STRING).withProp("talend.studio.type", "id_Character");
                withPrecision(entryBuilder, ignorePrecision, size);
                break;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
                entryBuilder.withType(STRING).withProp("talend.studio.type", "id_String");
                withPrecision(entryBuilder, ignorePrecision, size);
                withScale(entryBuilder, ignoreScale, scale);
            default:
                entryBuilder.withType(STRING).withProp("talend.studio.type", "id_String");
                break;
            }

            builder.withEntry(entryBuilder.build());

            log.warn("[addField] {} {} {}.", columnName, javaType, sqlType);
            return new ColumnInfo(columnName, sqlType, isNullable);
        } catch (final SQLException e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    private Schema.Entry.Builder withPrecision(Schema.Entry.Builder builder, boolean ignorePrecision, int precision) {
        if (ignorePrecision) {
            return builder;
        }

        builder.withProp("talend.studio.length", String.valueOf(precision));

        return builder;
    }

    private Schema.Entry.Builder withScale(Schema.Entry.Builder builder, boolean ignoreScale, int scale) {
        if (ignoreScale) {
            return builder;
        }

        builder.withProp("talend.studio.precision", String.valueOf(scale));
        return builder;
    }

    public void addColumn(final Record.Builder builder, final ColumnInfo columnInfo, final Object value) {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder()
                .withName(columnInfo.getColumnName())
                .withNullable(columnInfo.isNullable());
        switch (columnInfo.getSqlType()) {
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
        case java.sql.Types.INTEGER:
            if (value != null) {
                if (value instanceof Integer) {
                    builder.withInt(entryBuilder.withType(INT).build(), (Integer) value);
                } else if (value instanceof Short) {
                    builder.withInt(entryBuilder.withType(INT).build(), ((Short) value).intValue());
                } else {
                    builder.withLong(entryBuilder.withType(LONG).build(), Long.parseLong(value.toString()));
                }
            }
            break;
        case java.sql.Types.FLOAT:
        case java.sql.Types.REAL:
            if (value != null) {
                builder.withFloat(entryBuilder.withType(FLOAT).build(), (Float) value);
            }
            break;
        case java.sql.Types.DOUBLE:
            if (value != null) {
                builder.withDouble(entryBuilder.withType(DOUBLE).build(), (Double) value);
            }
            break;
        case java.sql.Types.BOOLEAN:
            if (value != null) {
                builder.withBoolean(entryBuilder.withType(BOOLEAN).build(), (Boolean) value);
            }
            break;
        case java.sql.Types.DATE:
            builder
                    .withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Date) value).getTime()));
            break;
        case java.sql.Types.TIME:
            builder
                    .withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Time) value).getTime()));
            break;
        case java.sql.Types.TIMESTAMP:
            builder
                    .withDateTime(entryBuilder.withType(DATETIME).build(),
                            value == null ? null : new Date(((java.sql.Timestamp) value).getTime()));
            break;
        case java.sql.Types.BINARY:
        case java.sql.Types.VARBINARY:
        case java.sql.Types.LONGVARBINARY:
            builder.withBytes(entryBuilder.withType(BYTES).build(), value == null ? null : (byte[]) value);
            break;
        case java.sql.Types.BIGINT:
        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CHAR:
        default:
            builder.withString(entryBuilder.withType(STRING).build(), value == null ? null : String.valueOf(value));
            break;
        }
    }
}
