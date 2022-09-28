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
import org.talend.components.jdbc.common.DBType;
import org.talend.components.jdbc.dataset.JDBCQueryDataSet;
import org.talend.components.jdbc.dataset.JDBCTableDataSet;
import org.talend.components.jdbc.datastore.JDBCDataStore;
import org.talend.components.jdbc.schema.CommonUtils;
import org.talend.components.jdbc.schema.Dbms;
import org.talend.components.jdbc.schema.JDBCTableMetadata;
import org.talend.components.jdbc.schema.SchemaInferer;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.context.RuntimeContext;
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

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;

@Slf4j
@Service
public class JDBCService implements Serializable {

    private static final long serialVersionUID = 1;

    // TODO get the classloader tool to use maven gav pathes to load the jdbc driver jars classes dynamiclly
    @Service
    private transient Resolver resolver;

    // this should be used in @CreateConnection and @CloseConnection action method,
    // that method should not be code called outside of JDBCService
    @RuntimeContext
    private transient Map<String, Object> context;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Suggestions("GUESS_DRIVER_CLASS")
    public SuggestionValues loadRecordTypes(@Option final List<org.talend.components.jdbc.common.Driver> driverJars)
            throws Exception {
        final List<SuggestionValues.Item> items = new ArrayList<>();

        // items.add(new SuggestionValues.Item("com.mysql.cj.jdbc.Driver", "com.mysql.cj.jdbc.Driver"));

        getDriverClasses(driverJars).stream().forEach(driverClass -> {
            items.add(new SuggestionValues.Item(driverClass, driverClass));
        });

        return new SuggestionValues(true, items);
    }

    private List<String> getDriverClasses(List<org.talend.components.jdbc.common.Driver> driverJars)
            throws IOException {
        // TODO check it if right
        List<String> driverClasses = new ArrayList<>();

        List<String> paths = Optional.ofNullable(driverJars)
                .orElse(Collections.emptyList())
                .stream()
                .map(driver -> convertMvnPath2TckPath(driver.getPath()))
                .collect(Collectors.toList());

        List<URL> urls = resolver.resolveFromDescriptor(paths).stream().map(file -> {
            try {
                return file.toURI().toURL();
            } catch (MalformedURLException e) {
                // TODO process it
                e.printStackTrace();
                return null;
            }
        }).collect(Collectors.toList());

        try {
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
        try (final DataSourceWrapper dataSource = this.createConnection(dataStore, false);
                final Connection ignored = dataSource.getConnection()) {

        } catch (Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, "success message, TODO, i18n");
    }

    @CreateConnection
    public DataSourceWrapper createConnection(@Option("configuration") final JDBCDataStore dataStore)
            throws SQLException {
        return createConnectionOrGetFromSharedConnectionPoolOrDataSource(dataStore, context, false);
    }

    @CloseConnection
    public CloseConnectionObject closeConnection() {
        return new CloseConnectionObject() {

            public boolean close() {
                Optional.ofNullable(this.getConnection())
                        .map(DataSourceWrapper.class::cast)
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

    private List<String> getSchemaNames(final JDBCDataStore dataStore) throws SQLException {
        List<String> result = new ArrayList<>();
        try (final DataSourceWrapper dataSource = createConnection(dataStore, false);
                final Connection conn = dataSource.getConnection()) {
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
        String jdbc_url = dataStore.getJdbcUrl();
        String username = dataStore.getUserId();
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

    public DataSourceWrapper createConnection(final JDBCDataStore dataStore, final boolean readonly)
            throws SQLException {
        // TODO check this readonly before: conn = createConnection(dataStore, readonly);
        JDBCDataSource dataSource = new JDBCDataSource(this.resolver, dataStore);
        Connection conn = dataSource.getConnection();
        // somebody add it for performance for dataprep
        if (readonly) {
            try {
                conn.setReadOnly(true);// TODO check why we get the value by "setting.isReadOnly()" before, here now
                // use "true" directly
            } catch (SQLException e) {
                log.debug("JDBC driver '{}' does not support read only mode.", dataStore.getJdbcClass(), e);
            }
        }

        return new DataSourceWrapper(dataSource, conn);
    }

    private static final String GLOBAL_CONNECTION_POOL_KEY = "GLOBAL_CONNECTION_POOL";

    private static final String KEY_DB_DATASOURCES_RAW = "KEY_DB_DATASOURCES_RAW";

    public DataSourceWrapper createConnectionOrGetFromSharedConnectionPoolOrDataSource(final JDBCDataStore dataStore,
            final Map<String, Object> context, final boolean readonly) throws SQLException {
        Connection conn = null;
        log.debug("Connection attempt to '{}' with the username '{}'", dataStore.getJdbcUrl(), dataStore.getUserId());

        if (dataStore.isUseSharedDBConnection()) {
            SharedConnectionsPool sharedConnectionPool = (SharedConnectionsPool) context
                    .get(GLOBAL_CONNECTION_POOL_KEY);
            log.debug("Uses shared connection with name: '{}'", dataStore.getSharedDBConnectionName());
            log.debug("Connection URL: '{}', User name: '{}'", dataStore.getJdbcUrl(), dataStore.getUserId());
            try {
                conn = sharedConnectionPool.getDBConnection(dataStore.getJdbcClass(), dataStore.getJdbcUrl(),
                        dataStore.getUserId(),
                        dataStore.getPassword(), dataStore.getSharedDBConnectionName());
                return new DataSourceWrapper(null, conn);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("class not found: " + e.getMessage());
            }
        } else if (dataStore.isUseDataSource()) {
            java.util.Map<String, DataSource> dataSources = (java.util.Map<String, javax.sql.DataSource>) context
                    .get(KEY_DB_DATASOURCES_RAW);
            if (dataSources != null) {
                DataSource datasource = dataSources.get(dataStore.getDataSourceAlias());
                if (datasource == null) {
                    throw new RuntimeException(
                            "No DataSource with alias: " + dataStore.getDataSourceAlias() + " available!");
                }
                conn = datasource.getConnection();
                if (conn == null) {
                    throw new RuntimeException("Unable to get a pooled database connection from pool");
                }

                return new DataSourceWrapper(null, conn);
            } else {
                return createConnection(dataStore, false);
            }
        } else {
            return createConnection(dataStore, readonly);
        }
    }

    // studio will pass like this : mvn:mysql/mysql-connector-java/8.0.18/jar
    // but tck here expect like this : mysql:mysql-connector-java:jar:8.0.18
    private static String convertMvnPath2TckPath(String mvnPath) {
        if (mvnPath == null) {
            return null;
        }
        if (mvnPath.startsWith("mvn:")) {
            return mvnPath.substring(4, mvnPath.lastIndexOf('/')).replace('/', ':');
        }

        return mvnPath;
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
                    .map(driver -> convertMvnPath2TckPath(driver.getPath()))
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

    public class DataSourceWrapper implements AutoCloseable {

        private JDBCDataSource dataSource;

        private Connection connection;

        public DataSourceWrapper(JDBCDataSource dataSource, Connection connection) {
            this.dataSource = dataSource;
            this.connection = connection;
        }

        public Connection getConnection() throws SQLException {
            if (connection != null) {
                return connection;
            }

            if (dataSource != null) {
                return dataSource.getConnection();
            }

            return null;
        }

        @Override
        public void close() throws SQLException {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }

            if (dataSource != null) {
                dataSource.close();
            }
        }

    }

    @DiscoverSchema(value = "JDBCQueryDataSet")
    public Schema guessSchemaByQuery(@Option final JDBCQueryDataSet dataSet) throws SQLException {
        // TODO provide a way to get the mapping files in studio platform, also this should work for cloud platform
        // no this for cloud platform
        URL mappingFileDir = null;

        // TODO dbTypeInComponentSetting exist for tjdbcinput, how to pass it?
        DBType dbTypeInComponentSetting = null;

        Dbms mapping = null;
        if (mappingFileDir != null) {
            mapping = CommonUtils.getMapping(mappingFileDir, dataSet.getDataStore(), null, dbTypeInComponentSetting);
        }

        try (final DataSourceWrapper dataSource = createConnection(dataSet.getDataStore(), false);
                final Connection conn = dataSource.getConnection()) {
            try (Statement statement = conn.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(dataSet.getSqlQuery())) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    return SchemaInferer.infer(recordBuilderFactory, metaData, mapping);
                }
            }
        }
    }

    @DiscoverSchema(value = "JDBCTableDataSet")
    public Schema guessSchemaByTable(@Option final JDBCTableDataSet dataSet) throws SQLException {
        // TODO provide a way to get the mapping files in studio platform, also this should work for cloud platform
        // no this for cloud platform
        URL mappingFileDir = null;

        // TODO dbTypeInComponentSetting exist for tjdbcinput, how to pass it?
        DBType dbTypeInComponentSetting = null;

        Dbms mapping = null;
        if (mappingFileDir != null) {
            mapping = CommonUtils.getMapping(mappingFileDir, dataSet.getDataStore(), null, dbTypeInComponentSetting);
        }

        try (final DataSourceWrapper dataSource = createConnection(dataSet.getDataStore(), false);
                final Connection conn = dataSource.getConnection()) {
            JDBCTableMetadata tableMetadata = new JDBCTableMetadata();
            tableMetadata.setDatabaseMetaData(conn.getMetaData()).setTablename(dataSet.getTableName());

            Schema schema = SchemaInferer.infer(recordBuilderFactory, tableMetadata, mapping);

            return schema;
        }
    }

    @Suggestions("FETCH_COLUMN_NAMES")
    public SuggestionValues fetchColumnNames(JDBCDataStore dataStore, String tableName) {
        if (true)
            throw new RuntimeException("i am running");

        // TODO return SchemaInfo elements
        return null;
    }

    @Getter
    @AllArgsConstructor
    public static class ColumnInfo {

        private final String columnName;

        private final int sqlType;

        private final boolean isNullable;
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

}
