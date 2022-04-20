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
package org.talend.components.jdbc.datastore;

import lombok.Data;
import lombok.ToString;
import org.talend.components.jdbc.common.Driver;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

@Data
@ToString(exclude = { "password" })
@GridLayout({
        @GridLayout.Row("jdbcUrl"),
        @GridLayout.Row("jdbcDriver"),
        @GridLayout.Row("jdbcClass"),
        @GridLayout.Row("userId"),
        @GridLayout.Row("password"),
        @GridLayout.Row("useSharedDBConnection"),
        @GridLayout.Row("sharedDBConnectionName"),
        @GridLayout.Row("useDataSource"),
        @GridLayout.Row("dataSourceAlias"),
        @GridLayout.Row("dbMapping")
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {
        @GridLayout.Row("useAutoCommit"),
        @GridLayout.Row("autoCommit")
})
@DataStore("JDBCDataStore")
@Checkable("CheckConnection")
@Documentation("A connection to a database")
public class JDBCDataStore implements Serializable {

    @Option
    @Documentation("jdbc url")
    private String jdbcUrl = "jdbc:";

    // TODO how to map to studio right ui widget
    // TODO how to use the right runtime to load jar or register jar path and pass it to studio or cloud by api
    // TODO need to support in cloud too? or how to hide it in cloud
    // TODO new Driver bean class?
    // TODO maybe change it to List<Bean> object
    @Option
    @Documentation("jdbc driver table")
    private List<Driver> jdbcDriver;// TODO can't use Driver bean class as not editable if that in ui

    @Option
    @Required
    @Suggestable(value = "GUESS_DRIVER_CLASS", parameters = { "jdbcDriver" })
    @Documentation("driver class")
    private String jdbcClass;

    @Option
    @Documentation("database user")
    private String userId;

    @Option
    @Credential
    @Documentation("database password")
    private String password;

    // TODO how to hide it in cloud, as sure no meaning for pipeline designer job
    // TODO how to hide it for tjdbcinput? expect only appear in tjdbcconnection
    // TODO hot to hide it for studio metadata as sure no meaning for studio metadata
    @Option
    @Documentation("use or register a shared DB connection")
    private boolean useSharedDBConnection;

    // TODO how to pass or get the shared connection in tck runtime?
    @Option
    @ActiveIf(target = "useSharedDBConnection", value = { "true" })
    @Documentation("shared DB connection name for register or fetch")
    private String sharedDBConnectionName;

    @Option
    @Documentation("use data source")
    private boolean useDataSource;

    // TODO how to pass or get the shared connection in tck runtime?
    // TODO how to hide it in cloud as sure no meaning for cloud
    @Option
    @ActiveIf(target = "useDataSource", value = { "true" })
    @Documentation("data source alias for fetch")
    private String dataSourceAlias;

    // TODO how to make it only appear in studio metadata?
    // TODO even can set it to studio component, but no meaning for tjdbcrow, how to explain this to user?
    // tcompv0 use this mapping before : "widget.type.mappingType":"MAPPING_TYPE", but clearly, that doesn't appear for
    // cloud
    // about runtime, that only works for studio metadata to fetch schema part, not for component button runtime/job
    // runtime
    @Option
    @Documentation("select db mapping file for type convert")
    private String dbMapping;

    // advanced setting

    // TODO how to hide it for studio metadata
    // TODO how to hide it for components like tjdbcinput/tjdbcrow
    @Option
    @Documentation("decide if call auto commit method")
    private boolean useAutoCommit;

    @Option
    @ActiveIf(target = "useAutoCommit", value = { "true" })
    @Documentation("if true, mean auto commit, else disable auto commit, as different database, default auto commit value is different")
    private boolean autoCommit;

}
