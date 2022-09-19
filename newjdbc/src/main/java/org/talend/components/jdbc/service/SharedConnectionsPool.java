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

import java.sql.Connection;
import java.sql.SQLException;

// TODO this is only for jdbc, not so generic for all connectors, and when jdbc connector exists, we need to add
// implement in common javajet like we do for tcompv0
public interface SharedConnectionsPool {

    Connection getDBConnection(final String dbDriver, final String url, final String userName, final String password,
            final String dbConnectionName) throws ClassNotFoundException, SQLException;

    Connection getDBConnection(final String dbDriver, final String url, final String dbConnectionName)
            throws ClassNotFoundException, SQLException;
}
