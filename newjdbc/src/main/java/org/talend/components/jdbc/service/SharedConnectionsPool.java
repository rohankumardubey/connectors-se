package org.talend.components.jdbc.service;

import java.sql.Connection;
import java.sql.SQLException;

//TODO this is only for jdbc, not so generic for all connectors, and when jdbc connector exists, we need to add implement in common javajet like we do for tcompv0
public interface SharedConnectionsPool {
    Connection getDBConnection(final String dbDriver, final String url, final String userName, final String password, final String dbConnectionName) throws ClassNotFoundException, SQLException;

    Connection getDBConnection(final String dbDriver, final String url, final String dbConnectionName) throws ClassNotFoundException, SQLException;
}
