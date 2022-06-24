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
package org.talend.components.jdbc.sp;

import org.talend.sdk.component.api.record.Schema;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * the mapping tool for JDBC
 * this class only work for tJDBCSP component now
 */
public class JDBCMapping {

    /**
     * fill the prepared statement object
     * 
     * @param index
     * @param statement
     * @param f
     * @param value
     * @throws SQLException
     */
    public static void setValue(int index, final PreparedStatement statement, final Schema.Entry f, final Object value)
            throws SQLException {
        Schema.Type type = f.getType();

        // TODO support decimal/short/byte/character
        if (value == null) {
            if (type == Schema.Type.STRING) {
                statement.setNull(index, Types.VARCHAR);
            } else if (type == Schema.Type.INT) {
                statement.setNull(index, Types.INTEGER);
            } else if (type == Schema.Type.DATETIME) {
                statement.setNull(index, Types.TIMESTAMP);
            } else if (type == Schema.Type.DECIMAL) {
                statement.setNull(index, Types.DECIMAL);
            } else if (type == Schema.Type.LONG) {
                statement.setNull(index, Types.BIGINT);
            } else if (type == Schema.Type.DOUBLE) {
                statement.setNull(index, Types.DOUBLE);
            } else if (type == Schema.Type.FLOAT) {
                statement.setNull(index, Types.FLOAT);
            } else if (type == Schema.Type.BOOLEAN) {
                statement.setNull(index, Types.BOOLEAN);
            } else if (type == Schema.Type.BYTES) {
                // TODO check it, now only make a progress and make sure no regression with current version
                // ARRAY is not common, don't exist on lots of database,
                // here only use the old way in javajet component for tjdbcoutput, not sure it works, maybe change it to
                // BLOB
                statement.setNull(index, Types.ARRAY);
            } else {
                statement.setNull(index, Types.JAVA_OBJECT);
            }

            return;
        }

        if (type == Schema.Type.STRING) {
            // Avro will convert string to {@link org.apache.avro.util.Utf8}
            statement.setString(index, String.valueOf(value));
        } else if (type == Schema.Type.INT) {
            statement.setInt(index, (Integer) value);
        } else if (type == Schema.Type.DATETIME) {
            java.util.Date date = (java.util.Date) value;
            statement.setTimestamp(index, new java.sql.Timestamp((date).getTime()));
        } else if (type == Schema.Type.DECIMAL) {
            BigDecimal dec = (BigDecimal) value;
            statement.setBigDecimal(index, dec);
        } else if (type == Schema.Type.LONG) {
            statement.setLong(index, (Long) value);
        } else if (type == Schema.Type.DOUBLE) {
            statement.setDouble(index, (Double) value);
        } else if (type == Schema.Type.FLOAT) {
            statement.setFloat(index, (Float) value);
        } else if (type == Schema.Type.BOOLEAN) {
            statement.setBoolean(index, (Boolean) value);
        } else if (type == Schema.Type.BYTES) {
            // TODO check it, now only make a progress and make sure no regression with current version
            statement.setBytes(index, (byte[]) value);
        } else {
            statement.setObject(index, value);
        }
    }

    /**
     * work for tJDBCSP components
     * 
     * @param f
     * @return
     */
    public static int getSQLTypeFromTckType(Schema.Entry f) {
        Schema.Type type = f.getType();

        // TODO support decimal/short/byte/character

        if (type == Schema.Type.STRING) {
            return Types.VARCHAR;
        } else if (type == Schema.Type.INT) {
            return Types.INTEGER;
        } else if (type == Schema.Type.DATETIME) {
            return Types.DATE;
        } else if (type == Schema.Type.DECIMAL) {
            return Types.DECIMAL;
        } else if (type == Schema.Type.LONG) {
            return Types.BIGINT;
        } else if (type == Schema.Type.DOUBLE) {
            return Types.DOUBLE;
        } else if (type == Schema.Type.FLOAT) {
            return Types.FLOAT;
        } else if (type == Schema.Type.BOOLEAN) {
            return Types.BOOLEAN;
        } else if (type == Schema.Type.BYTES) {
            // TODO check it, now only make a progress and make sure no regression with current version
            // TODO maybe make it to ARRAY or BLOB?
            return Types.OTHER;
        } else {
            return Types.OTHER;
        }
    }
}
