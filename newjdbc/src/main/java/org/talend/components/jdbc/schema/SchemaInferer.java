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
package org.talend.components.jdbc.schema;

import org.talend.components.jdbc.common.SchemaInfo;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.talend.sdk.component.api.record.Schema.Type.*;

public class SchemaInferer {

    public static Schema infer(RecordBuilderFactory recordBuilderFactory, ResultSetMetaData metadata, Dbms mapping)
            throws SQLException {
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(RECORD);

        int count = metadata.getColumnCount();
        for (int i = 1; i <= count; i++) {
            int size = metadata.getPrecision(i);
            int scale = metadata.getScale(i);
            boolean nullable = ResultSetMetaData.columnNullable == metadata.isNullable(i);

            int dbtype = metadata.getColumnType(i);
            String fieldName = metadata.getColumnLabel(i);
            String dbColumnName = metadata.getColumnName(i);

            // not necessary for the result schema from the query statement
            boolean isKey = false;

            String columnTypeName = metadata.getColumnTypeName(i).toUpperCase();

            // TODO no need to correct duplicated/invalid name here?
            String validName = fieldName;

            Schema.Entry.Builder entryBuilder = sqlType2Tck(recordBuilderFactory, size, scale, dbtype, nullable,
                    validName, dbColumnName, null, isKey, mapping,
                    columnTypeName);

            schemaBuilder.withEntry(entryBuilder.build());
        }

        return schemaBuilder.build();
    }

    public static Schema infer(RecordBuilderFactory recordBuilderFactory, JDBCTableMetadata tableMetadata, Dbms mapping)
            throws SQLException {
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(RECORD);

        DatabaseMetaData databaseMetdata = tableMetadata.getDatabaseMetaData();

        Set<String> keys = getPrimaryKeys(databaseMetdata, tableMetadata.getCatalog(), tableMetadata.getDbSchema(),
                tableMetadata.getTablename());

        try (ResultSet metadata = databaseMetdata.getColumns(tableMetadata.getCatalog(), tableMetadata.getDbSchema(),
                tableMetadata.getTablename(), null)) {
            if (!metadata.next()) {
                return null;
            }

            String tablename = metadata.getString("TABLE_NAME");

            do {
                int size = metadata.getInt("COLUMN_SIZE");
                int scale = metadata.getInt("DECIMAL_DIGITS");
                int dbtype = metadata.getInt("DATA_TYPE");
                boolean nullable = DatabaseMetaData.columnNullable == metadata.getInt("NULLABLE");

                String columnName = metadata.getString("COLUMN_NAME");
                boolean isKey = keys.contains(columnName);

                String defaultValue = metadata.getString("COLUMN_DEF");

                String columnTypeName = metadata.getString("TYPE_NAME");

                // TODO no need to correct duplicated/invalid name here?
                String validName = columnName;

                Schema.Entry.Builder entryBuilder = sqlType2Tck(recordBuilderFactory, size, scale, dbtype, nullable,
                        validName, columnName, defaultValue, isKey, mapping,
                        columnTypeName);

                schemaBuilder.withEntry(entryBuilder.build());
            } while (metadata.next());

            return schemaBuilder.build();
        }
    }

    private static Set<String> getPrimaryKeys(DatabaseMetaData databaseMetdata, String catalogName, String schemaName,
            String tableName) throws SQLException {
        Set<String> result = new HashSet<>();

        try (ResultSet resultSet = databaseMetdata.getPrimaryKeys(catalogName, schemaName, tableName)) {
            if (resultSet != null) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("COLUMN_NAME"));
                }
            }
        }

        return result;
    }

    private static Schema.Entry.Builder sqlType2Tck(RecordBuilderFactory recordBuilderFactory, int size, int scale,
            int dbtype, boolean nullable, String name, String dbColumnName,
            Object defaultValue, boolean isKey, Dbms mapping, String columnTypeName) {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder()
                .withName(name)
                .withRawName(dbColumnName)
                .withNullable(nullable)
                .withProp("talend.studio.key", String.valueOf(isKey));

        boolean isIgnoreLength = false;
        boolean isIgnorePrecision = false;

        // TODO here need to consider four cases to run:
        // 1. studio job run
        // 2. studio metadata trigger
        // 3. studio component button trigger, seems same with 2?
        // 4. cloud platform which don't have mapping files

        // 2&3 are called by (http rest api) or (hard api implement code to call by component-server/runtime inside
        // code), need to consider

        final boolean noMappingFiles = mapping == null;

        // TODO refactor the logic, now only make it works, as we have other more important limit to fix like system
        // property to pass for studio component server

        if (noMappingFiles) {// by java sql type if not find talend db mapping files
            // TODO check more
            switch (dbtype) {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
                entryBuilder.withType(INT).withProp("talend.studio.type", "id_Integer");
                // TODO process LONG by this : if (javaType.equals(Integer.class.getName()) ||
                // Short.class.getName().equals(javaType))
                break;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                entryBuilder.withType(FLOAT).withProp("talend.studio.type", "id_Float");
                break;
            case java.sql.Types.DOUBLE:
                entryBuilder.withType(DOUBLE).withProp("talend.studio.type", "id_Double");
                break;
            case java.sql.Types.BOOLEAN:
                entryBuilder.withType(BOOLEAN).withProp("talend.studio.type", "id_Boolean");
                break;
            case java.sql.Types.TIME:
                entryBuilder.withType(DATETIME)
                        .withProp("talend.studio.type", "id_Date");
                break;
            case java.sql.Types.DATE:
                entryBuilder.withType(DATETIME)
                        .withProp("talend.studio.type", "id_Date");
                break;
            case java.sql.Types.TIMESTAMP:
                entryBuilder.withType(DATETIME)
                        .withProp("talend.studio.type", "id_Date");
                break;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                entryBuilder.withType(BYTES).withProp("talend.studio.type", "id_byte[]");
                break;
            case java.sql.Types.BIGINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                entryBuilder.withType(STRING).withProp("talend.studio.type", "id_BigDecimal");
                break;
            case java.sql.Types.CHAR:
                entryBuilder.withType(STRING).withProp("talend.studio.type", "id_Character");
                break;
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
                entryBuilder.withType(STRING).withProp("talend.studio.type", "id_String");
            default:
                entryBuilder.withType(STRING).withProp("talend.studio.type", "id_String");
                break;
            }
        } else {
            // use talend db files if found
            MappingType<DbmsType, TalendType> mt = mapping.getDbmsMapping(columnTypeName);

            TalendType talendType;
            if (mt != null) {
                talendType = mt.getDefaultType();
                DbmsType sourceType = mt.getSourceType();

                isIgnoreLength = sourceType.isIgnoreLength();
                isIgnorePrecision = sourceType.isIgnorePrecision();
            } else {
                // if not find any mapping by current column db type name, map to studio sstring type
                talendType = TalendType.STRING;
            }

            entryBuilder.withProp("talend.studio.type", talendType.getName());

            entryBuilder.withType(convertTalendType2TckType(talendType));
        }

        // correct precision/scale/date pattern
        switch (dbtype) {
        case java.sql.Types.VARCHAR:
            setPrecision(entryBuilder, isIgnoreLength, size);
            break;
        case java.sql.Types.INTEGER:
            setPrecision(entryBuilder, isIgnoreLength, size);
            break;
        case java.sql.Types.DECIMAL:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            break;
        case java.sql.Types.BIGINT:
            setPrecision(entryBuilder, isIgnoreLength, size);
            break;
        case java.sql.Types.NUMERIC:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            break;
        case java.sql.Types.TINYINT:
            setPrecision(entryBuilder, isIgnoreLength, size);
            break;
        case java.sql.Types.DOUBLE:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            break;
        case java.sql.Types.FLOAT:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            break;
        case java.sql.Types.DATE:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            entryBuilder.withProp("talend.studio.pattern", "yyyy-MM-dd");
            break;
        case java.sql.Types.TIME:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            entryBuilder.withProp("talend.studio.pattern", "HH:mm:ss");
            break;
        case java.sql.Types.TIMESTAMP:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            entryBuilder.withProp("talend.studio.pattern", "yyyy-MM-dd HH:mm:ss.SSS");
            break;
        case java.sql.Types.BOOLEAN:
            break;
        case java.sql.Types.REAL:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            break;
        case java.sql.Types.SMALLINT:
            setPrecision(entryBuilder, isIgnoreLength, size);
            break;
        case java.sql.Types.LONGVARCHAR:
            setPrecision(entryBuilder, isIgnoreLength, size);
            break;
        case java.sql.Types.CHAR:
            setPrecision(entryBuilder, isIgnoreLength, size);
            break;
        default:
            setPrecision(entryBuilder, isIgnoreLength, size);
            setScale(entryBuilder, isIgnorePrecision, scale);
            break;
        }

        return entryBuilder;
    }

    private static void setPrecision(Schema.Entry.Builder entryBuilder, boolean ignorePrecision, int precision) {
        if (ignorePrecision) {
            return;
        }

        entryBuilder.withProp("talend.studio.length", String.valueOf(precision));
    }

    private static void setScale(Schema.Entry.Builder entryBuilder, boolean ignoreScale, int scale) {
        if (ignoreScale) {
            return;
        }

        entryBuilder.withProp("talend.studio.precision", String.valueOf(scale));
    }

    public static void fillValue(final Record.Builder builder, final Schema schema, final ResultSet resultSet)
            throws SQLException {
        List<Schema.Entry> entries = schema.getEntries();
        for (int index = 0; index < entries.size(); index++) {
            Schema.Entry entry = entries.get(index);
            Object value = resultSet.getObject(index + 1);

            if (value == null)
                continue;

            Schema.Type type = entry.getType();
            // though this works for studio schema fill, but also works for cloud as infer method will be executed
            // before this one
            String detail_type = entry.getProp("talend.studio.type");
            switch (type) {
            case STRING:
                builder.withString(entry, String.valueOf(value));
                break;
            case INT:
                builder.withInt(entry, resultSet.getInt(index + 1));
                break;
            case LONG:
                builder.withLong(entry, resultSet.getLong(index + 1));
                break;
            case FLOAT:
                builder.withFloat(entry, resultSet.getFloat(index + 1));
                break;
            case DOUBLE:
                builder.withDouble(entry, resultSet.getDouble(index + 1));
                break;
            case BOOLEAN:
                builder.withBoolean(entry, resultSet.getBoolean(index + 1));
                break;
            case DATETIME:
                builder.withDateTime(entry, resultSet.getDate(index + 1));
                break;
            case BYTES:
                builder.withBytes(entry, resultSet.getBytes(index + 1));
                break;
            default:
                builder.with(entry, String.valueOf(value));
                break;
            }
        }
    }

    private static Schema.Type convertTalendType2TckType(TalendType talendType) {
        switch (talendType) {
        case STRING:
            return STRING;
        case BOOLEAN:
            return BOOLEAN;
        case INTEGER:
            return INT;
        case LONG:
            return LONG;
        case DOUBLE:
            return DOUBLE;
        case FLOAT:
            return FLOAT;
        case BYTE:
            // no Schema.Type.BYTE
            return STRING;
        case BYTES:
            return BYTES;
        case SHORT:
            // no Schema.Type.SHORT
            return INT;
        case CHARACTER:
            // no Schema.Type.CHARACTER
            return STRING;
        case BIG_DECIMAL:
            // no Schema.Type.DECIMA
            return STRING;
        case DATE:
            return DATETIME;
        case OBJECT:
            return STRING;
        default:
            throw new UnsupportedOperationException("Unrecognized type " + talendType);
        }
    }

    public static Schema convertSchemaInfoList2TckSchema(List<SchemaInfo> infos,
            RecordBuilderFactory recordBuilderFactory) {
        final Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        convertBase(infos, recordBuilderFactory, schemaBuilder);
        return schemaBuilder.build();
    }

    public static Schema getRejectSchema(List<SchemaInfo> infos, RecordBuilderFactory recordBuilderFactory) {
        final Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);

        convertBase(infos, recordBuilderFactory, schemaBuilder);

        schemaBuilder.withEntry(recordBuilderFactory.newEntryBuilder()
                .withName("errorCode")
                .withType(STRING)
                .withProp("talend.studio.length", "255")
                .build());
        schemaBuilder.withEntry(recordBuilderFactory.newEntryBuilder()
                .withName("errorMessage")
                .withType(STRING)
                .withProp("talend.studio.length", "255")
                .build());

        return schemaBuilder.build();
    }

    private static void convertBase(List<SchemaInfo> infos, RecordBuilderFactory recordBuilderFactory,
            Schema.Builder schemaBuilder) {
        if (infos == null)
            return;

        infos.stream().forEach(info -> {
            Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            // TODO consider the valid name convert
            entryBuilder.withName(info.getLabel())
                    .withRawName(info.getOriginalDbColumnName())
                    .withNullable(info.isNullable())
                    .withComment(info.getComment())
                    .withDefaultValue(info.getDefaultValue())
                    // in studio, we use talend type firstly, not tck type, but in cloud, no talend type, only tck type,
                    // need to use this
                    // but only studio have the design schema which with raw db type and talend type, so no need to
                    // convert here as we will not use getType method
                    .withType(convertTalendType2TckType(TalendType.get(info.getTalendType())))
                    // TODO also define a pro for origin db type like VARCHAR? info.getType()
                    .withProp("talend.studio.type", info.getTalendType())
                    .withProp("talend.studio.key", String.valueOf(info.isKey()))
                    .withProp("talend.studio.pattern", info.getPattern())
                    // TODO how to treat differently for null, 0, empty?
                    .withProp("talend.studio.length", String.valueOf(info.getLength()))
                    .withProp("talend.studio.precision", String.valueOf(info.getPrecision()));

            schemaBuilder.withEntry(entryBuilder.build());
        });
    }

}
