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
package org.talend.components.jdbc.output;

import org.talend.sdk.component.api.record.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL build tool for only runtime, for design time, we use another one : QueryUtils which consider the context.var and
 * so on
 *
 */
public class JDBCSQLBuilder {

    private JDBCSQLBuilder() {
    }

    public static JDBCSQLBuilder getInstance() {
        return new JDBCSQLBuilder();
    }

    protected String getProtectedChar() {
        return "";
    }

    public String generateSQL4SelectTable(String tablename, Schema schema) {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT ");
        List<Schema.Entry> fields = schema.getEntries();
        boolean firstOne = true;
        for (Schema.Entry field : fields) {
            if (firstOne) {
                firstOne = false;
            } else {
                sql.append(", ");
            }
            String dbColumnName = field.getRawName();
            sql.append(tablename).append(".").append(dbColumnName);
        }
        sql.append(" FROM ").append(getProtectedChar()).append(tablename).append(getProtectedChar());

        return sql.toString();
    }

    public String generateSQL4DeleteTable(String tablename) {
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(getProtectedChar()).append(tablename).append(getProtectedChar());
        return sql.toString();
    }

    public class Column {

        public String columnLabel;

        public String dbColumnName;

        public String sqlStmt = "?";

        public boolean isKey;

        public boolean updateKey;

        public boolean deletionKey;

        public boolean updatable = true;

        public boolean insertable = true;

        public boolean addCol;

        public List<Column> replacements;

        void replace(Column replacement) {
            if (replacements == null) {
                replacements = new ArrayList<Column>();
            }

            replacements.add(replacement);
        }

        public boolean isReplaced() {
            return this.replacements != null && !this.replacements.isEmpty();
        }

    }

    public String generateSQL4Insert(String tablename, List<Column> columnList) {
        List<String> dbColumnNames = new ArrayList<>();
        List<String> expressions = new ArrayList<>();

        List<Column> all = getAllColumns(columnList);

        for (Column column : all) {
            if (column.insertable) {
                dbColumnNames.add(column.dbColumnName);
                expressions.add(column.sqlStmt);
            }
        }

        return generateSQL4Insert(tablename, dbColumnNames, expressions);
    }

    public List<Column> createColumnList(JDBCOutputConfig config, Schema schema) {
        Map<String, Column> columnMap = new HashMap<>();
        List<Column> columnList = new ArrayList<>();

        List<Schema.Entry> fields = schema.getEntries();

        for (Schema.Entry field : fields) {
            Column column = new Column();
            column.columnLabel = field.getName();
            // the javajet template have an issue for dynamic convert, it don't pass the origin column name
            String originName = field.getRawName();
            column.dbColumnName = originName != null ? originName : field.getName();

            boolean isKey = Boolean.valueOf(field.getProp("talend.studio.key"));
            if (isKey) {
                column.updateKey = true;
                column.deletionKey = true;
                column.updatable = false;
            } else {
                column.updateKey = false;
                column.deletionKey = false;
                column.updatable = true;
            }

            columnMap.put(field.getName(), column);
            columnList.add(column);
        }

        boolean enableFieldOptions = config.isUseFieldOptions();
        if (enableFieldOptions) {
            // TODO about dynamic support here:
            // for other javajet db output components, the dynamic support is static here, for example,
            // only can set all db columns in dynamic field all to update keys, can't use one db column in dynamic
            // column,
            // not sure which is expected, and now, here user can use one db column in dynamic column,
            // but user may not know the column label as it may be different with db column name as valid in java and
            // studio.
            // So here, we don't change anything, also as no much meaning for customer except making complex code
            List<FieldOption> fieldOptions = config.getFieldOptions();

            int i = 0;
            for (FieldOption fieldOption : fieldOptions) {
                String columnName = fieldOption.getColumnName();
                Column column = columnMap.get(columnName);
                if (column == null) {
                    throw new RuntimeException(columnName + " column label doesn't exist for current target table");
                }
                column.updateKey = fieldOption.isUpdateKey();
                column.deletionKey = fieldOption.isDeleteKey();
                column.updatable = fieldOption.isUpdatable();
                column.insertable = fieldOption.isInsertable();

                i++;
            }
        }

        List<AdditionalColumn> additionalColumns = config.getAdditionalColumns();

        // here is a closed list in UI, even can't choose dynamic column, so no need to consider dynamic here
        int i = 0;
        for (AdditionalColumn additionalColumn : additionalColumns) {
            String referenceColumn = additionalColumn.getRefColumn();
            int j = 0;
            Column currentColumn = null;
            for (Column column : columnList) {
                if (column.columnLabel.equals(referenceColumn)) {
                    currentColumn = column;
                    break;
                }
                j++;
            }

            String newDBColumnName = additionalColumn.getColumnName();
            String sqlExpression = additionalColumn.getSqlExpression();

            Position position = additionalColumn.getPosition();
            if (position == Position.AFTER) {
                Column newColumn = new Column();
                newColumn.columnLabel = newDBColumnName;
                newColumn.dbColumnName = newDBColumnName;
                newColumn.sqlStmt = sqlExpression;
                newColumn.addCol = true;

                columnList.add(j + 1, newColumn);
            } else if (position == Position.BEFORE) {
                Column newColumn = new Column();
                newColumn.columnLabel = newDBColumnName;
                newColumn.dbColumnName = newDBColumnName;
                newColumn.sqlStmt = sqlExpression;
                newColumn.addCol = true;

                columnList.add(j, newColumn);
            } else if (position == Position.REPLACE) {
                Column replacementColumn = new Column();
                replacementColumn.columnLabel = newDBColumnName;
                replacementColumn.dbColumnName = newDBColumnName;
                replacementColumn.sqlStmt = sqlExpression;

                Column replacedColumn = currentColumn;

                replacementColumn.isKey = replacedColumn.isKey;
                replacementColumn.updateKey = replacedColumn.updateKey;
                replacementColumn.deletionKey = replacedColumn.deletionKey;
                replacementColumn.insertable = replacedColumn.insertable;
                replacementColumn.updatable = replacedColumn.updatable;

                replacedColumn.replace(replacementColumn);
            }

            i++;
        }
        return columnList;
    }

    public String generateSQL4Insert(String tablename, Schema schema) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(getProtectedChar()).append(tablename).append(getProtectedChar()).append(" ");

        sb.append("(");

        List<Schema.Entry> fields = schema.getEntries();

        boolean firstOne = true;
        for (Schema.Entry field : fields) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            String dbColumnName = field.getRawName();
            sb.append(dbColumnName);
        }
        sb.append(")");

        sb.append(" VALUES ");

        sb.append("(");

        firstOne = true;
        for (@SuppressWarnings("unused")
        Schema.Entry field : fields) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            sb.append("?");
        }
        sb.append(")");

        return sb.toString();
    }

    private String generateSQL4Insert(String tablename, List<String> insertableDBColumns, List<String> expressions) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(getProtectedChar()).append(tablename).append(getProtectedChar()).append(" ");

        sb.append("(");
        boolean firstOne = true;
        for (String dbColumnName : insertableDBColumns) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            sb.append(dbColumnName);
        }
        sb.append(")");

        sb.append(" VALUES ");

        sb.append("(");

        firstOne = true;
        for (String expression : expressions) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            sb.append(expression);
        }
        sb.append(")");

        return sb.toString();
    }

    public String generateSQL4Delete(String tablename, List<Column> columnList) {
        List<String> deleteKeys = new ArrayList<>();
        List<String> expressions = new ArrayList<>();

        List<Column> all = getAllColumns(columnList);

        for (Column column : all) {
            if (column.deletionKey) {
                deleteKeys.add(column.dbColumnName);
                expressions.add(column.sqlStmt);
            }
        }

        return generateSQL4Delete(tablename, deleteKeys, expressions);
    }

    private String generateSQL4Delete(String tablename, List<String> deleteKeys, List<String> expressions) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ")
                .append(getProtectedChar())
                .append(tablename)
                .append(getProtectedChar())
                .append(" WHERE ");

        int i = 0;

        boolean firstOne = true;
        for (String dbColumnName : deleteKeys) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(" AND ");
            }

            sb.append(getProtectedChar())
                    .append(dbColumnName)
                    .append(getProtectedChar())
                    .append(" = ")
                    .append(expressions.get(i++));
        }

        return sb.toString();
    }

    private String generateSQL4Update(String tablename, List<String> updateValues, List<String> updateKeys,
            List<String> updateValueExpressions, List<String> updateKeyExpressions) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(getProtectedChar()).append(tablename).append(getProtectedChar()).append(" SET ");

        int i = 0;

        boolean firstOne = true;
        for (String dbColumnName : updateValues) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(",");
            }

            sb.append(getProtectedChar())
                    .append(dbColumnName)
                    .append(getProtectedChar())
                    .append(" = ")
                    .append(updateValueExpressions.get(i++));
        }

        i = 0;

        sb.append(" WHERE ");

        firstOne = true;
        for (String dbColumnName : updateKeys) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(" AND ");
            }

            sb.append(getProtectedChar())
                    .append(dbColumnName)
                    .append(getProtectedChar())
                    .append(" = ")
                    .append(updateKeyExpressions.get(i++));
        }

        return sb.toString();
    }

    public String generateSQL4Update(String tablename, List<Column> columnList) {
        List<String> updateValues = new ArrayList<>();
        List<String> updateValueExpressions = new ArrayList<>();

        List<String> updateKeys = new ArrayList<>();
        List<String> updateKeyExpressions = new ArrayList<>();

        List<Column> all = getAllColumns(columnList);

        for (Column column : all) {
            if (column.updatable) {
                updateValues.add(column.dbColumnName);
                updateValueExpressions.add(column.sqlStmt);
            }

            if (column.updateKey) {
                updateKeys.add(column.dbColumnName);
                updateKeyExpressions.add(column.sqlStmt);
            }
        }

        return generateSQL4Update(tablename, updateValues, updateKeys, updateValueExpressions, updateKeyExpressions);
    }

    private String generateQuerySQL4InsertOrUpdate(String tablename, List<String> updateKeys,
            List<String> updateKeyExpressions) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(1) FROM ")
                .append(getProtectedChar())
                .append(tablename)
                .append(getProtectedChar())
                .append(" WHERE ");

        int i = 0;

        boolean firstOne = true;
        for (String dbColumnName : updateKeys) {
            if (firstOne) {
                firstOne = false;
            } else {
                sb.append(" AND ");
            }

            sb.append(getProtectedChar())
                    .append(dbColumnName)
                    .append(getProtectedChar())
                    .append(" = ")
                    .append(updateKeyExpressions.get(i++));
        }

        return sb.toString();
    }

    public String generateQuerySQL4InsertOrUpdate(String tablename, List<Column> columnList) {
        List<String> updateKeys = new ArrayList<>();
        List<String> updateKeyExpressions = new ArrayList<>();

        List<Column> all = getAllColumns(columnList);

        for (Column column : all) {
            if (column.updateKey) {
                updateKeys.add(column.dbColumnName);
                updateKeyExpressions.add(column.sqlStmt);
            }
        }

        return generateQuerySQL4InsertOrUpdate(tablename, updateKeys, updateKeyExpressions);
    }

    private List<Column> getAllColumns(List<Column> columnList) {
        List<Column> result = new ArrayList<Column>();
        for (Column column : columnList) {
            if (column.replacements != null && !column.replacements.isEmpty()) {
                for (Column replacement : column.replacements) {
                    result.add(replacement);
                }
            } else {
                result.add(column);
            }
        }

        return result;
    }

}
