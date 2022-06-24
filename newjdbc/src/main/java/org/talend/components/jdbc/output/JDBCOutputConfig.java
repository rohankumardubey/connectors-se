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

import lombok.Data;
import org.talend.components.jdbc.dataset.JDBCTableDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

@Data
@GridLayout({
        @GridLayout.Row("dataSet"),
        @GridLayout.Row("dataAction"),
        @GridLayout.Row("clearData"),
        @GridLayout.Row("dieOnError")
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {
        @GridLayout.Row("dataSet"),
        @GridLayout.Row("commitEvery"),
        @GridLayout.Row("additionalColumns"),
        @GridLayout.Row("useFieldOptions"),
        @GridLayout.Row("fieldOptions"),
        @GridLayout.Row("debugQuery"),
        @GridLayout.Row("useBatch"),
        @GridLayout.Row("batchSize"),
        @GridLayout.Row("useQueryTimeout"),
        @GridLayout.Row("queryTimeout")

})
@Documentation("jdbc output")
public class JDBCOutputConfig implements Serializable {

    @Option
    @Documentation("table dataset")
    private JDBCTableDataSet dataSet;

    // TODO studio will add schema field auto
    // TODO but how to add guess schema auto for tjdbcoutput, which should guess from table

    @Option
    @Documentation("")
    private DataAction dataAction = DataAction.INSERT;

    @Option
    @Documentation("")
    private boolean clearData;

    @Option
    @Documentation("")
    private boolean dieOnError;

    @Option
    @Documentation("")
    private int commitEvery = 10000;

    // TODO additional columns table, how to link schema columns as select?
    @Option
    @Documentation("")
    private List<AdditionalColumn> additionalColumns;

    @Option
    @Documentation("")
    private boolean useFieldOptions;

    // TODO field options table, how to link schema columns as select and auto fill table row by row by schema columns?
    @Option
    @ActiveIf(target = "useFieldOptions", value = { "true" })
    @Documentation("")
    private List<FieldOption> fieldOptions;

    @Option
    @Documentation("")
    private boolean debugQuery;

    @Option
    @Documentation("")
    private boolean useBatch = true;

    // TODO be care this, seems have tck inside implement for this, conflict
    @Option
    @ActiveIf(target = "useBatch", value = { "true" })
    @Documentation("")
    private int batchSize = 10000;

    @Option
    @Documentation("")
    private boolean useQueryTimeout;

    @Option
    @ActiveIf(target = "useQueryTimeout", value = { "true" })
    @Documentation("")
    private int queryTimeout = 30;

}
