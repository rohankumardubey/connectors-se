/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.docdb.dataset;

import java.io.Serializable;

import lombok.Data;
import org.talend.components.docdb.datastore.DocDBDataStore;

import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

@Version(1)
@Data
@DataSet("DocDBDataSet")
@GridLayout({
        @GridLayout.Row({ "dataStore" }),
        @GridLayout.Row({ "collection" }),
        @GridLayout.Row({ "useQuery" }),
        @GridLayout.Row({ "query" }),
        @GridLayout.Row({ "mode" })
})
@Documentation("AWS DocDB Dataset")
public class DocDBDataSet implements Serializable {

    @Option
    @Documentation("DocDB Connection")
    private DocDBDataStore dataStore;

    @Option
    @Required
    @Documentation("Collection")
    private String collection;

    @Option
    @Documentation("Use Query")
    private boolean useQuery;

    @Option
    @Documentation("Query")
    @TextArea
    @ActiveIf(target = "useQuery", value = "true")
    private String query = "{}";

    @Option
    @Required
    @Documentation("Mode")
    private Mode mode = Mode.JSON;
}