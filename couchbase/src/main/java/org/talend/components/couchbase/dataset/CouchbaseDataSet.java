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
package org.talend.components.couchbase.dataset;

import static org.talend.components.couchbase.configuration.ConfigurationConstants.DETECT_SCHEMA;
import static org.talend.components.couchbase.configuration.ConfigurationConstants.DISCOVER_SCHEMA;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Version(1)
@Data
@DataSet("CouchbaseDataSet")
@GridLayout({ @GridLayout.Row({ "datastore" }), @GridLayout.Row({ "schema" }), @GridLayout.Row({ "bucket" }),
        @GridLayout.Row({ "documentType" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "datastore" }) })

@Documentation("Couchbase DataSet")
public class CouchbaseDataSet implements Serializable {

    @Option
    @Documentation("Connection.")
    private CouchbaseDataStore datastore;

    @Option
    @Documentation("Schema.")
    @Structure(type = Structure.Type.OUT, discoverSchema = DISCOVER_SCHEMA)
    @Suggestable(value = DETECT_SCHEMA, parameters = { "datastore", "bucket" })
    private List<String> schema = Collections.emptyList();

    @Option
    @Required
    @Documentation("Bucket name.")
    private String bucket;

    @Option
    @Required
    @Documentation("Document type.")
    private DocumentType documentType = DocumentType.JSON;
}