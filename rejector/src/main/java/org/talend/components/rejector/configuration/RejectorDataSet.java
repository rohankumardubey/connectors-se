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
package org.talend.components.rejector.configuration;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;

import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("RejectorDataSet")
@Version(1)
@Documentation("DataSet for Rejector connector.")
@GridLayout({ //
        @GridLayout.Row({ "dataStore" }), //
        @GridLayout.Row({ "informations" }), //
        @GridLayout.Row({ "anInteger" }), //
        @GridLayout.Row({ "timy", "daty", "zndaty" })//
})
public class RejectorDataSet implements Serializable {

    @Option
    @Documentation("DataStore.")
    private RejectorDataStore dataStore;

    @Option
    @Documentation("Schema.")
    @Structure(type = Structure.Type.OUT, discoverSchema = "RejectorDataSet")
    private List<String> schema;

    @Option
    @Documentation("Misc informations.")
    private String informations;

    @Option
    @Documentation("Time informations.")
    private LocalTime timy;

    @Option
    @Documentation("Date informations.")
    private LocalDate daty;

    @Option
    @Documentation("An Int.")
    private Integer anInteger;

    @Option
    @Documentation("Zoned informations.")
    private ZonedDateTime zndaty;
}
