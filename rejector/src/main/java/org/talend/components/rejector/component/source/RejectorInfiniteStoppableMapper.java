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
package org.talend.components.rejector.component.source;

import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.List;

import org.talend.components.rejector.service.UiServices;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@PartitionMapper(name = "RejectorInfiniteSource", infinite = true)
@Icon(value = IconType.CUSTOM, custom = "rejector")
@Version(1)
@Documentation("Mapper for Rejector.")
public class RejectorInfiniteMapper implements Serializable {

    private final RecordBuilderFactory recordBuilder;

    private final RejectorInputConfiguration configuration;

    private final UiServices uiServices;

    public RejectorInfiniteMapper(@Option("configuration") final RejectorInputConfiguration configuration,
            RecordBuilderFactory recordBuilder, UiServices uiServices) {
        this.configuration = configuration;
        this.recordBuilder = recordBuilder;
        this.uiServices = uiServices;
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<RejectorInfiniteMapper> split(@PartitionSize final long desiredSize) {
        return singletonList(this);
    }

    @Emitter
    public RejectorInfiniteGenerator createWorker() {
        return new RejectorInfiniteGenerator(configuration, recordBuilder, uiServices);
    }
}
