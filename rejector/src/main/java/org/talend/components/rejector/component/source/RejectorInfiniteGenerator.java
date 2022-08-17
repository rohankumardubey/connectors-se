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

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.Optional;
import java.util.Random;

import javax.annotation.PostConstruct;

import org.talend.components.rejector.service.UiServices;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Documentation("This component generates data from Rejector.")
public class RejectorInfiniteGenerator implements Serializable {

    private final RecordBuilderFactory recordBuilder;

    private Iterator<Record> items;

    private final RejectorInputConfiguration configuration;

    private final UiServices uiServices;

    private Schema schema;

    public RejectorInfiniteGenerator(@Option("configuration") final RejectorInputConfiguration configuration,
            RecordBuilderFactory recordBuilder, final UiServices uiServices) {
        this.configuration = configuration;
        this.recordBuilder = recordBuilder;
        this.uiServices = uiServices;
    }

    public Record generateRecord() {
        final int inty = Optional.ofNullable(configuration.getDataSet().getAnInteger()).orElse(1);
        final String infos = configuration.getDataSet().getInformations();
        final String url = configuration.getDataSet().getDataStore().getUrl();
        final int i = new Random(ZonedDateTime.now().toEpochSecond()).nextInt();
        return recordBuilder.newRecordBuilder(schema)
                .withString("nullStr", String.format("null-%03d - %s _%s", i, infos, url))
                .withInt("inty", 12020 * (i + inty))
                .withDouble("doubly", 20.4 * i)
                .withFloat("floaty", 10.6664f * i)
                .withBoolean("booly", true)
                .withDateTime("daty", ZonedDateTime.now().plusDays(i).plusHours(i))
                .withDateTime("daty2", ZonedDateTime.now().plusDays(i))
                .build();
    }

    @PostConstruct
    public void init() {
        schema = uiServices.guessSchema(configuration.getDataSet());
    }

    @Producer
    public Record generate() {
        try {
            return generateRecord();
        } catch (Exception e) {
            log.error("[generate] {}", e.getMessage());
            return null;
        }
    }
}
