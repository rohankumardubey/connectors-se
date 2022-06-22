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
package org.talend.components.rejector.component.sink;

import java.io.Serializable;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.rejector.service.I18nMessages;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Icon.IconType;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor(name = "RejectorProcessor")
@Icon(value = IconType.CUSTOM, custom = "rejector")
@Version(1)
@Documentation("A connector for testing rejects in Studio.")
public class RejectorProcessor implements Serializable {

    private final RejectorProcessorConfiguration configuration;

    private final I18nMessages i18n;

    private final RecordBuilderFactory factory;

    public RejectorProcessor(@Option("configuration") final RejectorProcessorConfiguration configuration,
            final RecordBuilderFactory factory, final I18nMessages messages) {
        this.configuration = configuration;
        this.factory = factory;
        this.i18n = messages;
    }

    @PostConstruct
    public void init() {
    }

    @PreDestroy()
    public void release() {
    }

    @BeforeGroup
    public void begin() {
    }

    @ElementListener
    public void bufferize(final Record data,
            @Output("main") OutputEmitter<Record> main,
            @Output("REJECT") OutputEmitter<Record> reject,
            @Output OutputEmitter<Record> unknown) {
        if (configuration.getDisplayRowStuct()) {
            log.warn("[bufferize] {} - (schema {}).", data, data.getSchema()
                    .getAllEntries()
                    .map(e -> String.format("\nschema for %s\t\t%s", e.getName(), e))
                    .collect(Collectors.joining(",")));
        }
        Schema flowSchema = factory.newSchemaBuilder(data.getSchema())
                .withEntry(factory.newEntryBuilder()
                        .withName("transformed")
                        .withType(Type.STRING)
                        .withNullable(true)
                        .build())
                .build();
        Schema rejectSchema = factory.newSchemaBuilder(data.getSchema())
                .withEntry(factory.newEntryBuilder()
                        .withName("errorMessage")
                        .withType(Type.STRING)
                        .withNullable(true)
                        .build())
                .build();
        final Builder rejectBuilder = factory.newRecordBuilder(rejectSchema);
        final Builder flowBuilder = factory.newRecordBuilder(flowSchema);
        data.getSchema().getEntries().forEach(entry -> {
            flowBuilder.with(entry, data.get(Object.class, entry.getName()));
            rejectBuilder.with(entry, data.get(Object.class, entry.getName()));
        });
        final String tmp = data.getSchema().getEntries().stream().map(Entry::getName).collect(Collectors.joining(","));
        Record rejectData = rejectBuilder
                .withString("errorMessage", "error:" + tmp)
                .build();
        Record flowData = flowBuilder
                .withString("transformed", tmp)
                .build();

        main.emit(flowData);
        reject.emit(rejectData);
        unknown.emit(data);
        // data.getSchema()
        // .getEntries()
        // .stream()
        // .filter(e -> e.getType().equals(Type.BYTES))
        // .forEach(e -> {
        // final byte[] b = data.getBytes(e.getName());
        // List<Byte> f = new ArrayList();
        // for (int i = 0; i < b.length; i++) {
        // f.add(b[i]);
        // }
        // log.warn("==========> {} is BYTES {}.", e.getName(), f);
        //
        // });
    }

    @AfterGroup
    public void commit() {
    }

}
