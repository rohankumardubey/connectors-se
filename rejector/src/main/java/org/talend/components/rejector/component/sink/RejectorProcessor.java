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
package org.talend.components.rejector.component.sink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
import org.talend.sdk.component.api.record.Schema.Type;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor(name = "RejectorProcessor")
@Icon(value = IconType.CUSTOM, custom = "rejector")
@Version(1)
@Documentation("A connector for testing rejects in Studio.")
public class RejectorProcessor implements Serializable {

    private final RejectorProcessorConfiguration configuration;

    private final I18nMessages i18n;

    public RejectorProcessor(@Option("configuration") final RejectorProcessorConfiguration configuration,
            final I18nMessages messages) {
        this.configuration = configuration;
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
    public void bufferize(final Record data, @Output("main") OutputEmitter<Record> main,
            @Output("REJECT") OutputEmitter<Record> reject) {
        log.warn("[bufferize] {} - (schema {}).", data, data.getSchema().getEntries().stream()
                .map(e -> String.format("%s/%s", e.getName(), e.getType())).collect(Collectors.joining(",")));
        data.getSchema().getEntries().stream().filter(e -> e.getType().equals(Type.BYTES)).forEach(e -> {
            final byte[] b = data.getBytes(e.getName());
            List<Byte> f = new ArrayList();
            for (int i = 0; i < b.length; i++) {
                f.add(b[i]);
            }
            log.warn("==========> {} is BYTES {}.", e.getName(), f);

        });
        main.emit(data);
        reject.emit(data);
    }

    @AfterGroup
    public void commit() {
    }

}
