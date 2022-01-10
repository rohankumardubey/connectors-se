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
package org.talend.components.rest.service;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointer;
import org.talend.sdk.component.api.record.RecordPointerFactory;

import java.util.Collection;
import java.util.function.UnaryOperator;

import static java.util.Optional.ofNullable;

public class RecordDictionary implements UnaryOperator<String> {

    private final Record record;

    private final RecordPointerFactory recordPointerFactory;

    public RecordDictionary(Record record, RecordPointerFactory recordPointerFactory) {
        this.record = record;
        this.recordPointerFactory = recordPointerFactory;
    }

    public String apply(String key) {
        final RecordPointer rp = recordPointerFactory.apply(key);
        try {
            final Object value = rp.getValue(record, Object.class);

            return ofNullable(value)
                    .filter(v -> !(Record.class.isInstance(v) || Collection.class.isInstance(v)))
                    .map(String::valueOf)
                    .orElse(null); // If other than null, then ':-' default syntax in place holder
                                   // is not taken into account
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

}
