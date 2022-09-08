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
package org.talend.components.common.stream.output.avro;

import java.util.Optional;
import java.util.function.Function;

import org.apache.avro.Schema;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AvroSchemaComparator {

    private final Schema first;

    private final Schema second;

    public boolean areSchemaCompatible() {
        if (first == null && second == null) {
            return true;
        }
        if (first == null || second == null) {
            return false;
        }
        if (first.getType() != second.getType()) {
            return false;
        }
        if (first.getType() == Schema.Type.UNION) {
            Schema realFirst = unwrapNull(first);
            Schema realSecond = unwrapNull(second);
            new AvroSchemaComparator(realFirst, realSecond).areSchemaCompatible();
        } else if (first.getType() == Schema.Type.ARRAY) {
            Schema realFirst = first.getElementType();
            Schema realSecond = second.getElementType();
            new AvroSchemaComparator(realFirst, realSecond).areSchemaCompatible();
        } else if (first.getType() == Schema.Type.RECORD) {

            return first.getFields()
                    .stream()
                    .map((Schema.Field f) -> Optional.ofNullable(this.second.getField(f.name()))
                            .map((Schema.Field s) -> new AvroSchemaComparator(f.schema(), s.schema())))
                    .allMatch((Optional<AvroSchemaComparator> op) -> op.isPresent() && op.get().areSchemaCompatible());
        }

        return true;
    }

    private Schema unwrapNull(Schema union) {
        return union.getTypes()
                .stream()
                .filter((Schema s) -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElse(null);
    }
}
