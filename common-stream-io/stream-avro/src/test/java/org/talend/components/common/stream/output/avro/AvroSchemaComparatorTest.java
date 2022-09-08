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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AvroSchemaComparatorTest {

    @Test
    void areSchemaCompatible() {
        Assertions.assertTrue(new AvroSchemaComparator(null, null).areSchemaCompatible());

        final Schema emptyRecordSchema = Schema.createRecord(Collections.emptyList());
        Assertions.assertFalse(new AvroSchemaComparator(null, emptyRecordSchema).areSchemaCompatible());

        Assertions.assertTrue(new AvroSchemaComparator(emptyRecordSchema, emptyRecordSchema).areSchemaCompatible());

        final Schema StringSchema = Schema.create(Schema.Type.STRING);
        Assertions.assertFalse(new AvroSchemaComparator(StringSchema, emptyRecordSchema).areSchemaCompatible());

        final Schema.Field f1 = new Schema.Field("f1", Schema.create(Schema.Type.STRING), "doc", "default");
        final Schema.Field f2 = new Schema.Field("f2", Schema.create(Schema.Type.INT), "doc", "7");
        final Schema union = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL));
        final Schema.Field f3 = new Schema.Field("f3", union, "doc", "7");

        final Schema array = Schema.createArray(Schema.create(Schema.Type.STRING));
        final Schema.Field f4 = new Schema.Field("f4", array, "doc", null);

        final Schema rec1 = Schema.createRecord("rec1", "doc", "namespace1", false, Arrays.asList(f1, f2, f3, f4));

        final Schema.Field f1Bis = new Schema.Field("f1", Schema.create(Schema.Type.STRING), "doc", "default");
        final Schema.Field f2Bis = new Schema.Field("f2", Schema.create(Schema.Type.INT), "doc", "7");
        final Schema.Field f3Bis = new Schema.Field("f3", union, "doc", "7");
        final Schema.Field f4Bis = new Schema.Field("f4", array, "doc", null);
        final Schema rec2 =
                Schema.createRecord("rec2", "doc", "namespace2", false, Arrays.asList(f1Bis, f2Bis, f3Bis, f4Bis));
        Assertions.assertTrue(new AvroSchemaComparator(rec1, rec2).areSchemaCompatible());

    }
}