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
package org.talend.components.common.stream.input.rawtext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.rawtext.ExtendedRawTextConfiguration;
import org.talend.components.common.stream.format.rawtext.RawTextConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class RawTextRecordReaderTest {

    @ParameterizedTest
    @CsvSource({ "ISO-8859-7,true", "ISO-8859-1,false" })
    public void rawTextTest(String encoding, boolean success) throws IOException {
        RawTextConfiguration rawTextConfiguration = new RawTextConfiguration();
        ExtendedRawTextConfiguration extendedRawTextConfiguration = new ExtendedRawTextConfiguration(encoding, false);

        RawTextReaderSupplier supplier = new RawTextReaderSupplier();
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

        final RecordReader reader = supplier.getReader(factory, rawTextConfiguration, extendedRawTextConfiguration);

        InputStream stream = RawTextRecordReaderTest.class.getResourceAsStream("/ISO-8859-7.txt");
        Iterator<Record> read = reader.read(stream);

        Assertions.assertTrue(read.hasNext());

        Record record = read.next();
        String content = record.getString("content");

        InputStream is = RawTextRecordReaderTest.class.getResourceAsStream("/UTF-8.txt");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[256];
        int n;
        while ((n = is.read(buffer, 0, buffer.length)) != -1) {
            baos.write(buffer, 0, n);
        }

        String expected = baos.toString("UTF-8");

        if (success) {
            Assertions.assertEquals(expected, content);
        } else {
            Assertions.assertNotEquals(expected, content);
        }

        Assertions.assertFalse(read.hasNext());

    }

}