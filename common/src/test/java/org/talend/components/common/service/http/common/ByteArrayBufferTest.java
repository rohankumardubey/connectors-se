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
package org.talend.components.common.service.http.common;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ByteArrayBufferTest {

    @Test
    void append() {
        final ByteArrayBuffer buffer = new ByteArrayBuffer(10);
        Assertions.assertEquals(0, buffer.length());
        Assertions.assertEquals(10, buffer.buffer().length);

        buffer.append("Hello".getBytes(StandardCharsets.UTF_8), 0, 5);

        Assertions.assertArrayEquals("Hello\0\0\0\0\0".getBytes(StandardCharsets.UTF_8), buffer.buffer());
        buffer.append("World".getBytes(StandardCharsets.UTF_8), 0, 5);
        Assertions.assertArrayEquals("HelloWorld".getBytes(StandardCharsets.UTF_8), buffer.buffer());

        buffer.append(65);

        Assertions.assertEquals('A', buffer.buffer()[10]);
    }
}