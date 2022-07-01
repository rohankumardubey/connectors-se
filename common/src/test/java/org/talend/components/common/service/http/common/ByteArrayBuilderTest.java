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

class ByteArrayBuilderTest {

    @Test
    void byteArrayTest() {
        ByteArrayBuilder array = new ByteArrayBuilder(20);
        array.charset(StandardCharsets.UTF_8);
        byte[] bytes = array.toByteArray();
        Assertions.assertEquals(0, bytes.length);

        array.append("Hello");
        Assertions.assertEquals("Hello", array.toString());
        array.reset();
        Assertions.assertEquals("", array.toString());
    }

}