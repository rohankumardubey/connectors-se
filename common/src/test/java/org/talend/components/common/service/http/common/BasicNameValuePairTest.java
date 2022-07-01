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

import java.util.Formatter;
import java.util.Locale;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BasicNameValuePairTest {

    @Test
    void testClone() {
        final BasicNameValuePair pair = new BasicNameValuePair("name", "value");
        final BasicNameValuePair pair2 = new BasicNameValuePair("name", "value2");

        final BasicNameValuePair clone = pair.clone();
        Assertions.assertEquals(pair, clone);
        Assertions.assertNotEquals(pair, pair2);

        Assertions.assertEquals("name=value", pair.toString());

        final BasicNameValuePair nullValue = new BasicNameValuePair("name", null);
        Assertions.assertEquals("name", nullValue.toString());
    }

}