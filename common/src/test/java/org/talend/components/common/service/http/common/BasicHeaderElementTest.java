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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BasicHeaderElementTest {

    @Test
    void headerTest() {
        BasicNameValuePair p1 = new BasicNameValuePair("p1Name", "p1Value");
        BasicNameValuePair p2 = new BasicNameValuePair("p2Name", "p2Value");
        final BasicHeaderElement element = new BasicHeaderElement("thename",
                "thevalue",
                new BasicNameValuePair[] { p1, p2 });
        final BasicHeaderElement clone = element.clone();
        Assertions.assertEquals(element, clone);
        final BasicHeaderElement element2 = new BasicHeaderElement("thename",
                "thevalue",
                new BasicNameValuePair[] { p1 });
        Assertions.assertNotEquals(element2, element);

        Assertions.assertEquals(2, element.getParameterCount());
        Assertions.assertSame(p2, element.getParameterByName("p2Name"));

        final String param = element.toString();
        Assertions.assertEquals("thename=thevalue; p1Name=p1Value; p2Name=p2Value", param);
    }
}