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
package org.talend.components.common.service.http;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RedirectContextTest {

    @Test
    void getHistory() {
        final RedirectContext c1 = new RedirectContext("http://base", 3, true, "GET", true);
        Assertions.assertEquals(0, c1.getHistory().size());

        final RedirectContext c2 = new RedirectContext(null, c1);
        Assertions.assertEquals(1, c2.getHistory().size());
        Assertions.assertSame(c2, c2.getHistory().get(0));

        final RedirectContext c3 = new RedirectContext(null, c2);
        Assertions.assertEquals(2, c3.getHistory().size());
        Assertions.assertSame(c3, c3.getHistory().get(0));
        Assertions.assertSame(c2, c3.getHistory().get(1));
    }
}