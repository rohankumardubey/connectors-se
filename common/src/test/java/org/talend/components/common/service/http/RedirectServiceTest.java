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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RedirectServiceTest {

    @Test
    void call() {
        final RedirectService redirect = new RedirectService();

        final RedirectContext previous = new RedirectContext("http://base", 3, true, "GET", false);

        final ResponseStringFake response1 = new ResponseStringFake(302, Collections.emptyMap(), "body");
        final RedirectContext context1 = new RedirectContext(response1, previous);
        Assertions.assertThrows(IllegalArgumentException.class, () -> redirect.call(context1));

        Map<String, List<String>> headers1 = new HashMap<>();
        headers1.put(RedirectService.LOCATION_HEADER, Collections.singletonList("/mylocation"));
        final ResponseStringFake response2 = new ResponseStringFake(302,
                headers1,
                "body");
        final RedirectContext context2 = new RedirectContext(response2, previous);
        final RedirectContext finalCtx = redirect.call(context2);
        Assertions.assertNotNull(finalCtx);
        Assertions.assertEquals("http://base/mylocation", finalCtx.getNextUrl());
    }

}