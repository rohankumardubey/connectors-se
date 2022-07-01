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
package org.talend.components.common.service.http.bearer;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.service.http.ConfConnectionFake;
import org.talend.sdk.component.api.service.http.Configurer;

class BearerAuthConfigurerTest {

    @Test
    void configure() {
        final BearerAuthConfigurer configurer = new BearerAuthConfigurer();

        Configurer.ConfigurerConfiguration cfg = new Configurer.ConfigurerConfiguration() {

            @Override
            public Object[] configuration() {
                return new Object[0];
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                if (type == String.class) {
                    return (T) "zzz";
                }
                return null;
            }
        };

        final ConfConnectionFake cnx = new ConfConnectionFake("GET", "http://test", "content");
        Assertions.assertNull(cnx.getHeaders().get("Authorization"));
        configurer.configure(cnx, cfg);
        final List<String> authorization = cnx.getHeaders().get("Authorization");
        Assertions.assertNotNull(authorization);
        Assertions.assertEquals(1, authorization.size());
        Assertions.assertEquals("Bearer zzz", authorization.get(0));
    }
}