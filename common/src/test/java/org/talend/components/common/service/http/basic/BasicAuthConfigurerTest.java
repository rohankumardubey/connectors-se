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
package org.talend.components.common.service.http.basic;

import java.util.Base64;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.service.http.ConfConnectionFake;
import org.talend.components.common.service.http.common.UserNamePassword;
import org.talend.sdk.component.api.service.http.Configurer;

class BasicAuthConfigurerTest {

    @Test
    void configure() {
        final BasicAuthConfigurer configurer = new BasicAuthConfigurer();
        final UserNamePassword userPwd = new UserNamePassword("user", "pwd");

        Configurer.ConfigurerConfiguration cfg = new Configurer.ConfigurerConfiguration() {

            @Override
            public Object[] configuration() {
                return new Object[0];
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                if (type == UserNamePassword.class) {
                    return (T) userPwd;
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
        final String auth = authorization.get(0);
        Assertions.assertTrue(auth.startsWith("Basic "), auth + " not start with 'Basic '");
        final String userName = new String(Base64.getDecoder().decode(auth.substring(6)));
        Assertions.assertEquals("user:pwd", userName);
    }
}