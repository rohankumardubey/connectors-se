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
package org.talend.components.common.service.http.digest;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NonceTest {

    @Test
    void renew() {
        final Nonce nonce = new Nonce();
        for (int i = 0; i < 300; i++) {
            final String nc = nonce.renew("123" + i);
            Assertions.assertEquals("00000001", nc);
            Assertions.assertNotNull(nonce.getCnonce());
            Assertions.assertEquals(16, nonce.getCnonce().length());
            final String cnonce = nonce.getCnonce();

            final String nc2 = nonce.renew("123" + i);
            Assertions.assertEquals("00000002", nc2);
            Assertions.assertEquals(16, nonce.getCnonce().length());
            Assertions.assertEquals(cnonce, nonce.getCnonce());
        }
    }
}