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

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.service.http.common.BasicHeader;
import org.talend.components.common.service.http.common.UserNamePassword;

class DigestSchemeTest {

    @Test
    void createDigestResponse() throws DigestScheme.AuthenticationException {
        final DigestScheme scheme = new DigestScheme();
        final BasicHeader authChallenge = new BasicHeader("WWW-Authenticate", "realm=XXX,nonce=1234,authvalue");
        DigestAuthContext context =
                new DigestAuthContext("http://hello/world", "GET",
                        "localhost", 8725, "BodyContent".getBytes(StandardCharsets.UTF_8),
                        new UserNamePassword("user", "pwd"));
        final String digestResponse = scheme.createDigestResponse("user", "pwd", authChallenge, context);
        Assertions.assertNotNull(digestResponse);
        Assertions.assertEquals(
                "Digest username=\"user\", realm=\"XXX\", nonce=\"1234\", uri=\"http://hello/world\", response=\"d28199342f62a08de0c28066b4f34253\", algorithm=MD5",
                digestResponse);

        final BasicHeader authChallenge2 =
                new BasicHeader("WWW-Authenticate", "realm=XXX,nonce=1234,qop=auth-int,auth");
        final String digestResponse2 = scheme.createDigestResponse("user", "pwd", authChallenge2, context);
        Assertions.assertNotNull(digestResponse2);

        Assertions.assertTrue(digestResponse2.contains("qop=auth-int"), "no qop ? " + digestResponse2);
        Assertions.assertTrue(digestResponse2.contains("cnonce="), "no cnonce ? " + digestResponse2);
        Assertions.assertTrue(digestResponse2.contains("algorithm=MD5"), "algorithm ? " + digestResponse2);
        Assertions.assertTrue(digestResponse2.contains("nc="), "no nc ? " + digestResponse2);
    }
}