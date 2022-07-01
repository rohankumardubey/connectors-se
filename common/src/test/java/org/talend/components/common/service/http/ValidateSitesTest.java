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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ValidateSitesTest {

    @ParameterizedTest
    @CsvSource(value = { //
            // Forbidden address if connectors.enable_local_network_access == true
            // loopback address
            "http://127.0.0.1/my/api, false, false, true, false", //
            "http://127.0.0.1/my/api, true, false, true, true", //
            "http://127.0.0.1:80/my/api, false, false, true, false", //
            "http://127.0.0.1:80/my/api, true, false, true, true", //
            "http://localhost/my/api, false, false, true, false", //
            "http://localhost/my/api, true, false, true, true", //
            "http://myapp.localhost.com:80/my/api, false, false, true, true", //
            "http://myapp.localhost.com:80/my/api, true, false, true, true", //
            // RFC1918 / Private Address Space : https://tools.ietf.org/html/rfc1918
            "http://10.0.0.0/my/api, false, false, true, false", //
            "http://10.0.0.0/my/api, true, false, true, true", //
            "http://10.10.0.0/my/api, false, false, true, false", //
            "http://10.10.0.0/my/api, true, false, true, true", //
            "http://10.10.10.0/my/api, false, false, true, false", //
            "http://10.10.10.0/my/api, true, false, true, true", //
            "http://10.10.10.10/my/api, false, false, true, false", //
            "http://10.10.10.10/my/api, true, false, true, true", //
            "http://10.255.255.255/my/api, false, false, true, false", //
            "http://10.255.255.255/my/api, true, false, true, true", //
            "http://172.16.0.0/my/api, false, false, true, false", //
            "http://172.16.0.0/my/api, true, false, true, true", //
            "http://172.16.255.0/my/api, false, false, true, false", //
            "http://172.16.255.0/my/api, true, false, true, true", //
            "http://172.25.25.25/my/api, false, false, true, false", //
            "http://172.25.25.25/my/api, true, false, true, true", //
            "http://172.31.0.0/my/api, false, false, true, false", //
            "http://172.31.0.0/my/api, true, false, true, true", //
            "http://172.31.0.255/my/api, false, false, true, false", //
            "http://172.31.0.255/my/api, true, false, true, true", //
            "http://192.168.0.0/my/api, false, false, true, false", //
            "http://192.168.0.0/my/api, true, false, true, true", //
            "http://192.168.128.10/my/api, false, false, true, false", //
            "http://192.168.128.10/my/api, true, false, true, true", //
            "http://192.168.255.255/my/api, false, false, true, false", //
            "http://192.168.255.255/my/api, true, false, true, true", //
            // Address found by pentesters
            "https://172.18.0.7/my/api, false, false, true, false", //
            "https://172.18.0.7/my/api, true, false, true, true", //
            // Multicast / Local subnetwork : https://en.wikipedia.org/wiki/Multicast_address
            "http://224.0.0.128:80/my/api, false, true, true, false", //
            "http://224.0.0.128:80/my/api, true, true, true, true", //
            "http://224.0.0.1:80/my/api, false, true, true, false", //
            "http://224.0.0.1:80/my/api, true, true, true, true", //
            "http://224.0.0.255:80/my/api, false, true, true, false", //
            "http://224.0.0.255:80/my/api, true, true, true, true", //
            //
            // Authorised addresses even if connectors.enable_local_network_access == true (external sites)
            "http://233.0.0.128:80/my/api, false, true, true, true", //
            "http://233.0.0.128:80/my/api, true, true, true, true", //
            "http://233.0.0.1:80/my/api, false, true, true, true", //
            "http://233.0.0.1:80/my/api, true, true, true, true", //
            "http://233.0.0.255:80/my/api, false, true, true, true", //
            "http://233.0.0.255:80/my/api, true, true, true, true", //
            "http://www.external.com/my/api, false, false, true, true", //
            "http://www.external.com/my/api, true, false, true, true", //
            //
            // Check if we disable multicast : https://en.wikipedia.org/wiki/Multicast_address
            "http://224.0.1.0:80/my/api, false, false, true, false", //
            "http://224.0.1.0:80/my/api, false, true, true, true", //
            "http://224.0.1.255:80/my/api, false, false, true, false", //
            "http://224.0.1.255:80/my/api, false, true, true, true", //
            //
            "http://224.0.2.0:80/my/api, false, false, true, false", //
            "http://224.0.2.0:80/my/api, false, true, true, true", //
            "http://224.0.255.255:80/my/api, false, false, true, false", //
            "http://224.0.255.255:80/my/api, false, true, true, true", //
            //
            "http://224.3.0.0:80/my/api, false, false, true, false", //
            "http://224.3.0.0:80/my/api, false, true, true, true", //
            "http://224.4.255.255:80/my/api, false, false, true, false", //
            "http://224.4.255.255:80/my/api, false, true, true, true", //
            //
            "http://232.0.0.0:80/my/api, false, false, true, false", //
            "http://232.0.0.0:80/my/api, false, true, true, true", //
            "http://232.255.255.255:80/my/api, false, false, true, false", //
            "http://232.255.255.255:80/my/api, false, true, true, true", //
            //
            "http://233.0.0.0:80/my/api, false, false, true, false", //
            "http://233.0.0.0:80/my/api, false, true, true, true", //
            "http://233.251.255.255:80/my/api, false, false, true, false", //
            "http://233.251.255.255:80/my/api, false, true, true, true", //
            //
            "http://233.252.0.0:80/my/api, false, false, true, false", //
            "http://233.252.0.0:80/my/api, false, true, true, true", //
            "http://233.252.255.255:80/my/api, false, false, true, false", //
            "http://233.255.255.255:80/my/api, false, true, true, true", //
            //
            "http://234.0.0.0:80/my/api, false, false, true, false", //
            "http://234.0.0.0:80/my/api, false, true, true, true", //
            "http://234.255.255.255:80/my/api, false, false, true, false", //
            "http://234.255.255.255:80/my/api, false, true, true, true", //
            //
            "http://239.0.0.0:80/my/api, false, false, true, false", //
            "http://239.0.0.0:80/my/api, false, true, true, true", //
            "http://239.255.255.255:80/my/api, false, false, true, false", //
            "http://239.255.255.255:80/my/api, false, true, true, false", // multicast address has site scope
            "http://www.google.fr, false, false, true, true",
            // HTTPS only
            "http://127.0.0.1/my/api, true, false, false, false", "https://127.0.0.1/my/api, true, false, false, true",
            "http://www.google.fr, false, false, false, false", "https://www.google.fr, false, false, true, true" })
    void valideSite(final String url, final boolean canAccessLocal, final boolean disableMulticast,
            final boolean allowNonSecure,
            final boolean expected) {

        ValidateSites.Environment env = (String globalName, String localName, String defaultValue) -> {
            if ("CONNECTORS_ENABLE_LOCAL_NETWORK_ACCESS".equals(globalName)) {
                return canAccessLocal;
            }
            if ("CONNECTORS_ENABLE_MULTICAST_NETWORK_ACCESS".equals(globalName)) {
                return disableMulticast;
            }
            if ("CONNECTORS_ENABLE_NON_SECURED_ACCESS".equals(globalName)) {
                return allowNonSecure;
            }
            return false;
        };
        final boolean validSite = ValidateSites.isValidSite(url, env);

        assertEquals(expected, validSite);
    }

    @Test
    void validateSiteWrongURL() {
        Assertions.assertFalse(ValidateSites.isValidSite("unknown://dd"));
    }

    @Test
    void envSys() {
        final String oldValue = System.getProperty("ValidatesSiteTestProp1");
        try {
            System.setProperty("ValidatesSiteTestProp1", "false");
            final boolean value = ValidateSites.systemEnvironment.getValue("NullValueExpected_1234",
                    "ValidatesSiteTestProp1",
                    "true");
            Assertions.assertFalse(value);
            System.clearProperty("ValidatesSiteTestProp1");
            final boolean value1 = ValidateSites.systemEnvironment.getValue("NullValueExpected_1234",
                    "ValidatesSiteTestProp1",
                    "true");
            Assertions.assertTrue(value1);
        } finally {
            if (oldValue == null) {
                System.clearProperty("ValidatesSiteTestProp1");
            } else {
                System.setProperty("ValidatesSiteTestProp1", oldValue);
            }
        }
    }

    @Test
    void buildMessage() {
        ValidateSites.Environment env = (String globalName, String localName, String defaultValue) -> true;
        final String message = ValidateSites.buildErrorMessage(this::message, "TheEndPoint", env);
        Assertions.assertTrue(message.contains("TheEndPoint"));
        Assertions.assertTrue(message.contains("local=true"));
        Assertions.assertTrue(message.contains("multicast=true"));
        Assertions.assertTrue(message.contains("non_secured=true"));
    }

    String message(String endPoint) {
        return "(" + endPoint + ": local=[local], multicast=[multicast], non_secured=[non_secured])";
    }

}