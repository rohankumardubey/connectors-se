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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Predicate;

import org.talend.daikon.security.url.URLUtils;

@Slf4j
public class ValidateSites {

    private final static List<String> ADDITIONAL_LOCAL_HOSTS = Arrays.asList(
            // local multicast from 224.0.0.0 to 224.0.0.255
            "224.0.0.");

    private final static class Constants {

        private Constants() {
        }

        // Constants for local access (env for system.getEnv, jvm for System.getProperty (-D option)).
        private static final String ENV_ENABLE_LOCAL_NETWORK_ACCESS = "CONNECTORS_ENABLE_LOCAL_NETWORK_ACCESS";

        private static final String JVM_ENABLE_LOCAL_NETWORK_ACCESS = "connectors.enable_local_network_access";

        // Constants for multicast access (env for system.getEnv, jvm for System.getProperty (-D option)).
        private static final String ENV_ENABLE_MULTICAST_NETWORK_ACCESS = "CONNECTORS_ENABLE_MULTICAST_NETWORK_ACCESS";

        private static final String JVM_ENABLE_MULTICAST_NETWORK_ACCESS = "connectors.enable_multicast_network_access";

        // Constants for uncrypted access (env for system.getEnv, jvm for System.getProperty (-D option)).
        private static final String ENV_ENABLE_NON_SECURED_ACCESS = "CONNECTORS_ENABLE_NON_SECURED_ACCESS";

        private static final String JVM_ENABLE_NON_SECURED_ACCESS = "connectors.enable_non_secured_access";
    }

    /**
     * Check if url / inetAddress is authorized or not.
     */
    @FunctionalInterface
    interface AccessChecker {

        boolean isAuthorized(final URL url, final InetAddress inetAddress);
    }

    /**
     * interface for environment.
     */
    @FunctionalInterface
    public interface Environment {

        boolean getValue(final String globalName, final String localName, final String defaultValue);
    }

    /** default system environment check jvm property first, then system env then defualt value */
    public static final Environment systemEnvironment =
            (String globalName, String localName, String defaultValue) -> Boolean.parseBoolean(
                    System.getProperty(localName, System.getenv().getOrDefault(globalName, defaultValue)));

    /**
     * Composed checker to Chain several checkers.
     */
    public static class GlobalAccessChecker implements AccessChecker {

        private final List<AccessChecker> elements;

        public GlobalAccessChecker(final List<AccessChecker> ba) {
            this.elements = new ArrayList<>(ba.size());
            this.elements.addAll(ba);
        }

        @Override
        public boolean isAuthorized(URL url, InetAddress inetAddress) {
            return this.elements.stream().allMatch((AccessChecker b) -> b.isAuthorized(url, inetAddress));
        }
    }

    /** build all checker based on AccessControls enum & environment. */
    public static AccessChecker buildChecker(final Environment env) {
        final ArrayList<AccessChecker> accesses = new ArrayList<>();
        for (AccessControls ac : AccessControls.values()) {
            if (!ac.isAuthorized.test(env)) {
                accesses.add(ac.accessChecker);
            }
        }
        accesses.trimToSize();
        if (accesses.size() == 0) {
            return (final URL url, final InetAddress inetAddress) -> true;
        }
        return new GlobalAccessChecker(accesses);
    }

    @RequiredArgsConstructor
    public enum AccessControls {

        LOCAL(
                (final URL url, final InetAddress inetAddress) -> !URLUtils.isLocalAddress(inetAddress)
                        && ADDITIONAL_LOCAL_HOSTS.stream()
                                .noneMatch(h -> url.getHost().contains(h)),
                (Environment env) -> env.getValue(Constants.ENV_ENABLE_LOCAL_NETWORK_ACCESS,
                        Constants.JVM_ENABLE_LOCAL_NETWORK_ACCESS, "true"),
                "[local]"),
        MULTICAST(
                (final URL url, final InetAddress inetAddress) -> !inetAddress.isMulticastAddress(),
                (Environment env) -> env.getValue(Constants.ENV_ENABLE_MULTICAST_NETWORK_ACCESS,
                        Constants.JVM_ENABLE_MULTICAST_NETWORK_ACCESS, "true"),
                "[multicast]"),

        NON_SECURED(
                (final URL url, final InetAddress inetAddress) -> "HTTPS"
                        .equals(url.getProtocol().toUpperCase(Locale.ENGLISH)),
                (Environment env) -> env.getValue(Constants.ENV_ENABLE_NON_SECURED_ACCESS,
                        Constants.JVM_ENABLE_NON_SECURED_ACCESS, "true"),
                "[non_secured]");

        private final AccessChecker accessChecker;

        public final Predicate<Environment> isAuthorized;

        final String messageToken;

        void replaceToken(final StringBuilder builder, final Environment env) {
            final int start = builder.indexOf(this.messageToken);
            if (start >= 0) {
                final boolean value = this.isAuthorized.test(env);
                builder.replace(start, start + this.messageToken.length(), Boolean.toString(value));
            }
        }

    }

    private ValidateSites() {
    }

    public static boolean isValidSite(final String base) {
        return isValidSite(base, ValidateSites.systemEnvironment);
    }

    public static boolean isValidSite(final String base, final Environment env) {
        final AccessChecker accessChecker = buildChecker(env);
        return isValidSite(base, accessChecker);
    }

    public static boolean isValidSite(final String surl, final AccessChecker access) {
        try {
            final URL url = new URL(surl);
            final String host = url.getHost();
            final InetAddress inetAddress = InetAddress.getByName(host);

            return access.isAuthorized(url, inetAddress);
        } catch (MalformedURLException e) {
            log.error(e.getMessage(), e);
            return false;
        } catch (UnknownHostException e) {
            return true;
        }
    }

    public static String buildErrorMessage(final Function<String, String> msgBuilder,
            final String endPoint) {
        return buildErrorMessage(msgBuilder, endPoint, ValidateSites.systemEnvironment);
    }

    public static String buildErrorMessage(final Function<String, String> msgBuilder,
            final String endPoint,
            final Environment env) {
        final StringBuilder builder = new StringBuilder(msgBuilder.apply(endPoint));
        Arrays.stream(AccessControls.values())
                .forEach(
                        (ValidateSites.AccessControls ctrl) -> ctrl.replaceToken(builder, env));
        return builder.toString();
    }
}
