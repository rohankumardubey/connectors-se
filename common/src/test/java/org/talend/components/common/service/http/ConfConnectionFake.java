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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.talend.sdk.component.api.service.http.Configurer;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConfConnectionFake implements Configurer.Connection {

    private final String method;

    private final String url;

    private final String payload;

    private final Map<String, List<String>> headers = new HashMap<>();

    @Override
    public String getMethod() {
        return method;
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return headers;
    }

    @Override
    public byte[] getPayload() {
        return payload.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Configurer.Connection withHeader(String name, String value) {
        this.headers.computeIfAbsent(name, (String key) -> new ArrayList<>()).add(value);
        return this;
    }

    @Override
    public Configurer.Connection withReadTimeout(int timeout) {
        return this;
    }

    @Override
    public Configurer.Connection withConnectionTimeout(int timeout) {
        return this;
    }

    @Override
    public Configurer.Connection withoutFollowRedirects() {
        return this;
    }
}
