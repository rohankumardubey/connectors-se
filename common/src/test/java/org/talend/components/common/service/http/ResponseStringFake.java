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

import java.util.List;
import java.util.Map;

import org.talend.sdk.component.api.service.http.Response;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ResponseStringFake implements Response<String> {

    private final int status;

    private final Map<String, List<String>> headers;

    private final String body;

    @Override
    public int status() {
        return this.status;
    }

    @Override
    public Map<String, List<String>> headers() {
        return headers;
    }

    @Override
    public String body() {
        return body;
    }

    @Override
    public <E> E error(Class<E> type) {
        return null;
    }
}
