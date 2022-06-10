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
package org.talend.components.cosmosDB;

import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;
import org.talend.sdk.component.runtime.serialization.LightContainer;

import java.util.Objects;

public class LightTestContainer implements LightContainer {

    @Override
    public ClassLoader classloader() {
        return Thread.currentThread().getContextClassLoader();
    }

    @Override
    public <T> T findService(Class<T> key) {
        if (Objects.equals(RecordBuilderFactory.class, key)) {
            return (T) new RecordBuilderFactoryImpl("test");
        }
        return null;
    }
}
