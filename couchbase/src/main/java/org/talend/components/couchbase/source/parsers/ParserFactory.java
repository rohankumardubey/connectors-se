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
package org.talend.components.couchbase.source.parsers;

import org.talend.components.couchbase.dataset.DocumentType;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class ParserFactory {

    private ParserFactory() {
    }

    public static DocumentParser createDocumentParser(DocumentType documentType, RecordBuilderFactory builderFactory) {
        if (documentType == DocumentType.BINARY) {
            return new BinaryParser(builderFactory);
        } else if (documentType == DocumentType.STRING) {
            return new StringParser(builderFactory);
        } else {
            // only BINARY or STRING document types are expected for ParserFactory
            throw new ComponentException("[Parser factory] Unexpected document type: " + documentType);
        }
    }
}
