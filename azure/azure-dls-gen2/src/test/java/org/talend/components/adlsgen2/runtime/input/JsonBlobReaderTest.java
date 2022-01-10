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
package org.talend.components.adlsgen2.runtime.input;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.ClientGen2Fake;
import org.talend.components.adlsgen2.FakeActiveDirectoryService;
import org.talend.components.adlsgen2.FakeResponse;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.common.formats.JSONFormatOptions;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.ComponentManager;

@WithComponents("org.talend.components.adlsgen2")
class JsonBlobReaderTest extends AdlsGen2TestBase {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Test
    void readRecord() {
        this.inputConfiguration.getDataSet().setFormat(FileFormat.JSON);
        this.dataSet.setJsonConfiguration(new JSONFormatOptions());

        final ComponentManager manager = componentsHandler.asManager();

        final JsonObject object1 = Json
                .createObjectBuilder() //
                .add("etag", "0x8D89D1980D8BD4B") //
                .add("name", "/paht1/file1.txt") //
                .add("contentLength", "120") //
                .add("lastModified", "2021-01-13") //
                .add("owner", "admin") //
                .build();
        final JsonObject object2 = Json
                .createObjectBuilder() //
                .add("etag", "0x8D89D1980D8BD4B") //
                .add("name", "/paht1/file2.txt") //
                .add("contentLength", "120") //
                .add("lastModified", "2021-01-13") //
                .add("owner", "admin") //
                .build();

        final JsonObject paths = Json
                .createObjectBuilder()
                .add("paths", Json
                        .createArrayBuilder() //
                        .add(object1) //
                        .add(object2) //
                        .build())
                .build();
        ClientGen2Fake fake = new ClientGen2Fake(new FakeResponse<>(200, paths, null, null)) {

            @Override
            public Response<InputStream> pathRead(Map<String, String> headers, String filesystem, String path,
                    Integer timeout,
                    Map<String, String> sas) {
                final InputStream input;
                if ("/paht1/file1.txt".equals(path)) {
                    input = new ByteArrayInputStream(
                            "[{ \"field1\": \"Fic1\", \"field2\": \"line1\"}, { \"field1\": \"Fic1\", \"field2\": \"line2\"}]"
                                    .getBytes(StandardCharsets.UTF_8));
                } else if ("/paht1/file2.txt".equals(path)) {
                    input = new ByteArrayInputStream(
                            "{ \"field1\": \"Fic2\", \"field2\": \"line1\"}".getBytes(StandardCharsets.UTF_8));
                } else {
                    throw new RuntimeException("Path '" + path + "' not expected here");
                }
                return new FakeResponse<>(200, input, null, null);
            }
        };
        ClientGen2Fake.inject(manager, fake);
        final AdlsGen2Service gen2Service = this.componentsHandler.findService(AdlsGen2Service.class);

        final JsonBlobReader reader = new JsonBlobReader(this.inputConfiguration, this.recordBuilderFactory,
                Json.createBuilderFactory(Collections.emptyMap()), gen2Service, new FakeActiveDirectoryService());

        final Record record1 = reader.readRecord();
        Assertions.assertEquals("Fic1", record1.getString("field1"));
        Assertions.assertEquals("line1", record1.getString("field2"));

        final Record record2 = reader.readRecord();
        Assertions.assertEquals("Fic1", record2.getString("field1"));
        Assertions.assertEquals("line2", record2.getString("field2"));

        final Record record3 = reader.readRecord();
        Assertions.assertEquals("Fic2", record3.getString("field1"));
        Assertions.assertEquals("line1", record3.getString("field2"));

        final Record endRecord = reader.readRecord();

        Assertions.assertNull(endRecord);
    }
}