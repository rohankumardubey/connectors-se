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
package org.talend.components.adlsgen2.service;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AdlsGen2ServiceTest {

    @Test
    void extractFileNameWithSpecialCharactersTest() {
        String linuxSupportedFileName = "someDir/file*\"<a!b;c>.txt";
        AdlsGen2Service service = new AdlsGen2Service();
        String fileName = service.extractFileName(linuxSupportedFileName);

        assertEquals(linuxSupportedFileName.substring(linuxSupportedFileName.lastIndexOf("/")), fileName);
    }

    @Test
    void extractDirNameWithSpecialCharactersTest() {
        String linuxSupportedFileName = "someDir\"*Dir;Name<>!/file*\"<abc>.txt";
        AdlsGen2Service service = new AdlsGen2Service();
        String dirName = service.extractFolderPath(linuxSupportedFileName);

        assertEquals(linuxSupportedFileName.substring(0, linuxSupportedFileName.lastIndexOf("/")), dirName);
    }
}