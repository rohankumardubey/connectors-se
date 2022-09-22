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
package org.talend.components.adlsgen2.get;

import java.io.Serializable;

import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

import static org.talend.components.adlsgen2.service.UIActionService.ACTION_FILESYSTEMS;
import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;

@Data
@GridLayout(value = { //
        @GridLayout.Row("connection"), //
        @GridLayout.Row("filesystem"), //
        @GridLayout.Row("blobPath"), //
        @GridLayout.Row("localFolder"), //
        @GridLayout.Row("includeSubDirectory"), //
        @GridLayout.Row("keepRemoteDirStructure"), //
        @GridLayout.Row("dieOnError"), //
})
@GridLayout(names = ADVANCED, value = { @GridLayout.Row({ "connection" }) })
@Documentation("ADLS get configuration")
public class AdlsGen2GetConfiguration implements Serializable {

    @Option
    @Required
    @Documentation("ADLS Gen2 Connection")
    private AdlsGen2Connection connection;

    @Option
    @Required
    @Suggestable(value = ACTION_FILESYSTEMS, parameters = { "connection" })
    @Documentation("FileSystem")
    private String filesystem;

    @Option
    @Required
    @Documentation("Local folder.")
    private String localFolder;

    @Option
    @Required
    @Documentation("Whether keep the remote directory structure.")
    private boolean keepRemoteDirStructure;

    @Option
    @Required
    @Documentation("Whether include subdirectory.")
    private boolean includeSubDirectory;

    @Option
    @Required
    @Documentation("The remote target directory.")
    private String blobPath;

    @Option
    @Documentation("Stop running when get error.")
    private boolean dieOnError;

}
