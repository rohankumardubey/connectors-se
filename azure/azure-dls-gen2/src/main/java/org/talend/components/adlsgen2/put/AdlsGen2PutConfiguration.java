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
package org.talend.components.adlsgen2.put;

import static org.talend.components.adlsgen2.service.UIActionService.ACTION_FILESYSTEMS;

import java.io.Serializable;
import java.util.List;

import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout(value = { //
        @GridLayout.Row("connection"), //
        @GridLayout.Row("filesystem"), //
        @GridLayout.Row("blobPath"), //
        @GridLayout.Row("localFolder"), //
        @GridLayout.Row("useFileList"), //
        @GridLayout.Row("files"), //
        @GridLayout.Row("overwrite"), //
        @GridLayout.Row("dieOnError"), //
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { //
        @GridLayout.Row({ "allowEscapePlusSymbol" }) })
@Documentation("ADLS get configuration")
public class AdlsGen2PutConfiguration implements Serializable {

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
    @Documentation("The remote target directory.")
    private String blobPath;

    @Option
    @Required
    @Documentation("Local folder")
    private String localFolder;

    @Option
    @Required
    @Documentation("Whether use file list.")
    private boolean useFileList;

    @Option
    @ActiveIf(target = "useFileList", value = "true")
    @Documentation("The file list.")
    private List<FileMask> files;

    @Data
    @OptionsOrder({ "fileMask", "newName" })
    @Documentation("A table for managing local files.")
    public static class FileMask implements Serializable {

        @Option
        @Required
        @Documentation("The file mask.")
        private String fileMask = "";

        @Option
        @Documentation("The new name.")
        private String newName;

    }

    @Option
    @Documentation("Whether overwrite an existing file.")
    private boolean overwrite;

    @Option
    @Documentation("Stop running when get error.")
    private boolean dieOnError;

    @Option
    @ActiveIf(target = "useFileList", value = "true")
    @Documentation("Allow to escape the ''+'' sign in filemask.")
    private boolean allowEscapePlusSymbol;

}
