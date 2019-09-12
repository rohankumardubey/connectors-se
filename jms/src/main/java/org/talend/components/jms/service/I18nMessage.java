/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.jms.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18nMessage {

    String errorEmptyURL();

    String errorLoadProvider(String driverId, String missingJars);

    String errorInvalidConnection();

    String errorStartMessagesDelivery();

    String errorCreateJMSInstance();

    String errorInstantiateConnectionFactory(String message);

    String errorCantSendMessage();

    String errorCantReceiveMessage();

    String warnProducerCantBeClosed();

    String warnConsumerCantBeClosed();

    String warnSessionCantBeClosed();

    String warnConnectionCantBeClosed();

    String warnJNDIContextCantBeClosed();

    String successConnection();
}
