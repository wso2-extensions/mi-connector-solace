/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.integration.connector.models;

/**
 * Carries the outcome of a Solace publish call back to the operation layer
 * so status properties and the response variable can be populated without
 * conflating transport errors with broker ACK outcomes.
 */
public class PublishResult {

    private final String ackStatus;
    private final boolean ackReceived;
    private final String correlationKey;
    private final String error;

    public PublishResult(String ackStatus, boolean ackReceived, String correlationKey, String error) {
        this.ackStatus = ackStatus;
        this.ackReceived = ackReceived;
        this.correlationKey = correlationKey;
        this.error = error;
    }

    public String getAckStatus() {
        return ackStatus;
    }

    public boolean isAckReceived() {
        return ackReceived;
    }

    public String getCorrelationKey() {
        return correlationKey;
    }

    public String getError() {
        return error;
    }
}
