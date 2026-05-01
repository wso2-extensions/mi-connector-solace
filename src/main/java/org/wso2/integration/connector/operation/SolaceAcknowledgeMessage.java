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

package org.wso2.integration.connector.operation;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;

import java.util.HashMap;
import java.util.Map;

/**
 * Acknowledges a message received by the inbound endpoint.
 * Should be called at the point in the mediation sequence where processing
 * is confirmed successful, so the broker can remove the message from the queue.
 *
 * Expects the inbound endpoint to have stored the original {@link BytesXMLMessage}
 * in the message context under the {@link SolaceConstants#SOLACE_INBOUND_MESSAGE} property.
 */
public class SolaceAcknowledgeMessage extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceAcknowledgeMessage.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        Object msgObj = messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_MESSAGE);
        if (msgObj == null) {
            handleException("No inbound Solace message found in message context, cannot acknowledge.", messageContext);
            return;
        }

        if (!(msgObj instanceof BytesXMLMessage)) {
            handleException("Invalid Solace message object.", messageContext);
            return;
        }

        BytesXMLMessage solaceMessage = (BytesXMLMessage) msgObj;

        try {
            solaceMessage.ackMessage();
            // Tell the inbound listener the message has been settled so it doesn't issue a
            // second ack post-mediation.
            messageContext.setProperty(SolaceConstants.SOLACE_INBOUND_MESSAGE_SETTLED, Boolean.TRUE);

            if (log.isDebugEnabled()) {
                log.debug("Message acknowledged successfully. AckMessageId: " + solaceMessage.getAckMessageId());
            }

            JSONObject response = new JSONObject();
            response.put("settled", true);
            response.put("outcome", "ACK");
            if (solaceMessage.getAckMessageId() != 0) {
                response.put("ackMessageId", solaceMessage.getAckMessageId());
            }
            response.put("settledAt", System.currentTimeMillis());

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("settled", true);
            attributes.put("outcome", "ACK");

            handleConnectorResponse(messageContext, responseVariable, overwriteBody,
                    response.toString(), null, attributes);
        } catch (Exception e) {
            handleException("Error acknowledging Solace message: " + e.getMessage(), e, messageContext);
        }
    }
}
