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
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.util.ConnectorUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Negatively acknowledges a message received by the inbound endpoint.
 * <p>
 * Supports two settlement outcomes:
 * <ul>
 *   <li><b>FAILED</b> — transient processing failure; the broker will redeliver
 *       the message (subject to max redelivery count).</li>
 *   <li><b>REJECTED</b> — permanent rejection; the broker moves the message to
 *       the Dead Message Queue (DMQ) if configured, otherwise discards it.</li>
 * </ul>
 * <p>
 * <b>Important:</b> The inbound endpoint must configure
 * {@code ConsumerFlowProperties.addRequiredSettlementOutcomes(Outcome.FAILED, Outcome.REJECTED)}
 * on its flow receiver for NACK to work. Without this, {@code settle()} will throw.
 */
public class SolaceNackMessage extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceNackMessage.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        Object msgObj = messageContext.getProperty(SolaceConstants.SOLACE_INBOUND_MESSAGE);
        if (msgObj == null) {
            handleException("No inbound Solace message found in message context, cannot NACK.", messageContext);
            return;
        }

        if (!(msgObj instanceof BytesXMLMessage)) {
            handleException("Invalid Solace message object.", messageContext);
            return;
        }

        BytesXMLMessage solaceMessage = (BytesXMLMessage) msgObj;

        String outcomeType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.OUTCOME_TYPE);
        if (StringUtils.isEmpty(outcomeType)) {
            outcomeType = SolaceConstants.OUTCOME_FAILED;
        }

        try {
            XMLMessage.Outcome outcome;
            if (SolaceConstants.OUTCOME_REJECTED.equalsIgnoreCase(outcomeType)) {
                outcome = XMLMessage.Outcome.REJECTED;
            } else {
                outcome = XMLMessage.Outcome.FAILED;
            }

            solaceMessage.settle(outcome);
            // Tell the inbound listener the message has been settled so it doesn't issue a
            // second ack/nack post-mediation — the broker treats double-settlement as a
            // protocol violation and (for FAILED) the conflicting ack disrupts redelivery
            // counting, causing infinite redelivery.
            messageContext.setProperty(SolaceConstants.SOLACE_INBOUND_MESSAGE_SETTLED, Boolean.TRUE);

            if (log.isDebugEnabled()) {
                log.debug("Message negatively acknowledged with outcome " + outcome
                        + ". AckMessageId: " + solaceMessage.getAckMessageId());
            }

            JSONObject response = new JSONObject();
            response.put("settled", true);
            response.put("outcome", outcome.name());
            if (solaceMessage.getAckMessageId() != 0) {
                response.put("ackMessageId", solaceMessage.getAckMessageId());
            }
            response.put("settledAt", System.currentTimeMillis());

            Map<String, Object> attributes = new HashMap<>();
            attributes.put("settled", true);
            attributes.put("outcome", outcome.name());
            
            handleConnectorResponse(messageContext, responseVariable, overwriteBody,
                    response.toString(), null, attributes);
        } catch (JCSMPException e) {
            handleException("Error settling Solace message with NACK: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Error negatively acknowledging Solace message: " + e.getMessage(), e, messageContext);
        }
    }
}
