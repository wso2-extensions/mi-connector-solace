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

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.utils.SolaceUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Synchronously polls one message from a Solace queue. Returns null on timeout.
 * The polled message's payload becomes the response variable's payload (or replaces
 * the message body if {@code overwriteBody} is true). Metadata such as messageId,
 * correlationId, and the received/timedOut flags are exposed as response attributes
 * (accessible via {@code ${vars.X.attributes.messageId}}) and as legacy
 * {@code solace.*} message-context properties.
 *
 * <p><b>Settlement model — depends on whether the operation runs inside a transaction:</b></p>
 * <ul>
 *   <li><b>Outside a transaction (default):</b> the message is acked on receipt, before
 *       this operation returns. If mediation fails afterwards the message is gone — no
 *       redelivery. Suitable for at-most-once / idempotent processing only.</li>
 *   <li><b>Inside a transaction</b> (caller wrapped this poll in {@code beginTransaction}
 *       and presence of the {@code solace.tx.connectionId} property is detected): the
 *       message is held by the broker until the surrounding {@code commit} (acked) or
 *       {@code rollback} (redelivered). This gives at-least-once semantics. Recommended
 *       for any consume-then-process flow where mediation can fail.</li>
 * </ul>
 *
 * <p>Manual ack/nack of polled messages is intentionally not supported — settlement is
 * either implicit (non-tx, on receipt) or transaction-scoped (commit/rollback).</p>
 */
public class SolacePollMessage extends AbstractConnectorOperation {

    private static final Log log = LogFactory.getLog(SolacePollMessage.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        // Detect transactional context the same way publish does — if the caller wrapped
        // this poll in beginTransaction, route to the transacted path so settlement is
        // deferred to commit/rollback (rollback redelivers; non-transacted poll loses).
        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        boolean isTransactional = (txId != null);
        if (isTransactional) {
            log.info("solace.poll: transactional path, txId=" + txId
                    + ", connectionName=" + connectionName);
        }

        SolaceConnection connection = null;
        try {
            if (isTransactional) {
                connection = SolaceTransactionRegistry.get(txId);
                if (connection == null) {
                    log.info("solace.poll: txId=" + txId + " not found in TransactionRegistry");
                    handleException("Transaction " + txId + " not found", messageContext);
                    return;
                }
                log.info("solace.poll: txId=" + txId
                        + " resolved to connectionId=" + connection.getConnectionId());
            } else {
                connection = (SolaceConnection) handler.getConnection(
                        SolaceConstants.CONNECTOR_NAME, connectionName);
                if (connection == null || !connection.isConnected()) {
                    handleException("Solace connection is not available or not connected.", messageContext);
                    return;
                }
            }

            String queueName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.QUEUE_NAME);
            if (StringUtils.isEmpty(queueName)) {
                handleException("Queue name is required for poll operation.", messageContext);
                return;
            }

            String timeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.POLL_TIMEOUT);
            long timeout = StringUtils.isNotEmpty(timeoutStr)
                    ? Long.parseLong(timeoutStr)
                    : SolaceConstants.DEFAULT_POLL_TIMEOUT_MS;

            String selector = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.SELECTOR);

            if (log.isDebugEnabled()) {
                log.debug("Polling queue '" + queueName + "' (timeout=" + timeout + "ms"
                        + (selector != null ? ", selector='" + selector + "'" : "")
                        + ", transactional=" + isTransactional + ")");
            }

            BytesXMLMessage message = isTransactional
                    ? connection.pollTransacted(queueName, timeout, selector)
                    : connection.pollMessage(queueName, timeout, selector);
            writeResult(messageContext, message, queueName, responseVariable, overwriteBody);

        } catch (JCSMPException e) {
            handleException("Solace poll failed: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Solace poll operation failed (connection: " + connectionName + ")",
                    e, messageContext);
        } finally {
            // Pinned (transactional) connection stays held until commit/rollback.
            if (!isTransactional && connection != null) {
                handler.returnConnection(SolaceConstants.CONNECTOR_NAME, connectionName, connection);
            }
        }
    }

    /**
     * Builds the response payload + attributes and routes them through
     * {@link #handleConnectorResponse} so {@code ${vars.X}} resolves correctly and
     * {@code overwriteBody} behaves identically to other framework operations.
     */
    private void writeResult(MessageContext messageContext, BytesXMLMessage message, String queueName,
                             String responseVariable, Boolean overwriteBody) {

        Map<String, Object> attributes = new HashMap<>();
        attributes.put(SolaceConstants.RESULT_DESTINATION, queueName);
        attributes.put(SolaceConstants.RESULT_RECEIVED, message != null);
        attributes.put(SolaceConstants.RESULT_TIMED_OUT, message == null);

        String payloadJson;

        if (message == null) {
            // Timeout path. Use a JSON envelope so callers can branch on receivedflag
            // without dereferencing a null payload.
            JSONObject envelope = new JSONObject();
            envelope.put(SolaceConstants.RESULT_RECEIVED, false);
            envelope.put(SolaceConstants.RESULT_TIMED_OUT, true);
            envelope.put(SolaceConstants.RESULT_DESTINATION, queueName);
            payloadJson = envelope.toString();
        } else {
            // Surface common metadata in legacy solace.* properties for downstream mediation.
            if (message.getMessageId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_MESSAGE_ID, message.getMessageId());
            }
            if (message.getCorrelationId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_CORRELATION_ID, message.getCorrelationId());
            }
            if (message.getApplicationMessageId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_ID,
                        message.getApplicationMessageId());
            }
            if (message.getApplicationMessageType() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_TYPE,
                        message.getApplicationMessageType());
            }
            if (message.getSenderId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_SENDER_ID, message.getSenderId());
            }
            if (message.getDeliveryMode() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE,
                        message.getDeliveryMode().name());
            }
            messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, queueName);
            messageContext.setProperty(SolaceConstants.SOLACE_REDELIVERED, message.getRedelivered());

            // Standard SMF headers + publisher-supplied userProperties — surfaced as
            // ${vars.X.attributes.*}. Done before contentType so the inferred content
            // type takes precedence over anything the publisher may have put there.
            SolaceUtils.populateMessageMetadata(message, attributes);

            byte[] bytes = extractRawPayload(message);
            String contentType = message.getHTTPContentType();
            if (StringUtils.isEmpty(contentType)) {
                contentType = inferContentType(message);
            }
            attributes.put("contentType", contentType);

            // The payload becomes either the body (when overwriteBody=true) or the
            // payload field of the response variable. handleConnectorResponse parses it
            // as JSON, so wrap non-JSON content in a JSON string literal.
            String body = new String(bytes, StandardCharsets.UTF_8);
            payloadJson = looksLikeJson(body) ? body : JSONObject.quote(body);
        }
        log.info("response: "+ payloadJson + " responseVariable: " + responseVariable 
            + " overwriteBody: " + overwriteBody + "Attributes: " + attributes.toString());
        handleConnectorResponse(messageContext, responseVariable, overwriteBody, payloadJson,
                null, attributes);
    }

    private boolean looksLikeJson(String s) {
        if (s == null || s.isEmpty()) return false;
        String t = s.trim();
        return t.startsWith("{") || t.startsWith("[");
    }

    private byte[] extractRawPayload(BytesXMLMessage message) {
        if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            return text != null ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        } else if (message instanceof XMLContentMessage) {
            String xml = ((XMLContentMessage) message).getXMLContent();
            return xml != null ? xml.getBytes(StandardCharsets.UTF_8) : new byte[0];
        } else if (message instanceof BytesMessage) {
            byte[] data = ((BytesMessage) message).getData();
            return data != null ? data : new byte[0];
        }
        byte[] data = message.getBytes();
        return data != null ? data : new byte[0];
    }

    private String inferContentType(BytesXMLMessage message) {
        if (message instanceof XMLContentMessage) {
            return "application/xml";
        }
        if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            if (text != null) {
                String trimmed = text.trim();
                if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                    return "application/json";
                }
                if (trimmed.startsWith("<")) {
                    return "application/xml";
                }
            }
            return "text/plain";
        }
        return "application/octet-stream";
    }
}
