/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
 * Licensed under the Apache License, Version 2.0
 */
package org.wso2.integration.connector.operation;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.Builder;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.SolaceUtils;

/**
 * Sends a request message and blocks until a reply arrives or the timeout expires.
 * DIRECT delivery uses the JCSMP {@code Requestor}; PERSISTENT/NON_PERSISTENT delivery
 * uses a temporary queue with a {@code FlowReceiver} for guaranteed request-reply.
 */
public class SolaceSendRequest extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolaceSendRequest.class);
    private static final long DEFAULT_TIMEOUT_MS = 30000L;

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
        SolaceConnection connection = null;

        try {
            connection = (SolaceConnection) handler.getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);
            if (connection == null || !connection.isConnected()) {
                handleException("Solace connection is not available or not connected.", messageContext);
                return;
            }

            // Extract parameters
            String destinationType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.DESTINATION_TYPE);
            if (StringUtils.isEmpty(destinationType)) {
                destinationType = SolaceConstants.DESTINATION_TYPE_TOPIC;
            }

            String destinationName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.DESTINATION_NAME);
            if (StringUtils.isEmpty(destinationName)) {
                handleException("Destination name is required for sendRequest operation.", messageContext);
                return;
            }

            String deliveryMode = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.DELIVERY_MODE);
            if (StringUtils.isEmpty(deliveryMode)) {
                deliveryMode = SolaceConstants.DELIVERY_MODE_DIRECT;
            }

            if (SolaceConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(destinationType)
                    && SolaceConstants.DELIVERY_MODE_DIRECT.equalsIgnoreCase(deliveryMode)) {
                handleException("Invalid delivery mode 'DIRECT' for QUEUE destination '" + destinationName
                        + "'. Queues require guaranteed delivery — use PERSISTENT or NON_PERSISTENT.",
                        messageContext);
                return;
            }

            String messageType = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.MESSAGE_TYPE);
            if (StringUtils.isEmpty(messageType)) {
                messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
            }
            messageType = messageType.toUpperCase();
            // Solace has no JSON message type — JSON payloads are carried as TEXT on the wire.
            if (!SolaceConstants.MESSAGE_TYPE_TEXT.equals(messageType)
                    && !SolaceConstants.MESSAGE_TYPE_BYTES.equals(messageType)
                    && !SolaceConstants.MESSAGE_TYPE_XML.equals(messageType)) {
                handleException("Unsupported messageType '" + messageType
                        + "'. Supported types: TEXT, BYTES, XML.", messageContext);
                return;
            }

            String timeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.REQUEST_TIMEOUT);
            long timeout = StringUtils.isNotEmpty(timeoutStr) ? Long.parseLong(timeoutStr) : DEFAULT_TIMEOUT_MS;

            // Get message payload and detect its content type for downstream subscribers
            String[] payloadAndType = SolaceUtils.extractPayloadAndContentType(messageContext);
            String payload = payloadAndType[0];
            String httpContentType = payloadAndType[1];
            org.apache.axis2.context.MessageContext axis2Mc =
                    ((Axis2MessageContext) messageContext).getAxis2MessageContext();

            // Build optional message properties
            SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext);

            // Send the request (DIRECT via Requestor, guaranteed via temp queue + FlowReceiver)
            BytesXMLMessage response;
            try {
                response = connection.sendRequest(destinationType, destinationName, payload, deliveryMode,
                        messageType, msgProperties, timeout, httpContentType);
            } catch (JCSMPRequestTimeoutException e) {
                handleException("sendRequest timed out after " + timeout + "ms. No response received from '"
                        + destinationName + "'.", e, messageContext);
                return;
            }

            if (response == null) {
                handleException("sendRequest returned no response from '" + destinationName + "'.",
                        messageContext);
                return;
            }

            // Extract raw payload bytes from response
            byte[] responseBytes = extractRawPayload(response);
            String contentType = response.getHTTPContentType();
            if (contentType == null || contentType.isEmpty()) {
                contentType = inferContentType(response);
            }

            // Set response properties in message context
            messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, destinationName);
            messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE, deliveryMode);
            if (response.getCorrelationId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_CORRELATION_ID, response.getCorrelationId());
            }
            if (response.getSenderId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_SENDER_ID, response.getSenderId());
            }
            if (response.getApplicationMessageId() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_ID,
                        response.getApplicationMessageId());
            }
            if (response.getApplicationMessageType() != null) {
                messageContext.setProperty(SolaceConstants.SOLACE_APPLICATION_MESSAGE_TYPE,
                        response.getApplicationMessageType());
            }
            if (response.getReceiveTimestamp() > 0) {
                messageContext.setProperty(SolaceConstants.SOLACE_REPLY_RECEIVE_TIMESTAMP,
                        response.getReceiveTimestamp());
            }

            // Use BuilderUtil to properly build the response based on content type
            axis2Mc.setProperty(Constants.Configuration.MESSAGE_TYPE, contentType);
            axis2Mc.setProperty(Constants.Configuration.CONTENT_TYPE, contentType);

            InputStream inputStream = new ByteArrayInputStream(responseBytes);
            Builder builder = BuilderUtil.getBuilderFromSelector(contentType, axis2Mc);
            OMElement documentElement = builder.processDocument(inputStream, contentType, axis2Mc);

            if (messageContext.getEnvelope().getBody().getFirstElement() != null) {
                messageContext.getEnvelope().getBody().getFirstElement().detach();
            }
            messageContext.getEnvelope().getBody().addChild(documentElement);

            if (log.isDebugEnabled()) {
                log.debug("Response received successfully for request to " + destinationName);
            }

        } catch (JCSMPException e) {
            handleException("Failed to execute sendRequest operation: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Failed to execute sendRequest operation: " + e.getMessage(), e, messageContext);
        } finally {
            if (connection != null) {
                handler.returnConnection(SolaceConstants.CONNECTOR_NAME, connectionName, connection);
            }
        }
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
        // Fallback: binary attachment
        byte[] data = message.getBytes();
        return data != null ? data : new byte[0];
    }

    private String inferContentType(BytesXMLMessage message) {
        if (message instanceof XMLContentMessage) {
            return "application/xml";
        } else if (message instanceof TextMessage) {
            String text = ((TextMessage) message).getText();
            if (text != null) {
                String trimmed = text.trim();
                if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                    return "application/json";
                } else if (trimmed.startsWith("<")) {
                    return "application/xml";
                }
            }
            return "text/plain";
        }
        return "application/octet-stream";
    }
}
