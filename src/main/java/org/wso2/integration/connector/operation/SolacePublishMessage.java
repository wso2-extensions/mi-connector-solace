/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
 * Licensed under the Apache License, Version 2.0
 */
package org.wso2.integration.connector.operation;

import com.solacesystems.jcsmp.JCSMPException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.json.JSONObject;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.models.PublishResult;
import org.wso2.integration.connector.models.SolaceMessageProperties;
import org.wso2.integration.connector.utils.SolaceUtils;

/**
 * Publishes a message to Solace (operation class).
 */
public class SolacePublishMessage extends AbstractConnectorOperation {
    private static final Log log = LogFactory.getLog(SolacePublishMessage.class);

    @Override
    public void execute(MessageContext messageContext, String responseVariable, Boolean overwriteBody) {
        String connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
        if (connectionName == null) {
            handleException("Connection name is not set.", messageContext);
            return;
        }
        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        try {
            // Check what happens if connection broke after fetching
            SolaceConnection connection = (SolaceConnection) handler.getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);
            if (connection == null || !connection.isConnected()) {
                handleException("Solace connection is not available or not connected.", messageContext);
                return;
            }

            // Extract parameters
            String destinationType = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.DESTINATION_TYPE);
            if (StringUtils.isEmpty(destinationType)) {
                destinationType = SolaceConstants.DESTINATION_TYPE_TOPIC;
            }
            String destinationName = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.DESTINATION_NAME);
            if (StringUtils.isEmpty(destinationName)) {
                handleException("Destination name is required for publishing a message.", messageContext);
                return;
            }

            String deliveryMode = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.DELIVERY_MODE);
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

            String messageType = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.MESSAGE_TYPE);
            if (StringUtils.isEmpty(messageType)) {
                messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
            }
            messageType = messageType.toUpperCase();
            // Solace has no JSON message type — JSON payloads are carried as TEXT on the wire.
            // XML uses Solace's XMLContentMessage so consumers can filter on the XML SMF header.
            if (!SolaceConstants.MESSAGE_TYPE_TEXT.equals(messageType)
                    && !SolaceConstants.MESSAGE_TYPE_BYTES.equals(messageType)
                    && !SolaceConstants.MESSAGE_TYPE_XML.equals(messageType)) {
                handleException("Unsupported messageType '" + messageType
                        + "'. Supported types: TEXT, BYTES, XML.", messageContext);
                return;
            }

            // Get message payload and detect its content type for downstream subscribers
            String[] payloadAndType = SolaceUtils.extractPayloadAndContentType(messageContext);
            String payload = payloadAndType[0];
            String httpContentType = payloadAndType[1];

            if (log.isDebugEnabled()) {
                log.debug("Publishing message to Solace with parameters: destinationType=" + destinationType
                        + ", destinationName=" + destinationName + ", deliveryMode=" + deliveryMode
                        + ", messageType=" + messageType + ", httpContentType=" + httpContentType);
            }

            // Build optional message properties
            SolaceMessageProperties msgProperties = SolaceUtils.buildMessageProperties(messageContext);

            // Publish ACK wait (guaranteed mode only)
            String waitForAckStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.WAIT_FOR_ACK);
            boolean waitForAck = StringUtils.isNotEmpty(waitForAckStr) && Boolean.parseBoolean(waitForAckStr);
            String ackTimeoutStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.ACK_TIMEOUT);
            long ackTimeout = StringUtils.isNotEmpty(ackTimeoutStr)
                    ? Long.parseLong(ackTimeoutStr)
                    : SolaceConstants.DEFAULT_ACK_TIMEOUT_MS;
            String continueOnAckFailureStr = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                    SolaceConstants.CONTINUE_ON_ACK_FAILURE);
            boolean continueOnAckFailure = StringUtils.isNotEmpty(continueOnAckFailureStr)
                    && Boolean.parseBoolean(continueOnAckFailureStr);

            // Publish the message
            try {
                PublishResult result = connection.publish(destinationType, destinationName, payload,
                        deliveryMode, messageType, msgProperties, waitForAck, ackTimeout, httpContentType);
                if (log.isDebugEnabled()) {
                    log.debug("Message '" + payload + "' published to " + destinationType + " '" + destinationName
                            + "' with delivery mode: " + deliveryMode + ", ackStatus=" + result.getAckStatus());
                }
                setResultInContext(messageContext, result, destinationType, destinationName, deliveryMode,
                        messageType, responseVariable);

                if (!result.isAckReceived() && waitForAck && !continueOnAckFailure
                        && (SolaceConstants.ACK_STATUS_NACK.equals(result.getAckStatus())
                        || SolaceConstants.ACK_STATUS_TIMEOUT.equals(result.getAckStatus()))) {
                    handleException("Publish failed for '" + destinationName + "': "
                            + result.getAckStatus() + " — " + result.getError(), messageContext);
                    return;
                }
            } catch (JCSMPException e) {
                handleException("Error publishing message to Solace: " + e.getMessage(), e, messageContext);
            } finally {
                // Return the connection to the pool
                handler.returnConnection(SolaceConstants.CONNECTOR_NAME, connectionName, connection);
            }
        } catch (Exception e) {
            handleException("Failed to get Solace connection: " + connectionName, e, messageContext);
        }
    }

    private void setResultInContext(MessageContext messageContext, PublishResult result,
                                    String destinationType, String destinationName, String deliveryMode,
                                    String messageType, String responseVariable) {
        messageContext.setProperty(SolaceConstants.SOLACE_DESTINATION, destinationName);
        messageContext.setProperty(SolaceConstants.SOLACE_DELIVERY_MODE, deliveryMode);
        messageContext.setProperty(SolaceConstants.SOLACE_ACK_STATUS, result.getAckStatus());
        messageContext.setProperty(SolaceConstants.SOLACE_ACK_RECEIVED, String.valueOf(result.isAckReceived()));
        if (result.getCorrelationKey() != null) {
            messageContext.setProperty(SolaceConstants.SOLACE_ACK_CORRELATION_KEY, result.getCorrelationKey());
        }
        if (result.getError() != null) {
            messageContext.setProperty(SolaceConstants.SOLACE_ACK_ERROR, result.getError());
        }

        if (StringUtils.isNotEmpty(responseVariable)) {
            JSONObject response = new JSONObject();
            response.put("destination", destinationName);
            response.put("destinationType", destinationType);
            response.put("deliveryMode", deliveryMode);
            response.put("messageType", messageType);
            response.put("ackStatus", result.getAckStatus());
            response.put("ackReceived", result.isAckReceived());
            response.put("correlationKey", result.getCorrelationKey() != null ? result.getCorrelationKey() : JSONObject.NULL);
            response.put("error", result.getError() != null ? result.getError() : JSONObject.NULL);
            response.put("publishedAt", System.currentTimeMillis());
            messageContext.setProperty(responseVariable, response.toString());
        }
    }
}
