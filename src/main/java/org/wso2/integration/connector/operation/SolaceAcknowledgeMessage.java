/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
 * Licensed under the Apache License, Version 2.0
 */
package org.wso2.integration.connector.operation;

import com.solacesystems.jcsmp.BytesXMLMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;

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
        } catch (Exception e) {
            handleException("Error acknowledging Solace message: " + e.getMessage(), e, messageContext);
        }
    }
}
