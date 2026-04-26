/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com)
 * Licensed under the Apache License, Version 2.0
 */
package org.wso2.integration.connector.operation;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnectorOperation;
import org.wso2.integration.connector.core.util.ConnectorUtils;

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

            if (log.isDebugEnabled()) {
                log.debug("Message negatively acknowledged with outcome " + outcome
                        + ". AckMessageId: " + solaceMessage.getAckMessageId());
            }
        } catch (JCSMPException e) {
            handleException("Error settling Solace message with NACK: " + e.getMessage(), e, messageContext);
        } catch (Exception e) {
            handleException("Error negatively acknowledging Solace message: " + e.getMessage(), e, messageContext);
        }
    }
}
