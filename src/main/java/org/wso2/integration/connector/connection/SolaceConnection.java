/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
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

package org.wso2.integration.connector.connection;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPRequestTimeoutException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.TransactedSession;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.Connection;
import org.wso2.integration.connector.core.connection.ConnectionConfig;
import org.wso2.integration.connector.models.PublishResult;
import org.wso2.integration.connector.models.SolaceMessageProperties;

/**
 * Manages a connection to the Solace event broker.
 * Wraps a JCSMPSession and XMLMessageProducer.
 */
public class SolaceConnection implements Connection {

    private static final Log log = LogFactory.getLog(SolaceConnection.class);

    private JCSMPSession session;
    private XMLMessageProducer producer;
    private XMLMessageConsumer consumer;
    private final String connectionId;
    private TransactedSession txSession;
    private XMLMessageProducer txProducer;
    private final Object txLock = new Object();

    /**
     * Tracks in-flight guaranteed publishes when the caller asked to block for the broker ACK.
     * Keyed by correlation key. The {@link PublishEventHandler} completes the future on ack or
     * error; the calling thread then unblocks from {@link #publish}.
     */
    private final ConcurrentMap<String, CompletableFuture<Void>> pendingAcks = new ConcurrentHashMap<>();

    /**
     * Creates a new SolaceConnection with a pre-built JCSMPSession.
     *
     * @param session      the JCSMP session
     * @param connectionId a unique identifier for this connection
     * @throws JCSMPException if creating the producer fails
     */
    public SolaceConnection(JCSMPSession session, String connectionId) throws JCSMPException {
        this.session = session;
        this.connectionId = connectionId;
        this.producer = session.getMessageProducer(new PublishEventHandler());
        // A synchronous consumer (null callback) is required by Requestor to receive replies
        this.consumer = session.getMessageConsumer((com.solacesystems.jcsmp.XMLMessageListener) null);
        this.consumer.start();
        log.info("Solace connection created: " + connectionId);
    }

    /**
     * Publishes a message to the specified destination. When {@code waitForAck} is true
     * and the delivery mode is guaranteed (PERSISTENT / NON_PERSISTENT), blocks until
     * the broker ACKs the message or the ACK times out — so the caller knows the broker
     * accepted the message before the mediation continues. DIRECT deliveries never
     * produce ACKs, so {@code waitForAck} has no effect for them.
     *
     * @param destinationType  TOPIC or QUEUE
     * @param destinationName  the name of the topic or queue
     * @param payload          the message payload as a string
     * @param deliveryMode     DIRECT, PERSISTENT, or NON_PERSISTENT
     * @param messageType      message type: TEXT, BYTES, or XML (default: TEXT; JSON travels as TEXT)
     * @param properties       additional message properties (may be null)
     * @param waitForAck       if true and delivery is guaranteed, block for broker ACK
     * @param ackTimeoutMillis maximum time to wait for the broker ACK (ignored when waitForAck=false)
     * @throws JCSMPException if publishing fails, the broker NACKs, or the ACK wait times out
     */
    public PublishResult publish(String destinationType, String destinationName, String payload,
                                 String deliveryMode, String messageType, SolaceMessageProperties properties,
                                 boolean waitForAck, long ackTimeoutMillis)
            throws JCSMPException {
        return publish(destinationType, destinationName, payload, deliveryMode, messageType, properties,
                waitForAck, ackTimeoutMillis, null);
    }

    public PublishResult publish(String destinationType, String destinationName, String payload,
                                 String deliveryMode, String messageType, SolaceMessageProperties properties,
                                 boolean waitForAck, long ackTimeoutMillis, String httpContentType)
            throws JCSMPException {

        // Check whether multiple calls cause error or handled internally
        Destination destination = resolveDestination(destinationType, destinationName);

        BytesXMLMessage message = createMessage(messageType, payload);
        if (httpContentType != null && !httpContentType.isEmpty()) {
            message.setHTTPContentType(httpContentType);
        }

        // Set delivery mode
        if (deliveryMode != null) {
            switch (deliveryMode.toUpperCase()) {
                case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                    message.setDeliveryMode(DeliveryMode.PERSISTENT);
                    break;
                case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                    message.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    break;
                case SolaceConstants.DELIVERY_MODE_DIRECT:
                default:
                    message.setDeliveryMode(DeliveryMode.DIRECT);
                    break;
            }
        }

        applyMessageProperties(message, properties);

        // Set correlation key for guaranteed delivery tracking
        boolean guaranteed = message.getDeliveryMode() != DeliveryMode.DIRECT;
        String correlationKey = null;
        CompletableFuture<Void> ackFuture = null;
        if (guaranteed) {
            correlationKey = destinationName + "-" + System.nanoTime();
            message.setCorrelationKey(correlationKey);
            if (waitForAck) {
                ackFuture = new CompletableFuture<>();
                pendingAcks.put(correlationKey, ackFuture);
            }
        }

        try {
            producer.send(message, destination);
        } catch (JCSMPException e) {
            if (correlationKey != null) {
                pendingAcks.remove(correlationKey);
            }
            throw e;
        }

        // Transport succeeded. Beyond this point we only distinguish ACK outcomes,
        // returning them as PublishResult so the caller can decide how to react.
        if (ackFuture != null) {
            try {
                ackFuture.get(ackTimeoutMillis, TimeUnit.MILLISECONDS);
                if (log.isDebugEnabled()) {
                    log.debug("Message published to " + destinationType + " '" + destinationName
                            + "' (ACK received, correlationKey=" + correlationKey + ")");
                }
                return new PublishResult(SolaceConstants.ACK_STATUS_ACK, true, correlationKey, null);
            } catch (TimeoutException e) {
                pendingAcks.remove(correlationKey);
                String msg = "Publish ACK timeout after " + ackTimeoutMillis + "ms";
                return new PublishResult(SolaceConstants.ACK_STATUS_TIMEOUT, false, correlationKey, msg);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                String msg = cause != null ? cause.getMessage() : "Broker NACK";
                return new PublishResult(SolaceConstants.ACK_STATUS_NACK, false, correlationKey, msg);
            } catch (InterruptedException e) {
                pendingAcks.remove(correlationKey);
                Thread.currentThread().interrupt();
                return new PublishResult(SolaceConstants.ACK_STATUS_NACK, false, correlationKey,
                        "Interrupted while waiting for publish ACK");
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Message published to " + destinationType + " '" + destinationName + "'");
        }
        String status = guaranteed ? SolaceConstants.ACK_STATUS_NOT_REQUESTED
                : SolaceConstants.ACK_STATUS_NOT_APPLICABLE;
        return new PublishResult(status, false, correlationKey, null);
    }

    /**
     * Sends a reply to the original inbound request message using JCSMP's native sendReply API.
     * The reply-to destination is derived from the original request. Correlation ID and
     * application message ID are echoed from the request when the caller has not supplied
     * explicit values via {@code properties}.
     *
     * @param inboundMessage the original request message received by the inbound endpoint
     * @param payload        the reply payload
     * @param deliveryMode   DIRECT, PERSISTENT, or NON_PERSISTENT (default: DIRECT)
     * @param messageType    message type: TEXT, BYTES, or XML (default: TEXT; JSON travels as TEXT)
     * @param properties     additional reply message properties (may be null)
     * @throws JCSMPException if publishing fails
     */
    public void sendReply(BytesXMLMessage inboundMessage, String payload, String deliveryMode,
                          String messageType, SolaceMessageProperties properties) throws JCSMPException {
        sendReply(inboundMessage, payload, deliveryMode, messageType, properties, null);
    }

    public void sendReply(BytesXMLMessage inboundMessage, String payload, String deliveryMode,
                          String messageType, SolaceMessageProperties properties,
                          String httpContentType) throws JCSMPException {

        BytesXMLMessage replyMessage = createMessage(messageType, payload);
        if (httpContentType != null && !httpContentType.isEmpty()) {
            replyMessage.setHTTPContentType(httpContentType);
        }

        if (deliveryMode != null) {
            switch (deliveryMode.toUpperCase()) {
                case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                    replyMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
                    break;
                case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                    replyMessage.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                    break;
                case SolaceConstants.DELIVERY_MODE_DIRECT:
                default:
                    replyMessage.setDeliveryMode(DeliveryMode.DIRECT);
                    break;
            }
        } else {
            replyMessage.setDeliveryMode(DeliveryMode.DIRECT);
        }

        // Echo correlationId and applicationMessageId from the request if the caller
        // didn't explicitly supply them — matches the JCSMP request-reply convention.
        if (properties == null || properties.getCorrelationId() == null) {
            if (inboundMessage.getCorrelationId() != null) {
                replyMessage.setCorrelationId(inboundMessage.getCorrelationId());
            }
        }
        if (properties == null || properties.getApplicationMessageId() == null) {
            if (inboundMessage.getApplicationMessageId() != null) {
                replyMessage.setApplicationMessageId(inboundMessage.getApplicationMessageId());
            }
        }

        applyMessageProperties(replyMessage, properties);

        // Set correlation key for guaranteed delivery tracking
        if (replyMessage.getDeliveryMode() != DeliveryMode.DIRECT) {
            String correlationKey = "reply-" + System.nanoTime();
            replyMessage.setCorrelationKey(correlationKey);
        }

        producer.sendReply(inboundMessage, replyMessage);
        if (log.isDebugEnabled()) {
            log.debug("Reply sent to " + inboundMessage.getReplyTo());
        }
    }

    /**
     * Performs a synchronous request-reply. For DIRECT delivery, uses the JCSMP
     * {@link Requestor} API. For guaranteed (PERSISTENT/NON_PERSISTENT) delivery,
     * provisions a temporary queue, sets it as the reply-to destination, and waits
     * for a reply via a {@link FlowReceiver} — this is the only JCSMP-supported
     * pattern for guaranteed request-reply.
     *
     * @param destinationType TOPIC or QUEUE
     * @param destinationName destination name
     * @param payload         request payload
     * @param deliveryMode    DIRECT, PERSISTENT, or NON_PERSISTENT
     * @param messageType     message type: TEXT, BYTES, or XML (JSON travels as TEXT)
     * @param properties      optional request message properties (may be null)
     * @param timeoutMillis   maximum time to wait for a reply
     * @return the reply message
     * @throws JCSMPRequestTimeoutException if no reply arrives within the timeout
     * @throws JCSMPException               if the request cannot be sent or flow setup fails
     */
    public BytesXMLMessage sendRequest(String destinationType, String destinationName, String payload,
                                       String deliveryMode, String messageType,
                                       SolaceMessageProperties properties, long timeoutMillis)
            throws JCSMPException {
        return sendRequest(destinationType, destinationName, payload, deliveryMode, messageType,
                properties, timeoutMillis, null);
    }

    public BytesXMLMessage sendRequest(String destinationType, String destinationName, String payload,
                                       String deliveryMode, String messageType,
                                       SolaceMessageProperties properties, long timeoutMillis,
                                       String httpContentType)
            throws JCSMPException {

        Destination destination = resolveDestination(destinationType, destinationName);
        DeliveryMode resolvedMode = resolveDeliveryMode(deliveryMode);

        BytesXMLMessage requestMessage = createMessage(messageType, payload);
        if (httpContentType != null && !httpContentType.isEmpty()) {
            requestMessage.setHTTPContentType(httpContentType);
        }
        requestMessage.setDeliveryMode(resolvedMode);
        applyMessageProperties(requestMessage, properties);

        if (resolvedMode == DeliveryMode.DIRECT) {
            // DIRECT: JCSMP Requestor handles reply-to temp topic + blocking receive
            Requestor requestor = session.createRequestor();
            return requestor.request(requestMessage, timeoutMillis, destination);
        }

        // Guaranteed request-reply: temp queue + FlowReceiver
        return sendGuaranteedRequest(requestMessage, destination, destinationName, timeoutMillis);
    }

    /**
     * Executes a guaranteed request-reply using a temporary queue.
     * The temp queue is provisioned, attached as the reply-to destination, and drained
     * by a short-lived FlowReceiver. Resources are released in the finally block to avoid
     * leaking flows or temp endpoints on errors.
     *
     * <p>A correlation ID is set on the request (generated if the caller didn't supply one)
     * and verified against the reply. If the reply's correlation ID doesn't match — which
     * shouldn't happen on a fresh temp queue but is defensive — a warning is logged.</p>
     */
    private BytesXMLMessage sendGuaranteedRequest(BytesXMLMessage requestMessage, Destination destination,
                                                  String destinationName, long timeoutMillis)
            throws JCSMPException {

        Queue tempQueue = session.createTemporaryQueue();
        requestMessage.setReplyTo(tempQueue);

        // Ensure the request carries a correlation ID we can verify against the reply.
        // If the caller already set one via properties, use it; otherwise generate one.
        String expectedCorrelationId = requestMessage.getCorrelationId();
        if (expectedCorrelationId == null) {
            expectedCorrelationId = "req-" + System.nanoTime();
            requestMessage.setCorrelationId(expectedCorrelationId);
        }

        // Set correlation key for guaranteed publish-ACK tracking
        String correlationKey = destinationName + "-req-" + System.nanoTime();
        requestMessage.setCorrelationKey(correlationKey);

        ConsumerFlowProperties flowProps = new ConsumerFlowProperties();
        flowProps.setEndpoint(tempQueue);
        flowProps.setStartState(true);

        FlowReceiver flowReceiver = null;
        try {
            flowReceiver = session.createFlow(null, flowProps);

            producer.send(requestMessage, destination);

            // Block until a reply arrives or the timeout expires
            BytesXMLMessage reply = flowReceiver.receive((int) timeoutMillis);
            if (reply == null) {
                throw new JCSMPRequestTimeoutException("No reply received within " + timeoutMillis + "ms");
            }

            // Verify the reply corresponds to our request. A mismatch is unlikely on a fresh
            // temp queue, but defensive logging makes cross-wiring easier to diagnose.
            String replyCorrelationId = reply.getCorrelationId();
            if (replyCorrelationId != null && !expectedCorrelationId.equals(replyCorrelationId)) {
                log.warn("Guaranteed request-reply correlation mismatch: expected='"
                        + expectedCorrelationId + "', received='" + replyCorrelationId
                        + "'. Returning the message regardless.");
            }

            // Acknowledge so the broker can remove it from the temp queue
            reply.ackMessage();
            return reply;
        } finally {
            if (flowReceiver != null) {
                flowReceiver.close();
            }
            // Temp queue is auto-cleaned when the session closes; no explicit deprovision needed.
        }
    }

    /**
     * Resolves a delivery mode string into the JCSMP enum. Null or unknown falls back to DIRECT.
     */
    private DeliveryMode resolveDeliveryMode(String deliveryMode) {
        if (deliveryMode == null) {
            return DeliveryMode.DIRECT;
        }
        switch (deliveryMode.toUpperCase()) {
            case SolaceConstants.DELIVERY_MODE_PERSISTENT:
                return DeliveryMode.PERSISTENT;
            case SolaceConstants.DELIVERY_MODE_NON_PERSISTENT:
                return DeliveryMode.NON_PERSISTENT;
            case SolaceConstants.DELIVERY_MODE_DIRECT:
            default:
                return DeliveryMode.DIRECT;
        }
    }

    /**
     * Creates a message of the specified type with the given payload. Supports TEXT, BYTES, and XML.
     * JSON payloads are carried as TEXT on the wire (Solace has no JSON message type); callers are
     * expected to pass messageType=TEXT for JSON content.
     */
    private BytesXMLMessage createMessage(String messageType, String payload) {
        if (messageType == null) {
            messageType = SolaceConstants.MESSAGE_TYPE_TEXT;
        }
        switch (messageType.toUpperCase()) {
            case SolaceConstants.MESSAGE_TYPE_BYTES:
                BytesMessage bytesMsg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                bytesMsg.setData(payload != null ? payload.getBytes() : new byte[0]);
                return bytesMsg;
            case SolaceConstants.MESSAGE_TYPE_XML:
                XMLContentMessage xmlMsg = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
                if (payload != null) {
                    xmlMsg.setXMLContent(payload);
                }
                return xmlMsg;
            case SolaceConstants.MESSAGE_TYPE_TEXT:
            default:
                TextMessage textMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                textMsg.setText(payload);
                return textMsg;
        }
    }

    /**
     * Applies optional SolaceMessageProperties to a message.
     */
    private void applyMessageProperties(BytesXMLMessage message, SolaceMessageProperties properties) {
        if (properties == null) {
            return;
        }
        if (properties.getCorrelationId() != null) {
            message.setCorrelationId(properties.getCorrelationId());
        }
        if (properties.getPriority() != null) {
            message.setPriority(properties.getPriority());
        }
        if (properties.getTimeToLive() != null) {
            message.setTimeToLive(properties.getTimeToLive());
        }
        if (properties.getApplicationMessageId() != null) {
            message.setApplicationMessageId(properties.getApplicationMessageId());
        }
        if (properties.getApplicationMessageType() != null) {
            message.setApplicationMessageType(properties.getApplicationMessageType());
        }
        if (properties.getSenderId() != null) {
            message.setSenderId(properties.getSenderId());
        }
        if (properties.getDmqEligible() != null) {
            message.setDMQEligible(properties.getDmqEligible());
        }
        if (properties.getElidingEligible() != null) {
            message.setElidingEligible(properties.getElidingEligible());
        }
        if (properties.getExpiration() != null) {
            message.setExpiration(properties.getExpiration());
        }
        if (properties.getReplyToDestinationType() != null && properties.getReplyToDestinationName() != null) {
            Destination replyTo = resolveDestination(properties.getReplyToDestinationType(),
                    properties.getReplyToDestinationName());
            message.setReplyTo(replyTo);
        }
        // Set user properties (custom key-value pairs)
        if (properties.getUserProperties() != null && !properties.getUserProperties().isEmpty()) {
            try {
                SDTMap userPropsMap = JCSMPFactory.onlyInstance().createMap();
                for (Map.Entry<String, String> entry : properties.getUserProperties().entrySet()) {
                    userPropsMap.putString(entry.getKey(), entry.getValue());
                }
                message.setProperties(userPropsMap);
            } catch (SDTException e) {
                log.error("Error setting user properties on message", e);
            }
        }
    }

    /**
     * Resolves a destination (topic or queue) from type and name.
     */
    private Destination resolveDestination(String type, String name) {
        if (SolaceConstants.DESTINATION_TYPE_QUEUE.equalsIgnoreCase(type)) {
            return JCSMPFactory.onlyInstance().createQueue(name);
        }
        return JCSMPFactory.onlyInstance().createTopic(name);
    }

    /**
     * Gets the underlying JCSMP session.
     *
     * @return the session
     */
    public JCSMPSession getSession() {
        return session;
    }

    /**
     * Gets the connection identifier.
     *
     * @return the connection ID
     */
    public String getConnectionId() {
        return connectionId;
    }

    /**
     * Checks if the connection is active.
     *
     * @return true if the session is not closed
     */
    public boolean isConnected() {
        return session != null && !session.isClosed();
    }

    public TransactedSession getOrCreateTransactedSession() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                txSession = session.createTransactedSession();
                ProducerFlowProperties producerProps = new ProducerFlowProperties();
                txProducer = txSession.createProducer(producerProps, new PublishEventHandler());
                log.info("Transacted session created on connection: " + connectionId);
            }
            return txSession;
        }
    }

    public void commitTransaction() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                throw new JCSMPException("No active transaction on connection: " + connectionId);
            }
            try {
                txSession.commit();
                log.info("Transaction committed on connection: " + connectionId);
            } finally {
                closeTransactedSessionInternal();
            }
        }
    }

    public void rollbackTransaction() throws JCSMPException {
        synchronized (txLock) {
            if (txSession == null) {
                throw new JCSMPException("No active transaction on connection: " + connectionId);
            }
            try {
                txSession.rollback();
                log.warn("Transaction rolled back on connection: " + connectionId);
            } finally {
                closeTransactedSessionInternal();
            }
        }
    }

    public boolean hasActiveTransaction() {
        synchronized (txLock) { return txSession != null; }
    }

    private void closeTransactedSessionInternal() {
        if (txProducer != null) { txProducer.close(); txProducer = null; }
        if (txSession != null)  { txSession.close();  txSession = null;  }
    }

    public void closeTransactedSession() {
        synchronized (txLock) { closeTransactedSessionInternal(); }
    }

    public PublishResult publishTransacted(String destinationType, String destinationName,
                                       String payload, String deliveryMode, String messageType,
                                       SolaceMessageProperties properties, String httpContentType)
        throws JCSMPException {
        synchronized (txLock) {
            if (txProducer == null) {
                throw new JCSMPException(
                    "publishTransacted called but no transacted session active on connection: "
                    + connectionId);
            }
            Destination destination = resolveDestination(destinationType, destinationName);
            BytesXMLMessage msg = createMessage(messageType, payload);
            if (httpContentType != null && !httpContentType.isEmpty()) {
                msg.setHTTPContentType(httpContentType);
            }
            msg.setDeliveryMode(resolveDeliveryMode(deliveryMode));
            applyMessageProperties(msg, properties);
            // Stable correlation key per transacted publish — surfaced in the PublishResult and
            // visible in broker-side message-spool browsing for "where did this message go" diagnostics.
            String correlationKey = "tx-" + connectionId + "-" + System.nanoTime();
            msg.setCorrelationKey(correlationKey);
            txProducer.send(msg, destination);
            return new PublishResult(SolaceConstants.ACK_STATUS_TX_PENDING, false, correlationKey, null);
        }
    }

    /**
     * Disconnects the session and releases resources.
     */
    public void disconnect() {
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }
        // Always release transacted resources, even if the underlying session was already
        // closed externally (e.g. during a reconnect storm) — prevents producer/session leaks.
        closeTransactedSession();
        if (session != null && !session.isClosed()) {
            session.closeSession();
            log.info("Solace connection closed: " + connectionId);
        }
        session = null;
    }

    @Override
    public void connect(ConnectionConfig connectionConfig) throws ConnectException {
        // Connection is established in the constructor
    }

    @Override
    public void close() throws ConnectException {
        disconnect();
    }

    /**
     * Handler for asynchronous publish events (acks and errors). Non-static so it can
     * signal the {@link #pendingAcks} futures owned by the surrounding connection —
     * required for the synchronous {@code waitForAck} path in {@link #publish}.
     */
    private class PublishEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

        @Override
        public void responseReceivedEx(Object correlationKey) {
            if (correlationKey instanceof String) {
                CompletableFuture<Void> future = pendingAcks.remove(correlationKey);
                if (future != null) {
                    future.complete(null);
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("Publish ACK received. CorrelationKey: " + correlationKey);
            }
        }

        @Override
        public void handleErrorEx(Object correlationKey, JCSMPException cause, long timestamp) {
            if (correlationKey instanceof String) {
                CompletableFuture<Void> future = pendingAcks.remove(correlationKey);
                if (future != null) {
                    future.completeExceptionally(cause);
                }
            }
            if (cause instanceof JCSMPErrorResponseException) {
                JCSMPErrorResponseException errResp = (JCSMPErrorResponseException) cause;
                log.error("Publish NACK - broker rejected message. CorrelationKey: " + correlationKey
                        + ", subcode: " + errResp.getSubcodeEx()
                        + ", responseCode: " + errResp.getResponseCode()
                        + ", info: " + errResp.getResponsePhrase(), cause);
            } else if (cause instanceof JCSMPTransportException) {
                log.error("Publish error - transport failure. CorrelationKey: " + correlationKey
                        + ", cause: " + cause.getMessage(), cause);
            } else {
                log.error("Publish error. CorrelationKey: " + correlationKey, cause);
            }
        }
    }
}
