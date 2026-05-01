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

package org.wso2.integration.connector.connection;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.pool.ConnectionFactory;
import org.wso2.integration.connector.core.util.ConnectorUtils;
import org.wso2.integration.connector.utils.SolaceUtils;

/**
 * Factory class for creating SolaceConnection instances for connection pooling.
 * Implements the connector core's ConnectionFactory interface.
 */
public class SolaceConnectionFactory implements ConnectionFactory {

    private static final Log log = LogFactory.getLog(SolaceConnectionFactory.class);

    private final JCSMPProperties jcsmpProperties;
    private final String connectionName;

    public SolaceConnectionFactory(MessageContext messageContext) {
        this.jcsmpProperties = SolaceUtils.buildJCSMPProperties(messageContext);
        
        String name = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.NAME);
        this.connectionName = StringUtils.isNotEmpty(name) ? name : SolaceConstants.CONNECTOR_NAME;
    }

    @Override
    public SolaceConnection makeObject() {
        try {
            JCSMPSession session = JCSMPFactory.onlyInstance().createSession(
                    jcsmpProperties, null, new SolaceSessionEventHandler());
            session.connect();

            return new SolaceConnection(session, connectionName);
        } catch (JCSMPException e) {
            log.error("Failed to create Solace connection", e);
            throw new RuntimeException("Failed to create Solace connection: " + e.getMessage(), e);
        }
    }

    @Override
    public void destroyObject(Object connection) {
        if (connection instanceof SolaceConnection) {
            ((SolaceConnection) connection).disconnect();
        }
    }

    @Override
    public boolean validateObject(Object connection) {
        if (connection instanceof SolaceConnection) {
            return ((SolaceConnection) connection).isConnected();
        }
        return false;
    }

    @Override
    public void activateObject(Object connection) {
        // No action needed for activation
    }

    @Override
    public void passivateObject(Object connection) {
        // No action needed for passivation
    }

    /**
     * Handler for session lifecycle events (reconnecting, connection lost, etc.).
     */
    private static class SolaceSessionEventHandler implements SessionEventHandler {

        @Override
        public void handleEvent(SessionEventArgs event) {
            SessionEvent sessionEvent = event.getEvent();
            if (sessionEvent == SessionEvent.RECONNECTING) {
                log.warn("Solace session reconnecting: " + event.getInfo());
            } else if (sessionEvent == SessionEvent.RECONNECTED) {
                log.info("Solace session reconnected: " + event.getInfo());
            } else if (sessionEvent == SessionEvent.DOWN_ERROR) {
                log.error("Solace session down: " + event.getInfo()
                        + ", responseCode=" + event.getResponseCode()
                        + ", exception=" + event.getException());
            } else if (sessionEvent == SessionEvent.SUBSCRIPTION_ERROR) {
                log.error("Solace subscription error: " + event.getInfo());
            } else {
                log.info("Solace session event: " + sessionEvent + " - " + event.getInfo());
            }
        }
    }
}
