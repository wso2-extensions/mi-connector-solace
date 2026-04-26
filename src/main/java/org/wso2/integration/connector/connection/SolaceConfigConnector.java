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

import static org.wso2.integration.connector.core.util.ConnectorUtils.getPoolConfiguration;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.ManagedLifecycle;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;

/**
 * Initializes and manages the Solace connection.
 * This is the "init" operation of the connector that creates a session to the Solace broker.
 */
public class SolaceConfigConnector extends AbstractConnector implements ManagedLifecycle {

    private static final Log log = LogFactory.getLog(SolaceConfigConnector.class);

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {

        String connectionName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
            SolaceConstants.NAME);
        if (StringUtils.isEmpty(connectionName)) {
            connectionName = SolaceConstants.CONNECTOR_NAME;
        }

        ConnectionHandler handler = ConnectionHandler.getConnectionHandler();

        // Check if connection already exists
        if (!handler.checkIfConnectionExists(SolaceConstants.CONNECTOR_NAME, connectionName)) {
            handler.createConnection(SolaceConstants.CONNECTOR_NAME, connectionName,
                    new SolaceConnectionFactory(messageContext), getPoolConfiguration(messageContext));
        }

        if (log.isDebugEnabled()) {
            String host = (String) ConnectorUtils.lookupTemplateParamater(messageContext, SolaceConstants.HOST);
            String vpnName = (String) ConnectorUtils.lookupTemplateParamater(messageContext,
                SolaceConstants.VPN_NAME);
            log.debug("Solace connection initialized - Host: " + host + ", VPN: " + vpnName
                + ", Name: " + connectionName);
        }
    }

    @Override
    public void init(SynapseEnvironment synapseEnvironment) {
        // Nothing to initialize
    }

    @Override
    public void destroy() {
        // Close all connections on destroy
        try {
            ConnectionHandler handler = ConnectionHandler.getConnectionHandler();
            handler.shutdownConnections(SolaceConstants.CONNECTOR_NAME);
            log.info("Solace connector connections shut down.");
        } catch (Exception e) {
            log.error("Error shutting down Solace connections", e);
        }
    }
}
