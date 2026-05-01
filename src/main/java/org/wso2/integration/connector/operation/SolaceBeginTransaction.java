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

import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;

public class SolaceBeginTransaction extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        // Nested begin would orphan the outer tx — its TX_CONNECTION_ID would be
        // overwritten and the outer pinned connection could only be reclaimed by the watchdog.
        log.info("solace.beginTransaction: invoked");
        String existingTxId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        if (existingTxId != null) {
            log.info("solace.beginTransaction: rejected — active transaction already in scope: " + existingTxId);
            throw new ConnectException(
                    "solace.beginTransaction called inside an active transaction scope: " + existingTxId);
        }
        String connectionName = null;
        SolaceConnection connection = null;
        boolean registered = false;
        try {
            connectionName = (String) messageContext.getProperty(SolaceConstants.NAME);
            if (connectionName == null) {
                handleException("Connection name is not set.", messageContext);
                return;
            }
            
            String timeoutStr = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, SolaceConstants.TX_TIMEOUT_MILLIS);
            long timeoutMillis = (timeoutStr != null && !timeoutStr.isEmpty())
                    ? Long.parseLong(timeoutStr)
                    : Long.parseLong(SolaceConstants.DEFAULT_TX_TIMEOUT_MILLIS);
            log.info("solace.beginTransaction: connectionName=" + connectionName
                    + ", timeoutMillis=" + timeoutMillis);

            connection = (SolaceConnection) ConnectionHandler.getConnectionHandler()
                    .getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);
            if (connection == null) {
                log.info("solace.beginTransaction: connection '" + connectionName + "' not available from pool");
                throw new ConnectException("Solace connection '" + connectionName + "' is not available");
            }
            log.info("solace.beginTransaction: pinned connection acquired (connectionId="
                    + connection.getConnectionId() + ")");

            connection.getOrCreateTransactedSession();
            log.info("solace.beginTransaction: transacted session ready on connectionId="
                    + connection.getConnectionId());

            String txId = SolaceTransactionRegistry.register(connection, connectionName, timeoutMillis);
            registered = true;
            log.info("solace.beginTransaction: registered txId=" + txId + " in TransactionRegistry");

            ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                    .setProperty(SolaceConstants.TX_CONNECTION_ID, txId);

            log.info("solace.beginTransaction: started txId=" + txId + " (timeout=" + timeoutMillis + "ms)");
        } catch (Exception e) {
            log.error("solace.beginTransaction: failed (connectionName=" + connectionName + "): " + e.getMessage(), e);
            // Registration didn't happen → ownership stays with us, return to pool so the
            // connection isn't lost. Once registered, the registry owns the lifecycle
            // (commit/rollback/watchdog will release it).
            if (connection != null && !registered) {
                try {
                    ConnectionHandler.getConnectionHandler().returnConnection(
                            SolaceConstants.CONNECTOR_NAME, connectionName, connection);
                    log.info("solace.beginTransaction: returned unregistered connection to pool after failure");
                } catch (Exception releaseEx) {
                    log.warn("Failed to return Solace connection to pool after failed beginTransaction",
                            releaseEx);
                }
            }
            throw new ConnectException(e, "Failed to begin Solace transaction");
        }
    }
}
