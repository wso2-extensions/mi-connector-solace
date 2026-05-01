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
import org.wso2.integration.connector.connection.SolaceTransactionRegistry;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;

public class SolaceCommit extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        log.info("solace.commit: invoked");
        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        if (txId == null) {
            log.info("solace.commit: rejected — no transaction in scope");
            throw new ConnectException("solace.commit called outside a transaction scope");
        }
        log.info("solace.commit: txId=" + txId + " resolved from message context");
        SolaceTransactionRegistry.Entry entry = SolaceTransactionRegistry.unregister(txId);
        if (entry == null) {
            log.info("solace.commit: txId=" + txId + " not found in registry"
                    + " (already committed/rolled back/timed out)");
            throw new ConnectException("Transaction " + txId + " not found (already committed/rolled back/timed out?)");
        }
        log.info("solace.commit: txId=" + txId + " unregistered; committing on connectionId="
                + entry.connection.getConnectionId() + " (connectionName=" + entry.connectionName + ")");
        try {
            entry.connection.commitTransaction();
            log.info("solace.commit: txId=" + txId + " committed successfully");
        } catch (Exception e) {
            log.error("solace.commit: txId=" + txId + " commit failed: " + e.getMessage(), e);
            throw new ConnectException(e, "Failed to commit Solace transaction " + txId);
        } finally {
            // Registry entry is already gone — strip the context marker and release the
            // pinned connection regardless of commit outcome so the flow doesn't carry a
            // dead txId into the next operation.
            ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                    .removeProperty(SolaceConstants.TX_CONNECTION_ID);
            ConnectionHandler.getConnectionHandler().returnConnection(
                    SolaceConstants.CONNECTOR_NAME, entry.connectionName, entry.connection);
            log.info("solace.commit: txId=" + txId + " connection returned to pool");
        }
    }
}
