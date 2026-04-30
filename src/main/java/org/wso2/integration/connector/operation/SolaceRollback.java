package org.wso2.integration.connector.operation;

import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.integration.connector.connection.TransactionRegistry;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;

public class SolaceRollback extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        log.info("solace.rollback: invoked");
        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        if (txId == null) {
            log.info("solace.rollback: rejected — no transaction in scope");
            throw new ConnectException("solace.rollback called outside a transaction scope");
        }
        log.info("solace.rollback: txId=" + txId + " resolved from message context");
        TransactionRegistry.Entry entry = TransactionRegistry.unregister(txId);
        if (entry == null) {
            log.info("solace.rollback: txId=" + txId + " not found in registry"
                    + " (already committed/rolled back/timed out)");
            throw new ConnectException("Transaction " + txId + " not found (already committed/rolled back/timed out?)");
        }
        log.info("solace.rollback: txId=" + txId + " unregistered; rolling back on connectionId="
                + entry.connection.getConnectionId() + " (connectionName=" + entry.connectionName + ")");
        try {
            entry.connection.rollbackTransaction();
            log.info("solace.rollback: txId=" + txId + " rolled back successfully");
        } catch (Exception e) {
            log.error("solace.rollback: txId=" + txId + " rollback failed: " + e.getMessage(), e);
            throw new ConnectException(e, "Failed to rollback Solace transaction " + txId);
        } finally {
            ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                    .removeProperty(SolaceConstants.TX_CONNECTION_ID);
            ConnectionHandler.getConnectionHandler().returnConnection(
                    SolaceConstants.CONNECTOR_NAME, entry.connectionName, entry.connection);
            log.info("solace.rollback: txId=" + txId + " connection returned to pool");
        }
    }
}
