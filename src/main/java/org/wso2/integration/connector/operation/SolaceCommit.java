package org.wso2.integration.connector.operation;

import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.integration.connector.connection.TransactionRegistry;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;

public class SolaceCommit extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        String txId = (String) ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                .getProperty(SolaceConstants.TX_CONNECTION_ID);
        if (txId == null) {
            throw new ConnectException("solace.commit called outside a transaction scope");
        }
        TransactionRegistry.Entry entry = TransactionRegistry.unregister(txId);
        if (entry == null) {
            throw new ConnectException("Transaction " + txId + " not found (already committed/rolled back/timed out?)");
        }
        try {
            entry.connection.commitTransaction();

            ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                    .removeProperty(SolaceConstants.TX_CONNECTION_ID);
        } catch (Exception e) {
            throw new ConnectException(e, "Failed to commit Solace transaction " + txId);
        } finally {
            ConnectionHandler.getConnectionHandler().returnConnection(
                    SolaceConstants.CONNECTOR_NAME, entry.connectionName, entry.connection);
        }
    }
}
