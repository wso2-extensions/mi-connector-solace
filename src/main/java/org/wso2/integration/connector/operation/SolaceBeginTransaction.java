package org.wso2.integration.connector.operation;

import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.integration.connector.connection.SolaceConnection;
import org.wso2.integration.connector.connection.TransactionRegistry;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.AbstractConnector;
import org.wso2.integration.connector.core.ConnectException;
import org.wso2.integration.connector.core.connection.ConnectionHandler;
import org.wso2.integration.connector.core.util.ConnectorUtils;

public class SolaceBeginTransaction extends AbstractConnector {

    @Override
    public void connect(MessageContext messageContext) throws ConnectException {
        try {
            String connectionName = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, SolaceConstants.NAME);
            String timeoutStr = (String) ConnectorUtils.lookupTemplateParamater(
                    messageContext, SolaceConstants.TX_TIMEOUT_MILLIS);
            long timeoutMillis = (timeoutStr != null && !timeoutStr.isEmpty())
                    ? Long.parseLong(timeoutStr)
                    : Long.parseLong(SolaceConstants.DEFAULT_TX_TIMEOUT_MILLIS);

            SolaceConnection connection = (SolaceConnection) ConnectionHandler.getConnectionHandler()
                    .getConnection(SolaceConstants.CONNECTOR_NAME, connectionName);

            connection.getOrCreateTransactedSession();
            String txId = TransactionRegistry.register(connection, connectionName, timeoutMillis);

            ((Axis2MessageContext) messageContext).getAxis2MessageContext()
                    .setProperty(SolaceConstants.TX_CONNECTION_ID, txId);

            log.info("Solace transaction started: " + txId
                    + " (timeout=" + timeoutMillis + "ms)");
        } catch (Exception e) {
            throw new ConnectException(e, "Failed to begin Solace transaction");
        }
    }
}
