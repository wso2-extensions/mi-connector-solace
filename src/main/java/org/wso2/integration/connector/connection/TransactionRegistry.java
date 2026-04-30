package org.wso2.integration.connector.connection;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.integration.connector.constants.SolaceConstants;
import org.wso2.integration.connector.core.connection.ConnectionHandler;

/**
 * Tracks active transactional connections by transaction id, with an auto-rollback
 * watchdog so a forgotten commit/rollback doesn't leak the transacted session.
 */
public final class TransactionRegistry {

    private static final Log log = LogFactory.getLog(TransactionRegistry.class);

    private static final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService watchdog =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "solace-tx-watchdog");
                t.setDaemon(true);
                return t;
            });

    private TransactionRegistry() {}

    public static String register(SolaceConnection connection, String connectionName, long timeoutMillis) {
        String txId = UUID.randomUUID().toString();
        Entry entry = new Entry(connection, connectionName);
        entry.timeoutFuture = watchdog.schedule(() -> autoRollback(txId),
                timeoutMillis, TimeUnit.MILLISECONDS);
        entries.put(txId, entry);
        log.info("TransactionRegistry: registered txId=" + txId + " (connectionName=" + connectionName
                + ", connectionId=" + connection.getConnectionId() + ", timeoutMillis=" + timeoutMillis
                + ", activeCount=" + entries.size() + ")");
        return txId;
    }

    public static SolaceConnection get(String txId) {
        Entry e = entries.get(txId);
        if (e == null) {
            log.info("TransactionRegistry: lookup miss for txId=" + txId
                    + " (activeCount=" + entries.size() + ")");
            return null;
        }
        return e.connection;
    }

    public static Entry unregister(String txId) {
        Entry e = entries.remove(txId);
        if (e != null && e.timeoutFuture != null) {
            e.timeoutFuture.cancel(false);
            log.info("TransactionRegistry: unregistered txId=" + txId
                    + " (connectionName=" + e.connectionName + ", activeCount=" + entries.size() + ")");
            return e;
        }
        log.info("TransactionRegistry: unregister miss for txId=" + txId
                + " (activeCount=" + entries.size() + ")");
        return null;
    }

    private static void autoRollback(String txId) {
        Entry e = entries.remove(txId);
        if (e == null) return;
        log.warn("Transaction " + txId + " timed out — auto-rolling back (connectionName="
                + e.connectionName + ", connectionId=" + e.connection.getConnectionId() + ")");
        try {
            e.connection.rollbackTransaction();
            log.info("TransactionRegistry: auto-rollback completed for txId=" + txId);
        } catch (Exception ex) {
            log.error("Auto-rollback failed for transaction " + txId, ex);
        } finally {
            try {
                ConnectionHandler.getConnectionHandler().returnConnection(
                        SolaceConstants.CONNECTOR_NAME, e.connectionName, e.connection);
                log.info("TransactionRegistry: returned connection to pool after auto-rollback for txId="
                        + txId);
            } catch (Exception ex) {
                log.error("Failed to return Solace connection to pool after auto-rollback for tx "
                        + txId, ex);
            }
        }
    }

    public static class Entry {
        public final SolaceConnection connection;
        public final String connectionName;
        ScheduledFuture<?> timeoutFuture;
        Entry(SolaceConnection c, String n) {
            this.connection = c;
            this.connectionName = n;
        }
    }
}
