package io.hydrolix.connector.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * Singleton, do-nothing "implementation" of {@link ConnectorTransactionHandle} since we're read-only.
 * <p/>
 * Not a Scala <code>object</code> for JSON serialization reasons
 */
public enum HdxTrinoTransactionHandle implements ConnectorTransactionHandle {
    INSTANCE;
}
