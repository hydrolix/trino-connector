package io.hydrolix.connector.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum HdxTrinoTransactionHandle implements ConnectorTransactionHandle {
    INSTANCE;
}
