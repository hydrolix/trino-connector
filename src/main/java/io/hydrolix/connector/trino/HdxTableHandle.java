package io.hydrolix.connector.trino;

import io.trino.spi.connector.ConnectorTableHandle;

public record HdxTableHandle(String db, String table) implements ConnectorTableHandle {

}