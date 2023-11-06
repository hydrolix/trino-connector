package io.hydrolix.connector.trino;

import io.trino.spi.connector.ColumnHandle;

public record HdxColumnHandle(String name) implements ColumnHandle { }
