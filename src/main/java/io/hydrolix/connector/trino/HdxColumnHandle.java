package io.hydrolix.connector.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Optional;

/**
 * A Trino wrapper around a Hydrolix column.
 *
 * @param name      name of the column
 * @param trinoType Trino type of the column, once known
 */
public record HdxColumnHandle(String name, Optional<Type> trinoType) implements ColumnHandle { }
