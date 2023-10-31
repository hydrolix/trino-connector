package io.hydrolix.connector.trino;

import io.hydrolix.connectors.HdxDbPartition;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Collections;
import java.util.List;

public record HdxTrinoSplit(HdxDbPartition part) implements ConnectorSplit {
    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return part.dataSize();
    }

    @Override
    public List<HostAddress> getAddresses() {
        return Collections.emptyList();
    }

    @Override
    public Object getInfo() {
        return "info";
    }
}
