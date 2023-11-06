package io.hydrolix.connector.trino;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.OptBoolean;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public record HdxTrinoSplit(
  String partition,
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC", lenient = OptBoolean.TRUE)
  Instant minTimestamp,
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC", lenient = OptBoolean.TRUE)
  Instant maxTimestamp,
  Long manifestSize,
  Long dataSize,
  Long indexSize,
  Long rows,
  Long memSize,
  String rootPath,
  String shardKey,
  Boolean active,
  Optional<UUID> storageId
) implements ConnectorSplit {
    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return dataSize;
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
