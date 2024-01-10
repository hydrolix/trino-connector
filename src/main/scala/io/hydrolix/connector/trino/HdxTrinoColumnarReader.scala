package io.hydrolix.connector.trino

import java.io.InputStream

import io.trino.spi.{Page, `type` => ttypes}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.api.HdxStorageSettings
import io.hydrolix.connectors.partitionreader.HdxPartitionReader
import io.hydrolix.connectors.types.ArrayType
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPartitionScanPlan, JSON}

final class HdxTrinoColumnarReader(val           info: HdxConnectionInfo,
                                   val        storage: HdxStorageSettings,
                                   val primaryKeyName: String,
                                   val           scan: HdxPartitionScanPlan,
                                   val     doneSignal: Page,
                                                limit: Option[Long])
  extends HdxPartitionReader[Page](doneSignal, "jsonc")
{
  private val logger = LoggerFactory.getLogger(getClass)

  override protected def handleStdout(stdout: InputStream): Unit = {
    val trinoSchemaByName = scan.schema.fields.zipWithIndex.map { case (sf, i) =>
      val atyp = TrinoTypes
        .coreToTrino(ArrayType(sf.`type`, true))
        .getOrElse(sys.error(s"Can't convert column type ${sf.`type`}"))
        .asInstanceOf[ttypes.ArrayType]

      sf.name -> (atyp -> i)
    }.toMap

    val parser = JSON.objectMapper.createParser(stdout)
    var recordsEmitted = 0

    TrinoColumnarJson.parseStream(
      parser,
      trinoSchemaByName,
      { () =>
        logger.warn("stdout stream was empty!")
        enqueue(doneSignal)
      },
      { page =>
        if (limit.exists(_ <= recordsEmitted)) {
          logger.info(s"Emitted $recordsEmitted already, limit is ${limit.get}... Exiting early")
          enqueue(doneSignal)
        } else {
          recordsEmitted += page.getPositionCount
          enqueue(page)
        }
      }
    )
  }
}
