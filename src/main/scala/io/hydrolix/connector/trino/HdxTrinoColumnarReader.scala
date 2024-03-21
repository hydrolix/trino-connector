package io.hydrolix.connector.trino

import java.io.InputStream

import com.typesafe.scalalogging.Logger
import io.trino.spi.{Page, `type` => ttypes}

import io.hydrolix.connectors.api.HdxStorageSettings
import io.hydrolix.connectors.partitionreader.HdxPartitionReader
import io.hydrolix.connectors.types.ArrayType
import io.hydrolix.connectors.{HdxConnectionInfo, HdxPartitionScanPlan, JSON}

final class HdxTrinoColumnarReader(val       info: HdxConnectionInfo,
                                   val    storage: HdxStorageSettings,
                                   val       scan: HdxPartitionScanPlan,
                          override val doneSignal: Page,
                                            limit: Option[Long])
  extends HdxPartitionReader[Page](doneSignal, "jsonc")
{
  private val logger = Logger(getClass)

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
        enqueue(doneSignal)
      },
      { page =>
        if (limit.exists(_ <= recordsEmitted)) {
          logger.info(s"Emitted $recordsEmitted already, limit is ${limit.get}... Exiting early")
          enqueue(doneSignal)
          false
        } else {
          logger.debug(s"Got page with ${page.getPositionCount} rows")
          recordsEmitted += page.getPositionCount
          enqueue(page)
          true
        }
      }
    )
  }
}
