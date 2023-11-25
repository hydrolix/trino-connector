package io.hydrolix.connector.trino

import java.util.Optional
import scala.jdk.CollectionConverters._

import io.trino.spi.{`type` => ttypes}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.{types => coretypes}

object TrinoTypes {
  private val logger = LoggerFactory.getLogger(getClass)

  def coreToTrino(typ: coretypes.ValueType): Option[ttypes.Type] = {
    typ match {
      case coretypes.Int8Type          => Some(ttypes.TinyintType.TINYINT)
      case coretypes.UInt8Type         => Some(ttypes.SmallintType.SMALLINT)
      case coretypes.Int16Type         => Some(ttypes.SmallintType.SMALLINT)
      case coretypes.UInt16Type        => Some(ttypes.IntegerType.INTEGER)
      case coretypes.Int32Type         => Some(ttypes.IntegerType.INTEGER)
      case coretypes.UInt32Type        => Some(ttypes.BigintType.BIGINT)
      case coretypes.Int64Type         => Some(ttypes.BigintType.BIGINT)
      case coretypes.UInt64Type        => Some(TrinoUInt64Type)
      case coretypes.Float32Type       => Some(ttypes.RealType.REAL)
      case coretypes.Float64Type       => Some(ttypes.DoubleType.DOUBLE)
      case coretypes.StringType        => Some(ttypes.VarcharType.VARCHAR)
      case coretypes.BooleanType       => Some(ttypes.BooleanType.BOOLEAN)
      case coretypes.TimestampType(p)  => Some(ttypes.TimestampType.createTimestampType(p))
      case coretypes.DecimalType(p, s) => Some(ttypes.DecimalType.createDecimalType(p, s))
      case coretypes.ArrayType(elt, _) =>
        coreToTrino(elt).map(new ttypes.ArrayType(_))
      case coretypes.MapType(kt, vt, _) =>
        for {
          kt <- coreToTrino(kt)
          vt <- coreToTrino(vt)
        } yield {
          new ttypes.MapType(kt, vt, new ttypes.TypeOperators()) // TODO wtf is this?
        }
      case coretypes.StructType(fields) =>
        val trinoFields = fields.flatMap { fld =>
          coreToTrino(fld.`type`).map { typ =>
            new ttypes.RowType.Field(
              Optional.of(fld.name),
              typ
            )
          }
        }

        if (trinoFields.size != fields.size) {
          None
        } else {
          Some(ttypes.RowType.from(trinoFields.asJava))
        }

      case other =>
        logger.warn(s"Can't translate core type $other to Trino")
        None
    }
  }

  def trinoToCore(trinoType: ttypes.Type): Option[coretypes.ValueType] = {
    trinoType match {
      case ttypes.BooleanType.BOOLEAN             => Some(coretypes.BooleanType)
      case ttypes.VarcharType.VARCHAR             => Some(coretypes.StringType)
      case ttypes.TinyintType.TINYINT             => Some(coretypes.Int8Type)
      case ttypes.SmallintType.SMALLINT           => Some(coretypes.Int16Type)
      case ttypes.IntegerType.INTEGER             => Some(coretypes.Int32Type)
      case ttypes.BigintType.BIGINT               => Some(coretypes.Int64Type)
      case ttypes.TimestampType.TIMESTAMP_SECONDS => Some(coretypes.TimestampType.Seconds)
      case ttypes.TimestampType.TIMESTAMP_MILLIS  => Some(coretypes.TimestampType.Millis)
      case ttypes.TimestampType.TIMESTAMP_MICROS  => Some(coretypes.TimestampType.Micros)
      case at: ttypes.ArrayType =>
        trinoToCore(at.getElementType).map { typ =>
          coretypes.ArrayType(typ, true)
        }
      case mt: ttypes.MapType =>
        for {
          kt <- trinoToCore(mt.getKeyType)
          vt <- trinoToCore(mt.getValueType)
        } yield {
          coretypes.MapType(kt, vt, true)
        }
      case other =>
        logger.warn(s"Can't translate Trino type $other to core")
        None
    }
  }
}
