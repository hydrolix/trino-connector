package io.hydrolix.connector.trino

import java.util.Optional
import scala.jdk.CollectionConverters._

import io.trino.spi.{`type` => ttype}
import io.hydrolix.connectors.{types => coretypes}

object TrinoTypes {
  def coreToTrino(typ: coretypes.ValueType): ttype.Type = {
    typ match {
      case coretypes.Int8Type          => ttype.TinyintType.TINYINT
      case coretypes.UInt8Type         => ttype.SmallintType.SMALLINT
      case coretypes.Int16Type         => ttype.SmallintType.SMALLINT
      case coretypes.UInt16Type        => ttype.IntegerType.INTEGER
      case coretypes.Int32Type         => ttype.IntegerType.INTEGER
      case coretypes.UInt32Type        => ttype.BigintType.BIGINT
      case coretypes.Int64Type         => ttype.BigintType.BIGINT
      case coretypes.UInt64Type        => ttype.DecimalType.createDecimalType(20, 0)
      case coretypes.Float32Type       => ttype.RealType.REAL
      case coretypes.Float64Type       => ttype.DoubleType.DOUBLE
      case coretypes.StringType        => ttype.VarcharType.VARCHAR
      case coretypes.BooleanType       => ttype.BooleanType.BOOLEAN
      case coretypes.TimestampType(p)  => ttype.TimestampType.createTimestampType(p)
      case coretypes.DecimalType(p, s) => ttype.DecimalType.createDecimalType(p, s)
      case coretypes.ArrayType(elt, _) =>
        val telt = coreToTrino(elt)
        new ttype.ArrayType(telt)
      case coretypes.MapType(kt, vt, _) =>
        val tkt = coreToTrino(kt)
        val tvt = coreToTrino(vt)
        new ttype.MapType(tkt, tvt, new ttype.TypeOperators()) // TODO wtf is this?
      case coretypes.StructType(fields @ _*) =>
        val tfields = fields.map { fld =>
          new ttype.RowType.Field(
            Optional.of(fld.name),
            coreToTrino(fld.`type`)
          )
        }

        ttype.RowType.from(tfields.asJava)
      case other =>
        sys.error(s"Can't translate core type $other to Trino")
    }
  }

  def trinoToCore(ttyp: ttype.Type): coretypes.ValueType = {
    ttyp match {
      case ttype.BooleanType.BOOLEAN => coretypes.BooleanType
      case ttype.VarcharType.VARCHAR => coretypes.StringType
      case ttype.TinyintType.TINYINT => coretypes.Int8Type
      case ttype.SmallintType.SMALLINT => coretypes.Int16Type
      case ttype.IntegerType.INTEGER => coretypes.Int32Type
      case ttype.BigintType.BIGINT => coretypes.Int64Type
      case ttype.TimestampType.TIMESTAMP_SECONDS => coretypes.TimestampType.Seconds
      case ttype.TimestampType.TIMESTAMP_MILLIS => coretypes.TimestampType.Millis
      case ttype.TimestampType.TIMESTAMP_MICROS => coretypes.TimestampType.Micros
      case at: ttype.ArrayType =>
        coretypes.ArrayType(trinoToCore(at.getElementType), true)
      case mt: ttype.MapType =>
        coretypes.MapType(trinoToCore(mt.getKeyType), trinoToCore(mt.getValueType), true)
    }
  }
}
