package io.hydrolix.connector.trino

import java.{lang => jl, util => ju}
import scala.annotation.unused
import scala.jdk.CollectionConverters._

import com.google.common.collect.ImmutableList
import io.trino.spi.`type`.{Int128, LongTimestamp}
import io.trino.spi.expression._
import io.trino.spi.{`type` => ttypes}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.{instantToMicros, microsToInstant, types => coretypes}

object TrinoExpressions {
  @unused private val logger = LoggerFactory.getLogger(getClass)

  def trinoToCore(expr: ConnectorExpression): Expr[_] = {
    if (expr.getType == ttypes.BooleanType.BOOLEAN) {
      TrinoPredicates.trinoToCore(expr)
    } else {
      expr match {
        case TLit(ttypes.BooleanType.BOOLEAN,      jl.Boolean.TRUE)  => BooleanLiteral.True
        case TLit(ttypes.BooleanType.BOOLEAN,      jl.Boolean.FALSE) => BooleanLiteral.False
        case TLit(ttypes.VarcharType.VARCHAR,   s: jl.String)        => StringLiteral(s)
        case TLit(ttypes.TinyintType.TINYINT,   b: jl.Byte)          => Int8Literal(b.byteValue())
        case TLit(ttypes.SmallintType.SMALLINT, s: jl.Short)         => Int16Literal(s.shortValue())
        case TLit(ttypes.IntegerType.INTEGER,   i: jl.Integer)       => Int32Literal(i.intValue())
        case TLit(ttypes.BigintType.BIGINT,     l: jl.Long)          => Int64Literal(l.longValue())
        case TLit(ttypes.RealType.REAL,         f: jl.Float)         => Float32Literal(f.floatValue())
        case TLit(ttypes.DoubleType.DOUBLE,     d: jl.Double)        => Float64Literal(d.doubleValue())

        case TLit(tt: ttypes.TimestampType, l: jl.Long) if tt.isShort => TimestampLiteral(microsToInstant(l.longValue()))
        case TLit(_: ttypes.TimestampType, lt: LongTimestamp)         => TimestampLiteral(fixed12ToInstant(lt.getEpochMicros, lt.getPicosOfMicro))
        case TLit(dt: ttypes.DecimalType, l: jl.Long) if dt.isShort   => DecimalLiteral(decodeShortDecimal(dt, l.longValue()))
        case TLit(dt: ttypes.DecimalType, i: Int128)                  => DecimalLiteral(decodeLongDecimal(dt, i))
        // TODO arrays
        // TODO maps
        // TODO structs
      }
    }
  }

  private val op2Trino = Map(
    ComparisonOp.LT -> StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
    ComparisonOp.LE -> StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
    ComparisonOp.EQ -> StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
    ComparisonOp.NE -> StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME,
    ComparisonOp.GE -> StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
    ComparisonOp.GT -> StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
  )

  def coreToTrino(schema: coretypes.StructType, scan: Call, expr: Expr[Any]): ConnectorExpression = {
    expr match {
      case GetField(name, typ) =>
        new FieldDereference(TrinoTypes.coreToTrino(typ).getOrElse(sys.error(s"Can't translate $name: $typ to Trino type")), scan, schema.fields.indexWhere(_.name == name))

      case Comparison(left, op, right) =>
        new Call(
          ttypes.BooleanType.BOOLEAN,
          op2Trino(op),
          ju.Arrays.asList(
            coreToTrino(schema, scan, left),
            coreToTrino(schema, scan, right)
          )
        )

      case And(left, right) =>
        new Call(
          ttypes.BooleanType.BOOLEAN,
          StandardFunctions.AND_FUNCTION_NAME,
          ImmutableList.of(
            coreToTrino(schema, scan, left),
            coreToTrino(schema, scan, right)
          )
        )
      case Or(left, right) =>
        new Call(
          ttypes.BooleanType.BOOLEAN,
          StandardFunctions.OR_FUNCTION_NAME,
          ImmutableList.of(
            coreToTrino(schema, scan, left),
            coreToTrino(schema, scan, right)
          )
        )
      case Not(kid) =>
        new Call(
          ttypes.BooleanType.BOOLEAN,
          StandardFunctions.NOT_FUNCTION_NAME,
          ju.Arrays.asList(coreToTrino(schema, scan, kid))
        )

      case StringLiteral(s)       => new Constant(s, ttypes.VarcharType.VARCHAR)
      case BooleanLiteral(b)      => new Constant(b, ttypes.BooleanType.BOOLEAN)
      case Int8Literal(b)         => new Constant(b, ttypes.TinyintType.TINYINT)
      case UInt8Literal(s)        => new Constant(s, ttypes.SmallintType.SMALLINT)
      case TimestampLiteral(inst) => new Constant(instantToMicros(inst), ttypes.TimestampType.TIMESTAMP_MICROS)
    }
  }
}

object TLit {
  def unapply(expr: ConnectorExpression): Option[(ttypes.Type, AnyRef)] = {
    expr match {
      case const: Constant => Some((const.getType, const.getValue))
      case _ => None
    }
  }
}

object TPred {
  def unapply(expr: ConnectorExpression): Option[(FunctionName, List[ConnectorExpression])] = {
    expr match {
      case call: Call if call.getFunctionName.getCatalogSchema.isEmpty && call.getType == ttypes.BooleanType.BOOLEAN =>
        Some((call.getFunctionName, call.getArguments.asScala.toList))
      case _ => None
    }
  }
}
