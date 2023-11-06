package io.hydrolix.connector.trino

import java.{lang => jl}
import scala.jdk.CollectionConverters.CollectionHasAsScala

import io.trino.spi.block.LongArrayBlock
import io.trino.spi.{`type` => ttypes}
import io.trino.spi.expression.{Call, ConnectorExpression, Constant, FieldDereference, FunctionName, StandardFunctions}
import io.trino.spi.predicate.{Domain, SortedRangeSet}

import io.hydrolix.connectors.expr.{And, BooleanLiteral, Equal, Expr, Float32Literal, Float64Literal, GreaterEqual, GreaterThan, Int16Literal, Int32Literal, Int64Literal, Int8Literal, LessEqual, LessThan, Not, NotEqual, Or, StringLiteral, TimestampLiteral}
import io.hydrolix.connectors.{microsToInstant, types => coretypes}

object TrinoExpressions {
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
        case TLit(st: ttypes.TimestampType, l: jl.Long) if st.isShort =>
          TimestampLiteral(microsToInstant(l.longValue()))
        // TODO long timestamps?
        // TODO decimals
        // TODO arrays
        // TODO maps
        // TODO structs
      }
    }
  }

  def domainToCore(domain: Domain): Expr[_] = {
    val coretype = TrinoTypes.trinoToCore(domain.getType)

    domain.getValues match {
      case srs: SortedRangeSet if srs.getSortedRanges.isInstanceOf[LongArrayBlock] =>
    }
  }
}

object TrinoPredicates {
  def trinoToCore(expr: ConnectorExpression): Expr[Boolean] = {
    expr match {
      case TPred(StandardFunctions.AND_FUNCTION_NAME, args) =>
        And(args.map(trinoToCore))
      case TPred(StandardFunctions.OR_FUNCTION_NAME, args) =>
        Or(args.map(trinoToCore))
      case TPred(StandardFunctions.NOT_FUNCTION_NAME, List(arg)) =>
        Not(trinoToCore(arg))
      case TPred(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME, List(l, r)) =>
        LessThan(TrinoExpressions.trinoToCore(l), TrinoExpressions.trinoToCore(r))
      case TPred(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, List(l, r)) =>
        LessEqual(TrinoExpressions.trinoToCore(l), TrinoExpressions.trinoToCore(r))
      case TPred(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME, List(l, r)) =>
        GreaterThan(TrinoExpressions.trinoToCore(l), TrinoExpressions.trinoToCore(r))
      case TPred(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, List(l, r)) =>
        GreaterEqual(TrinoExpressions.trinoToCore(l), TrinoExpressions.trinoToCore(r))
      case TPred(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME, List(l, r)) =>
        Equal(TrinoExpressions.trinoToCore(l), TrinoExpressions.trinoToCore(r))
      case TPred(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME, List(l, r)) =>
        NotEqual(TrinoExpressions.trinoToCore(l), TrinoExpressions.trinoToCore(r))
    }
  }
}

object TCall {
  def unapply(expr: ConnectorExpression): Option[(ttypes.Type, String, List[ConnectorExpression])] = {
    expr match {
      case call: Call if call.getFunctionName.getCatalogSchema.isEmpty => 
        Some((call.getType, call.getFunctionName.getName, call.getArguments.asScala.toList))
      case _ => None
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
