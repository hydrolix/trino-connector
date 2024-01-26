package io.hydrolix.connector.trino

import scala.jdk.CollectionConverters._

import io.trino.spi.`type`.{TimestampType, VarcharType}
import io.trino.spi.block.{Fixed12Block, LongArrayBlock, VariableWidthBlock}
import io.trino.spi.expression.{ConnectorExpression, StandardFunctions}
import io.trino.spi.predicate.{Domain, EquatableValueSet, SortedRangeSet}
import io.trino.spi.{`type` => ttypes}
import org.slf4j.LoggerFactory

import io.hydrolix.connector.trino.Enumerable.{F12BisEnumerable, LABIsEnumerable, VWBIsEnumerable}
import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.types.ArrayType
import io.hydrolix.connectors.{types => coretypes}

object TrinoPredicates {
  private val logger = LoggerFactory.getLogger(getClass)

  def trinoToCore(expr: ConnectorExpression): Expr[Boolean] = {
    expr match {
      case TPred(StandardFunctions.AND_FUNCTION_NAME, args) =>
        val preds = args.map(trinoToCore)
        preds.reduceLeft(And)
      case TPred(StandardFunctions.OR_FUNCTION_NAME, args) =>
        val preds = args.map(trinoToCore)
        preds.reduceLeft(Or)
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

  /**
   * This tries to create a Trino [[Domain]] (basically a more declarative predicate) to a core [[Expr]][Boolean].
   *
   * @param field  the name of the field the Domain is matching on
   * @param domain the Trino Domain to try to translate
   * @return `Some(expr)` if the Domain can be translated to a Core expression, or `None` if not
   */
  def domainToCore(field: String, domain: Domain, offset: Int): Option[Expr[Boolean]] = {
    domain.getType match {
      case VarcharType.VARCHAR =>
        domain.getValues match {
          case eq: EquatableValueSet if eq.getType == ttypes.VarcharType.VARCHAR =>
            val strings = collection.mutable.ArrayBuffer[String]()
            for (entry <- eq.getEntries.asScala) {
              // TODO what if there are nulls in it?
              strings ++= entry.getBlock.asInstanceOf[VariableWidthBlock].all
            }
            Some(In(GetField(field, coretypes.StringType), ArrayLiteral(strings.toSeq, ArrayType(coretypes.StringType))))
          case _ =>
            logger.warn(s"Can't translate $field: $domain")
            None
        }

      case _: TimestampType =>
        domain.getValues match {
          case srs: SortedRangeSet =>
            val block = srs.getSortedRanges
            if (block.getPositionCount != 2) {
              logger.warn(s"Timestamp predicate sortedRangeSet had ${block.getPositionCount} values; expected exactly 2")
              None
            } else if (block.isNull(0) && block.isNull(1)) {
              logger.warn(s"Timestamp predicate sortedRangeSet had two null values; expected exactly 1")
              None
            } else {
              val Seq(mLo, mHi) = srs.getSortedRanges match {
                case lb: LongArrayBlock => lb.asInstants.map(Option(_).map(_.minusSeconds(offset)))
                case fb: Fixed12Block => fb.asInstants.map(Option(_).map(_.minusSeconds(offset)))
                case other => sys.error(s"Timestamp range block was ${other.getClass.getSimpleName}; not Long or Fixed12")
              }

              val get = GetField(field, coretypes.TimestampType.Millis)

              (mLo, mHi) match {
                case (None, Some(hi)) =>
                  // Lower bound is null; this is <= max
                  Some(LessEqual(get, TimestampLiteral(hi)))

                case (Some(lo), None) =>
                  // Upper bound is null; this is >= min
                  Some(GreaterEqual(get, TimestampLiteral(lo)))

                case (Some(lo), Some(hi)) =>
                  // Both bounds are present; this is BETWEEN
                  Some(And(
                    GreaterEqual(get, TimestampLiteral(lo)),
                    LessEqual(get, TimestampLiteral(hi))
                  ))
                case (None, None) => sys.error("Time range with no bounds!")
              }
            }
          case other =>
            logger.info(s"No useful conversion for $field: $domain values: $other")
            None
        }
      case other =>
        logger.info(s"Can't translate $field: $domain of type $other to a core expression")
        None
    }
  }
}
