package io.hydrolix.connector.trino

import scala.jdk.CollectionConverters._

import io.trino.spi.block.LongArrayBlock
import io.trino.spi.expression.{ConnectorExpression, StandardFunctions}
import io.trino.spi.predicate.{Domain, EquatableValueSet, SortedRangeSet}
import io.trino.spi.{`type` => ttypes}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.{microsToInstant, types => coretypes}

object TrinoPredicates {
  private val logger = LoggerFactory.getLogger(getClass)

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

  /**
   * This tries to create a Trino [[Domain]] (basically a more declarative predicate) to a core [[Expr]][Boolean].
   *
   * @param field  the name of the field the Domain is matching on
   * @param domain the Trino Domain to try to translate
   * @return `Some(expr)` if the Domain can be translated to a Core expression, or `None` if not
   */
  def domainToCore(field: String, domain: Domain): Option[Expr[Boolean]] = {
    TrinoTypes.trinoToCore(domain.getType) match {
      case Some(coretypes.StringType) =>
        domain.getValues match {
          case eq: EquatableValueSet if eq.getType == ttypes.VarcharType.VARCHAR =>
            val strings = collection.mutable.ArrayBuffer[String]()
            for (entry <- eq.getEntries.asScala) {
              var offset = 0
              for (i <- 0 until entry.getBlock.getPositionCount) {
                val len = entry.getBlock.getSliceLength(i)
                strings += entry.getBlock.getSlice(i, offset, len).toStringUtf8
                offset += len
              }
            }
            Some(In(GetField(field, coretypes.StringType), ArrayLiteral(strings.toList, coretypes.StringType)))
          case _ =>
            logger.warn(s"Can't translate $field: $domain")
            None
        }
      case Some(coretypes.TimestampType(_)) =>
        domain.getValues match {
          case srs: SortedRangeSet =>
            srs.getSortedRanges match {
              case lb: LongArrayBlock =>
                if (lb.getPositionCount == 2) {
                  if (lb.isNull(0) && lb.isNull(1)) {
                    logger.warn("earliest and latest both null?!")
                    None
                  } else {
                    val get = GetField(field, coretypes.TimestampType.Millis)
                    if (lb.isNull(0)) {
                      // Lower bound is null; this is <= max
                      Some(LessEqual(get, TimestampLiteral(microsToInstant(lb.getLong(1, 0)))))
                    } else if (lb.isNull(1)) {
                      // Upper bound is null; this is >= min
                      Some(GreaterEqual(get, TimestampLiteral(microsToInstant(lb.getLong(0, 0)))))
                    } else {
                      // Both bounds are present; this is BETWEEN
                      Some(And(List(
                        GreaterEqual(get, TimestampLiteral(microsToInstant(lb.getLong(0, 0)))),
                        LessEqual(get, TimestampLiteral(microsToInstant(lb.getLong(1, 0))))
                      )))
                    }
                  }
                } else {
                  logger.warn(s"Got ${lb.getPositionCount} values in LongArrayBlock, expected exactly 2")
                  None
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
