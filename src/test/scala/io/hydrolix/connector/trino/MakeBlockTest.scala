package io.hydrolix.connector.trino

import io.trino.spi.`type`.TypeUtils
import io.trino.spi.block.SqlRow
import io.trino.spi.{`type` => ttypes}
import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connectors.expr.{ArrayLiteral, StringLiteral, StructLiteral}
import io.hydrolix.connectors.types.ArrayType
import io.hydrolix.connectors.{types => coretypes}

class MakeBlockTest {
  @Test def testArrayOfStrings(): Unit = doStuff(List("one", "two", "three"), new ttypes.ArrayType(ttypes.VarcharType.VARCHAR))
  @Test def testArrayOfArrayOfStrings(): Unit = {
    doStuff(
      List(
        List("1", "one", "*"),
        List("2", "two", "**"),
        List("3", "three", "***")
      ),
      new ttypes.ArrayType(new ttypes.ArrayType(ttypes.VarcharType.VARCHAR))
    )
  }

  private val arrayOfStrings: ArrayType = coretypes.ArrayType(coretypes.StringType, false)

  private val arrayOfArrayOfStrings: ArrayType = coretypes.ArrayType(arrayOfStrings)
  private val rowType = coretypes.StructType(List(
    coretypes.StructField("s", coretypes.StringType),
    coretypes.StructField("ss", arrayOfStrings),
    coretypes.StructField("sss", arrayOfArrayOfStrings)
  ))

  private val row = StructLiteral(
    Map(
      "s" -> StringLiteral("one"),
      "ss" -> ArrayLiteral(List("one", "two"), arrayOfStrings),
      "sss" -> ArrayLiteral(List(
        ArrayLiteral(List("1", "one", "*"), coretypes.StringType),
        ArrayLiteral(List("2", "two", "**"), coretypes.StringType),
        ArrayLiteral(List("3", "three", "***"), coretypes.StringType)
      ), arrayOfStrings, false)
    ),
    rowType
  )

  @Test
  def testConvertRow(): Unit = {
    val trow = convertRow(row)
    assertEquals(row.values.size, trow.getFieldCount)
  }

  private def convertRow(row: StructLiteral): SqlRow = {
    val trinoRowType = TrinoTypes.coreToTrino(row.`type`).get.asInstanceOf[ttypes.RowType]

    val blocks = for ((value, i) <- row.values.zipWithIndex) yield {
      val valueType = row.`type`.fields(i)
      val trinoValue = valueType match {
        case _ => ???
      }

      TypeUtils.writeNativeValue(trinoRowType.getFields.get(i).getType, value)
    }
    new SqlRow(0, blocks.toArray)
  }

  private def doStuff[T](ss: Iterable[Any], arrayType: ttypes.ArrayType): Unit = {
    val block = arrayType.getElementType match {
      case nested: ttypes.ArrayType =>
        val kidBlocks = ss.map(ks => makeArrayBlock(ks.asInstanceOf[Iterable[Any]], nested))
        makeArrayBlock(kidBlocks, arrayType)
      case _ =>
        makeArrayBlock(ss, arrayType)
    }

    assertEquals(ss.size, block.getPositionCount)
  }
}
