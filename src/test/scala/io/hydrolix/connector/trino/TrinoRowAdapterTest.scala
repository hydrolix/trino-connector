package io.hydrolix.connector.trino

import com.fasterxml.jackson.databind.node.ObjectNode
import org.junit.Test

import io.hydrolix.connectors.JSON
import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.types._

class TrinoRowAdapterTest {
  private val nestedStructType = StructType(List(StructField("nested.i", Int32Type), StructField("nested.s", StringType)))

  private val structType = StructType(List(
    StructField("b", BooleanType),
    StructField("i8", Int8Type),
    StructField("u8", UInt8Type),
    StructField("i16", Int16Type),
    StructField("u16", UInt16Type),
    StructField("i32", Int32Type),
    StructField("u32", UInt32Type),
    StructField("i64", Int64Type),
    StructField("u64", UInt64Type),
    StructField("f32", Float32Type),
    StructField("f64", Float64Type),
    StructField("s", StringType),
    StructField("d", DecimalType(20, 3)),
    StructField("array[i32!]", ArrayType(Int32Type, false)),
    StructField("array[i32?]", ArrayType(Int32Type, true)),
    StructField("map[string, f64!]", MapType(StringType, Float64Type, false)),
    StructField("nested", nestedStructType)
  ))

  private val structLiteral = StructLiteral(Map(
    "b" -> true,
    "i8" -> 123,
    "u8" -> 234,
    "i16" -> 31337,
    "u16" -> 65535,
    "i32" -> 2 * 1024 * 1024,
    "u32" -> 4L * 1024 * 1024,
    "i64" -> 2L * 1024 * 1024 * 1024,
    "u64" -> java.math.BigDecimal.valueOf(4L * 1024 * 1024 * 1024),
    "f32" -> 32.0f,
    "f64" -> 64.0d,
    "s" -> "hello world!",
    "d" -> java.math.BigDecimal.valueOf(3.14159265),
    "array[i32!]" -> List(1, 2, 3),
    "array[i32?]" -> List(10, null, 20, null, 30),
    "map[string, f64!]" -> Map("one" -> 1.0, "two" -> 2.0, "three" -> 3.0),
    "nested" -> StructLiteral(Map("nested.i" -> 123, "nested.s" -> "yolo"), nestedStructType)
  ), structType)

  private val row =
    """{
      |  "b":true,
      |  "i8":123,
      |  "u8":234,
      |  "i16":31337,
      |  "u16":65535,
      |  "i32":2097152,
      |  "u32":4194304,
      |  "f32":32.0,
      |  "f64":64.0,
      |  "s":"hello world!",
      |  "d":3.14159265,
      |  "array[i32!]":[1,2,3],
      |  "array[i32?]":[10,null,20,null,30],
      |  "map[string, f64!]":{
      |    "one":1.0,
      |    "two":2.0,
      |    "three":3.0
      |  },
      |  "nested":{
      |    "nested.i":123,
      |    "nested.s":"yolo"
      |   }
      |}""".stripMargin

  @Test
  def testStuff(): Unit = {
    val rowNode = JSON.objectMapper.readTree(row)
    val trinoRow = TrinoRowAdapter.row(-1, structType, rowNode.asInstanceOf[ObjectNode])
    println(trinoRow)
  }
}
