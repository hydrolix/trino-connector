package io.hydrolix.connector.trino

import java.time.Instant

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airlift.slice.Slices
import io.trino.operator.scalar.{MapKeys, MapValues}
import io.trino.spi.`type`.{SmallintType, Type, VarcharType}
import io.trino.spi.block._
import io.trino.spi.{`type` => ttypes}
import org.junit.Assert.assertEquals
import org.junit.Test

import io.hydrolix.connector.trino.Enumerable.VWBIsEnumerable
import io.hydrolix.connector.trino.TestUtil.{NodeStuff, christmasTreeStructType}
import io.hydrolix.connector.trino.json.parse.TrinoValueParser
import io.hydrolix.connector.trino.json.print.TrinoValuePrinter
import io.hydrolix.connectors.types._
import io.hydrolix.connectors.{Etc, JSON}

class TrinoColumnarJsonTest {
  private def rowTypeToColumnarType(fields: StructField*) = {
    val arrays = fields.map(sf => StructField(sf.name, ArrayType(sf.`type`, true), true)).toList
    StructType(List(
      StructField("rows", UInt32Type),
      StructField("cols", StructType(arrays))
    ))
  }

  val smallJson = """{
    |  "rows": 3,
    |  "cols": {
    |    "timestamp": [
    |      "1970-01-01T00:00:01.001Z",
    |      "1970-01-01T00:00:02.002Z",
    |      "1970-01-01T00:00:03.003Z"
    |    ],
    |    "string": [
    |      "one",
    |      "two",
    |      "three"
    |    ],
    |    "array<string!>": [
    |      ["1a", "1b", "1c", "1d"],
    |      ["2a", "2b", "2c", "2d"],
    |      ["3a", "3b", "3c", "3d"]
    |    ],
    |    "map<string,string!>":[
    |      {"k1a":"v1a"},
    |      {"k2a":"v2a", "k2b":"v2b"},
    |      {"k3a":"v3a", "k3b":"v3b", "k3c":"v3c"}
    |    ]
    |  }
    |}
    |""".stripMargin

  val coreRowType = StructType(List(
    StructField("timestamp", TimestampType.Millis, true),
    StructField("string", StringType, true),
    StructField("array<string!>", ArrayType(StringType, true)),
    StructField("map<string,string!>", MapType(StringType, StringType, false))
  ))
  val trinoRowType = TrinoTypes.coreToTrino(coreRowType).get.asInstanceOf[ttypes.RowType]

  val coreColumnarType = rowTypeToColumnarType(coreRowType.fields: _*)
  val trinoColumnarType = TrinoTypes.coreToTrino(coreColumnarType).get.asInstanceOf[ttypes.RowType]

  case class FooRow(
    timestamp: Instant,
    string: String,
    `array<string!>`: List[String],
    `map<string,string!>`: Map[String, String]
  )

  @Test
  def basicParsingOfColumnarJson(): Unit = {
    // TODO put some nulls in here too

    val trinoSchemaByName = coreRowType.fields.zipWithIndex.map { case (sf, i) =>
      sf.name -> (new ttypes.ArrayType(TrinoTypes.coreToTrino(sf.`type`).get) -> i)
    }.toMap

    val targetTree = JSON.objectMapper.readValue[ObjectNode](smallJson)
    val colsObj = targetTree.get("cols").asInstanceOf[ObjectNode]

    TrinoColumnarJson.parseStream(
      JSON.objectMapper.createParser(smallJson),
      trinoSchemaByName,
      {() => sys.error("wat")},
      { page =>
        val timestamps = page.getBlock(0)
        val strings = page.getBlock(1)
        val arraysOfStrings = page.getBlock(2)
        val maps = page.getBlock(3)

        val timestampsJson = TrinoValuePrinter(trinoSchemaByName("timestamp")._1).print(timestamps, 0)
        val stringsJson = TrinoValuePrinter(trinoSchemaByName("string")._1).print(strings, 0)
        val arraysOfStringsJson = TrinoValuePrinter(trinoSchemaByName("array<string!>")._1).print(arraysOfStrings, 0)
        val mapsJson = TrinoValuePrinter(trinoSchemaByName("map<string,string!>")._1).print(maps, 0)

        assertEquals(colsObj.get("timestamp"), timestampsJson)
        assertEquals(colsObj.get("string"), stringsJson)
        assertEquals(colsObj.get("array<string!>"), arraysOfStringsJson)
        assertEquals(colsObj.get("map<string,string!>"), mapsJson)

        println(s"Got page: $page")
      }
    )
  }

  @Test
  def sigh2(): Unit = {
    val is = getClass.getResourceAsStream("/100.jsonc")
    val parser = JSON.objectMapper.createParser(is)

    val trinoSchemaByName = christmasTreeStructType.fields.zipWithIndex.map { case (sf, i) =>
      val atyp = TrinoTypes
        .coreToTrino(ArrayType(sf.`type`, true))
        .getOrElse(sys.error(s"Can't convert column type ${sf.`type`}"))
        .asInstanceOf[ttypes.ArrayType]

      sf.name -> (atyp -> i)
    }.toMap

    TrinoColumnarJson.parseStream(
      parser,
      trinoSchemaByName,
      { () => sys.error("Expected non-empty") },
      { page =>
        // TODO actually check the contents
        println(page)
      }
    )
  }

  @Test
  def arbitraryNestingOfPrimitiveArrays(): Unit = {
    val type0 = SmallintType.SMALLINT
    val type1 = ArrayOf(type0)
    val type2 = ArrayOf(type1)
    val type3 = ArrayOf(type2)
    val type4 = ArrayOf(type3)

    check(type0, "1")
    check(type1, "[1,2]")
    check(type2,
      """[
        |  [10],
        |  [20,21],
        |  [30,31,32,33]
        |]""".stripMargin)
    check(type3,
      """[
        |  [
        |    [100]
        |  ],
        |  [
        |    [200],
        |    [210,211]
        |  ],
        |  [
        |    [300],
        |    [310,311],
        |    [320,321,322]
        |  ],
        |  [
        |    [400],
        |    [410,411],
        |    [420,421,422],
        |    [430,431,432,434]
        |  ]
        |]""".stripMargin)
    check(type4,
      """[
        |  [
        |    [
        |      [100],
        |      [110,111]
        |    ]
        |  ],
        |  [
        |    [
        |      [200],
        |      [210,211],
        |      [220,221,222]
        |    ]
        |  ],
        |  [
        |    [
        |      [300],
        |      [310,311],
        |      [320,321,322],
        |      [330,331,332,333]
        |    ]
        |  ],
        |  [
        |    [
        |      [400],
        |      [410,411],
        |      [420,421,422],
        |      [430,431,432,433],
        |      [440,441,442,443,444]
        |    ]
        |  ],
        |  [
        |    [
        |      [500],
        |      [510,511],
        |      [520,521,522],
        |      [530,531,532,533],
        |      [540,541,542,543,544],
        |      [550,551,552,553,554,555]
        |    ]
        |  ]
        |]""".stripMargin)
  }

  private def check(trinoType: Type, value: String): Unit = {
    val parsed = JSON.objectMapper.readValue[JsonNode](value)

    val vb = {
      val th0 = TrinoValueParser(trinoType)
      val bb0 = th0.makeBlockBuilder
      th0.parseAndAppend(parser(value), bb0)
      th0.finish(bb0)
    }

    val printed = TrinoValuePrinter(trinoType).print(vb.getLoadedBlock, 0)

    assertEquals(
      parsed.normalizeNumbers(),
      printed.normalizeNumbers()
    )
  }

  private def parser(value: String) = {
    JSON.objectMapper.createParser(value).also { p =>
      p.nextToken()
    }
  }

  /** This is just confirming my understanding of how Trino maps work */
  @Test
  def buildArrayOfMapsManually(): Unit = {
    val parent = new MapBlockBuilder(MapOf(VarcharType.VARCHAR, VarcharType.VARCHAR), null, 1)

    val child1 = parent.newBlockBuilderLike(1, null).asInstanceOf[MapBlockBuilder]
    child1.buildEntry { (kb, vb) =>
      VarcharType.VARCHAR.writeSlice(kb, Slices.utf8Slice("k1a"))
      VarcharType.VARCHAR.writeSlice(vb, Slices.utf8Slice("v1a"))
      VarcharType.VARCHAR.writeSlice(kb, Slices.utf8Slice("k1b"))
      VarcharType.VARCHAR.writeSlice(vb, Slices.utf8Slice("v1b"))
    }
    parent.append(child1.buildValueBlock(), 0)

    val child2 = parent.newBlockBuilderLike(1, null).asInstanceOf[MapBlockBuilder]
    child2.buildEntry { (kb, vb) =>
      VarcharType.VARCHAR.writeSlice(kb, Slices.utf8Slice("k2a"))
      VarcharType.VARCHAR.writeSlice(vb, Slices.utf8Slice("v2a"))
      VarcharType.VARCHAR.writeSlice(kb, Slices.utf8Slice("k2b"))
      VarcharType.VARCHAR.writeSlice(vb, Slices.utf8Slice("v2b"))
      VarcharType.VARCHAR.writeSlice(kb, Slices.utf8Slice("k2c"))
      VarcharType.VARCHAR.writeSlice(vb, Slices.utf8Slice("v2c"))
    }
    parent.append(child2.buildValueBlock(), 0)

    val mb = parent.buildValueBlock()

    assertEquals("two arrays-of-maps were constructed", 2, mb.getPositionCount)
    val map1 = mb.getMap(0)
    val map2 = mb.getMap(1)

    assertEquals("first array has two pairs", 2, map1.getSize)
    assertEquals("second array has three pairs", 3, map2.getSize)

    val keys1 = MapKeys.getKeys(VarcharType.VARCHAR, map1).asInstanceOf[VariableWidthBlock]
    val values1 = MapValues.getValues(VarcharType.VARCHAR, map1).asInstanceOf[VariableWidthBlock]

    assertEquals(List("k1a", "k1b"), keys1.all)
    assertEquals(List("v1a", "v1b"), values1.all)

    val keys2 = MapKeys.getKeys(VarcharType.VARCHAR, map2).asInstanceOf[VariableWidthBlock]
    val values2 = MapValues.getValues(VarcharType.VARCHAR, map2).asInstanceOf[VariableWidthBlock]

    assertEquals(List("k2a", "k2b", "k2c"), keys2.all)
    assertEquals(List("v2a", "v2b", "v2c"), values2.all)
  }
}
