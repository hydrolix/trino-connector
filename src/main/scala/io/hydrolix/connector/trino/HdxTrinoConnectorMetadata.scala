package io.hydrolix.connector.trino

import java.time.Instant
import java.{util => ju}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import io.trino.spi.`type`.TimestampType
import io.trino.spi.block.LongArrayBlock
import io.trino.spi.connector._
import io.trino.spi.predicate.{SortedRangeSet, TupleDomain}
import org.slf4j.LoggerFactory

import io.hydrolix.connectors.{HdxTableCatalog, Types}

final class HdxTrinoConnectorMetadata(val catalog: HdxTableCatalog) extends ConnectorMetadata {
  private val logger = LoggerFactory.getLogger(getClass)

  override def listSchemaNames(session: ConnectorSession): ju.List[String] = {
    catalog.listNamespaces().map(_.head).asJava
  }

  override def listTables(session: ConnectorSession, schemaName: ju.Optional[String]): ju.List[SchemaTableName] = {
    catalog.listTables(schemaName.toScala.toList).map { path =>
      new SchemaTableName(path.head, path(1))
    }.asJava
  }

  override def getTableHandle(session: ConnectorSession, tableName: SchemaTableName): ConnectorTableHandle = {
    new HdxTableHandle(tableName.getSchemaName, tableName.getTableName)
  }

  override def getTableMetadata(session: ConnectorSession, table: ConnectorTableHandle): ConnectorTableMetadata = {
    val handle = table.asInstanceOf[HdxTableHandle]
    val hdxTable = catalog.loadTable(List(handle.db, handle.table))
    new ConnectorTableMetadata(
      new SchemaTableName(handle.db, handle.table),
      hdxTable.hdxCols.map { case (name, hcol) =>
        val vt = Types.hdxToValueType(hcol.hdxType)
        new ColumnMetadata(name, TrinoTypes.coreToTrino(vt))
      }.toList.asJava
    )
  }

  override def getColumnHandles(session: ConnectorSession, tableHandle: ConnectorTableHandle): ju.Map[String, ColumnHandle] = {
    val handle = tableHandle.asInstanceOf[HdxTableHandle]
    val hdxTable = catalog.loadTable(List(handle.db, handle.table))

    hdxTable.schema.fields.map { sf =>
      sf.name -> new HdxColumnHandle(sf.name)
    }.toMap[String, ColumnHandle].asJava
  }


  override def getColumnMetadata(session: ConnectorSession,
                             tableHandle: ConnectorTableHandle,
                            columnHandle: ColumnHandle)
                                        : ColumnMetadata =
  {
    val chandle = columnHandle.asInstanceOf[HdxColumnHandle]
    val thandle = tableHandle.asInstanceOf[HdxTableHandle]
    val hdxTable = catalog.loadTable(List(thandle.db, thandle.table))

    new ColumnMetadata(
      chandle.name,
      TrinoTypes.coreToTrino(hdxTable.schema.byName(chandle.name).`type`)
    )
  }

  override def applyFilter(session: ConnectorSession,
                            handle: ConnectorTableHandle,
                        constraint: Constraint)
                                  : ju.Optional[ConstraintApplicationResult[ConnectorTableHandle]] =
  {
    val tbl = handle.asInstanceOf[HdxTableHandle]

    val hdxTable = catalog.loadTable(List(tbl.db, tbl.table))

    // constraint.summary.domains is a Map(HdxColumnHandle(timestamp) -> SortedRangeSet(type=timestamp(3), sortedRanges=LongArrayBlock(positionCount=2))

    super.applyFilter(session, handle, constraint)
  }
}
