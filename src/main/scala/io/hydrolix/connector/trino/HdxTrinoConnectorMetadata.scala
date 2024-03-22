package io.hydrolix.connector.trino

import java.time.Instant
import java.util.{Optional, OptionalLong}
import java.{util => ju}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import com.typesafe.scalalogging.Logger
import io.trino.spi.`type`._
import io.trino.spi.connector._
import io.trino.spi.expression.{ConnectorExpression, Variable}

import io.hydrolix.connector.trino.HdxTrinoSplitManager.HdxDbPartitionOps
import io.hydrolix.connectors.expr._
import io.hydrolix.connectors.{HdxConnectionInfo, HdxJdbcSession, HdxTableCatalog, Types, WyHash, types => coretypes}

final class HdxTrinoConnectorMetadata(val info: HdxConnectionInfo, val catalog: HdxTableCatalog) extends ConnectorMetadata {
  private val logger = Logger(getClass)

  override def listSchemaNames(session: ConnectorSession): ju.List[String] = {
    catalog.listNamespaces().map(_.head).asJava
  }

  override def listTables(session: ConnectorSession, schemaName: ju.Optional[String]): ju.List[SchemaTableName] = {
    catalog.listTables(schemaName.toScala.toList).map { path =>
      new SchemaTableName(path.head, path(1))
    }.asJava
  }

  override def getTableHandle(session: ConnectorSession,
                            tableName: SchemaTableName,
                         startVersion: ju.Optional[ConnectorTableVersion],
                           endVersion: ju.Optional[ConnectorTableVersion])
                                     : ConnectorTableHandle =
  {
    new HdxTableHandle(
      tableName.getSchemaName,
      tableName.getTableName,
      ju.Collections.emptyList(), // Don't know which partitions we need to read yet
      ju.Collections.emptyList(), // Don't know which columns we need to read yet
      OptionalLong.empty()
    )
  }

  override def getTableMetadata(session: ConnectorSession, table: ConnectorTableHandle): ConnectorTableMetadata = {
    val handle = table.asInstanceOf[HdxTableHandle]
    val hdxTable = catalog.loadTable(List(handle.db, handle.table))
    new ConnectorTableMetadata(
      new SchemaTableName(handle.db, handle.table),
      hdxTable.hdxCols.map { case (name, hdxCol) =>
        val vt = Types.hdxToValueType(hdxCol.hdxType)
        new ColumnMetadata(name, TrinoTypes.coreToTrino(vt).getOrElse(sys.error(s"Can't translate $name: ${hdxCol.hdxType} to Trino type")))
      }.toList.asJava
    )
  }

  override def getColumnHandles(session: ConnectorSession, tableHandle: ConnectorTableHandle): ju.Map[String, ColumnHandle] = {
    val handle = tableHandle.asInstanceOf[HdxTableHandle]
    val hdxTable = catalog.loadTable(List(handle.db, handle.table))

    hdxTable.schema.fields.map { sf =>
      sf.name -> new HdxColumnHandle(sf.name, Optional.empty())
    }.toMap[String, ColumnHandle].asJava
  }

  override def getColumnMetadata(session: ConnectorSession,
                             tableHandle: ConnectorTableHandle,
                            columnHandle: ColumnHandle)
                                        : ColumnMetadata =
  {
    val col = columnHandle.asInstanceOf[HdxColumnHandle]

    if (col.trinoType.isPresent) {
      new ColumnMetadata(col.name, col.trinoType.get())
    } else {
      val tbl = tableHandle.asInstanceOf[HdxTableHandle]
      val hdxTable = catalog.loadTable(List(tbl.db, tbl.table))

      val coreType = hdxTable.schema.byName(col.name).`type`
      val trinoType = TrinoTypes.coreToTrino(coreType).getOrElse(sys.error(s"Can't translate ${col.name}: $coreType to Trino type"))

      new ColumnMetadata(col.name, trinoType)
    }
  }

  override def applyFilter(session: ConnectorSession,
                            handle: ConnectorTableHandle,
                        constraint: Constraint)
                                  : ju.Optional[ConstraintApplicationResult[ConnectorTableHandle]] =
  {
    val tbl = handle.asInstanceOf[HdxTableHandle]
    if (!tbl.splits.isEmpty) {
      // Nothing further to do here
      return ju.Optional.empty()
    }

    val hdxTable = catalog.loadTable(List(tbl.db, tbl.table))

    val availableColumns = tbl.columns.asScala.toSet ++ constraint.getAssignments.values.asInstanceOf[ju.Collection[HdxColumnHandle]].asScala

    val pk = availableColumns
      .find(_.name == hdxTable.primaryKeyField)
      .getOrElse(sys.error(s"Couldn't find primary key field ${hdxTable.primaryKeyField} in Trino schema"))

    val sk = hdxTable.shardKeyField.map { skf =>
      availableColumns
        .find(_.name == skf)
        .getOrElse(sys.error(s"Couldn't find shard key field $skf in Trino schema"))
    }

    val pushed = constraint.getSummary.getDomains.toScala.map(_.asScala).getOrElse(Map()).flatMap {
      case (`pk`, dom) if dom.getType.isInstanceOf[TimestampWithTimeZoneType] =>
        // Matching on primary timestamp field
        TrinoPredicates.domainToCore(pk.name, dom)
      case (ch: HdxColumnHandle, dom) if sk.contains(ch) && dom.getType == VarcharType.VARCHAR =>
        // Matching on shard key field
        TrinoPredicates.domainToCore(ch.name, dom)
      case other =>
        logger.info(s"Predicate not pushable; will eval after scan: $other")
        None
    }

    // TODO this is duplicated from connectors-core
    val minTimestamp = pushed.collectFirst {
      case GreaterEqual(GetField(hdxTable.primaryKeyField, coretypes.TimestampType(_)), TimestampLiteral(inst)) => inst
      case GreaterThan(GetField(hdxTable.primaryKeyField, coretypes.TimestampType(_)), TimestampLiteral(inst)) => inst.plusSeconds(1)
    }

    val maxTimestamp = pushed.collectFirst {
      case LessEqual(GetField(hdxTable.primaryKeyField, coretypes.TimestampType(_)), TimestampLiteral(inst)) => inst
      case LessThan(GetField(hdxTable.primaryKeyField, coretypes.TimestampType(_)), TimestampLiteral(inst)) => inst.minusSeconds(1)
    }

    val shardKeyHashes = pushed.collectFirst {
      case Equal(GetField(fld, coretypes.StringType), StringLiteral(s)) if hdxTable.shardKeyField.contains(fld) => Set(s)
      case In(GetField(fld, coretypes.StringType), ArrayLiteral(ss, coretypes.ArrayType(coretypes.StringType, _), _)) if hdxTable.shardKeyField.contains(fld) =>
        ss.asInstanceOf[Seq[String]].toSet
    }.getOrElse(Set()).map(WyHash(_))

    if (minTimestamp.isEmpty && maxTimestamp.isEmpty && shardKeyHashes.isEmpty) {
      logger.warn("No predicates useful for partition pruning, this will be slow :(")
    }

    val parts = HdxJdbcSession(info).collectPartitions(tbl.db, tbl.table, minTimestamp, maxTimestamp, shardKeyHashes)

    // All the partitions that need to be read, stuffed into the TableHandle
    val tableWithSplits = tbl.withSplits(parts.map(_.toSplit).asJava)

    ju.Optional.of(
      new ConstraintApplicationResult(
        tableWithSplits,
        constraint.getSummary, // Nothing can be completely pushed -- even shard key can have hash collisions
        false // TODO maybe not?
      ))
  }

  override def applyProjection(session: ConnectorSession,
                                handle: ConnectorTableHandle,
                           projections: ju.List[ConnectorExpression],
                           assignments: ju.Map[String, ColumnHandle])
                                      : Optional[ProjectionApplicationResult[ConnectorTableHandle]] =
  {
    // projections are Variable expressions with name & type
    // assignments relate Variable names to ColumnHandles

    val tbl = handle.asInstanceOf[HdxTableHandle]
    if (!tbl.columns.isEmpty) {
      // Already been here
      return Optional.empty()
    }

    val assignmentsOut = projections.asScala.flatMap {
      case v: Variable =>
        val name = v.getName
        val trinoType = v.getType

        Some(new Assignment(name, new HdxColumnHandle(name, Optional.of(trinoType)), trinoType))
      case other =>
        logger.warn(s"Couldn't make column assignment from $other; skipping")
        None
    }.toList

    val colsOut = assignmentsOut.map(_.getColumn.asInstanceOf[HdxColumnHandle])

    Optional.of(new ProjectionApplicationResult(
      tbl.withColumns(colsOut.asJava),
      projections,
      assignmentsOut.asJava,
      false // TODO maybe not?
    ))
  }

  override def applyLimit(session: ConnectorSession,
                           handle: ConnectorTableHandle,
                            limit: Long)
                                 : Optional[LimitApplicationResult[ConnectorTableHandle]] =
  {
    val tbl = handle.asInstanceOf[HdxTableHandle]
    if (!tbl.limit.isEmpty) {
      // Already been here
      return Optional.empty()
    }

    Optional.of(
      new LimitApplicationResult(
        tbl.withLimit(limit),
        false, // We only return pages
        false
      )
    )
  }
}
