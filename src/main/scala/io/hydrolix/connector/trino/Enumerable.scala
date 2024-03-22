package io.hydrolix.connector.trino

import java.time.{Instant, ZoneOffset}
import java.{lang => jl}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import io.airlift.slice.Slice
import io.forktrino.`type`.DateTimes
import io.trino.spi.`type`.{TimestampType, TimestampWithTimeZoneType}
import io.trino.spi.block._

/**
 * Things that can be rendered from a collection of nullable B values to a collection of nullable T values
 *
 * @tparam B type of the boxed values
 * @tparam T type of the unboxed values
 */
trait Enumerable[B >: Null <: AnyRef, T >: Null <: AnyRef] {
  val size: Int

  def get(i: Int): B

  def unbox(t: B): T

  def all: List[T] = {
    val out = mutable.ArrayBuffer[T]()
    for (i <- 0 until size) {
      val b = get(i)
      if (b == null) {
        out += null
      } else {
        out += unbox(b)
      }
    }
    out.toList
  }
}

object TimestampDecoding {
  def asInstants(block: Block, tt: TimestampType): List[Instant] = {
    val out = ArrayBuffer[Instant]()
    for (i <- 0 until block.getPositionCount) {
      if (block.isNull(i)) {
        out += null
      } else {
        val zdt = DateTimes.toLocalDateTime(tt, block, i)
        out += zdt.toInstant(ZoneOffset.UTC)
      }
    }
    out.toList
  }

  def asInstants(block: Block, tt: TimestampWithTimeZoneType): List[Instant] = {
    val out = ArrayBuffer[Instant]()
    for (i <- 0 until block.getPositionCount) {
      if (block.isNull(i)) {
        out += null
      } else {
        val zdt = DateTimes.toZonedDateTime(tt, block, i)
        out += zdt.toInstant
      }
    }
    out.toList
  }
}

object Enumerable {
  implicit class VWBIsEnumerable(val block: VariableWidthBlock) extends Enumerable[Slice, String] {
    override val size: Int = block.getPositionCount
    override def get(i: Int): Slice = if (block.isNull(i)) null else block.getSlice(i)
    override def unbox(t: Slice): String = t.toStringUtf8
  }

  implicit class LABIsEnumerable(val block: LongArrayBlock) extends Enumerable[jl.Long, jl.Long] {
    override val size: Int = block.getPositionCount
    override def get(i: Int): jl.Long = if (block.isNull(i)) null else block.getLong(i)
    override def unbox(t: jl.Long): jl.Long = t
  }

  implicit class IABIsEnumerable(val block: IntArrayBlock) extends Enumerable[jl.Integer, jl.Integer] {
    override val size: Int = block.getPositionCount
    override def get(i: Int): jl.Integer = if (block.isNull(i)) null else block.getInt(i)
    override def unbox(t: jl.Integer): jl.Integer = t
  }

  implicit class BABIsEnumerable(val block: ByteArrayBlock) extends Enumerable[jl.Byte, jl.Byte] {
    override val size: Int = block.getPositionCount
    override def get(i: Int): jl.Byte = if (block.isNull(i)) null else block.getByte(i)
    override def unbox(t: jl.Byte): jl.Byte = t
  }

  implicit class F12BisEnumerable(val block: Fixed12Block) extends Enumerable[(Long, Int), (Long, Int)] {
    override val size: Int = block.getPositionCount

    override def get(i: Int): (Long, Int) = {
      if (block.isNull(i)) {
        null
      } else {
        val first = block.getFixed12First(i)
        val second = block.getFixed12Second(i)
        (first, second)
      }
    }

    override def unbox(t: (Long, Int)): (Long, Int) = t
  }
}
