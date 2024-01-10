package io.hydrolix.connector

import java.math.MathContext
import java.time.Instant
import java.{math => jm}
import scala.annotation.unused

import io.forktrino.`type`.DateTimes._
import io.trino.spi.Page
import io.trino.spi.`type`._

package object trino {
  val emptyPage = new Page(0)

  object ArrayOf {
    def apply(t: Type) = new ArrayType(t)
    def unapply(t: Type): Option[Type] = t match {
      case arr: ArrayType => Some(arr.getElementType)
      case _ => None
    }
  }

  object MapOf {
    def apply(kt: Type, vt: Type) = new MapType(kt, vt, new TypeOperators())
    def unapply(t: MapType): Option[(Type, Type)] = t match {
      case map: MapType => Some((map.getKeyType, map.getValueType))
      case _ => None
    }
  }

  def fixed12ToInstant(epochMicros: Long, picosOfMicro: Int): Instant = {
    val epochSecond = scaleEpochMicrosToSeconds(epochMicros)
    val nanoFraction = getMicrosOfSecond(epochMicros) * NANOSECONDS_PER_MICROSECOND + (roundToNearest(picosOfMicro, PICOSECONDS_PER_NANOSECOND) / PICOSECONDS_PER_NANOSECOND).toInt

    Instant.ofEpochSecond(epochSecond, nanoFraction)
  }

  def decodeShortDecimal(dt: DecimalType, value: Long) = {
    val unscaledValue = jm.BigInteger.valueOf(value)
    new jm.BigDecimal(unscaledValue, dt.getScale, new jm.MathContext(dt.getPrecision))
  }

  def decodeLongDecimal(dt: DecimalType, value: Int128): jm.BigDecimal = {
    val unscaled = value.toBigInteger
    new jm.BigDecimal(unscaled, dt.getScale, new MathContext(dt.getPrecision))
  }

  val TrinoUInt64Type = DecimalType.createDecimalType(20, 0)

  val isoR = """\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3,9})?Z""".r
  val spaceR = """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3,9})?""".r

  implicit class TestSeqStuff[A](val list: Seq[A]) extends AnyVal {
    @unused
    def unzip4[A1, A2, A3, A4](implicit asProduct4: A => Product4[A1, A2, A3, A4]): (Seq[A1], Seq[A2], Seq[A3], Seq[A4]) = {
      val b1 = Seq.newBuilder[A1]
      val b2 = Seq.newBuilder[A2]
      val b3 = Seq.newBuilder[A3]
      val b4 = Seq.newBuilder[A4]

      list.foreach { xyz =>
        val four = asProduct4(xyz)
        b1 += four._1
        b2 += four._2
        b3 += four._3
        b4 += four._4
      }
      (b1.result(), b2.result(), b3.result(), b4.result())
    }
  }

  @unused
  def zip4[A, A1, A2, A3, A4](a1s: Seq[A1],
                              a2s: Seq[A2],
                              a3s: Seq[A3],
                              a4s: Seq[A4])
                      (implicit f: (A1, A2, A3, A4) => A)
                                 : Seq[A] =
  {
    val as = Seq.newBuilder[A]

    a1s.lazyZip(a2s).lazyZip(a3s).lazyZip(a4s).map { case (a1, a2, a3, a4) =>
      as += f(a1, a2, a3, a4)
    }

    as.result()
  }
}
