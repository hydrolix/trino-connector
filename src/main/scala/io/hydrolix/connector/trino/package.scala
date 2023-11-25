package io.hydrolix.connector

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import io.trino.spi.`type`.{ArrayType, DecimalType, TypeUtils}
import io.trino.spi.block.Block

package object trino {
  def serialize(obj: Serializable): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(obj)
    oos.close()
    baos.toByteArray
  }

  def deserialize[T <: AnyRef](bytes: Array[Byte]) = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    ois.readObject().asInstanceOf[T]
  }

  def makeArrayBlock[T](ss: Iterable[T], arrayType: ArrayType): Block = {
    val arrayBuilder = arrayType.createBlockBuilder(null, 0)

    if (ss.nonEmpty) {
      for (s <- ss) {
        arrayBuilder.buildEntry { elementBuilder =>
          TypeUtils.writeNativeValue(arrayType.getElementType, elementBuilder, s)
        }
      }
    }

    arrayBuilder.build()
  }

  val TrinoUInt64Type = DecimalType.createDecimalType(20)
}
