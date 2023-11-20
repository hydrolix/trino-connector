package io.hydrolix.connector

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

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

  implicit class NullOps[T <: AnyRef](val raw: T) extends AnyVal {
    def ifNull(msg: String): T = {
      if (raw == null) sys.error(msg)
      raw
    }
  }
}
