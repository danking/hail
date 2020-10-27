package is.hail.services.shuffler

import is.hail.expr.ir._
import is.hail.types.encoded._
import is.hail.types.virtual._
import is.hail.io._
import is.hail.expr.ir.IRParser
import io.vertx.core.buffer.Buffer

object BufferWire {
  def writeString(out: Buffer, s: String): Unit = {
    val bytes = s.getBytes(utfCharset)
    out.appendInt(bytes.length)
    out.appendBytes(bytes)
  }


  def writeTStruct(out: Buffer, x: TStruct): Unit = {
    writeString(out, x.parsableString())
  }

  def writeEType(out: Buffer, x: EType): Unit = {
    val bytes = x.parsableString().getBytes(utfCharset)
    out.appendInt(bytes.length)
    out.appendBytes(bytes)
  }

  def writeEBaseStruct(out: Buffer, x: EBaseStruct): Unit = {
    writeEType(out, x)
  }

  def writeStringArray(out: Buffer, x: Array[String]): Unit = {
    val n = x.length
    out.appendInt(n)
    var i = 0
    while (i < n) {
      writeString(out, x(i))
      i += 1
    }
  }

  def writeSortFieldArray(out: Buffer, x: IndexedSeq[SortField]): Unit = {
    out.appendInt(x.length)
    x.foreach { sf =>
      writeString(out, sf.field)
      out.appendByte(sf.sortOrder.serialize)
    }
  }

  def writeByteArray(out: Buffer, x: Array[Byte]): Unit = {
    val n = x.length
    out.appendInt(n)
    out.appendBytes(x)
  }

  class BufferReader(
    private[this] val b: Buffer,
    private[this] var i: Int = 0
  ) {
    def readInt(): Int = {
      val x = b.getInt(i)
      i += 4
      x
    }

    def readByte(): Byte = {
      val x = b.getByte(i)
      i += 1
      x
    }

    def readBytes(length: Int): Array[Byte] = {
      val bytes = b.getBytes(i, i + length)
      i += length
      bytes
    }

    def readString(): String = {
      val length = readInt()
      new String(readBytes(length), utfCharset)
    }

    def readTStruct(): TStruct = {
      IRParser.parseStructType(readString())
    }

    def readEType(): EType = {
      IRParser.parse(readString(), EType.eTypeParser)
    }

    def readEBaseStruct(): EBaseStruct = {
      readEType().asInstanceOf[EBaseStruct]
    }

    def readStringArray(): Array[String] = {
      val n = readInt()
      val a = new Array[String](n)
      var i = 0
      while (i < n) {
        a(i) = readString()
        i += 1
      }
      a
    }

    def readSortFieldArray(): IndexedSeq[SortField] = {
      val n = readInt()
      val a = new Array[SortField](n)
      var i = 0
      while (i < n) {
        val field = readString()
        val sortOrder = SortOrder.deserialize(readByte())
        a(i) = SortField(field, sortOrder)
        i += 1
      }
      a
    }

    def readByteArray(): Array[Byte] = {
      val n = readInt()
      readBytes(n)
    }
  }
}
