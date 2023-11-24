package is.hail

import java.io.OutputStreamWriter
import java.nio._
import java.nio.channels._
import java.nio.charset._

import is.hail.asm4s._
import is.hail.types.virtual.Type
import is.hail.utils._
import is.hail.io.fs.FS

package object io {
  type VCFFieldAttributes = Map[String, String]
  type VCFAttributes = Map[String, VCFFieldAttributes]
  type VCFMetadata = Map[String, VCFAttributes]

  val utfCharset = Charset.forName("UTF-8")

  def exportTypes(filename: String, fs: FS, info: Array[(String, Type)]) {
    val sb = new StringBuilder
    using(new OutputStreamWriter(fs.create(filename))) { out =>
      info.foreachBetween { case (name, t) =>
        sb.append(prettyIdentifier(name))
        sb.append(":")
        t.pretty(sb, 0, compact = true)
      } { sb += ',' }

      out.write(sb.result())
    }
  }

  def readExactly(n: Int, buf: ByteBuffer, in: ReadableByteChannel): Unit = {
    assert(buf.remaining() == n)
    var nRead = in.read(buf)
    if (nRead == -1) {
      throw new RuntimeException(s"unexpected end of block $nRead $n")
    }
    while (nRead != n) {
      val next = in.read(buf)
      if (next == -1) {
        throw new RuntimeException(s"unexpected end of block $nRead $n")
      }
      nRead += next
    }
  }
}
