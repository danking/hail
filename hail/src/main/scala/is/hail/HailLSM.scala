package is.hail

import is.hail.annotations.Region
import is.hail.utils._
import is.hail.expr.types.physical.PType
import is.hail.io.TypedCodecSpec
import java.io.{ DataInput, DataOutput, EOFException, File, IOException, InputStream, OutputStream }
import com.indeed.lsmtree.core._
import com.indeed.util.serialization._
import com.indeed.util.serialization.array._
import java.util.Comparator

class DataOutputIsOutputStream(
  var out: DataOutput
) extends OutputStream {
  override def close() { }
  override def flush() { }
  override def write(b: Array[Byte]) {
    out.write(b)
  }
  override def write(b: Array[Byte], off: Int, len: Int) {
    out.write(b, off, len)
  }
  override def write(b: Int) {
    out.write(b)
  }
}

class DataInputIsInputStream(
  var in: DataInput
) extends InputStream {
  override def available() = 0
  override def close() { }
  override def mark(readlimit: Int) { }
  override def markSupported() = false
  override def read(): Int = {
    in.readByte()
  }
  override def read(b: Array[Byte]): Int = {
    try {
      in.readFully(b)
      b.length
    } catch {
      case e: EOFException =>
        fatal("""this is probably a bug in the implementation of this class.
                |a consumer asked to fill an array of bytes but we didn't
                |have enough bytes to fill it. that should be OK, but DataInput
                |makes this difficult to do efficiently.""".stripMargin, e)
    }
  }
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    try {
      in.readFully(b, off, len)
      b.length
    } catch {
      case e: EOFException =>
        fatal("""this is probably a bug in the implementation of this class.
                |a consumer asked to fill an array of bytes but we didn't
                |have enough bytes to fill it. that should be OK, but DataInput
                |makes this difficult to do efficiently.""".stripMargin, e)
    }
  }
  override def reset() = throw new IOException("unsupported")
  override def skip(n: Long) = {
    assert(n >= 0)
    assert(n <= (1 << 31) - 1)
    in.skipBytes(n.asInstanceOf[Int])
  }
}

class RegionValueSerializer(
  t: PType,
  codecSpec: TypedCodecSpec,
  var region: Region
) extends Serializer[Long] {
  private[this] val outShim = new DataOutputIsOutputStream(null)
  private[this] val inShim = new DataInputIsInputStream(null)
  private[this] val enc = codecSpec.buildEncoder(t)(outShim)
  private[this] val dec = codecSpec.buildDecoder(codecSpec.encodedVirtualType)._2(inShim)

  def write(l: Long, out: DataOutput): Unit = {
    if (outShim.out != null)
      enc.flush()
    outShim.out = out
    enc.writeRegionValue(null, l)
  }

  def read(in: DataInput): Long = {
    inShim.in = in
    dec.readRegionValue(region)
  }
}

class HailLSM (
  path: String,
  key: PType,
  keyCodecSpec: TypedCodecSpec,
  value: PType,
  valueCodecSpec: TypedCodecSpec,
  region: Region
) extends AutoCloseable {
  val ord = value.unsafeOrdering
  val store = new StoreBuilder[Long, Long](new File(path),
    new RegionValueSerializer(key, keyCodecSpec, region),
    new RegionValueSerializer(value, valueCodecSpec, region)
  ).setComparator(value.unsafeOrdering).build()

  def close() = region.close()
}
