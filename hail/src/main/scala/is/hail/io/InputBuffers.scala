package is.hail.io

import java.io._
import java.nio._
import java.nio.channels._
import java.nio.file._
import java.util
import java.util.UUID
import java.util.function.Supplier

import is.hail.annotations.{Memory, Region}
import is.hail.io.compress.LZ4
import is.hail.utils._

import com.github.luben.zstd.{Zstd, ZstdDecompressCtx}
import org.apache.commons.codec.binary.Hex

trait InputBuffer extends Closeable {
  def close(): Unit

  def seek(offset: Long): Unit

  def readByte(): Byte

  def read(buf: ByteBuffer, toOff: Int, n: Int)

  def readInt(): Int

  def readLong(): Long

  def readFloat(): Float

  def readDouble(): Double

  def readBytes(toRegion: Region, toOff: Long, n: Int): Unit

  def skipBoolean(): Unit = skipByte()

  def skipByte(): Unit

  def skipInt(): Unit

  def skipLong(): Unit

  def skipFloat(): Unit

  def skipDouble(): Unit

  def skipBytes(n: Int): Unit

  def readDoubles(to: DoubleBuffer, off: Int, n: Int): Unit = ???

  def readDoubles(to: DoubleBuffer): Unit = ???

  def readBoolean(): Boolean = readByte() != 0

  def readUTF(): String
}

trait InputBlockBuffer extends Spec with Closeable {
  def close(): Unit

  def seek(offset: Long)

  def readBlock(buf: ByteBuffer): Int
}

final class MemoryInputBuffer(mb: MemoryBuffer) extends InputBuffer {
  def close() {}

  def seek(offset: Long) = ???

  def readByte(): Byte = mb.readByte()

  def readInt(): Int = mb.readInt()

  def readLong(): Long = mb.readLong()

  def readFloat(): Float = mb.readFloat()

  def readDouble(): Double = mb.readDouble()

  def readBytes(toRegion: Region, toOff: Long, n: Int): Unit = mb.readBytes(toOff, n)

  def readBytesArray(n: Int): Array[Byte] = {
    var arr = new Array[Byte](n)
    mb.readBytesArray(arr, n)
    arr
  }

  def skipByte(): Unit = mb.skipByte()

  def skipInt(): Unit = mb.skipInt()

  def skipLong(): Unit = mb.skipLong()

  def skipFloat(): Unit = mb.skipFloat()

  def skipDouble(): Unit = mb.skipDouble()

  def skipBytes(n: Int): Unit = mb.skipBytes(n)

  def read(buf: ByteBuffer, toOff: Int, n: Int): Unit = ???

  def readUTF(): String = ???
}

final class StreamInputBuffer(private[this] val in: SeekableByteChannel) extends InputBuffer {
  private[this] val BUF_SIZE = 32 * 1024
  private[this] val buf = ByteBuffer.allocateDirect(BUF_SIZE)
  buf.order(ByteOrder.nativeOrder()) // AFAICT: Hail uses little-endian (least-significant first,
                                     // aka backwards), is that intentional or an accident?
  buf.limit(0)

  def close(): Unit = in.close()

  def seek(offset: Long): Unit = {
    in.position(offset)
    buf.limit(buf.position())
  }

  private[this] def require(n: Int): Unit = {
    assert(n < BUF_SIZE/2)
    if (buf.remaining() < n) {
      buf.compact()
      in.read(buf)
      buf.flip()
      assert(buf.remaining() >= n) //FIXME
    }
  }

  def readByte(): Byte = {
    require(1)
    buf.get()
  }

  def read(buf: ByteBuffer, toOff: Int, n: Int) = ???

  def readInt(): Int = {
    require(4)
    buf.getInt()
  }

  def readLong(): Long = {
    require(8)
    buf.getLong()
  }

  def readFloat(): Float = {
    require(4)
    buf.getFloat()
  }

  def readDouble(): Double = {
    require(8)
    buf.getDouble()
  }

  // FIXME: regions need to use buffers.
  def readBytes(toRegion: Region, toOff: Long, n: Int): Unit = ???

  def skipByte(): Unit = {
    require(1)
    buf.position(buf.position() + 1)
  }

  def skipInt(): Unit = {
    require(4)
    buf.position(buf.position() + 4)
  }

  def skipLong(): Unit = {
    require(8)
    buf.position(buf.position() + 8)
  }

  def skipFloat(): Unit = {
    require(4)
    buf.position(buf.position() + 4)
  }

  def skipDouble(): Unit = {
    require(8)
    buf.position(buf.position() + 8)
  }

  def skipBytes(n: Int): Unit = {
    if (n < buf.remaining()) {
      buf.position(buf.position() + n)
    } else {
      val excessSkips = n - buf.remaining()
      in.position(in.position() + excessSkips)
      buf.limit(buf.position())
    }
  }

  def readUTF(): String = ???
}

final class LEB128InputBuffer(in: InputBuffer) extends InputBuffer {
  def close() {
    in.close()
  }

  def seek(offset: Long): Unit = in.seek(offset)

  def readByte(): Byte = {
    in.readByte()
  }

  override def read(buf: ByteBuffer, toOff: Int, n: Int) = ???

  def readInt(): Int = {
    var b: Byte = readByte()
    var x: Int = b & 0x7f
    var shift: Int = 7
    while ((b & 0x80) != 0) {
      b = readByte()
      x |= ((b & 0x7f) << shift)
      shift += 7
    }
    x
  }

  def readLong(): Long = {
    var b: Byte = readByte()
    var x: Long = b & 0x7fL
    var shift: Int = 7
    while ((b & 0x80) != 0) {
      b = readByte()
      x |= ((b & 0x7fL) << shift)
      shift += 7
    }
    x
  }

  def readFloat(): Float = in.readFloat()

  def readDouble(): Double = in.readDouble()

  def readBytes(toRegion: Region, toOff: Long, n: Int): Unit = in.readBytes(toRegion, toOff, n)

  def skipByte(): Unit = in.skipByte()

  def skipInt() {
    var b: Byte = readByte()
    while ((b & 0x80) != 0)
      b = readByte()
  }

  def skipLong() {
    var b: Byte = readByte()
    while ((b & 0x80) != 0)
      b = readByte()
  }

  def skipFloat(): Unit = in.skipFloat()

  def skipDouble(): Unit = in.skipDouble()

  def skipBytes(n: Int): Unit = in.skipBytes(n)

  def readUTF(): String = ???
}

final class BlockingInputBuffer(blockSize: Int, in: InputBlockBuffer) extends InputBuffer {
  private[this] val buf = ByteBuffer.allocateDirect(blockSize * 2) // FIXME: This should really be a funciton of my InputBlockBuffer
  buf.order(ByteOrder.nativeOrder()) // AFAICT: Hail uses little-endian (least-significant first,
                                     // aka backwards), is that intentional or an accident?
  buf.limit(0)

  private[this] def ensure(n: Int) {
    // System.err.println(s"ensure $n ${buf.remaining()}")
    if (buf.remaining() < n) {
      buf.compact()
      // FIXME: do I need a loop here?
      val len = in.readBlock(buf)
      assert(len != -1)
      buf.flip()
      assert(buf.remaining() >= n)
    }
  }

  def close() {
    in.close()
  }

  def seek(offset: Long): Unit = {
    // System.err.println(s"seek $offset ")
    in.seek(offset)
    buf.limit(buf.position())
  }

  def readByte(): Byte = {
    ensure(1)
    val x = buf.get()
    // System.err.println(s"readByte $x")
    x
  }

  def readInt(): Int = {
    ensure(4)
    val x = buf.getInt()
    // System.err.println(s"readInt $x")
    x
  }

  def readLong(): Long = {
    ensure(8)
    val x = buf.getLong()
    // System.err.println(s"readLong $x")
    x
  }

  def readFloat(): Float = {
    ensure(4)
    val x = buf.getFloat()
    // System.err.println(s"readFloat $x")
    x
  }

  def readDouble(): Double = {
    ensure(8)
    val x = buf.getDouble()
    // System.err.println(s"readDouble $x")
    x
  }

  def readBytes(toRegion: Region, toOff0: Long, n0: Int) = {
    val bytes = new Array[Byte](n0)
    ensure(n0)
    buf.get(bytes)
    // System.err.println(s"readBytes $n0 " + Hex.encodeHexString(bytes))
    Region.storeBytes(toOff0, bytes)
  }

  override def read(arr: ByteBuffer, toOff0: Int, n0: Int) = ???

  def skipByte() {
    ensure(1)
    buf.position(buf.position() + 1)
    // System.err.println(s"skipByte")
  }

  def skipInt() {
    ensure(4)
    buf.position(buf.position() + 4)
    // System.err.println(s"skipInt")
  }

  def skipLong() {
    ensure(8)
    buf.position(buf.position() + 8)
    // System.err.println(s"skipLong")
  }

  def skipFloat() {
    ensure(4)
    buf.position(buf.position() + 4)
    // System.err.println(s"skipFloat")
  }

  def skipDouble() {
    ensure(8)
    buf.position(buf.position() + 8)
    // System.err.println(s"skipDouble")
  }

  def skipBytes(n0: Int) {
    var remaining = buf.remaining() - n0
    // System.err.println(s"skipping $n0 while ${buf.remaining()} ${remaining}")
    while (remaining < 0) {
      buf.clear()
      val nRead = in.readBlock(buf)
      assert(nRead != -1)
      buf.flip()
      remaining = nRead + remaining
      // System.err.println(s"skipping $n0, read $nRead $remaining")
    }
    buf.position(buf.limit() - remaining)
  }

  def readUTF(): String = ???
}

final class StreamBlockInputBuffer(in: SeekableByteChannel) extends InputBlockBuffer {
  private[this] val BUF_SIZE = 8 * 1024 * 1024
  private[this] val buf = ByteBuffer.allocateDirect(BUF_SIZE) // FIXME: what *should* be done here
  buf.order(ByteOrder.nativeOrder()) // AFAICT: Hail uses little-endian (least-significant first,
                                     // aka backwards), is that intentional or an accident?
  buf.limit(0)

  private[this] def require(n: Int): Unit = {
    assert(n < BUF_SIZE/2)
    if (buf.remaining() < n) {
      buf.compact()
      in.read(buf)
      buf.flip()
      assert(buf.remaining() >= n) //FIXME
    }
  }

  def close() {
    in.close()
  }

  // this takes a virtual offset and will seek the underlying stream to offset >> 16
  def seek(offset: Long): Unit = in.position(offset >> 16) // FIXME: is this really the correct thing to do?

  def readBlock(dst: ByteBuffer): Int = {
    require(4)
    val blockLen = buf.getInt()
    assert(blockLen > 0)
    require(blockLen)
    val lim = buf.limit()
    buf.limit(buf.position() + blockLen)
    dst.put(buf)
    buf.limit(lim)
    blockLen
  }
}

final class LZ4InputBlockBuffer(lz4: LZ4, blockSize: Int, in: InputBlockBuffer) extends InputBlockBuffer {
  private[this] val comp = ByteBuffer.allocateDirect(8 + lz4.maxCompressedLength(blockSize))
  comp.order(ByteOrder.nativeOrder()) // AFAICT: Hail uses little-endian (least-significant first,
                                      // aka backwards), is that intentional or an accident?
  comp.limit(0)

  def close() {
    in.close()
  }

  def seek(offset: Long): Unit = in.seek(offset)

  def readBlock(buf: ByteBuffer): Int = {
    comp.clear()
    val compLen = in.readBlock(comp) - 4  // 4 bytes used by decompressed length
    assert(compLen != -1)
    comp.flip()
    val decompLen = comp.getInt()
    lz4.decompress(buf, buf.position(), decompLen, comp, comp.position(), compLen)
    decompLen
  }
}

final class LZ4SizeBasedCompressingInputBlockBuffer(lz4: LZ4, blockSize: Int, in: InputBlockBuffer) extends InputBlockBuffer {
  private[this] val comp = ByteBuffer.allocateDirect(8 + lz4.maxCompressedLength(blockSize))
  comp.order(ByteOrder.nativeOrder()) // AFAICT: Hail uses little-endian (least-significant first,
                                      // aka backwards), is that intentional or an accident?
  comp.limit(0)

  def close() {
    in.close()
  }

  def seek(offset: Long): Unit = in.seek(offset)

  def readBlock(buf: ByteBuffer): Int = {
    comp.clear()
    val blockLen = in.readBlock(comp)
    assert(blockLen != -1)
    comp.flip()
    val flag = comp.getInt()

    val decompLen = flag match {
      case 0 =>
        buf.put(comp)
        blockLen - 4
      case 1 =>
        val compLen = blockLen - 8
        val decompLen = comp.getInt()
        lz4.decompress(buf, buf.position(), decompLen, comp, comp.position(), compLen)
        decompLen
      case _ => throw new RuntimeException(s"bad flag: $flag")
    }

    decompLen
  }
}

object ZstdDecompressLib {
  val instance = ThreadLocal.withInitial(new Supplier[ZstdDecompressCtx]() { def get: ZstdDecompressCtx = new ZstdDecompressCtx() })
}

final class ZstdInputBlockBuffer(blockSize: Int, in: InputBlockBuffer) extends InputBlockBuffer {
  private[this] val zstd = ZstdDecompressLib.instance.get
  private[this] val comp = ByteBuffer.allocateDirect(4 + Zstd.compressBound(blockSize).toInt)
  comp.order(ByteOrder.nativeOrder()) // AFAICT: Hail uses little-endian (least-significant first,
                                      // aka backwards), is that intentional or an accident?
  comp.limit(0)

  def close(): Unit = {
    in.close()
  }

  def seek(offset: Long): Unit = in.seek(offset)

  def readBlock(buf: ByteBuffer): Int = {
    comp.clear()
    val blockLen = in.readBlock(comp)
    assert(blockLen != -1)
    comp.flip()
    // System.err.println(s"comp ${comp.position()} ${comp.limit()} $blockLen buf: ${buf.position()} ${buf.limit()} ${buf.capacity()}")
    val compLen = blockLen - 4
    val decompLen = comp.getInt()
    // System.err.println(s"decompLen $decompLen")

    zstd.decompress(buf, comp)

    decompLen
  }
}

final class ZstdSizedBasedInputBlockBuffer(blockSize: Int, in: InputBlockBuffer) extends InputBlockBuffer {
  private[this] val zstd = ZstdDecompressLib.instance.get
  private[this] val comp = ByteBuffer.allocateDirect(4 + Zstd.compressBound(blockSize).toInt)
  comp.order(ByteOrder.nativeOrder()) // AFAICT: Hail uses little-endian (least-significant first,
                                      // aka backwards), is that intentional or an accident?
  comp.limit(0)

  def close(): Unit = {
    in.close()
  }

  def seek(offset: Long): Unit = in.seek(offset)

  def readBlock(buf: ByteBuffer): Int = {
    comp.clear()
    val blockLen = in.readBlock(comp)
    // System.err.println(s"comp ${comp.position()} ${comp.limit()} $blockLen $in")
    assert(blockLen != -1)
    comp.flip()
    // System.err.println(s"comp ${comp.position()} ${comp.limit()} $blockLen buf: ${buf.position()} ${buf.limit()} ${buf.capacity()}")
    val compLen = blockLen - 4
    val decomp = comp.getInt()

    val decompLen = if (decomp % 2 == 0) {
      buf.put(comp)
      compLen
    } else {
      val decompLen = decomp >>> 1
      // System.err.println(s"decompLen $decompLen")
      zstd.decompress(buf, comp)
      decompLen
    }

    decompLen
  }
}
