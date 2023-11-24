package is.hail.io

import is.hail.compatibility.LZ4BlockBufferSpec
import is.hail.rvd.AbstractRVDSpec
import is.hail.utils._
import java.io._
import java.nio._
import java.nio.channels._

import is.hail.asm4s._
import is.hail.io.compress.LZ4
import org.json4s.{ DefaultFormats, Formats, ShortTypeHints }

import org.json4s.jackson.JsonMethods
import org.json4s.{Extraction, JValue}
import org.apache.commons.codec.binary.Hex

object BufferSpec {
  val zstdCompressionLEB: BufferSpec = LEB128BufferSpec(
    BlockingBufferSpec(64 * 1024,
      ZstdBlockBufferSpec(64 * 1024,
        new StreamBlockBufferSpec)))

  val default: BufferSpec = zstdCompressionLEB

  val blockedUncompressed: BufferSpec = BlockingBufferSpec(32 * 1024,
    new StreamBlockBufferSpec)
  val unblockedUncompressed: BufferSpec = new StreamBufferSpec

  val wireSpec: BufferSpec = LEB128BufferSpec(
    BlockingBufferSpec(64 * 1024,
      ZstdSizedBasedBlockBufferSpec(64 * 1024,
        /*minCompressionSize=*/256,
        new StreamBlockBufferSpec)))
  val memorySpec: BufferSpec = wireSpec

  // longtime default spec
  val lz4HCCompressionLEB: BufferSpec = LEB128BufferSpec(
    BlockingBufferSpec(32 * 1024,
      LZ4HCBlockBufferSpec(32 * 1024,
        new StreamBlockBufferSpec)))

  val blockSpecs: Array[BufferSpec] = Array(
    BlockingBufferSpec(64 * 1024,
      new StreamBlockBufferSpec),
    BlockingBufferSpec(32 * 1024,
      LZ4HCBlockBufferSpec(32 * 1024,
        new StreamBlockBufferSpec)),
    BlockingBufferSpec(32 * 1024,
      LZ4FastBlockBufferSpec(32 * 1024,
        new StreamBlockBufferSpec)),
    BlockingBufferSpec(64 * 1024,
      ZstdBlockBufferSpec(64 * 1024,
        new StreamBlockBufferSpec)),
    new StreamBufferSpec)

  val specs: Array[BufferSpec] = blockSpecs.flatMap { blockSpec =>
    Array(blockSpec, LEB128BufferSpec(blockSpec))
  }

  def parse(s: String): BufferSpec = {
    import AbstractRVDSpec.formats
    JsonMethods.parse(s).extract[BufferSpec]
  }

  def parseOrDefault(
    s: String,
    default: BufferSpec = BufferSpec.default
  ): BufferSpec = if (s == null) default else parse(s)

  val shortTypeHints = ShortTypeHints(List(
      classOf[BlockBufferSpec],
      classOf[LZ4BlockBufferSpec],
      classOf[LZ4HCBlockBufferSpec],
      classOf[LZ4FastBlockBufferSpec],
      classOf[LZ4SizeBasedBlockBufferSpec],
      classOf[ZstdBlockBufferSpec],
      classOf[StreamBlockBufferSpec],
      classOf[BufferSpec],
      classOf[LEB128BufferSpec],
      classOf[BlockingBufferSpec],
      classOf[StreamBufferSpec]
    ), typeHintFieldName = "name")
}

trait BufferSpec extends Spec {
  def buildInputBuffer(in: SeekableByteChannel): InputBuffer

  def buildInputBuffer(in: InputStream): InputBuffer =
    buildInputBuffer(new FakeSeekableByteChannelInputStream(in))

  def buildOutputBuffer(out: OutputStream): OutputBuffer

  def buildCodeInputBuffer(in: Code[InputStream]): Code[InputBuffer] =
    buildCodeInputBufferNio(Code.newInstance[FakeSeekableByteChannelInputStream, InputStream](in))

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBuffer]

  def buildCodeOutputBuffer(in: Code[OutputStream]): Code[OutputBuffer]
}

final case class LEB128BufferSpec(child: BufferSpec) extends BufferSpec {
  def buildInputBuffer(in: SeekableByteChannel): InputBuffer = new LEB128InputBuffer(child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBuffer = new LEB128OutputBuffer(child.buildOutputBuffer(out))

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBuffer] =
    Code.newInstance[LEB128InputBuffer, InputBuffer](child.buildCodeInputBufferNio(in))

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBuffer] =
    Code.newInstance[LEB128OutputBuffer, OutputBuffer](child.buildCodeOutputBuffer(out))
}

final case class BlockingBufferSpec(blockSize: Int, child: BlockBufferSpec) extends BufferSpec {
  require(blockSize <= (1 << 16))

  def buildInputBuffer(in: SeekableByteChannel): InputBuffer = new BlockingInputBuffer(blockSize, child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBuffer = new BlockingOutputBuffer(blockSize, child.buildOutputBuffer(out))

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBuffer] =
    Code.newInstance[BlockingInputBuffer, Int, InputBlockBuffer](blockSize, child.buildCodeInputBufferNio(in))

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBuffer] =
    Code.newInstance[BlockingOutputBuffer, Int, OutputBlockBuffer](blockSize, child.buildCodeOutputBuffer(out))
}

trait BlockBufferSpec extends Spec {
  def buildInputBuffer(in: SeekableByteChannel): InputBlockBuffer

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBlockBuffer]

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBlockBuffer]
}

abstract class LZ4BlockBufferSpecCommon extends BlockBufferSpec {
  require(blockSize <= (1 << 16))

  def typeName: String

  def lz4: LZ4

  def stagedlz4: Code[LZ4]

  def blockSize: Int

  def child: BlockBufferSpec

  def buildInputBuffer(in: SeekableByteChannel): InputBlockBuffer = new LZ4InputBlockBuffer(lz4, blockSize, child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer = new LZ4OutputBlockBuffer(lz4, blockSize, child.buildOutputBuffer(out))

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBlockBuffer] =
    Code.newInstance[LZ4InputBlockBuffer, LZ4, Int, InputBlockBuffer](stagedlz4, blockSize, child.buildCodeInputBufferNio(in))

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBlockBuffer] =
    Code.newInstance[LZ4OutputBlockBuffer, LZ4, Int, OutputBlockBuffer](stagedlz4, blockSize, child.buildCodeOutputBuffer(out))
}

final case class LZ4HCBlockBufferSpec(blockSize: Int, child: BlockBufferSpec)
    extends LZ4BlockBufferSpecCommon {
  def lz4 = LZ4.hc
  def stagedlz4: Code[LZ4] = Code.invokeScalaObject0[LZ4](LZ4.getClass, "hc")
  def typeName = "LZ4HCBlockBufferSpec"
}

final case class LZ4FastBlockBufferSpec(blockSize: Int, child: BlockBufferSpec)
    extends LZ4BlockBufferSpecCommon {
  def lz4 = LZ4.fast
  def stagedlz4: Code[LZ4] = Code.invokeScalaObject0[LZ4](LZ4.getClass, "fast")
  def typeName = "LZ4FastBlockBufferSpec"
}

final case class LZ4SizeBasedBlockBufferSpec(compressorType: String, blockSize: Int, minCompressionSize: Int, child: BlockBufferSpec)
    extends BlockBufferSpec {
  def lz4: LZ4 = compressorType match {
    case "hc" => LZ4.hc
    case "fast" => LZ4.fast
  }

  def stagedlz4: Code[LZ4] = Code.invokeScalaObject0[LZ4](LZ4.getClass, "fast")
  def typeName = "LZ4SizeBasedBlockBufferSpec"

  def buildInputBuffer(in: SeekableByteChannel): InputBlockBuffer = new LZ4SizeBasedCompressingInputBlockBuffer(lz4, blockSize, child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer = new LZ4SizeBasedCompressingOutputBlockBuffer(lz4, blockSize, minCompressionSize, child.buildOutputBuffer(out))

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBlockBuffer] =
    Code.newInstance[LZ4SizeBasedCompressingInputBlockBuffer, LZ4, Int, InputBlockBuffer](stagedlz4, blockSize, child.buildCodeInputBufferNio(in))

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBlockBuffer] =
    Code.newInstance[LZ4SizeBasedCompressingOutputBlockBuffer, LZ4, Int, Int, OutputBlockBuffer](stagedlz4, blockSize, minCompressionSize, child.buildCodeOutputBuffer(out))
}

final case class ZstdBlockBufferSpec(blockSize: Int, child: BlockBufferSpec) extends BlockBufferSpec {
  require(blockSize <= (1 << 16))

  def buildInputBuffer(in: SeekableByteChannel): InputBlockBuffer = new ZstdInputBlockBuffer(blockSize, child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer = new ZstdOutputBlockBuffer(blockSize, child.buildOutputBuffer(out))

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBlockBuffer] =
    Code.newInstance[ZstdInputBlockBuffer, Int, InputBlockBuffer](blockSize, child.buildCodeInputBufferNio(in))

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBlockBuffer] =
    Code.newInstance[ZstdOutputBlockBuffer, Int, OutputBlockBuffer](blockSize, child.buildCodeOutputBuffer(out))
}

final case class ZstdSizedBasedBlockBufferSpec(blockSize: Int, minCompressionSize: Int, child: BlockBufferSpec) extends BlockBufferSpec {
  require(blockSize <= (1 << 16))

  def buildInputBuffer(in: SeekableByteChannel): InputBlockBuffer = new ZstdSizedBasedInputBlockBuffer(blockSize, child.buildInputBuffer(in))

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer = new ZstdSizedBasedOutputBlockBuffer(blockSize, minCompressionSize, child.buildOutputBuffer(out))

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBlockBuffer] =
    Code.newInstance[ZstdSizedBasedInputBlockBuffer, Int, InputBlockBuffer](blockSize, child.buildCodeInputBufferNio(in))

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBlockBuffer] =
    Code.newInstance[ZstdSizedBasedOutputBlockBuffer, Int, Int, OutputBlockBuffer](blockSize, minCompressionSize, child.buildCodeOutputBuffer(out))
}

object StreamBlockBufferSpec {
  def extract(jv: JValue): StreamBlockBufferSpec = new StreamBlockBufferSpec
}

final class StreamBlockBufferSpec extends BlockBufferSpec {
  def buildInputBuffer(in: SeekableByteChannel): InputBlockBuffer = new StreamBlockInputBuffer(in)

  def buildOutputBuffer(out: OutputStream): OutputBlockBuffer = new StreamBlockOutputBuffer(out)

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBlockBuffer] =
    Code.newInstance[StreamBlockInputBuffer, SeekableByteChannel](in)

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBlockBuffer] =
    Code.newInstance[StreamBlockOutputBuffer, OutputStream](out)

  override def equals(other: Any): Boolean = other.isInstanceOf[StreamBlockBufferSpec]
}

final class StreamBufferSpec extends BufferSpec {
  override def buildInputBuffer(in: SeekableByteChannel): InputBuffer = new StreamInputBuffer(in)

  override def buildOutputBuffer(out: OutputStream): OutputBuffer = new StreamOutputBuffer(out)

  def buildCodeInputBufferNio(in: Code[SeekableByteChannel]): Code[InputBuffer] =
    Code.newInstance[StreamInputBuffer, SeekableByteChannel](in)

  def buildCodeOutputBuffer(out: Code[OutputStream]): Code[OutputBuffer] =
    Code.newInstance[StreamOutputBuffer, OutputStream](out)

  override def equals(other: Any): Boolean = other.isInstanceOf[StreamBufferSpec]
}

class FakeSeekableByteChannelInputStream(
  private[this] val in: InputStream
) extends SeekableByteChannel {
  private[this] val ch = Channels.newChannel(in)

  def read(dst: ByteBuffer): Int = {
    val pos = dst.position()
    val n = ch.read(dst)
    dst.position(pos)
    val bytes = new Array[Byte](n)
    dst.get(bytes)
    // System.err.println(s"Wrapped inputstream read $n " + Hex.encodeHexString(bytes))
    dst.position(pos + n)
    n
  }

  def write(src: ByteBuffer): Int = throw new NonWritableChannelException()

  def position: Long = ???

  def position(newPosition: Long): SeekableByteChannel = ???

  def size(): Long = ???

  def truncate(size: Long): SeekableByteChannel = throw new NonWritableChannelException()

  def close() = ch.close()

  def isOpen() = ch.isOpen()
}
