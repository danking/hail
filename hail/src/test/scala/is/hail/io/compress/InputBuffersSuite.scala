package is.hail.io

import java.io._
import java.nio._
import is.hail.io.compress.LZ4
import org.scalatest.testng.TestNGSuite
import org.testng.annotations.Test

class InputBuffersSuitte extends TestNGSuite {
  @Test
  def skipBytesSimpleLZ4() {
    val baos = new ByteArrayOutputStream()
    val out = new BlockingOutputBuffer(
      32 * 1024,
      new LZ4OutputBlockBuffer(
        LZ4.fast,
        32 * 1024,
        new StreamBlockOutputBuffer(baos)))
    (0 until 62500).foreach { x =>
      out.writeDouble(x)
    }
    out.flush()
    val bais = new ByteArrayInputStream(baos.toByteArray())
    val in = new BlockingInputBuffer(
      32 * 1024,
      new LZ4InputBlockBuffer(
        LZ4.fast,
        32 * 1024,
        new StreamBlockInputBuffer(bais)))
    in.skipBytes(32768 * 8)
    val arr = new Array[Double](62500 - 32 * 1024)
    in.readDoubles(arr, 0, 62500 - 32 * 1024)
    assert(arr(0) == 32768.0)
    assert(arr.toSeq == (32768 until 62500).toSeq)
  }

  @Test
  def skipBytesSimpleNoCompression() {
    val baos = new ByteArrayOutputStream()
    val out = new BlockingOutputBuffer(
      32 * 1024,
      new StreamBlockOutputBuffer(baos))
    (0 until 62500).foreach { x =>
      out.writeDouble(x)
    }
    out.flush()
    val bais = new ByteArrayInputStream(baos.toByteArray())
    val in = new BlockingInputBuffer(
      32 * 1024,
      new StreamBlockInputBuffer(bais))
    in.skipBytes(32768 * 8)
    val arr = new Array[Double](62500 - 32 * 1024)
    in.readDoubles(arr, 0, 62500 - 32 * 1024)
    assert(arr(0) == 32768.0)
    assert(arr.toSeq == (32768 until 62500).toSeq)
  }
}
