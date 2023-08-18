package is.hail.io.fs

import is.hail.io.compress.{BGzipInputStream, BGzipOutputStream}
import is.hail.services._
import is.hail.utils._
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import java.io._
import java.nio.charset._
import java.util.zip.GZIPOutputStream

trait CompressionCodec {
  def makeInputStream(is: InputStream): InputStream

  def makeOutputStream(os: OutputStream): OutputStream
}

object GZipCompressionCodec extends CompressionCodec {
  // java.util.zip.GZIPInputStream does not support concatenated files/multiple blocks
  def makeInputStream(is: InputStream): InputStream = new GzipCompressorInputStream(is, true)

  def makeOutputStream(os: OutputStream): OutputStream = new GZIPOutputStream(os)
}

object BGZipCompressionCodec extends CompressionCodec {
  def makeInputStream(is: InputStream): InputStream = new BGzipInputStream(is)

  def makeOutputStream(os: OutputStream): OutputStream = new BGzipOutputStream(os)
}
