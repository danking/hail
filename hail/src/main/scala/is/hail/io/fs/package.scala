package is.hail.io

import java.io.{DataInputStream, DataOutputStream, InputStream, OutputStream}

package object fs {
  type PositionedInputStream = InputStream with Positioned

  type SeekableInputStream = InputStream with Seekable

  type SeekableDataInputStream = DataInputStream with Seekable

  type PositionedOutputStream = OutputStream with Positioned

  type PositionedDataOutputStream = DataOutputStream with Positioned

  def outputStreamToPositionedDataOutputStream(os: OutputStream): PositionedDataOutputStream =
    new WrappedPositionedDataOutputStream(
      new WrappedPositionOutputStream(
        os))

  def dropTrailingSlash(path: String): String = {
    if (path.isEmpty)
      return path

    if (path.last != '/')
      return path

    var i = path.length - 1
    while (i > 0 && path(i - 1) == '/')
      i -= 1
    path.substring(0, i)
  }

  def getCodecFromPath(path: String, gzAsBGZ: Boolean = false): CompressionCodec =
    getCodecFromExtension(getExtension(path), gzAsBGZ)

  def getCodecExtension(path: String): String = {
    val ext = getExtension(path)
    if (ext == ".gz" || ext == ".bgz" || ext == ".tbi")
      ext
    else
      ""
  }

  private[this] def getCodecFromExtension(extension: String, gzAsBGZ: Boolean = false): CompressionCodec = {
    extension match {
      case ".gz" =>
        if (gzAsBGZ)
          BGZipCompressionCodec
        else
          GZipCompressionCodec
      case ".bgz" =>
        BGZipCompressionCodec
      case ".tbi" =>
        BGZipCompressionCodec
      case _ =>
        null
    }
  }

  private[this] def getExtension(path: String): String = {
    var i = path.length - 1
    while (i >= 0) {
      if (i == 0)
        return ""

      val c = path(i)
      if (c == '.') {
        if (path(i - 1) == '/')
          return ""
        else
          return path.substring(i)
      }
      if (c == '/')
        return ""
      i -= 1
    }

    throw new AssertionError("unreachable")
  }
}
