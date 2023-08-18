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
}
