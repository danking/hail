trait Positioned {
  def getPosition: Long
}

trait Seekable extends Positioned {
  def seek(pos: Long): Unit
}

class WrappedSeekableDataInputStream(is: SeekableInputStream) extends DataInputStream(is) with Seekable {
  def getPosition: Long = is.getPosition

  def seek(pos: Long): Unit = is.seek(pos)
}

class WrappedPositionedDataOutputStream(os: PositionedOutputStream) extends DataOutputStream(os) with Positioned {
  def getPosition: Long = os.getPosition
}

class WrappedPositionOutputStream(os: OutputStream) extends OutputStream with Positioned {
  private[this] var count: Long = 0L

  override def flush(): Unit = os.flush()

  override def write(i: Int): Unit = {
    os.write(i)
    count += 1
  }

  override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    os.write(bytes, off, len)
  }

  override def close(): Unit = {
    os.close()
  }

  def getPosition: Long = count
}

abstract class FSSeekableInputStream extends InputStream with Seekable {
  protected[this] var closed: Boolean = false
  private[this] var pos: Long = 0
  private[this] var eof: Boolean = false

  protected[this] val bb: ByteBuffer = ByteBuffer.allocate(8 * 1024 * 1024)
  bb.limit(0)

  def fill(): Int

  override def read(): Int = {
    if (eof)
      return -1

    if (bb.remaining() == 0) {
      val nRead = fill()
      if (nRead == -1) {
        eof = true
        return -1
      }
    }

    pos += 1
    bb.get().toInt & 0xff
  }

  override def read(bytes: Array[Byte], off: Int, len: Int): Int = {
    if (eof)
      return -1

    if (bb.remaining() == 0) {
      val nRead = fill()
      if (nRead == -1) {
        eof = true
        return -1
      }
    }

    val toTransfer = math.min(len, bb.remaining())
    bb.get(bytes, off, toTransfer)
    pos += toTransfer
    toTransfer
  }

  protected def physicalSeek(newPos: Long): Unit

  def seek(newPos: Long): Unit = {
    eof = false
    val distance = newPos - pos
    val bufferSeekPosition = bb.position() + distance
    if (bufferSeekPosition >= 0 && bufferSeekPosition < bb.limit()) {
      assert(bufferSeekPosition <= Int.MaxValue)
      bb.position(bufferSeekPosition.toInt)
    } else {
      bb.clear()
      bb.limit(0)
      if (bb.remaining() != 0) {
        assert(false, bb.remaining().toString())
      }
      physicalSeek(newPos)
    }
    pos = newPos
  }

  def getPosition: Long = pos
}

abstract class FSPositionedOutputStream(val capacity: Int) extends OutputStream with Positioned {
  protected[this] var closed: Boolean = false
  protected[this] val bb: ByteBuffer = ByteBuffer.allocate(capacity)
  protected[this] var pos: Long = 0

   def flush(): Unit

   def write(i: Int): Unit = {
    if (bb.remaining() == 0)
      flush()
    bb.put(i.toByte)
    pos += 1
  }

   override def write(bytes: Array[Byte], off: Int, len: Int): Unit = {
    var i = off
    var remaining = len
    while (remaining > 0) {
      if (bb.remaining() == 0)
        flush()
      val toTransfer = math.min(bb.remaining(), remaining)
      bb.put(bytes, i, toTransfer)
      i += toTransfer
      remaining -= toTransfer
      pos += toTransfer
    }
  }

  def getPosition: Long = pos
}
