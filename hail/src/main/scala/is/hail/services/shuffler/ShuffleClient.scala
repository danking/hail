package is.hail.services.shuffler

import java.io.IOException
import java.net.Socket
import java.util.UUID
import java.util.function.Supplier

import is.hail._
import is.hail.annotations._
import is.hail.asm4s._
import is.hail.expr.ir._
import is.hail.types.virtual._
import is.hail.types.physical._
import is.hail.services._
import is.hail.services.tls._
import is.hail.io._
import is.hail.utils._
import is.hail.utils.richUtils.UnexpectedEndOfFileHailException
import javax.net.ssl._
import org.apache.log4j.Logger

import scala.collection.mutable

object ShuffleClient {
  private val clients = ThreadLocal.withInitial(new Supplier[mutable.Map[IndexedSeq[Byte], ShuffleClient]]() {
    def get() = mutable.Map[IndexedSeq[Byte], ShuffleClient]()
  })

  def socket(): (UUID, Socket) = tcp.openConnection("shuffler", 443)

  def codeSocket(): Code[Socket] =
    Code.invokeScalaObject0[Socket](ShuffleClient.getClass, "socket")

  def getOrCreate(
    shuffleType: TShuffle,
    uuid: Array[Byte],
    rowEncodingPType: PType,
    keyEncodingPType: PType
  ): ShuffleClient = {
    clients.get().get(uuid) match {
      case None =>
        val client = new ShuffleClient(shuffleType, uuid, rowEncodingPType, keyEncodingPType)
        clients.get().put(uuid, client)
        client
      case Some(client) =>
        assert(client.shuffleType == shuffleType)
        client
    }
  }
}

object CodeShuffleClient {
  def createValue(cb: CodeBuilderLike, shuffleType: Code[TShuffle]): ValueShuffleClient =
    new ValueShuffleClient(
      cb.newField[ShuffleClient]("shuffleClient", create(shuffleType)))

  def getOrCreateValue(cb: CodeBuilderLike, shuffleType: Code[TShuffle], uuid: Code[Array[Byte]]): ValueShuffleClient =
    new ValueShuffleClient(
      cb.newField[ShuffleClient]("shuffleClient", getOrCreate(shuffleType, uuid)))

  def getOrCreateValue(
    cb: CodeBuilderLike,
    shuffleType: Code[TShuffle],
    uuid: Code[Array[Byte]],
    rowEncodingPType: Code[PType],
    keyEncodingPType: Code[PType]
  ): ValueShuffleClient =
    new ValueShuffleClient(
      cb.newField[ShuffleClient](
        "shuffleClient",
        getOrCreate(shuffleType, uuid, rowEncodingPType, keyEncodingPType)))

  def create(shuffleType: Code[TShuffle]): Code[ShuffleClient] =
    Code.newInstance[ShuffleClient, TShuffle](shuffleType)

  def getOrCreate(shuffleType: Code[TShuffle], uuid: Code[Array[Byte]]): Code[ShuffleClient] =
    Code.invokeScalaObject4[TShuffle, Array[Byte], PType, PType, ShuffleClient](
      ShuffleClient.getClass,
      "getOrCreate",
      shuffleType, uuid, Code._null, Code._null)

  def getOrCreate(
    shuffleType: Code[TShuffle],
    uuid: Code[Array[Byte]],
    rowEncodingPType: Code[PType],
    keyEncodingPType: Code[PType]
  ): Code[ShuffleClient] =
    Code.invokeScalaObject4[TShuffle, Array[Byte], PType, PType, ShuffleClient](
      ShuffleClient.getClass,
      "getOrCreate",
      shuffleType, uuid, rowEncodingPType, keyEncodingPType)
}

class ValueShuffleClient(
  val code: Value[ShuffleClient]
) extends AnyVal {
  def start(): Code[Unit] =
    code.invoke[Unit]("start")

  def startPut(): Code[Unit] =
    code.invoke[Unit]("startPut")

  def putValue(value: Code[Long]): Code[Unit] =
    code.invoke[Long, Unit]("putValue", value)

  def putValueDone(): Code[Unit] =
    code.invoke[Unit]("putValueDone")

  def endPut(): Code[Unit] =
    code.invoke[Unit]("endPut")

  def uuid(): Code[Array[Byte]] =
    code.invoke[Array[Byte]]("uuid")

  def startGet(
    start: Code[Long],
    startInclusive: Code[Boolean],
    end: Code[Long],
    endInclusive: Code[Boolean]
  ): Code[Unit] =
    code.invoke[Long, Boolean, Long, Boolean, Unit](
      "startGet", start, startInclusive, end, endInclusive)

  def getValue(region: Code[Region]): Code[Long] =
    code.invoke[Region, Long]("getValue", region)

  def getValueFinished(): Code[Boolean] =
    code.invoke[Boolean]("getValueFinished")

  def getDone(): Code[Unit] =
    code.invoke[Unit]("getDone")

  def startPartitionBounds(nPartitions: Code[Int]): Code[Unit] =
    code.invoke[Int, Unit]("startPartitionBounds", nPartitions)

  def partitionBoundsValue(region: Code[Region]): Code[Long] =
    code.invoke[Region, Long]("partitionBoundsValue", region)

  def partitionBoundsValueFinished(): Code[Boolean] =
    code.invoke[Boolean]("partitionBoundsValueFinished")

  def endPartitionBounds(): Code[Unit] =
    code.invoke[Unit]("endPartitionBounds")

  def stop(): Code[Unit] =
    code.invoke[Unit]("stop")

  def close(): Code[Unit] =
    code.invoke[Unit]("close")
}

class ShuffleClient (
  val shuffleType: TShuffle,
  var uuid: Array[Byte],
  rowEncodingPType: Option[PType],
  keyEncodingPType: Option[PType]
) extends AutoCloseable {
  private[this] val log = Logger.getLogger(getClass.getName())

  def this(shuffleType: TShuffle) = this(shuffleType, null, None, None)

  def this(shuffleType: TShuffle, rowEncodingPType: PType, keyEncodingPType: PType) =
    this(shuffleType, null, Option(rowEncodingPType), Option(keyEncodingPType))

  def this(shuffleType: TShuffle, uuid: Array[Byte], rowEncodingPType: PType, keyEncodingPType: PType) =
    this(shuffleType, uuid, Option(rowEncodingPType), Option(keyEncodingPType))

  def this(shuffleType: TShuffle, uuid: Array[Byte]) =
    this(shuffleType, uuid, None, None)

  val codecs = {
    ExecutionTimer.logTime("ShuffleClient.codecs") { timer =>
      using(new ExecuteContext("/tmp", "file:///tmp", null, null, Region(), timer)) { ctx =>
        new ShuffleCodecSpec(ctx, shuffleType, rowEncodingPType, keyEncodingPType)
      }
    }
  }

  private[this] val idAndSocket = ShuffleClient.socket()
  private[this] var connectionId: UUID = idAndSocket._1
  private[this] var s: Socket = idAndSocket._2
  def log_info(msg: String): Unit = {
    log.info(s"${connectionId}: ${msg}")
  }
  log_info("opened a socket")
  private[this] var in = shuffleBufferSpec.buildInputBuffer(s.getInputStream())
  private[this] var out = shuffleBufferSpec.buildOutputBuffer(s.getOutputStream())

  private[this] def retry[T](block: => T): T = {
    while(true) {
      try {
        return block
      } catch {
        case exc @ (_: IOException | _: UnexpectedEndOfFileHailException) =>
          log_info(s"connection lost due to ${exc}, reconnecting")
          val (_connectionId, _s) = ShuffleClient.socket()
          connectionId = _connectionId
          s = _s
          in = shuffleBufferSpec.buildInputBuffer(s.getInputStream)
          out = shuffleBufferSpec.buildOutputBuffer(s.getOutputStream)
      }
    }
    throw new RuntimeException("unreachable")  // scala cannot infer this is unreachable
  }

  private[this] def startOperation(op: Byte) = {
    assert(op != Wire.EOS)
    out.writeByte(op)
    if (op != Wire.START) {
      assert(uuid != null)
      log_info(s"operation $op uuid ${uuidToString(uuid)}")
      Wire.writeByteArray(out, uuid)
    }
  }

  // FIXME: cannot retry start b/c garbage
  def start(): Unit = {
    startOperation(Wire.START)
    log_info(s"start")
    Wire.writeTStruct(out, shuffleType.rowType)
    Wire.writeEBaseStruct(out, shuffleType.rowEType)
    Wire.writeSortFieldArray(out, shuffleType.keyFields)
    log_info(s"using ${shuffleType.keyEType}")
    Wire.writeEBaseStruct(out, shuffleType.keyEType)
    out.flush()
    uuid = Wire.readByteArray(in)
    assert(uuid.length == Wire.ID_SIZE, s"${uuid.length} ${Wire.ID_SIZE}")
    log_info(s"start done")
  }

  private[this] var encoder: Encoder = null

  def startPut(): Unit = {
    log_info(s"put")
    startOperation(Wire.PUT)
    out.flush()
    encoder = codecs.makeRowEncoder(out)
  }

  // FIXME: PUT is not idempotent
  def put(values: Array[Long]): Unit = {
    startPut()
    writeRegionValueArray(encoder, values)
    endPut()
  }

  def putValue(value: Long): Unit = {
    encoder.writeByte(1)
    encoder.writeRegionValue(value)
  }

  def putValueDone(): Unit = {
    encoder.writeByte(0)
  }

  def endPut(): Unit = {
    // fixme: server needs to send uuid for the successful partition
    encoder.flush()
    assert(in.readByte() == 0.toByte)
    log_info(s"put done")
  }

  private[this] var decoder: Decoder = null

  def startGet(
    start: Long,
    startInclusive: Boolean,
    end: Long,
    endInclusive: Boolean
  ): Unit = {
    log_info(s"get ${Region.pretty(codecs.keyEncodingPType, start)} ${startInclusive} " +
      s"${Region.pretty(codecs.keyEncodingPType, end)} ${endInclusive}")
    val keyEncoder = codecs.makeKeyEncoder(out)
    decoder = codecs.makeRowDecoder(in)
    startOperation(Wire.GET)
    keyEncoder.writeRegionValue(start)
    keyEncoder.writeByte(if (startInclusive) 1.toByte else 0.toByte)
    keyEncoder.writeRegionValue(end)
    keyEncoder.writeByte(if (endInclusive) 1.toByte else 0.toByte)
    keyEncoder.flush()
    log_info(s"get receiving values")
  }

  def get(
    region: Region,
    start: Long,
    startInclusive: Boolean,
    end: Long,
    endInclusive: Boolean
  ): Array[Long] = retry {
    startGet(start, startInclusive, end, endInclusive)
    val values = readRegionValueArray(region, decoder)
    getDone()
    values
  }

  def getValue(region: Region): Long = {
    decoder.readRegionValue(region)
  }

  def getValueFinished(): Boolean = {
    decoder.readByte() == 0.toByte
  }

  def getDone(): Unit = {
    log_info(s"get done")
  }

  private[this] var keyDecoder: Decoder = null

  def startPartitionBounds(
    nPartitions: Int
  ): Unit = {
    log_info(s"partitionBounds")
    startOperation(Wire.PARTITION_BOUNDS)
    out.writeInt(nPartitions)
    out.flush()
    log_info(s"partitionBounds receiving values")
    keyDecoder = codecs.makeKeyDecoder(in)
  }

  def partitionBounds(region: Region, nPartitions: Int): Array[Long] = retry {
    startPartitionBounds(nPartitions)
    val keys = readRegionValueArray(region, keyDecoder, nPartitions + 1)
    endPartitionBounds()
    keys
  }

  def partitionBoundsValue(region: Region): Long = {
    keyDecoder.readRegionValue(region)
  }

  def partitionBoundsValueFinished(): Boolean = {
    keyDecoder.readByte() == 0.toByte
  }

  def endPartitionBounds(): Unit = {
    log_info(s"partitionBounds done")
  }

  def stop(): Unit = retry {
    startOperation(Wire.STOP)
    out.flush()
    log_info(s"stop")
    assert(in.readByte() == 0.toByte)
    log_info(s"stop done")
  }

  def close(): Unit = retry {
    log_info(s"close")
//    log_info(s"close")
//    out.writeByte(Wire.EOS)
//    out.flush()
//    assert(in.readByte() == Wire.EOS)
//    using(s) { _ => () }  // close with proper exception suppression notices
  }
}
