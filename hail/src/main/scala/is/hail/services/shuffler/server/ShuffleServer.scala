package is.hail.services.shuffler.server

import java.net._
import java.security.SecureRandom
import java.util.UUID
import java.util.concurrent.{ConcurrentSkipListMap, Executors, _}

import is.hail.annotations.Region
import is.hail.expr.ir._
import is.hail.types.encoded._
import is.hail.types.virtual._
import is.hail.io._
import is.hail.services.tls._
import is.hail.services.shuffler._
import is.hail.services.tcp
import is.hail.services.tcp.HailSocket
import is.hail.utils._
import javax.net.ssl._
import org.apache.log4j.Logger

import scala.annotation.switch

class Handler (
  private[this] val server: ShuffleServer,
  private[this] val connectionId: UUID,
  private[this] val s: Socket
) extends Runnable {
  private[this] val log = Logger.getLogger(getClass.getName())
  private[this] val in = shuffleBufferSpec.buildInputBuffer(s.getInputStream)
  private[this] val out = shuffleBufferSpec.buildOutputBuffer(s.getOutputStream)
  private[this] val random = new SecureRandom();

  def log_info(msg: String): Unit = {
    log.info(s"${connectionId}: ${msg}")
  }

  def run(): Unit = {
    try {
      log_info(s"handle")
      try {
        var continue = true
        while (continue) {
          val op = in.readByte()
          log_info(s"operation ${op}")
            (op: @switch) match {
            case Wire.START => start()
            case Wire.PUT => put()
            case Wire.GET => get()
            case Wire.STOP => stop()
            case Wire.PARTITION_BOUNDS => partitionBounds()
            case Wire.EOS =>
              log_info(s"client ended session, replying, then exiting cleanly")
              eos()
              continue = false
            case op => fatal(s"bad operation number $op")
          }
        }
      } finally {
        s.close()
      }
    } catch {
      case e: Exception =>
        log.warn(s"exception while serving", e)
    }
  }

  def readShuffleUUID(): Shuffle = {
    val uuid = Wire.readByteArray(in)
    assert(uuid.length == Wire.ID_SIZE, s"${uuid.length} ${Wire.ID_SIZE}")
    log_info(s"uuid ${uuidToString(uuid)}")
    val shuffle = server.shuffles.get(uuid)
    if (shuffle == null) {
      throw new RuntimeException(s"shuffle does not exist ${uuidToString(uuid)}")
    }
    shuffle
  }

  def start(): Unit = {
    log_info(s"start")
    val rowType = Wire.readTStruct(in)
    log_info(s"start got row type ${rowType}")
    val rowEType = Wire.readEBaseStruct(in)
    log_info(s"start got row encoded type ${rowEType}")
    val keyFields = Wire.readSortFieldArray(in)
    log_info(s"start got key fields ${keyFields.mkString("[", ",", "]")}")
    val keyEType = Wire.readEBaseStruct(in)
    log_info(s"start got key encoded type ${keyEType}")
    val uuid = new Array[Byte](Wire.ID_SIZE)
    random.nextBytes(uuid)
    server.shuffles.put(uuid, new Shuffle(uuid, TShuffle(keyFields, rowType, rowEType, keyEType)))
    Wire.writeByteArray(out, uuid)
    log_info(s"start wrote uuid")
    out.flush()
    log_info(s"start flush")
    log_info(s"start done")
  }

  def put(): Unit = {
    log_info(s"put")
    val shuffle = readShuffleUUID()
    shuffle.put(in, out)
    log_info(s"put done")
  }

  def get(): Unit = {
    log_info(s"get")
    val shuffle = readShuffleUUID()
    shuffle.get(in, out)
    log_info(s"get done")
  }

  def stop(): Unit = {
    log_info(s"stop")
    val uuid = Wire.readByteArray(in)
    assert(uuid.length == Wire.ID_SIZE, s"${uuid.length} ${Wire.ID_SIZE}")
    val shuffle = server.shuffles.remove(uuid)
    if (shuffle != null) {
      shuffle.close()
    }
    out.writeByte(0.toByte)
    out.flush()
    log_info(s"stop done")
  }

  def partitionBounds(): Unit = {
    log_info(s"partitionBounds")
    val shuffle = readShuffleUUID()
    shuffle.partitionBounds(in, out)
    log_info(s"partitionBounds done")
  }

  def eos(): Unit = {
    out.writeByte(Wire.EOS)
    out.flush()
  }
}

class Shuffle (
  uuid: Array[Byte],
  shuffleType: TShuffle
) extends AutoCloseable {
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val rootRegion = Region()
  private[this] val codecs = {
    ExecutionTimer.logTime("Shuffle.codecs") { timer =>
      using(new ExecuteContext("/tmp", "file:///tmp", null, null, rootRegion, timer)) { ctx =>
        new ShuffleCodecSpec(ctx, shuffleType)
      }
    }
  }

  private[this] val store = new LSM(s"/tmp/${uuidToString(uuid)}", codecs)

  private[this] def makeRegion(): Region = {
    val region = Region()
    rootRegion.addReferenceTo(region)
    region
  }

  def close(): Unit = {
    rootRegion.close()
  }

  def put(in: InputBuffer, out: OutputBuffer) {
    val decoder = codecs.makeRowDecoder(in)
    val region = makeRegion()
    var hasNext = in.readByte()
    assert(hasNext != -1)
    while (hasNext == 1) {
      val off = decoder.readRegionValue(region)
      val koff = codecs.keyDecodedPType.copyFromAddress(region, codecs.rowDecodedPType, off, false)
      store.put(koff, off)
      hasNext = in.readByte()
    }
    // fixme: server needs to send uuid for the successful partition
    out.writeByte(0)
    out.flush()
  }

  def get(in: InputBuffer, out: OutputBuffer) {
    val region = makeRegion()
    val keyDecoder = codecs.makeKeyDecoder(in)
    val encoder = codecs.makeRowEncoder(out)
    val start = keyDecoder.readRegionValue(region)
    val startInclusive = keyDecoder.readByte() == 1.toByte
    val end = keyDecoder.readRegionValue(region)
    val endInclusive = keyDecoder.readByte() == 1.toByte

    log.info(s"get start ${rvstr(codecs.keyDecodedPType, start)} ${startInclusive} end ${rvstr(codecs.keyDecodedPType, end)} ${endInclusive}")
    val it = store.iterator(start, startInclusive)
    var continue = it.hasNext
    val inRange =
      if (endInclusive) (key: Long) => store.keyOrd.lteq(key, end)
      else              (key: Long) => store.keyOrd.lt(key, end)
    while (continue) {
      val kv = it.next
      val k = kv.getKey
      val v = kv.getValue
      continue = inRange(k)
      if (continue) {
        encoder.writeByte(1)
        encoder.writeRegionValue(v)
        continue = it.hasNext
      }
    }
    encoder.writeByte(0)
    encoder.flush()
  }

  def partitionBounds(in: InputBuffer, out: OutputBuffer) {
    val nPartitions = in.readInt()

    val keyEncoder = codecs.makeKeyEncoder(out)

    log.info(s"partitionBounds ${nPartitions}")
    val keys = store.partitionKeys(nPartitions)
    assert((nPartitions == 0 && keys.length == 0) ||
      keys.length == nPartitions + 1)
    writeRegionValueArray(keyEncoder, keys)
    keyEncoder.flush()
  }
}

object ShuffleServer {
  def main(args: Array[String]): Unit =
    using(new ShuffleServer())(_.serve())
}

class ShuffleServer() extends AutoCloseable {
  val port = 443
  val log = Logger.getLogger(this.getClass.getName());

  val shuffles = new ConcurrentSkipListMap[Array[Byte], Shuffle](new SameLengthByteArrayComparator())

  val executor = Executors.newCachedThreadPool()
  var stopped = false

  val ss = tcp.serverSocket(port)

  def serveInBackground(): Future[_] =
    executor.submit(new Runnable() { def run(): Unit = serve() })

  def serve(): Unit = {
    try {
      log.info(s"serving on ${port}")
      while (true) {
        log.info(s"accepted")
        val (connectionId, s) = ss.accept()
        executor.execute(new Handler(this, connectionId, s))
      }
    } catch {
      case se: SocketException =>
        if (stopped) {
          log.info(s"exiting")
          return
        } else {
          fatal("unexpected closed server socket", se)
        t
    }
  }

  def stop(): Unit = {
    log.info(s"stopping")
    stopped = true
    ss.close()
    executor.shutdownNow()
  }

  def close(): Unit = stop()
}
