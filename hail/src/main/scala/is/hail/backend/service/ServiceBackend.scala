package is.hail.backend.service

import java.io._
import java.nio.charset._
import java.net._
import java.nio.charset.StandardCharsets
import java.util.concurrent._

import is.hail.HAIL_REVISION
import is.hail.HailContext
import is.hail.annotations._
import is.hail.asm4s._
import is.hail.backend.{Backend, BackendContext, BroadcastValue, HailTaskContext}
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.lowering.{DArrayLowering, LoweringPipeline, TableStage, TableStageDependency}
import is.hail.expr.ir.{Compile, ExecuteContext, IR, IRParser, Literal, MakeArray, MakeTuple, ShuffleRead, ShuffleWrite, SortField, ToStream}
import is.hail.io.fs.{FS, GoogleStorageFS, SeekableDataInputStream, ServiceCacheableFS}
import is.hail.linalg.BlockMatrix
import is.hail.rvd.RVDPartitioner
import is.hail.services._
import is.hail.services.batch_client.BatchClient
import is.hail.services.shuffler.ShuffleClient
import is.hail.types._
import is.hail.types.encoded._
import is.hail.types.physical._
import is.hail.types.physical.stypes.PTypeReferenceSingleCodeType
import is.hail.types.virtual._
import is.hail.utils._
import is.hail.variant.ReferenceGenome
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, Formats}
import org.newsclub.net.unix.{AFUNIXServerSocket, AFUNIXSocketAddress}

import scala.annotation.switch
import scala.reflect.ClassTag
import scala.{concurrent => scalaConcurrent}


class ServiceBackendContext(
  @transient val sessionID: String,
  val billingProject: String,
  val bucket: String
) extends BackendContext with Serializable {
  def tokens(): Tokens =
    new Tokens(Map((DeployConfig.get.defaultNamespace, sessionID)))
}

object ServiceBackend {
  private val log = Logger.getLogger(getClass.getName())
}

class User(
  val username: String,
  val tmpdir: String,
  val fs: GoogleStorageFS)

class ServiceBackend(
  val revision: String,
  val jarLocation: String,
  var name: String,
  val scratchDir: String = sys.env.get("HAIL_WORKER_SCRATCH_DIR").getOrElse("")
) extends Backend {
  import ServiceBackend.log

  private[this] var batchCount = 0
  private[this] val users = new ConcurrentHashMap[String, User]()
  private[this] implicit val ec = scalaConcurrent.ExecutionContext.fromExecutorService(
    Executors.newCachedThreadPool())

  def addUser(username: String, key: String): Unit = synchronized {
    val previous = users.put(username, new User(username, "/tmp", new GoogleStorageFS(key)))
    assert(previous == null)
  }

  def userContext[T](username: String, timer: ExecutionTimer)(f: (ExecuteContext) => T): T = {
    val user = users.get(username)
    assert(user != null, username)
    ExecuteContext.scoped(user.tmpdir, "file:///tmp", this, user.fs, timer, null)(f)
  }

  def defaultParallelism: Int = 10

  def broadcast[T: ClassTag](_value: T): BroadcastValue[T] = {
    using(new ObjectOutputStream(new ByteArrayOutputStream())) { os =>
      try {
        os.writeObject(_value)
      } catch {
        case e: Exception =>
          fatal(_value.toString, e)
      }
    }
    new BroadcastValue[T] with Serializable {
      def value: T = _value
    }
  }

  def parallelizeAndComputeWithIndex(
    _backendContext: BackendContext,
    _fs: FS,
    collection: Array[Array[Byte]],
    dependency: Option[TableStageDependency] = None
  )(f: (Array[Byte], HailTaskContext, FS) => Array[Byte]
  ): Array[Array[Byte]] = {
    val backendContext = _backendContext.asInstanceOf[ServiceBackendContext]
    val fs = _fs.asInstanceOf[ServiceCacheableFS]
    val n = collection.length
    val token = tokenUrlSafe(32)
    val root = s"gs://${ backendContext.bucket }/tmp/hail/query/$token"

    log.info(s"parallelizeAndComputeWithIndex: $token: nPartitions $n")
    log.info(s"parallelizeAndComputeWithIndex: $token: writing f and contexts")

    val uploadFunction = scalaConcurrent.Future {
      retryTransientErrors {
        using(new ObjectOutputStream(fs.createCachedNoCompression(s"$root/f"))) { os =>
          os.writeObject(f)
        }
      }
    }

    val uploadContexts = scalaConcurrent.Future {
      retryTransientErrors {
        using(fs.createCachedNoCompression(s"$root/contexts")) { os =>
          var o = 12L * n
          var i = 0
          while (i < n) {
            val len = collection(i).length
            os.writeLong(o)
            os.writeInt(len)
            i += 1
            o += len
          }
          log.info(s"parallelizeAndComputeWithIndex: $token: writing contexts")
          collection.foreach { context =>
            os.write(context)
          }
        }
      }
    }

    val batchClient = BatchClient.fromSessionID(backendContext.sessionID) // FIXME: how is this working?
    val createBatch = scalaConcurrent.Future {
      val jobs = new Array[JObject](n)
      var i = 0
      while (i < n) {
        jobs(i) = JObject(
          "always_run" -> JBool(false),
          "job_id" -> JInt(i + 1),
          "parent_ids" -> JArray(List()),
          "process" -> JObject(
            "command" -> JArray(List(
              JString("is.hail.backend.service.Worker"),
              JString(revision),
              JString(jarLocation),
              JString(root),
              JString(s"$i"))),
            "type" -> JString("jvm")),
          "mount_tokens" -> JBool(true))
        i += 1
      }

      log.info(s"parallelizeAndComputeWithIndex: $token: running job")

      batchClient.create(
        JObject(
          "billing_project" -> JString(backendContext.billingProject),
          "n_jobs" -> JInt(n),
          "token" -> JString(token),
          "attributes" -> JObject("name" -> JString(name + "_" + batchCount))),
        jobs)
    }

    scalaConcurrent.Await.result(uploadFunction, scalaConcurrent.duration.Duration.Inf)
    scalaConcurrent.Await.result(uploadContexts, scalaConcurrent.duration.Duration.Inf)
    val batchId = scalaConcurrent.Await.result(createBatch, scalaConcurrent.duration.Duration.Inf)

    val batch = batchClient.waitForBatch(batchId)
    batchCount += 1
    implicit val formats: Formats = DefaultFormats
    val batchID = (batch \ "id").extract[Int]
    val batchState = (batch \ "state").extract[String]
    if (batchState != "success")
      throw new RuntimeException(s"batch $batchID failed: $batchState")

    log.info(s"parallelizeAndComputeWithIndex: $token: reading results")

    val r = new Array[Array[Byte]](n)

    def readResult(i: Int): scalaConcurrent.Future[Unit] = scalaConcurrent.Future {
      r(i) = retryTransientErrors {
        using(fs.openCachedNoCompression(s"$root/result.$i")) { is =>
          IOUtils.toByteArray(is)
        }
      }
      log.info(s"result $i complete")
    }

    scalaConcurrent.Await.result(
      scalaConcurrent.Future.sequence(
        Array.tabulate(n)(readResult).toFastIndexedSeq),
      scalaConcurrent.duration.Duration.Inf)

    log.info(s"all results complete")
    r
  }

  def stop(): Unit = ()

  def valueType(tmpdir: String, s: String): String = {
    ExecutionTimer.logTime("ServiceBackend.valueType") { timer =>
      val fs = retryTransientErrors {
        using(new FileInputStream(s"$scratchDir/gsa-key/key.json")) { is =>
          new GoogleStorageFS(IOUtils.toString(is, Charset.defaultCharset().toString())).asCacheable()
        }
      }
      ExecuteContext.scoped(tmpdir, "file:///tmp", this, fs, timer, null) { ctx =>
        val x = IRParser.parse_value_ir(ctx, s)
        x.typ.toString
      }
    }
  }

  def tableType(tmpdir: String, s: String): String = {
    ExecutionTimer.logTime("ServiceBackend.tableType") { timer =>
      val fs = retryTransientErrors {
        using(new FileInputStream(s"$scratchDir/gsa-key/key.json")) { is =>
          new GoogleStorageFS(IOUtils.toString(is, Charset.defaultCharset().toString())).asCacheable()
        }
      }
      ExecuteContext.scoped(tmpdir, "file:///tmp", this, fs, timer, null) { ctx =>
        val x = IRParser.parse_table_ir(ctx, s)
        val t = x.typ
        val jv = JObject("global" -> JString(t.globalType.toString),
          "row" -> JString(t.rowType.toString),
          "row_key" -> JArray(t.key.map(f => JString(f)).toList))
        JsonMethods.compact(jv)
      }
    }
  }

  def matrixTableType(tmpdir: String, s: String): String = {
    ExecutionTimer.logTime("ServiceBackend.matrixTableType") { timer =>
      val fs = retryTransientErrors {
        using(new FileInputStream(s"$scratchDir/gsa-key/key.json")) { is =>
          new GoogleStorageFS(IOUtils.toString(is, Charset.defaultCharset().toString())).asCacheable()
        }
      }
      ExecuteContext.scoped(tmpdir, "file:///tmp", this, fs, timer, null) { ctx =>
        val x = IRParser.parse_matrix_ir(ctx, s)
        val t = x.typ
        val jv = JObject("global" -> JString(t.globalType.toString),
          "col" -> JString(t.colType.toString),
          "col_key" -> JArray(t.colKey.map(f => JString(f)).toList),
          "row" -> JString(t.rowType.toString),
          "row_key" -> JArray(t.rowKey.map(f => JString(f)).toList),
          "entry" -> JString(t.entryType.toString))
        JsonMethods.compact(jv)
      }
    }
  }

  def blockMatrixType(tmpdir: String, s: String): String = {
    ExecutionTimer.logTime("ServiceBackend.blockMatrixType") { timer =>
      val fs = retryTransientErrors {
        using(new FileInputStream(s"$scratchDir/gsa-key/key.json")) { is =>
          new GoogleStorageFS(IOUtils.toString(is, Charset.defaultCharset().toString())).asCacheable()
        }
      }
      ExecuteContext.scoped(tmpdir, "file:///tmp", this, fs, timer, null) { ctx =>
        val x = IRParser.parse_blockmatrix_ir(ctx, s)
        val t = x.typ
        val jv = JObject("element_type" -> JString(t.elementType.toString),
          "shape" -> JArray(t.shape.map(s => JInt(s)).toList),
          "is_row_vector" -> JBool(t.isRowVector),
          "block_size" -> JInt(t.blockSize))
        JsonMethods.compact(jv)
      }
    }
  }

  def referenceGenome(tmpdir: String, name: String): String = {
    ReferenceGenome.getReference(name).toJSONString
  }

  private[this] def execute(ctx: ExecuteContext, _x: IR): Option[(Annotation, PType)] = {
    val x = LoweringPipeline.darrayLowerer(true)(DArrayLowering.All).apply(ctx, _x)
      .asInstanceOf[IR]
    if (x.typ == TVoid) {
      val (_, f) = Compile[AsmFunction1RegionUnit](ctx,
        FastIndexedSeq(),
        FastIndexedSeq[TypeInfo[_]](classInfo[Region]), UnitInfo,
        x,
        optimize = true)

      f(ctx.fs, 0, ctx.r)(ctx.r)
      None
    } else {
      val (Some(PTypeReferenceSingleCodeType(pt)), f) = Compile[AsmFunction1RegionLong](ctx,
        FastIndexedSeq(),
        FastIndexedSeq[TypeInfo[_]](classInfo[Region]), LongInfo,
        MakeTuple.ordered(FastIndexedSeq(x)),
        optimize = true)

      val a = f(ctx.fs, 0, ctx.r)(ctx.r)
      val retPType = pt.asInstanceOf[PBaseStruct]
      Some((new UnsafeRow(retPType, ctx.r, a).get(0), retPType.types(0)))
    }
  }

  def execute(tmpdir: String, sessionID: String, billingProject: String, bucket: String, code: String, token: String): String = {
    ExecutionTimer.logTime("ServiceBackend.execute") { timer =>
      val fs = retryTransientErrors {
        using(new FileInputStream(s"$scratchDir/gsa-key/key.json")) { is =>
          new GoogleStorageFS(IOUtils.toString(is, Charset.defaultCharset().toString())).asCacheable()
        }
      }
      ExecuteContext.scoped(tmpdir, "file:///tmp", this, fs, timer, null) { ctx =>
        log.info(s"executing: ${token}")
        ctx.backendContext = new ServiceBackendContext(sessionID, billingProject, bucket)

        execute(ctx, IRParser.parse_value_ir(ctx, code)) match {
          case Some((v, t)) =>
            JsonMethods.compact(
              JObject(List("value" -> JSONAnnotationImpex.exportAnnotation(v, t.virtualType),
                "type" -> JString(t.virtualType.toString))))
          case None =>
            JsonMethods.compact(
              JObject(List("value" -> null, "type" -> JString(TVoid.toString))))
        }
      }
    }
  }

  def flags(): String = {
    JsonMethods.compact(JObject(HailContext.get.flags.available.toArray().map { case f: String =>
      val v = HailContext.getFlag(f)
      f -> (if (v == null) JNull else JString(v))
    }: _*))
  }

  def getFlag(name: String): String = {
    val v = HailContext.getFlag(name)
    JsonMethods.compact(if (v == null) JNull else JString(v))
  }

  def setFlag(name: String, value: String): String = {
    val v = HailContext.getFlag(name)
    HailContext.setFlag(name, value)
    JsonMethods.compact(if (v == null) JNull else JString(v))
  }

  def unsetFlag(name: String): String = {
    val v = HailContext.getFlag(name)
    HailContext.setFlag(name, null)
    JsonMethods.compact(if (v == null) JNull else JString(v))
  }

  def lowerDistributedSort(
    ctx: ExecuteContext,
    stage: TableStage,
    sortFields: IndexedSeq[SortField],
    relationalLetsAbove: Map[String, IR],
    rowTypeRequiredness: RStruct
  ): TableStage = {
    val region = ctx.r
    val rowType = stage.rowType
    val keyFields = sortFields.map(_.field).toArray
    val keyType = rowType.typeAfterSelectNames(keyFields)
    val rowEType = EType.fromTypeAndAnalysis(rowType, rowTypeRequiredness).asInstanceOf[EBaseStruct]
    val keyEType = EType.fromTypeAndAnalysis(keyType, rowTypeRequiredness.select(keyFields)).asInstanceOf[EBaseStruct]
    val shuffleType = TShuffle(sortFields, rowType, rowEType, keyEType)
    val shuffleClient = new ShuffleClient(shuffleType, ctx)
    assert(keyType == shuffleClient.codecs.keyType)
    val keyDecodedPType = shuffleClient.codecs.keyDecodedPType
    shuffleClient.start()
    val uuid = shuffleClient.uuid

    ctx.ownCleanup({ () =>
      using(new ShuffleClient(shuffleType, uuid, ctx)) { shuffleClient =>
        shuffleClient.stop()
      }
    })

    try {
      val Some((successfulPartitionIdsAndGlobals, pType)) = execute(
        ctx,
        stage.mapCollectWithGlobals
          (relationalLetsAbove)
          { partition => ShuffleWrite(Literal(shuffleType, uuid), partition) }
          { (rows, globals) => MakeTuple.ordered(Seq(rows, globals)) })
      val globals = SafeRow(
        successfulPartitionIdsAndGlobals.asInstanceOf[UnsafeRow].t,
        successfulPartitionIdsAndGlobals.asInstanceOf[UnsafeRow].offset
      ).get(1)

      val partitionBoundsPointers = shuffleClient.partitionBounds(region, stage.numPartitions)
      val partitionIntervals = partitionBoundsPointers.zip(partitionBoundsPointers.drop(1)).map { case (l, r) =>
        Interval(SafeRow(keyDecodedPType, l), SafeRow(keyDecodedPType, r), includesStart = true, includesEnd = false)
      }
      val last = partitionIntervals.last
      partitionIntervals(partitionIntervals.length - 1) = Interval(
        last.left.point, last.right.point, includesStart = true, includesEnd = true)

      val partitioner = new RVDPartitioner(keyType, partitionIntervals.toFastIndexedSeq)

      TableStage(
        globals = Literal(stage.globalType, globals),
        partitioner = partitioner,
        TableStageDependency.none,
        contexts = ToStream(MakeArray(
          partitionIntervals.map(interval => Literal(TInterval(keyType), interval)),
          TArray(TInterval(keyType)))),
        interval => ShuffleRead(Literal(shuffleType, uuid), interval))
    } finally {
      shuffleClient.close()
    }
  }

  def persist(backendContext: BackendContext, id: String, value: BlockMatrix, storageLevel: String): Unit = ???

  def unpersist(backendContext: BackendContext, id: String): Unit = ???

  def getPersistedBlockMatrix(backendContext: BackendContext, id: String): BlockMatrix = ???

  def getPersistedBlockMatrixType(backendContext: BackendContext, id: String): BlockMatrixType = ???

  def loadReferencesFromDataset(
    tmpdir: String,
    billingProject: String,
    bucket: String,
    path: String
  ): String = {
    ExecutionTimer.logTime("ServiceBackend.loadReferencesFromDataset") { timer =>
      val fs = retryTransientErrors {
        using(new FileInputStream(s"$scratchDir/gsa-key/key.json")) { is =>
          new GoogleStorageFS(IOUtils.toString(is, Charset.defaultCharset().toString())).asCacheable()
        }
      }
      ExecuteContext.scoped(tmpdir, "file:///tmp", this, fs, timer, null) { ctx =>
        ReferenceGenome.fromHailDataset(ctx.fs, path)
      }
    }
  }
}

class EndOfInputException extends RuntimeException

object ServiceBackendSocketAPI {
  private val log = Logger.getLogger(getClass.getName())
}

class ServiceBackendSocketAPI(backend: ServiceBackend, socket: Socket) extends Thread {
  import ServiceBackendSocketAPI._

  private[this] val LOAD_REFERENCES_FROM_DATASET = 1
  private[this] val VALUE_TYPE = 2
  private[this] val TABLE_TYPE = 3
  private[this] val MATRIX_TABLE_TYPE = 4
  private[this] val BLOCK_MATRIX_TYPE = 5
  private[this] val REFERENCE_GENOME = 6
  private[this] val EXECUTE = 7
  private[this] val FLAGS = 8
  private[this] val GET_FLAG = 9
  private[this] val UNSET_FLAG = 10
  private[this] val SET_FLAG = 11
  private[this] val ADD_USER = 12
  private[this] val GOODBYE = 254

  private[this] val in = socket.getInputStream
  private[this] val out = socket.getOutputStream

  private[this] val dummy = new Array[Byte](8)

  def read(bytes: Array[Byte], off: Int, n: Int): Unit = {
    assert(off + n <= bytes.length)
    var read = 0
    while (read < n) {
      val r = in.read(bytes, off + read, n - read)
      if (r < 0) {
        throw new EndOfInputException
      } else {
        read += r
      }
    }
  }

  def readInt(): Int = {
    read(dummy, 0, 4)
    Memory.loadInt(dummy, 0)
  }

  def readLong(): Long = {
    read(dummy, 0, 8)
    Memory.loadLong(dummy, 0)
  }

  def readBytes(): Array[Byte] = {
    val n = readInt()
    val bytes = new Array[Byte](n)
    read(bytes, 0, n)
    bytes
  }

  def readString(): String = new String(readBytes(), StandardCharsets.UTF_8)

  def writeBool(b: Boolean): Unit = {
    out.write(if (b) 1 else 0)
  }

  def writeInt(v: Int): Unit = {
    Memory.storeInt(dummy, 0, v)
    out.write(dummy, 0, 4)
  }

  def writeLong(v: Long): Unit = {
    Memory.storeLong(dummy, 0, v)
    out.write(dummy)
  }

  def writeBytes(bytes: Array[Byte]): Unit = {
    writeInt(bytes.length)
    out.write(bytes)
  }

  def writeString(s: String): Unit = writeBytes(s.getBytes(StandardCharsets.UTF_8))

  def eventLoop(): Unit = {
    var continue = true
    while (continue) {
      val cmd = readInt()

      (cmd: @switch) match {
        case LOAD_REFERENCES_FROM_DATASET =>
          val username = readString()
          val billingProject = readString()
          val bucket = readString()
          val path = readString()
          try {
            val result = backend.loadReferencesFromDataset(username, billingProject, bucket, path)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case VALUE_TYPE =>
          val username = readString()
          val s = readString()
          try {
            val result = backend.valueType(username, s)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case TABLE_TYPE =>
          val username = readString()
          val s = readString()
          try {
            val result = backend.tableType(username, s)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case MATRIX_TABLE_TYPE =>
          val username = readString()
          val s = readString()
          try {
            val result = backend.matrixTableType(username, s)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case BLOCK_MATRIX_TYPE =>
          val username = readString()
          val s = readString()
          try {
            val result = backend.blockMatrixType(username, s)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case REFERENCE_GENOME =>
          val username = readString()
          val name = readString()
          try {
            val result = backend.referenceGenome(username, name)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case EXECUTE =>
          val tmpdir = readString()
          val sessionId = readString()
          val billingProject = readString()
          val bucket = readString()
          val code = readString()
          val token = readString()
          try {
            val result = backend.execute(tmpdir, sessionId, billingProject, bucket, code, token)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case FLAGS =>
          try {
            val result = backend.flags()
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case GET_FLAG =>
          val name = readString()
          try {
            val result = backend.getFlag(name)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case SET_FLAG =>
          val name = readString()
          val value = readString()
          try {
            val result = backend.setFlag(name, value)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case UNSET_FLAG =>
          val name = readString()
          try {
            val result = backend.unsetFlag(name)
            writeBool(true)
            writeString(result)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case ADD_USER =>
          val name = readString()
          val gsaKey = readString()
          try {
            val result = backend.addUser(name, gsaKey)
            writeBool(true)
          } catch {
            case t: Throwable =>
              writeBool(false)
              writeString(formatException(t))
          }

        case GOODBYE =>
          continue = false
          writeInt(GOODBYE)
      }
    }
  }

  override def run(): Unit = {
    try {
      eventLoop()
    } catch {
      case t: Throwable =>
        log.info("ServiceBackendSocketAPI caught exception", t)
    } finally {
      socket.close()
    }
  }
}

object ServiceBackendSocketAPI2 {
  def main(argv: Array[String]): Unit = {
    assert(argv.length == 6, argv.toFastIndexedSeq)

    val scratchDir = argv(0)
    val revision = argv(1)
    val jarLocation = argv(2)
    val name = argv(3)
    val input = argv(4)
    val output = argv(5)

    val backend = new ServiceBackend(revision, jarLocation, name, scratchDir)
    if (HailContext.isInitialized) {
      HailContext.get.backend = backend
    } else {
      HailContext(backend, "hail.log", false, false, 50, skipLoggingConfiguration = true, 3)
    }
    val fs = retryTransientErrors {
      using(new FileInputStream(s"$scratchDir/gsa-key/key.json")) { is =>
        new GoogleStorageFS(IOUtils.toString(is, Charset.defaultCharset().toString())).asCacheable()
      }
    }

    val deployConfig = DeployConfig.fromConfigFile(
      s"$scratchDir/deploy-config/deploy-config.json")
    DeployConfig.set(deployConfig)
    val userTokens = Tokens.fromFile(s"$scratchDir/user-tokens/tokens.json")
    Tokens.set(userTokens)
    tls.setSSLConfigFromDir(s"$scratchDir/ssl-config")

    val sessionId = userTokens.namespaceToken(deployConfig.defaultNamespace)
    retryTransientErrors( {
      using(fs.openNoCompression(input)) { in =>
        retryTransientErrors( {
          using(fs.createNoCompression(output)) { out =>
            new ServiceBackendSocketAPI2(backend, in, out, sessionId).executeOneCommand()
            out.flush()
          }
        }, retry404 = true)
      }
    }, retry404 = true)
  }
}

class ServiceBackendSocketAPI2(
  private[this] val backend: ServiceBackend,
  private[this] val in: InputStream,
  private[this] val out: OutputStream,
  private[this] val sessionId: String
) extends Thread {
  import ServiceBackendSocketAPI2._

  private[this] val LOAD_REFERENCES_FROM_DATASET = 1
  private[this] val VALUE_TYPE = 2
  private[this] val TABLE_TYPE = 3
  private[this] val MATRIX_TABLE_TYPE = 4
  private[this] val BLOCK_MATRIX_TYPE = 5
  private[this] val REFERENCE_GENOME = 6
  private[this] val EXECUTE = 7
  private[this] val FLAGS = 8
  private[this] val GET_FLAG = 9
  private[this] val UNSET_FLAG = 10
  private[this] val SET_FLAG = 11
  private[this] val ADD_USER = 12
  private[this] val GOODBYE = 254

  private[this] val dummy = new Array[Byte](8)

  def read(bytes: Array[Byte], off: Int, n: Int): Unit = {
    assert(off + n <= bytes.length)
    var read = 0
    while (read < n) {
      val r = in.read(bytes, off + read, n - read)
      if (r < 0) {
        throw new EndOfInputException
      } else {
        read += r
      }
    }
  }

  def readInt(): Int = {
    read(dummy, 0, 4)
    Memory.loadInt(dummy, 0)
  }

  def readLong(): Long = {
    read(dummy, 0, 8)
    Memory.loadLong(dummy, 0)
  }

  def readBytes(): Array[Byte] = {
    val n = readInt()
    val bytes = new Array[Byte](n)
    read(bytes, 0, n)
    bytes
  }

  def readString(): String = new String(readBytes(), StandardCharsets.UTF_8)

  def writeBool(b: Boolean): Unit = {
    out.write(if (b) 1 else 0)
  }

  def writeInt(v: Int): Unit = {
    Memory.storeInt(dummy, 0, v)
    out.write(dummy, 0, 4)
  }

  def writeLong(v: Long): Unit = {
    Memory.storeLong(dummy, 0, v)
    out.write(dummy)
  }

  def writeBytes(bytes: Array[Byte]): Unit = {
    writeInt(bytes.length)
    out.write(bytes)
  }

  def writeString(s: String): Unit = writeBytes(s.getBytes(StandardCharsets.UTF_8))

  def executeOneCommand(): Unit = {
    val cmd = readInt()

    (cmd: @switch) match {
      case LOAD_REFERENCES_FROM_DATASET =>
        val tmpdir = readString()
        val billingProject = readString()
        val bucket = readString()
        val path = readString()
        try {
          val result = backend.loadReferencesFromDataset(tmpdir, billingProject, bucket, path)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case VALUE_TYPE =>
        val tmpdir = readString()
        val s = readString()
        try {
          val result = backend.valueType(tmpdir, s)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case TABLE_TYPE =>
        val tmpdir = readString()
        val s = readString()
        try {
          val result = backend.tableType(tmpdir, s)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case MATRIX_TABLE_TYPE =>
        val tmpdir = readString()
        val s = readString()
        try {
          val result = backend.matrixTableType(tmpdir, s)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case BLOCK_MATRIX_TYPE =>
        val tmpdir = readString()
        val s = readString()
        try {
          val result = backend.blockMatrixType(tmpdir, s)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case REFERENCE_GENOME =>
        val tmpdir = readString()
        val name = readString()
        try {
          val result = backend.referenceGenome(tmpdir, name)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case EXECUTE =>
        val tmpdir = readString()
        val billingProject = readString()
        val bucket = readString()
        val code = readString()
        val token = readString()
        try {
          val result = backend.execute(tmpdir, sessionId, billingProject, bucket, code, token)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case FLAGS =>
        try {
          val result = backend.flags()
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case GET_FLAG =>
        val name = readString()
        try {
          val result = backend.getFlag(name)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case SET_FLAG =>
        val name = readString()
        val value = readString()
        try {
          val result = backend.setFlag(name, value)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case UNSET_FLAG =>
        val name = readString()
        try {
          val result = backend.unsetFlag(name)
          writeBool(true)
          writeString(result)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case ADD_USER =>
        val name = readString()
        val gsaKey = readString()
        try {
          val result = backend.addUser(name, gsaKey)
          writeBool(true)
        } catch {
          case t: Throwable =>
            writeBool(false)
            writeString(formatException(t))
        }

      case GOODBYE =>
        writeInt(GOODBYE)
    }
  }
}


object ServiceBackendMain {
  private val log = Logger.getLogger(getClass.getName())

  def main(argv: Array[String]): Unit = {
    assert(argv.length == 1, argv.toFastIndexedSeq)
    val udsAddress = argv(0)
    val queryGCSPathEnvVar = System.getenv("HAIL_QUERY_GCS_PATH")
    assert(queryGCSPathEnvVar != null)
    val queryGCSJarPath = queryGCSPathEnvVar + "/jars/"
    val executor = Executors.newCachedThreadPool()
    val backend = new ServiceBackend(HAIL_REVISION, queryGCSJarPath + HAIL_REVISION + ".jar", "server")
    HailContext(backend, "hail.log", false, false, 50, skipLoggingConfiguration = true, 3)

    val ss = AFUNIXServerSocket.newInstance()
    ss.bind(new AFUNIXSocketAddress(new File(udsAddress)))
    try {
      log.info(s"serving on ${udsAddress}")
      while (true) {
        val sock = ss.accept()
        try {
          log.info(s"accepted")
          executor.execute(new ServiceBackendSocketAPI(backend, sock))
        } catch {
          case e: SocketException => {
            log.info(s"exception while handing socket to thread", e)
            sock.close()
          }
        }
      }
    } catch {
      case se: SocketException =>
        fatal("unexpected closed server socket", se)
    }
  }
}
