package is.hail.io.fs

import is.hail.backend.BroadcastValue
import is.hail.services._
import is.hail.utils._
import is.hail.{HailContext, HailFeatureFlags}
import org.apache.commons.io.IOUtils
import org.apache.hadoop

import java.io._
import java.nio.charset._
import java.nio.file.FileSystems
import scala.collection.mutable
import scala.io.Source


trait FSURL[T <: FSURL[T]] {
  def getPath: String
  def withPath(newPath: String): T
  def addPathComponent(component: String): T
  def fromString(s: String): T

  override def toString(): String
}

trait FileStatus {
  def getPath: String
  def getActualUrl: String
  def getModificationTime: java.lang.Long
  def getLen: Long
  def isSymlink: Boolean
  def getOwner: String
}

trait FileListEntry extends FileStatus {
  def isFile: Boolean
  def isDirectory: Boolean
}

class BlobStorageFileStatus(
  actualUrl: String, modificationTime: java.lang.Long, size: Long
) extends FileStatus {
  def getPath: String = dropTrailingSlash(actualUrl) // getPath is a backwards compatible method: in the past, Hail dropped trailing slashes
  def getActualUrl: String = actualUrl
  def getModificationTime: java.lang.Long = modificationTime
  def getLen: Long = size
  def isSymlink: Boolean = false
  def getOwner: String = null
}

class BlobStorageFileListEntry(
  actualUrl: String, modificationTime: java.lang.Long, size: Long, isDir: Boolean
) extends BlobStorageFileStatus(
  actualUrl, modificationTime, size
) with FileListEntry {
  def isDirectory: Boolean = isDir
  def isFile: Boolean = !isDir
  override def toString: String = s"BSFLE($actualUrl $modificationTime $size $isDir)"

}

object FS {
  def cloudSpecificFS(
    credentialsPath: String,
    flags: Option[HailFeatureFlags]
  ): FS = retryTransientErrors {
    val cloudSpecificFS = using(new FileInputStream(credentialsPath)) { is =>
      val credentialsStr = Some(IOUtils.toString(is, Charset.defaultCharset()))
      sys.env.get("HAIL_CLOUD") match {
        case Some("gcp") =>
          val requesterPaysConfiguration = flags.flatMap { flags =>
            RequesterPaysConfiguration.fromFlags(
              flags.get("gcs_requester_pays_project"), flags.get("gcs_requester_pays_buckets")
            )
          }
          new GoogleStorageFS(credentialsStr, requesterPaysConfiguration)
        case Some("azure") =>
          new AzureStorageFS(credentialsStr)
        case Some(cloud) =>
          throw new IllegalArgumentException(s"Bad cloud: $cloud")
        case None =>
          throw new IllegalArgumentException(s"HAIL_CLOUD must be set.")
      }
    }

    new RouterFS(Array(cloudSpecificFS, new HadoopFS(new SerializableHadoopConfiguration(new hadoop.conf.Configuration()))))
  }

  def fileListEntryFromIterator[T <: FSURL[T]](
    url: T,
    it: Iterator[FileListEntry],
  ): FileListEntry = {
    val prefix = dropTrailingSlash(url.toString)
    val prefixWithSlash = prefix + "/"

    var continue = it.hasNext
    var fileFle: FileListEntry = null
    var dirFle: FileListEntry = null
    System.err.println(s"prefix=$prefix")
    System.err.println(s"prefixWithSlash=$prefixWithSlash")
    while (continue) {
      val fle = it.next()

      System.err.println(s"fle.getActualUrl=${fle.getActualUrl}")

      if (fle.getActualUrl == prefix) {
        assert(fle.isFile)
        fileFle = fle
      }

      if (fle.getActualUrl == prefixWithSlash) {
        assert(fle.isDirectory)
        dirFle = fle
      }

      System.err.println(s"lte=${(fle.getActualUrl <= prefixWithSlash)}")
      continue = it.hasNext && (fle.getActualUrl <= prefixWithSlash)
    }

    if (fileFle != null) {
      if (dirFle != null) {
        throw new FileAndDirectoryException(prefix)
      } else {
        fileFle
      }
    } else {
      if (dirFle != null) {
        dirFle
      } else {
        throw new FileNotFoundException(url.toString)
      }
    }
  }
}

abstract class FS extends Serializable {
  type URL <: FSURL[URL]

  def validUrl(filename: String): Boolean

  def parseUrl(filename: String): URL

  //////////////////////////////////////////////////////////////////////////////
  // Read

  def openNoCompression(filename: String): SeekableDataInputStream

  def open(path: String, codec: CompressionCodec): InputStream = {
    val is = openNoCompression(path)
    if (codec != null)
      codec.makeInputStream(is)
    else
      is

  }

  def open(path: String): InputStream =
    open(path, gzAsBGZ = false)

  def open(path: String, gzAsBGZ: Boolean): InputStream =
    open(path, getCodecFromPath(path, gzAsBGZ))

  def readNoCompression(filename: String): Array[Byte] = retryTransientErrors {
    using(openNoCompression(filename)) { is =>
      IOUtils.toByteArray(is)
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Write

  def createNoCompression(filename: String): PositionedDataOutputStream

  def create(path: String): OutputStream = {
    val os = createNoCompression(path)

    val codec = getCodecFromPath(path, gzAsBGZ = false)
    if (codec != null)
      codec.makeOutputStream(os)
    else
      os
  }

  def write(filename: String)(writer: OutputStream => Unit) =
    using(create(filename))(writer)

  def writePDOS(filename: String)(writer: PositionedDataOutputStream => Unit) =
    using(create(filename))(os => writer(outputStreamToPositionedDataOutputStream(os)))

  def touch(filename: String): Unit = {
    using(createNoCompression(filename))(_ => ())
  }

  //////////////////////////////////////////////////////////////////////////////
  // Delete

  def delete(filename: String, recursive: Boolean)

  //////////////////////////////////////////////////////////////////////////////
  // Directories

  def mkDir(dirname: String): Unit = ()

  def listDirectory(filename: String): Array[FileListEntry]

  def listDirectory(url: URL): Array[FileListEntry] = listDirectory(url.toString)

  //////////////////////////////////////////////////////////////////////////////
  // Metadata

  def fileStatus(filename: String): FileStatus

  def fileStatus(url: URL): FileStatus = fileStatus(url.toString)

  def getFileListEntry(filename: String): FileListEntry

  def getFileListEntry(url: URL): FileListEntry = getFileListEntry(url.toString)

  def getFileSize(filename: String): Long = fileStatus(filename).getLen

  def isFile(filename: String): Boolean = {
    try {
      getFileListEntry(filename).isFile
    } catch {
      case _: FileNotFoundException => false
    }
  }

  def isDir(filename: String): Boolean = {
    try {
      getFileListEntry(filename).isDirectory
    } catch {
      case _: FileNotFoundException => false
    }
  }

  def exists(filename: String): Boolean = {
    try {
      getFileListEntry(filename)
      true
    } catch {
      case _: FileNotFoundException => false
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Glob

  def globAll(filenames: Iterable[String]): Array[FileListEntry] = {
    filenames.flatMap { filename =>
      val fles = glob(filename)
      if (fles.isEmpty)
        warn(s"'$filename' refers to no files")
      fles
    }.toArray
  }

  def glob(filename: String): Array[FileListEntry] = glob(parseUrl(filename))

  def glob(url: URL): Array[FileListEntry] = {
    val path = dropTrailingSlash(url.getPath)

    val components = if (path == "") Array.empty else path.split("/")

    val javaFS = FileSystems.getDefault

    val ab = new mutable.ArrayBuffer[FileListEntry]()
    def f(prefix: URL, fle: FileListEntry, i: Int): Unit = {
      assert(!prefix.getPath.endsWith("/"), prefix)

      if (i == components.length) {
        var t = fle
        if (t == null) {
          try {
            t = getFileListEntry(prefix)
          } catch {
            case _: FileNotFoundException =>
          }
        }
        if (t != null)
          ab += t
      }

      if (i < components.length) {
        val c = components(i)
        if (containsWildcard(c)) {
          val m = javaFS.getPathMatcher(s"glob:$c")
          for (cfs <- listDirectory(prefix)) {
            val p = dropTrailingSlash(cfs.getPath)
            val d = p.drop(prefix.toString.length + 1)
            if (m.matches(javaFS.getPath(d))) {
              f(prefix.fromString(p), cfs, i + 1)
            }
          }
        } else
          f(prefix.addPathComponent(c), null, i + 1)
      }
    }

    f(url.withPath(""), null, 0)
    ab.toArray
  }

  //////////////////////////////////////////////////////////////////////////////
  // Et cetera

  def makeQualified(path: String): String

  def deleteOnExit(filename: String): Unit = {
    Runtime.getRuntime.addShutdownHook(
      new Thread(() => delete(filename, recursive = false)))
  }

  def copy(src: String, dst: String, deleteSource: Boolean = false) {
    using(openNoCompression(src)) { is =>
      using(createNoCompression(dst)) { os =>
        IOUtils.copy(is, os)
      }
    }
    if (deleteSource)
      delete(src, recursive = false)
  }

  def copyRecode(src: String, dst: String, deleteSource: Boolean = false) {
    using(open(src)) { is =>
      using(create(dst)) { os =>
        IOUtils.copy(is, os)
      }
    }
    if (deleteSource)
      delete(src, recursive = false)
  }

  def readLines[T](
    filename: String,
    filtAndReplace: TextInputFilterAndReplace = TextInputFilterAndReplace()
  )(
    reader: Iterator[WithContext[String]] => T
  ): T = {
    using(open(filename)) {
      is =>
        val lines = Source.fromInputStream(is)
          .getLines()
          .zipWithIndex
          .map {
            case (value, position) =>
              val source = Context(value, filename, Some(position))
              WithContext(value, source)
          }
        reader(filtAndReplace(lines))
    }
  }

  def writeTable(
    filename: String,
    lines: Traversable[String],
    header: Option[String] = None
  ): Unit = {
    using(new OutputStreamWriter(create(filename))) { fw =>
      header.foreach { h =>
        fw.write(h)
        fw.write('\n')
      }
      lines.foreach { line =>
        fw.write(line)
        fw.write('\n')
      }
    }
  }

  def copyMerge(
    sourceFolder: String,
    destinationFile: String,
    numPartFilesExpected: Int,
    deleteSource: Boolean = true,
    header: Boolean = true,
    partFilesOpt: Option[IndexedSeq[String]] = None
  ) {
    if (!exists(sourceFolder + "/_SUCCESS"))
      fatal("write failed: no success indicator found")

    delete(destinationFile, recursive = true) // overwriting by default

    val headerFLEs = glob(sourceFolder + "/header")

    if (header && headerFLEs.isEmpty)
      fatal(s"Missing header file")
    else if (!header && headerFLEs.nonEmpty)
      fatal(s"Found unexpected header file")

    val partitions = partFilesOpt match {
      case None => glob(sourceFolder + "/part-*")
      case Some(files) => files.map(f => fileStatus(sourceFolder + "/" + f)).toArray
    }

    val partitionsInOrder = partitions.sortBy(part => getPartNumber(part.getPath))

    if (partitionsInOrder.length != numPartFilesExpected)
      fatal(s"Expected $numPartFilesExpected part files but found ${ partitionsInOrder.length }")

    val filesToMerge: Array[FileStatus] = headerFLEs ++ partitionsInOrder

    info(s"merging ${ filesToMerge.length } files totalling " +
      s"${ readableBytes(filesToMerge.map(_.getLen).sum) }...")

    val (_, dt) = time {
      copyMergeList(filesToMerge, destinationFile, deleteSource)
    }

    info(s"while writing:\n    $destinationFile\n  merge time: ${ formatTime(dt) }")

    if (deleteSource) {
      delete(sourceFolder, recursive = true)
      if (header)
        delete(sourceFolder + ".header", recursive = false)
    }
  }

  def copyMergeList(
    srcFileStatuses: Array[FileStatus],
    destFilename: String,
    deleteSource: Boolean = true
  ) {
    val isBGzip = BGZipCompressionCodec == getCodecFromPath(destFilename)

    require(srcFileStatuses.forall(_.getPath != destFilename))

    using(createNoCompression(destFilename)) { os =>
      var i = 0
      while (i < srcFileStatuses.length) {
        val fileStatus = srcFileStatuses(i)
        val lenAdjust: Long = if (isBGzip && i < srcFileStatuses.length - 1)
          -28
        else
          0
        using(openNoCompression(fileStatus.getPath)) { is =>
          hadoop.io.IOUtils.copyBytes(is, os,
            fileStatus.getLen + lenAdjust,
            false)
        }
        i += 1
      }
    }

    if (deleteSource) {
      srcFileStatuses.foreach { fileStatus =>
        delete(fileStatus.getPath, recursive = true)
      }
    }
  }

  def concatenateFiles(sourceNames: Array[String], destFilename: String): Unit = {
    val fileStatuses = sourceNames.map(fileStatus(_))

    info(s"merging ${ fileStatuses.length } files totalling " +
      s"${ readableBytes(fileStatuses.map(_.getLen).sum) }...")

    val (_, timing) = time(copyMergeList(fileStatuses, destFilename, deleteSource = false))

    info(s"while writing:\n    $destFilename\n  merge time: ${ formatTime(timing) }")
  }

  lazy val broadcast: BroadcastValue[FS] = HailContext.backend.broadcast(this)

  def getConfiguration(): Any

  def setConfiguration(config: Any): Unit

  private[this] def containsWildcard(path: String): Boolean = {
    var i = 0
    while (i < path.length) {
      val c = path(i)
      if (c == '\\') {
        i += 1
        if (i < path.length)
          i += 1
        else
          return false
      } else if (c == '*' || c == '{' || c == '?' || c == '[')
        return true

      i += 1
    }

    false
  }
}
