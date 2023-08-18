package is.hail.io.fs

import is.hail.utils._

import org.apache.hadoop
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}

import java.io._

class HadoopFileListEntry(fs: hadoop.fs.FileStatus) extends FileListEntry {
  val normalizedPath = fs.getPath

  def getPath: String = fs.getPath.toString

  def getActualUrl: String = fs.getPath.toString

  def getModificationTime: java.lang.Long = fs.getModificationTime

  def getLen: Long = fs.getLen

  def isDirectory: Boolean = fs.isDirectory

  def isFile: Boolean = fs.isFile

  def isSymlink: Boolean = fs.isSymlink

  def getOwner: String = fs.getOwner
}

object HadoopFS {
  def toPositionedOutputStream(os: FSDataOutputStream): PositionedOutputStream =
    new OutputStream with Positioned {
      private[this] var closed: Boolean = false

      override def write(i: Int): Unit = os.write(i)

      override def write(bytes: Array[Byte], off: Int, len: Int): Unit = os.write(bytes, off, len)

      override def flush(): Unit = if (!closed) os.flush()

      override def close(): Unit = {
        if (!closed) {
          os.close()
          closed = true
        }
      }

      def getPosition: Long = os.getPos
    }

  def toSeekableInputStream(is: FSDataInputStream): SeekableInputStream =
    new InputStream with Seekable {
      private[this] var closed: Boolean = false

      override def read(): Int = is.read()

      override def read(bytes: Array[Byte], off: Int, len: Int): Int = is.read(bytes, off, len)

      override def skip(n: Long): Long = is.skip(n)

      override def close(): Unit = {
        if (!closed) {
          is.close()
          closed = true
        }
      }

      def seek(pos: Long): Unit = is.seek(pos)

      def getPosition: Long = is.getPos
    }
}


case class LocalFSURL(val path: String) extends FSURL[LocalFSURL] {
  def addPathComponent(c: String): LocalFSURL = LocalFSURL(s"$path/$c")
  def withPath(newPath: String): LocalFSURL = {
    val uri = new java.net.URI(path)
    if (uri.getScheme == null) {
      LocalFSURL(newPath)
    } else {
      assert(uri.getScheme == "file")
      LocalFSURL("file://" + newPath)
    }
  }
  def getPath: String = {
    val splits = path.split("://")
    String.join("://", splits.slice(1, splits.length): _*)
  }
  def fromString(s: String): LocalFSURL = LocalFSURL(s)
  override def toString(): String = path
}

class HadoopFS(private[this] var conf: SerializableHadoopConfiguration) extends FS {
  type URL = LocalFSURL

  def validUrl(filename: String): Boolean = {
    val uri = new java.net.URI(filename)
    uri.getScheme == null || uri.getScheme == "file"
  }

  def parseUrl(filename: String): LocalFSURL = {
    LocalFSURL(filename)
  }

  def openNoCompression(filename: String): SeekableDataInputStream = {
    val fs = getFileSystem(filename)
    val hPath = new hadoop.fs.Path(filename)
    val is = try {
      fs.open(hPath)
    } catch {
      case e: FileNotFoundException =>
        if (isDir(filename))
          throw new FileNotFoundException(s"'$filename' is a directory (or native Table/MatrixTable)")
        else
          throw e
    }

    new WrappedSeekableDataInputStream(
      HadoopFS.toSeekableInputStream(is))
  }

  def createNoCompression(filename: String): PositionedDataOutputStream = {
    val fs = getFileSystem(filename)
    val hPath = new hadoop.fs.Path(filename)
    val os = fs.create(hPath)
    new WrappedPositionedDataOutputStream(
      HadoopFS.toPositionedOutputStream(os))
  }

  def getFileSystem(filename: String): hadoop.fs.FileSystem = {
    new hadoop.fs.Path(filename).getFileSystem(conf.value)
  }

  def delete(filename: String, recursive: Boolean) {
    getFileSystem(filename).delete(new hadoop.fs.Path(filename), recursive)
  }

  override def mkDir(dirname: String): Unit = {
    getFileSystem(dirname).mkdirs(new hadoop.fs.Path(dirname))
  }

  def listDirectory(filename: String): Array[FileListEntry] = {
    val fs = getFileSystem(filename)
    val hPath = new hadoop.fs.Path(filename)
    var statuses = fs.globStatus(hPath)
    if (statuses == null) {
      throw new FileNotFoundException(filename)
    } else {
      statuses.par.map(_.getPath)
        .flatMap(fs.listStatus(_))
        .map(new HadoopFileListEntry(_))
        .toArray
    }
  }

  override def glob(filename: String): Array[FileListEntry] = {
    val fs = getFileSystem(filename)
    val path = new hadoop.fs.Path(filename)

    var files = fs.globStatus(path)
    if (files == null)
      files = Array.empty
    log.info(s"globbing path $filename returned ${ files.length } files: ${ files.map(_.getPath.getName).mkString(",") }")
    files.map(fle => new HadoopFileListEntry(fle))
  }

  override def fileStatus(filename: String): FileStatus = {
    val fle = getFileListEntry(filename)
    if (fle.isDirectory) {
      throw new FileNotFoundException(fle.getPath)
    }
    fle
  }

  override def getFileListEntry(filename: String): FileListEntry = {
    val p = new hadoop.fs.Path(filename)
    new HadoopFileListEntry(p.getFileSystem(conf.value).getFileStatus(p))
  }

  def makeQualified(path: String): String = {
    val ppath = new hadoop.fs.Path(path)
    val pathFS = ppath.getFileSystem(conf.value)
    pathFS.makeQualified(ppath).toString
  }

  override def deleteOnExit(filename: String): Unit = {
    val ppath = new hadoop.fs.Path(filename)
    val pathFS = ppath.getFileSystem(conf.value)
    pathFS.deleteOnExit(ppath)
  }

  def supportsScheme(scheme: String): Boolean = {
    if (scheme == "") {
      true
    } else {
      try {
        hadoop.fs.FileSystem.getFileSystemClass(scheme, conf.value)
        true
      } catch {
        case e: hadoop.fs.UnsupportedFileSystemException => false
        case e: Exception => throw e
      }
    }
  }

  def getConfiguration(): SerializableHadoopConfiguration = conf

  def setConfiguration(_conf: Any): Unit = {
    conf = _conf.asInstanceOf[SerializableHadoopConfiguration]
  }
}
