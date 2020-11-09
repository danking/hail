package is.hail.services.tcp

import java.io.{Closeable, DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.UUID

import is.hail.services.tls.getSSLContext
import javax.net.ssl.SSLServerSocket

class ServerSocket(port: Int) extends Closeable {
  private[this] val ss = {
    val ssl = getSSLContext
    val ssf = ssl.getServerSocketFactory()
    ssf.createServerSocket(port).asInstanceOf[SSLServerSocket]
  }

  def accept(): (UUID, Socket) = {
    val s = ss.accept()
    val in = new DataInputStream(s.getInputStream)
    val out = new DataOutputStream(s.getOutputStream())

    val sessionId = new Array[Byte](32)
    in.read(sessionId)
    in.skipBytes(32)

    val uuid = UUID.randomUUID()
    out.writeLong(uuid.getMostSignificantBits)
    out.writeLong(uuid.getLeastSignificantBits)
    (uuid, s)
  }

  def close(): Unit = {
    ss.close()
  }

}
