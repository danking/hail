package is.hail.services.tcp

import java.io.{Closeable, DataInputStream}
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
    val connectionIdMostSignificant = in.readLong()
    val connectionIdLeastSignificant = in.readLong()

    (new UUID(connectionIdMostSignificant, connectionIdLeastSignificant), s)
  }

  def close(): Unit = {
    ss.close()
  }

}
