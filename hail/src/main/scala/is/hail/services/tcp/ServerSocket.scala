package is.hail.services.tcp

import java.io.{Closeable, DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.UUID

import is.hail.services.{DeployConfig, Requester}
import is.hail.services.tls.getSSLContext
import javax.net.ssl.SSLServerSocket
import org.apache.http.client.methods.HttpGet

class ServerSocket(port: Int) extends Closeable {
  private[this] val ss = {
    val ssl = getSSLContext
    val ssf = ssl.getServerSocketFactory()
    ssf.createServerSocket(port).asInstanceOf[SSLServerSocket]
  }

  private[this] val auth = new Requester("auth")
  private[this] val authUrl = DeployConfig.get.baseUrl("batch")

  def accept(): (UUID, Socket) = {
    val s = ss.accept()
    val in = new DataInputStream(s.getInputStream)
    val out = new DataOutputStream(s.getOutputStream())

    val sessionId = new Array[Byte](32)
    in.read(sessionId)
    in.skipBytes(32)  // internal auth is only for routers and gateways

    auth.request(new HttpGet(s"$authUrl))

    val uuid = UUID.randomUUID()
    out.writeLong(uuid.getMostSignificantBits)
    out.writeLong(uuid.getLeastSignificantBits)
    (uuid, s)
  }

  def close(): Unit = {
    ss.close()
  }

}
