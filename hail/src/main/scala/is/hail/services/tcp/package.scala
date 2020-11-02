package is.hail.services

import java.io.{DataInputStream, DataOutputStream}
import java.net.{ConnectException, Socket}

import is.hail.services.tls._
import java.nio.charset.StandardCharsets
import java.util.{Base64, UUID}

package object tcp {
  def openConnection(service: String, port: Int): (UUID, Socket) = {
    val deployConfig = DeployConfig.get
    val ns = deployConfig.getServiceNamespace(service)
    deployConfig.location match {
      case "k8s" =>
        val s = socket(s"${service}.${ns}", port)
        val uuid = UUID.randomUUID()
        val out = new DataOutputStream(s.getOutputStream())
        out.writeLong(uuid.getMostSignificantBits)
        out.writeLong(uuid.getLeastSignificantBits)
        (uuid, s)
      case "gce" =>
        proxySocket("hail", 5000, service, ns, port)
      case "external" =>
        proxySocket("hail.is", 5000, service, ns, port)
    }
  }

  def serverSocket(port: Int): ServerSocket = new ServerSocket(port)

  private[this] def proxySocket(proxyHost: String,
                                proxyPort: Int,
                                service: String,
                                ns: String,
                                port: Int
                               ): (UUID, Socket) = {
    val s = socket(proxyHost, proxyPort)
    val in = new DataInputStream(s.getInputStream)
    val out = new DataOutputStream(s.getOutputStream)
    val tokens = Tokens.get
    val defaultSessionId = tokens.namespaceToken("default")
    val defaultSessionIdBytes = Base64.getUrlDecoder.decode(defaultSessionId)
    assert(defaultSessionIdBytes.length == 32)
    out.write(defaultSessionIdBytes)
    if (ns != "default") {
      val namespacedSessionId = tokens.namespaceToken(ns)
      val namespacedSessionIdBytes = Base64.getUrlDecoder.decode(namespacedSessionId)
      assert(namespacedSessionIdBytes.length == 32)
      out.write(namespacedSessionIdBytes)
    } else {
      out.write(new Array[Byte](32))
    }
    out.writeInt(ns.length)
    out.write(ns.getBytes(StandardCharsets.UTF_8))
    out.writeInt(service.length)
    out.write(service.getBytes(StandardCharsets.UTF_8))
    out.writeShort(port)
    out.flush()
    if (in.read() != 1)
      throw new HailTCPProxyConnectionError(s"${service}.${ns}:${port}")
    val connectionIdMostSignificant = in.readLong()
    val connectionIdLeastSignificant = in.readLong()

    (new UUID(connectionIdMostSignificant, connectionIdLeastSignificant), s)
  }

  private[this] def socket(host: String, port: Int): Socket = {
    var s: Socket = null
    var attempts = 0
    while (s == null) {
      try {
        s = getSSLContext.getSocketFactory().createSocket(host, port)
      } catch {
        case e: ConnectException =>
          if (attempts % 10 == 0) {
            log.warn(s"retrying socket connect to ${host}:${port} after receiving ${e}")
          }
          attempts += 1
      }
    }
    assert(s != null)
    s
  }
}
