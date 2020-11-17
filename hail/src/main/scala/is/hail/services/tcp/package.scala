package is.hail.services

import java.io.{DataInputStream, DataOutputStream, OutputStream}
import java.net.{ConnectException, Socket}

import is.hail.services.tls._
import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService
import java.util.{Base64, UUID}

package object tcp {
  def openConnection(service: String, port: Int): (UUID, Socket) = {
    val deployConfig = DeployConfig.get
    val ns = deployConfig.getServiceNamespace(service)
    deployConfig.location match {
      case "k8s" =>
        openDirectConnection(service, ns, port)
      case "gce" =>
        openProxiedConnection("hail", 5000, service, ns, port)
      case "external" =>
        openProxiedConnection("hail.is", 5000, service, ns, port)
    }
  }

  private[this] def openDirectConnection(service: String,
                                         ns: String,
                                         port: Int): (UUID, Socket) = {
    var s: Socket = null
    var in: DataInputStream = null
    var out: DataOutputStream = null
    var connected = false
    while (!connected) {
      s = socket(s"${service}.${ns}", port)
      in = new DataInputStream(s.getInputStream)
      out = new DataOutputStream(s.getOutputStream)

      writeSessionIds(ns, out)

      val isSuccess = in.read()
      if (isSuccess == -1) {
        log.info("first byte was end of file, retrying connection")
      } else if (isSuccess != 1) {
        throw new HailTCPConnectionError(s"${service}.${ns}:${port} ${isSuccess}")
      } else {
        connected = true
      }
    }

    val connectionIdMostSignificant = in.readLong()
    val connectionIdLeastSignificant = in.readLong()

    (new UUID(connectionIdMostSignificant, connectionIdLeastSignificant), s)
  }

  private[this] def openProxiedConnection(proxyHost: String,
                                          proxyPort: Int,
                                          service: String,
                                          ns: String,
                                          port: Int
                                         ): (UUID, Socket) = {
    var s: Socket = null
    var in: DataInputStream = null
    var out: DataOutputStream = null
    var connected = false
    while (!connected) {
      s = socket(proxyHost, proxyPort)
      in = new DataInputStream(s.getInputStream)
      out = new DataOutputStream(s.getOutputStream)

      writeSessionIds(ns, out)

      out.writeInt(ns.length)
      out.write(ns.getBytes(StandardCharsets.UTF_8))

      out.writeInt(service.length)
      out.write(service.getBytes(StandardCharsets.UTF_8))

      out.writeShort(port)
      out.flush()

      val isSuccess = in.read()
      if (isSuccess == -1) {
        log.info("first byte was end of file, retrying connection")
      } else if (isSuccess != 1) {
        throw new HailTCPConnectionError(s"${service}.${ns}:${port} ${isSuccess}")
      } else {
        connected = true
      }
    }

    val connectionIdMostSignificant = in.readLong()
    val connectionIdLeastSignificant = in.readLong()

    (new UUID(connectionIdMostSignificant, connectionIdLeastSignificant), s)
  }

  private[this] val sessionIdDecoder = Base64.getUrlDecoder
  def sessionIdDecodeFromStr(id: String): Array[Byte] = sessionIdDecoder.decode(id)

  private[this] val sessionIdEncoder = Base64.getUrlEncoder
  def sessionIdEncodeToStr(id: Array[Byte]): String = sessionIdEncoder.encodeToString(id)

  private[this] def writeSessionIds(ns: String, out: OutputStream): Unit = {
    val tokens = Tokens.get
    val defaultSessionId = tokens.namespaceToken("default")
    val defaultSessionIdBytes = sessionIdDecodeFromStr(defaultSessionId)
    assert(defaultSessionIdBytes.length == 32)
    out.write(defaultSessionIdBytes)
    if (ns != "default") {
      val namespacedSessionId = tokens.namespaceToken(ns)
      val namespacedSessionIdBytes = sessionIdDecodeFromStr(namespacedSessionId)
      assert(namespacedSessionIdBytes.length == 32)
      out.write(namespacedSessionIdBytes)
    } else {
      out.write(new Array[Byte](32))
    }
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
