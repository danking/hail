package is.hail.services.tcp

import java.net.Socket
import java.util.UUID

import is.hail.services.UserInfo
import org.apache.log4j.{LogManager, Logger}

class TCPConnection(
  val userInfo: UserInfo,
  val connectionId: UUID,
  val s: Socket
) {
  val log: Logger = LogManager.getLogger(s"TCPConnection(${userInfo.email}, $connectionId, ${s.getInetAddress})")
}
