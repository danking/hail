package is.hail.expr.types.physical

import is.hail.annotations.Region
import is.hail.asm4s.{Code, MethodBuilder, Value}
case object PCanonicalStringOptional extends PCanonicalString(false)
case object PCanonicalStringRequired extends PCanonicalString(true)

class PCanonicalString(val required: Boolean) extends PString {
  def _asIdent = "string"

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean): Unit = sb.append("PCString")

  override def byteSize: Long = 8

  lazy val binaryFundamentalType: PBinary = PBinary(required)

  def copyFromType(mb: MethodBuilder, region: Value[Region], srcPType: PType, srcAddress: Code[Long], deepCopy: Boolean): Code[Long] = {
    this.fundamentalType.copyFromType(
      mb, region, srcPType.asInstanceOf[PString].fundamentalType, srcAddress, deepCopy
    )
  }

  def copyFromTypeAndStackValue(mb: MethodBuilder, region: Value[Region], srcPType: PType, stackValue: Code[_], deepCopy: Boolean): Code[_] =
    this.copyFromType(mb, region, srcPType, stackValue.asInstanceOf[Code[Long]], deepCopy)

  def copyFromType(region: Region, srcPType: PType, srcAddress: Long, deepCopy: Boolean): Long  = {
    this.fundamentalType.copyFromType(
      region, srcPType.asInstanceOf[PString].fundamentalType, srcAddress, deepCopy
    )
  }

  override def containsPointers: Boolean = true

  def bytesOffset(boff: Long): Long =
    this.fundamentalType.bytesOffset(boff)

  def bytesOffset(boff: Code[Long]): Code[Long] =
    this.fundamentalType.bytesOffset(boff)

  def loadLength(boff: Long): Int =
    this.fundamentalType.loadLength(boff)

  def loadLength(boff: Code[Long]): Code[Int] =
    this.fundamentalType.loadLength(boff)

  def loadString(bAddress: Long): String =
    new String(this.fundamentalType.loadBytes(bAddress))

  def loadString(bAddress: Code[Long]): Code[String] =
    Code.newInstance[String, Array[Byte]](this.fundamentalType.loadBytes(bAddress))

  def allocateAndStoreString(region: Region, str: String): Long = {
    val byteRep = str.getBytes()
    val dstAddrss = this.fundamentalType.allocate(region, byteRep.length)
    this.fundamentalType.store(dstAddrss, byteRep)
    dstAddrss
  }

  def allocateAndStoreString(mb: MethodBuilder, region: Value[Region], str: Code[String]): Code[Long] = {
    val dstAddress = mb.newField[Long]
    val byteRep = mb.newField[Array[Byte]]
    Code(
      byteRep := str.invoke[Array[Byte]]("getBytes"),
      dstAddress := fundamentalType.allocate(region, byteRep.length),
      fundamentalType.store(dstAddress, byteRep),
      dstAddress)
  }

  def constructAtAddress(mb: MethodBuilder, addr: Code[Long], region: Value[Region], srcPType: PType, srcAddress: Code[Long], deepCopy: Boolean): Code[Unit] =
    fundamentalType.constructAtAddress(mb, addr, region, srcPType.fundamentalType, srcAddress, deepCopy)

  def constructAtAddress(addr: Long, region: Region, srcPType: PType, srcAddress: Long, deepCopy: Boolean): Unit =
    fundamentalType.constructAtAddress(addr, region, srcPType.fundamentalType, srcAddress, deepCopy)

  def setRequired(required: Boolean) = if(required == this.required) this else PCanonicalString(required)
}

object PCanonicalString {
  def apply(required: Boolean = false): PCanonicalString = if (required) PCanonicalStringRequired else PCanonicalStringOptional

  def unapply(t: PString): Option[Boolean] = Option(t.required)
}
