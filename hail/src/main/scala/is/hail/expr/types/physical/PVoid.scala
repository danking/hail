package is.hail.expr.types.physical
import is.hail.annotations.{CodeOrdering, Region}
import is.hail.asm4s.{Code, MethodBuilder, Value}
import is.hail.expr.ir.EmitMethodBuilder
import is.hail.expr.types.virtual.{TVoid, Type}

case object PVoid extends PType {
  def virtualType: Type = TVoid

  override val required = true

  def _asIdent = "void"

  override def _pretty(sb: StringBuilder, indent: Int, compact: Boolean): Unit = sb.append("PVoid")

  def codeOrdering(mb: EmitMethodBuilder, other: PType): CodeOrdering = null

  def copyFromType(mb: MethodBuilder, region: Value[Region], srcPType: PType, srcAddress: Code[Long], deepCopy: Boolean) =
    throw new UnsupportedOperationException("PVoid copyFromType is currently undefined")

  def copyFromType(region: Region, srcPType: PType, srcAddress: Long, deepCopy: Boolean) =
    throw new UnsupportedOperationException("PVoid copyFromType is currently undefined")

  def copyFromTypeAndStackValue(mb: MethodBuilder, region: Value[Region], srcPType: PType, stackValue: Code[_], deepCopy: Boolean): Code[_] =
    stackValue

  def constructAtAddress(mb: MethodBuilder, addr: Code[Long], region: Value[Region], srcPType: PType, srcAddress: Code[Long], deepCopy: Boolean): Code[Unit] =
    throw new NotImplementedError(s"$this is not constructable")

  def constructAtAddress(addr: Long, region: Region, srcPType: PType, srcAddress: Long, deepCopy: Boolean): Unit =
    throw new NotImplementedError(s"$this is not constructable")

  def setRequired(required: Boolean) = PVoid
}
