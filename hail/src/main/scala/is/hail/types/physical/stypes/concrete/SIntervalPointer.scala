package is.hail.types.physical.stypes.concrete

import is.hail.annotations.{CodeOrdering, Region}
import is.hail.asm4s.{BooleanInfo, Code, IntInfo, LongInfo, Settable, SettableBuilder, TypeInfo, Value}
import is.hail.expr.ir.{EmitCodeBuilder, EmitMethodBuilder, IEmitCode, SortOrder}
import is.hail.types.physical.stypes.interfaces.SInterval
import is.hail.types.physical.stypes.{SCode, SType}
import is.hail.types.physical.{PCanonicalInterval, PCode, PInterval, PIntervalCode, PIntervalValue, PSettable, PType}
import is.hail.utils.FastIndexedSeq


case class SIntervalPointer(pType: PInterval) extends SInterval {
  def codeOrdering(mb: EmitMethodBuilder[_], other: SType, so: SortOrder): CodeOrdering = pType.codeOrdering(mb, other.pType, so)

  def coerceOrCopy(cb: EmitCodeBuilder, region: Value[Region], value: SCode, deepCopy: Boolean): SCode = {
    new SIntervalPointerCode(this, pType.store(cb, region, value, deepCopy))
  }

  def codeTupleTypes(): IndexedSeq[TypeInfo[_]] = FastIndexedSeq(LongInfo, IntInfo, IntInfo)

  def loadFrom(cb: EmitCodeBuilder, region: Value[Region], pt: PType, addr: Code[Long]): SCode = {
    pt match {
      case t: PCanonicalInterval if t.equalModuloRequired(this.pType) =>
        new SIntervalPointerCode(this, addr)
      case _ =>
        new SIntervalPointerCode(this, pType.store(cb, region, pt.loadCheapPCode(cb, addr), false))
    }
  }

  def fromSettables(settables: IndexedSeq[Settable[_]]): SIntervalPointerSettable = {
    val IndexedSeq(a: Settable[Long@unchecked], includesStart: Settable[Boolean@unchecked], includesEnd: Settable[Boolean@unchecked]) = settables
    assert(a.ti == LongInfo)
    assert(includesStart.ti == BooleanInfo)
    assert(includesEnd.ti == BooleanInfo)
    new SIntervalPointerSettable(this, a, includesStart, includesEnd)
  }

  def fromCodes(codes: IndexedSeq[Code[_]]): SIntervalPointerCode = {
    val IndexedSeq(a: Code[Long@unchecked]) = codes
    assert(a.ti == LongInfo)
    new SIntervalPointerCode(this, a)
  }
}


object SIntervalPointerSettable {
  def apply(sb: SettableBuilder, st: SIntervalPointer, name: String): SIntervalPointerSettable = {
    new SIntervalPointerSettable(st,
      sb.newSettable[Long](s"${ name }_a"),
      sb.newSettable[Boolean](s"${ name }_includes_start"),
      sb.newSettable[Boolean](s"${ name }_includes_end"))
  }
}

class SIntervalPointerSettable(
  val st: SIntervalPointer,
  a: Settable[Long],
  val includesStart: Settable[Boolean],
  val includesEnd: Settable[Boolean]
) extends PIntervalValue with PSettable {
  def get: PIntervalCode = new SIntervalPointerCode(st, a)

  val pt: PInterval = st.pType

  def settableTuple(): IndexedSeq[Settable[_]] = FastIndexedSeq(a, includesStart, includesEnd)

  def loadStart(cb: EmitCodeBuilder): IEmitCode =
    IEmitCode(cb,
      !(pt.startDefined(a)),
      pt.pointType.loadCheapPCode(cb, pt.loadStart(a)))

  def startDefined(cb: EmitCodeBuilder): Code[Boolean] = pt.startDefined(a)

  def loadEnd(cb: EmitCodeBuilder): IEmitCode =
    IEmitCode(cb,
      !(pt.endDefined(a)),
      pt.pointType.loadCheapPCode(cb, pt.loadEnd(a)))

  def endDefined(cb: EmitCodeBuilder): Code[Boolean] = pt.endDefined(a)

  def store(cb: EmitCodeBuilder, pc: PCode): Unit = {
    cb.assign(a, pc.asInstanceOf[SIntervalPointerCode].a)
    cb.assign(includesStart, pt.includesStart(a.load()))
    cb.assign(includesEnd, pt.includesEnd(a.load()))
  }

  // FIXME orderings should take emitcodes/iemitcodes
  def isEmpty(cb: EmitCodeBuilder): Code[Boolean] = {
    val gt = cb.emb.getCodeOrdering(pt.pointType, CodeOrdering.Gt())
    val gteq = cb.emb.getCodeOrdering(pt.pointType, CodeOrdering.Gteq())

    val start = cb.memoize(loadStart(cb), "start")
    val end = cb.memoize(loadEnd(cb), "end")
    includesStart && includesEnd.mux(
      gt((start.m, start.v), (end.m, end.v)),
      gteq((start.m, start.v), (end.m, end.v))
    )
  }

}

class SIntervalPointerCode(val st: SIntervalPointer, val a: Code[Long]) extends PIntervalCode {
  override def pt: PInterval = st.pType

  def code: Code[_] = a

  def codeTuple(): IndexedSeq[Code[_]] = FastIndexedSeq(a)

  def includesStart(): Code[Boolean] = pt.includesStart(a)

  def includesEnd(): Code[Boolean] = pt.includesEnd(a)

  def memoize(cb: EmitCodeBuilder, name: String, sb: SettableBuilder): PIntervalValue = {
    val s = SIntervalPointerSettable(sb, st, name)
    cb.assign(s, this)
    s
  }

  def memoize(cb: EmitCodeBuilder, name: String): PIntervalValue = memoize(cb, name, cb.localBuilder)

  def memoizeField(cb: EmitCodeBuilder, name: String): PIntervalValue = memoize(cb, name, cb.fieldBuilder)
}
