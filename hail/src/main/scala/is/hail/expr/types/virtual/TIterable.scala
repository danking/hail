package is.hail.expr.types.virtual

import is.hail.utils.FastSeq

abstract class TIterable extends Type {
  def elementType: Type

  override def children = FastSeq(elementType)
}
