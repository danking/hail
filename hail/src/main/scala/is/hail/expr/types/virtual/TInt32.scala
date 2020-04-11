package is.hail.expr.types.virtual

import is.hail.annotations._
import is.hail.check.Arbitrary._
import is.hail.check.Gen

import scala.reflect.{ClassTag, _}

case object TInt32 extends TIntegral {
  def _toPretty = "Int32"

  override def pyString(sb: StringBuilder): Unit = {
    sb.append("int32")
  }

  def _typeCheck(a: Any): Boolean = a.isInstanceOf[Int]

  override def genNonmissingValue: Gen[Annotation] = arbitrary[Int]

  override def scalaClassTag: ClassTag[java.lang.Integer] = classTag[java.lang.Integer]

  val ordering: ExtendedOrdering =
    ExtendedOrdering.extendToNull(implicitly[Ordering[Int]])
}
