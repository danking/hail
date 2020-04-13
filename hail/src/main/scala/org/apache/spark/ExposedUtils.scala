package org.apache.spark

import org.apache.spark.serializer._
import org.apache.spark.util._

import scala.reflect._

object ExposedUtils {
  def clone[T: ClassTag](value: T, sc: SparkContext): T =
    clone(value, sc.env.serializer.newInstance())

  def clone[T: ClassTag](value: T, serializer: SerializerInstance): T =
    Utils.clone(value, serializer)
}
