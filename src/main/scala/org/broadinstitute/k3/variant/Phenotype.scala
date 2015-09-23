package org.broadinstitute.k3.variant

object Phenotype extends Enumeration {
  type Phenotype = Value
  val Control = Value("1")
  val Case = Value("2")
}
