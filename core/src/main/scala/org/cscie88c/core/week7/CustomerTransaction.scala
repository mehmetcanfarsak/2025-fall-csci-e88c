package org.cscie88c.core.week7

import scala.io.Source
import scala.util.{Try, Success, Failure}

final case class CustomerTransaction(
    customerId: String,
    transactionDate: String,
    transactionAmount: Double
)

object CustomerTransaction {
  // add companion object methods below
}
