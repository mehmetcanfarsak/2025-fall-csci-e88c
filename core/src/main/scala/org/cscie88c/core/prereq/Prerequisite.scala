package org.cscie88c.core.prereq

import scala.io.Source
import scala.util.Using

object Prerequisite {

  // read lines from file and convert each row to CustomerTransaction record
  def fromCSV(csvFile: String): List[CustomerTransaction] = {
    Using.resource(Source.fromFile(csvFile)) { source =>
      source
        .getLines()
        .drop(1) // drop header
        .map(CustomerTransaction.apply)
        .toList
    }
  }

  // a function for returning a list retail transactions with amounts greater than $100
  def getFilteredTransactions(
      transactions: List[CustomerTransaction]
  ): List[CustomerTransaction] =
    transactions.filter(_.transactionAmount > 100)

  // a function for returning a tuple of month and year given a string of the form "day-month-year"
  def getMonthAndYear(date: String): (String, String) = {
    val month = date.split("-")(1)
    val year = date.split("-")(2)
    (month, year) // auto-tuple
  }

  // a function for returning the average transaction amount for a given list of transactions
  def getAverageTransactionAmount(
      transactions: List[CustomerTransaction]
  ): Double = {
    val total = transactions.map(_.transactionAmount).sum
    total / transactions.length
  }

  // a function for returning a map of average transaction amount by month and year
  def getAverageTransactionAmountByMonthAndYear(
      transactions: List[CustomerTransaction]
  ): Map[(String, String), Double] = {
    val groupedTransactions = transactions.groupBy { transaction =>
      getMonthAndYear(transaction.transactionDate)
    }

    groupedTransactions.map { case (monthYear, txns) =>
      val avgAmount = getAverageTransactionAmount(txns)
      (monthYear, avgAmount)
    }
  }
}
