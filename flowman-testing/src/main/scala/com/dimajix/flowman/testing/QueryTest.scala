package com.dimajix.flowman.testing

import java.util.TimeZone

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.scalatest.Assertions


trait QueryTest { this:Assertions =>
    /**
      * Runs the plan and makes sure the answer matches the expected result.
      * If there was exception during the execution or the contents of the DataFrame does not
      * match the expected result, an error message will be returned. Otherwise, a [[None]] will
      * be returned.
      *
      * @param df the [[DataFrame]] to be executed
      * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
      */
    def checkAnswer(
                       df: DataFrame,
                       expectedAnswer: Seq[Row]): Unit = {
        //val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
        val result = try {
            val generated = df.collect().toSeq
            sameRows(expectedAnswer, generated, false).map { results =>
                s"""
                   |Results do not match for query:
                   |Timezone: ${TimeZone.getDefault}
                   |Timezone Env: ${sys.env.getOrElse("TZ", "")}
                   |
        |${df.queryExecution}
                   |== Results ==
                   |$results
       """.stripMargin
            }
        } catch {
            case e: Exception =>
                val errorMessage =
                    s"""
                       |Exception thrown while executing query:
                       |${df.queryExecution}
                       |== Exception ==
                       |$e
                       |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
                Some(errorMessage)
        }

        result match {
            case Some(errorMessage) => fail(errorMessage)
            case None =>
        }
    }

    def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
        checkAnswer(df, Seq(expectedAnswer))
    }

    def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit = {
        checkAnswer(df, expectedAnswer.collect())
    }

    def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
        // Converts data to types that we can do equality comparison using Scala collections.
        // For BigDecimal type, the Scala type has a better definition of equality test (similar to
        // Java's java.math.BigDecimal.compareTo).
        // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
        // equality test.
        val converted: Seq[Row] = answer.map(prepareRow)
        if (!isSorted) converted.sortBy(_.toString()) else converted
    }

    // We need to call prepareRow recursively to handle schemas with struct types.
    def prepareRow(row: Row): Row = {
        Row.fromSeq(row.toSeq.map {
            case null => null
            case d: java.math.BigDecimal => BigDecimal(d)
            // Convert array to Seq for easy equality checkJob.
            case b: Array[_] => b.toSeq
            case r: Row => prepareRow(r)
            case o => o
        })
    }

    def sideBySide(left: Seq[String], right: Seq[String]): Seq[String] = {
        val maxLeftSize = left.map(_.length).max
        val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
        val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

        leftPadded.zip(rightPadded).map {
            case (l, r) => (if (l == r) " " else "!") + l + (" " * ((maxLeftSize - l.length) + 3)) + r
        }
    }

    private def genError(expectedAnswer: Seq[Row],
                            sparkAnswer: Seq[Row],
                            isSorted: Boolean = false): String = {
        val getRowType: Option[Row] => String = row =>
            row.map(row =>
                if (row.schema == null) {
                    "struct<>"
                } else {
                    s"${row.schema.catalogString}"
                }).getOrElse("struct<>")

        s"""
           |== Results ==
           |${
            sideBySide(
                s"== Correct Answer - ${expectedAnswer.size} ==" +:
                    getRowType(expectedAnswer.headOption) +:
                    prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
                s"== Spark Answer - ${sparkAnswer.size} ==" +:
                    getRowType(sparkAnswer.headOption) +:
                    prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")
        }
    """.stripMargin
    }

    def sameRows(expectedAnswer: Seq[Row],
                    sparkAnswer: Seq[Row],
                    isSorted: Boolean = false): Option[String] = {
        if (prepareAnswer(expectedAnswer, isSorted) != prepareAnswer(sparkAnswer, isSorted)) {
            return Some(genError(expectedAnswer, sparkAnswer, isSorted))
        }
        None
    }
}
