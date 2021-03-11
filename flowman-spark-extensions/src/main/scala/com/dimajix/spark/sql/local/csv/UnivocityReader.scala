/*
 * Copyright 2021 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.spark.sql.local.csv

import java.io.Reader
import java.math.BigDecimal
import java.text.NumberFormat
import java.util.Locale

import scala.util.Try
import scala.util.control.NonFatal

import com.univocity.parsers.csv.CsvParser
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String

import com.dimajix.spark.sql.RowParser
import com.dimajix.util.DateTimeUtils


class UnivocityReader(schema: StructType, val options:CsvOptions) {
    private val tokenizer = new CsvParser(options.asParserSettings)

    private val settings = options.asParserSettings
    settings.setHeaders(schema.fieldNames: _*)

    private val parser = new RowParser(schema,
        RowParser.Options(
            nullValue = options.nullValue,
            nanValue = options.nanValue,
            negativeInf = options.negativeInf,
            positiveInf = options.positiveInf,
            timeZone = options.timeZone,
            timestampFormat = options.timestampFormat,
            dateFormat = options.dateFormat,
            useInternalTypes = true,
            addExtraColumns = false,
            removeExtraColumns = false
        ))

    /**
      * Parses a single CSV string and turns it into either one resulting row or no row (if the
      * the record is malformed).
      */
    def parse(input: String): Row = parser.parse(tokenizer.parseLine(input))
}


object UnivocityReader {

    /**
      * Parses a stream that contains CSV strings and turns it into an iterator of tokens.
      */
    def tokenizeStream(
                          reader: Reader,
                          shouldDropHeader: Boolean,
                          tokenizer: CsvParser): Iterator[Array[String]] = {
        convertStream(reader, shouldDropHeader, tokenizer)(tokens => tokens)
    }

    private def convertStream[T](
                                    reader: Reader,
                                    shouldDropHeader: Boolean,
                                    tokenizer: CsvParser)(convert: Array[String] => T) = new Iterator[T] {
        tokenizer.beginParsing(reader)
        private var nextRecord = {
            if (shouldDropHeader) {
                tokenizer.parseNext()
            }
            tokenizer.parseNext()
        }

        override def hasNext: Boolean = nextRecord != null

        override def next(): T = {
            if (!hasNext) {
                throw new NoSuchElementException("End of stream")
            }
            val curRecord = convert(nextRecord)
            nextRecord = tokenizer.parseNext()
            curRecord
        }
    }

    /**
      * Parses an iterator that contains CSV strings and turns it into an iterator of rows.
      */
    def parseIterator(
                         lines: Iterator[String],
                         shouldDropHeader: Boolean,
                         parser: UnivocityReader): Iterator[Row] = {
        val options = parser.options

        val linesWithoutHeader = if (shouldDropHeader) {
            // Note that if there are only comments in the first block, the header would probably
            // be not dropped.
            CsvUtils.dropHeaderLine(lines, options)
        } else {
            lines
        }

        val filteredLines: Iterator[String] =
            CsvUtils.filterCommentAndEmpty(linesWithoutHeader, options)
        filteredLines.map(parser.parse)
    }
}
