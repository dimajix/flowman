/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

import com.univocity.parsers.csv.CsvParser
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.spark.sql.RowParser


class UnivocityReader(val schema: StructType, val options:CsvOptions) {
    private val tokenizer = new CsvParser(options.asParserSettings)

    //private val settings = options.asParserSettings
    //settings.setHeaders(schema.fieldNames: _*)

    private val parser = new RowParser(schema,
        RowParser.Options(
            nullValue = options.nullValue,
            nanValue = options.nanValue,
            negativeInf = options.negativeInf,
            positiveInf = options.positiveInf,
            timeZone = options.timeZone,
            timestampFormat = options.timestampFormat,
            dateFormat = options.dateFormat,
            addExtraColumns = options.addExtraColumns,
            removeExtraColumns = options.removeExtraColumns
        ))

    /**
      * Parses a single CSV string and turns it into either one resulting row or no row (if the
      * the record is malformed).
      */
    def parse(input: String): Row = parser.parse(tokenizer.parseLine(input), input)
}


object UnivocityReader {
    def inferSchema(lines: Iterator[String],
                    options: CsvOptions) : StructType = {
        val tokenizer = new CsvParser(options.asParserSettings)
        CsvUtils.skipComments(lines, options)
        if (!lines.hasNext)
            throw new IllegalArgumentException("File does not contain a header")

        val columns =
            if (options.headerFlag) {
                tokenizer.parseLine(lines.next()).toSeq
            }
            else {
                tokenizer.parseLine(lines.next()).indices.map(i => s"_${i+1}")
            }

        StructType(columns.map(c => StructField(c, StringType)))
    }

    /**
      * Parses an iterator that contains CSV strings and turns it into an iterator of rows.
      */
    def parseIterator(lines: Iterator[String], parser: UnivocityReader): Iterator[Row] = {
        val options = parser.options
        val shouldDropHeader = options.headerFlag

        val linesWithoutHeader = if (shouldDropHeader) {
            // Note that if there are only comments in the first block, the header would probably
            // be not dropped.
            CsvUtils.dropHeaderLine(lines, options)
        } else {
            lines
        }

        val filteredLines: Iterator[String] = CsvUtils.filterCommentAndEmpty(linesWithoutHeader, options)
        filteredLines.map(parser.parse)
    }
}
