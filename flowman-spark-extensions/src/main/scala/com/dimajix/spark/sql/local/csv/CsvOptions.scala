/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright (C) 2018 The Flowman Authors
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

import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.TimeZone

import com.univocity.parsers.csv.CsvFormat
import com.univocity.parsers.csv.CsvParserSettings
import com.univocity.parsers.csv.CsvWriterSettings
import com.univocity.parsers.csv.UnescapedQuoteHandling
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.ParseMode
import org.apache.spark.sql.catalyst.util.PermissiveMode


class CsvOptions(parameters:Map[String,String]) {
    private val defaultTimeZoneId = "UTC"

    private def getChar(paramName: String, default: Char): Char = {
        val paramValue = parameters.get(paramName)
        paramValue match {
            case None => default
            case Some(null) => default
            case Some(value) if value.length == 0 => '\u0000'
            case Some(value) if value.length == 1 => value.charAt(0)
            case _ => throw new RuntimeException(s"$paramName cannot be more than one character")
        }
    }
    private def getBool(paramName: String, default: Boolean = false): Boolean = {
        val param = parameters.getOrElse(paramName, default.toString)
        if (param == null) {
            default
        } else if (param.toLowerCase(Locale.ROOT) == "true") {
            true
        } else if (param.toLowerCase(Locale.ROOT) == "false") {
            false
        } else {
            throw new Exception(s"$paramName flag can be true or false")
        }
    }

    val parseMode: ParseMode =
        parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)
    val columnNameOfCorruptRecord =
        parameters.getOrElse("columnNameOfCorruptRecord", "")
    val delimiter = CsvUtils.toChar(
        parameters.getOrElse("sep", parameters.getOrElse("delimiter", ",")))
    val encoding = parameters.getOrElse("encoding",
        parameters.getOrElse("charset", StandardCharsets.UTF_8.name()))
    val newline = parameters.getOrElse("newline", "\n")
    val quote = getChar("quote", '\"')
    val escape = getChar("escape", '\\')
    val comment = getChar("comment", '\u0000')
    val addExtraColumns = getBool("addExtraColumns", true)
    val removeExtraColumns = getBool("removeExtraColumns", true)

    val quoteAll = getBool("quoteAll", false)
    val escapeQuotes = getBool("escapeQuotes", true)
    val ignoreLeadingWhiteSpaceFlag = getBool("ignoreLeadingWhiteSpace", true)
    val ignoreTrailingWhiteSpaceFlag = getBool("ignoreTrailingWhiteSpace", true)

    val headerFlag = getBool("header", false)
    val inferSchemaFlag = getBool("inferSchema", false)

    val nullValue = parameters.getOrElse("nullValue", "")
    val nanValue = parameters.getOrElse("nanValue", "NaN")
    val positiveInf = parameters.getOrElse("positiveInf", "Inf")
    val negativeInf = parameters.getOrElse("negativeInf", "-Inf")

    val isCommentSet = this.comment != '\u0000'

    val timeZone: TimeZone = DateTimeUtils.getTimeZone(
        parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

    // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
    val dateFormat: FastDateFormat =
        FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

    val timestampFormat: FastDateFormat =
        FastDateFormat.getInstance(
            parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)


    def asWriterSettings : CsvWriterSettings = {
        val format = new CsvFormat
        format.setLineSeparator(newline)
        format.setDelimiter(delimiter)
        format.setQuote(quote)
        format.setQuoteEscape(escape)
        val settings = new CsvWriterSettings
        settings.setFormat(format)
        settings.setQuoteAllFields(quoteAll)
        settings.setQuoteEscapingEnabled(escapeQuotes)
        settings.setSkipEmptyLines(true)
        settings.setNullValue(nullValue)
        settings.setEmptyValue(nullValue)
        settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceFlag)
        settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceFlag)
        settings
    }

    def asParserSettings: CsvParserSettings = {
        val settings = new CsvParserSettings()
        val format = settings.getFormat
        format.setDelimiter(delimiter)
        format.setQuote(quote)
        format.setQuoteEscape(escape)
        format.setComment(comment)
        settings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceFlag)
        settings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceFlag)
        settings.setReadInputOnSeparateThread(false)
        settings.setInputBufferSize(16384)
        //settings.setMaxColumns(maxColumns)
        settings.setNullValue(nullValue)
        //settings.setMaxCharsPerColumn(maxCharsPerColumn)
        settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_DELIMITER)
        settings
    }
}
