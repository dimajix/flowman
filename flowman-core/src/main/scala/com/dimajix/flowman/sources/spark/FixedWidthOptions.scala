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
 * Adapted for fixed width format 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.sources.spark

import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.TimeZone

import com.univocity.parsers.fixed.FieldAlignment
import com.univocity.parsers.fixed.FixedWidthFields
import com.univocity.parsers.fixed.FixedWidthWriterSettings
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.StructType


class FixedWidthOptions(
    @transient val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String)
    extends Serializable {

    def this(parameters: Map[String, String],defaultTimeZoneId: String) = {
        this(
            CaseInsensitiveMap(parameters),
            defaultTimeZoneId)
    }

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

    private def getInt(paramName: String, default: Int): Int = {
        val paramValue = parameters.get(paramName)
        paramValue match {
            case None => default
            case Some(null) => default
            case Some(value) => try {
                value.toInt
            } catch {
                case e: NumberFormatException =>
                    throw new RuntimeException(s"$paramName should be an integer. Found $value")
            }
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

    val charset: String = parameters.getOrElse("encoding",
        parameters.getOrElse("charset", StandardCharsets.UTF_8.name()))

    val headerFlag: Boolean = getBool("header")

    val alignment: FieldAlignment = FieldAlignment.valueOf(parameters.getOrElse("alignment", "left").toUpperCase(Locale.ROOT))

    // For write, both options were `true` by default. We leave it as `true` for
    // backwards compatibility.
    val ignoreLeadingWhiteSpaceFlagInWrite: Boolean = getBool("ignoreLeadingWhiteSpace", default = true)
    val ignoreTrailingWhiteSpaceFlagInWrite: Boolean = getBool("ignoreTrailingWhiteSpace", default = true)

    val nullValue: String = parameters.getOrElse("nullValue", "")
    val padding: Char = getChar("padding", '\0')

    val numbersLeadingZeros: Boolean = getBool("numbersLeadingZeros", default=false)
    val numbersPositiveSign: Boolean = getBool("numbersPositiveSign", default=false)

    val compressionCodec: Option[String] = {
        val name = parameters.get("compression").orElse(parameters.get("codec"))
        name.map(CompressionCodecs.getCodecClassName)
    }

    val timeZone: TimeZone = DateTimeUtils.getTimeZone(
        parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

    // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
    val dateFormat: FastDateFormat =
        FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

    val timestampFormat: FastDateFormat =
        FastDateFormat.getInstance(
            parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)

    val maxColumns: Int = getInt("maxColumns", 20480)

    val maxCharsPerColumn: Int = getInt("maxCharsPerColumn", -1)

    val inputBufferSize = 128

    def asWriterSettings(schema:StructType) : FixedWidthWriterSettings = {
        val fieldLengths = new FixedWidthFields
        schema.fields.foreach(field => fieldLengths.addField(field.name, field.metadata.getLong("size").toInt, alignment, padding))
        val writerSettings = new FixedWidthWriterSettings(fieldLengths)
        val format = writerSettings.getFormat
        //format.setLookupWildcard()
        format.setPadding(padding)
        writerSettings.setDefaultAlignmentForHeaders(alignment)
        //writerSettings.setUseDefaultPaddingForHeaders()
        writerSettings.setWriteLineSeparatorAfterRecord(true)
        writerSettings.setHeaderWritingEnabled(false)
        writerSettings.setMaxCharsPerColumn(maxCharsPerColumn)
        writerSettings.setMaxColumns(maxColumns)
        writerSettings.setIgnoreLeadingWhitespaces(ignoreLeadingWhiteSpaceFlagInWrite)
        writerSettings.setIgnoreTrailingWhitespaces(ignoreTrailingWhiteSpaceFlagInWrite)
        writerSettings.setNullValue(nullValue)
        writerSettings.setEmptyValue(nullValue)
        writerSettings.setSkipEmptyLines(true)
        writerSettings
    }
}
