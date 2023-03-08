/*
 * Copyright (C) 2020 The Flowman Authors
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

package com.dimajix.flowman.util

import java.io.OutputStreamWriter

import com.univocity.parsers.csv.CsvWriter
import com.univocity.parsers.csv.CsvWriterSettings
import org.apache.spark.sql.DataFrame


object ConsoleUtils {
    def showDataFrame(df:DataFrame, limit: Int = 100, csv:Boolean=false) : Unit = {
        if (csv) {
            val result = df.limit(limit).collect()
            val writer = new OutputStreamWriter(Console.out)
            try {
                val csvWriter = new CsvWriter(writer, new CsvWriterSettings())
                csvWriter.writeHeaders(df.columns: _*)
                result.foreach { record =>
                    val fields = record.toSeq.map {
                        case null => null
                        case f => f.toString
                    }
                    csvWriter.writeRow(fields: _*)
                }
            }
            finally {
                writer.flush()
            }
        }
        else {
            df.show(limit)
        }
    }
}
