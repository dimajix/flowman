/*
 * Copyright 2020 Kaya Kupferschmidt
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
import org.apache.commons.lang3.StringUtils
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

    def showTable(records:Seq[Product], columns:Seq[String]) : Unit = {
        println(showTableString(records, columns))
    }

    def showTableString(records:Seq[Product], columns:Seq[String]) : String = {
        def toString(value:Any) : String = {
            value match {
                case seq:Seq[_] => seq.mkString(",")
                case map:Map[_,_] => map.map(kv => kv._1.toString + "=" + kv._2.toString).mkString(",")
                case x:Option[_] => if (x.isEmpty) "" else x.get.toString
                case x:Any => x.toString
            }
        }

        val stringRecords = records.map(_.productIterator.map(toString).toList)
        val columnWidths =
            stringRecords.foldLeft(columns.map(_.length)) { (l,r) =>
                l.zip(r.map(_.length))
                    .map(lr => scala.math.max(lr._1, lr._2))
            }
            .toArray

        val rows = Seq(columns) ++ stringRecords
        val paddedRows = rows.map { row =>
            row.zipWithIndex.map { case (cell, i) =>
                StringUtils.leftPad(cell, columnWidths(i))
            }
        }

        // Create SeparatorLine
        val sb = new StringBuilder
        val sep = columnWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

        // column names
        paddedRows.head.addString(sb, "|", "|", "|\n")
        sb.append(sep)

        // data
        paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
        sb.append(sep)
        sb.toString()
    }
}
