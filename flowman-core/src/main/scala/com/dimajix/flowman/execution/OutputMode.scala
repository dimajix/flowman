/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import java.util.Locale

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.streaming.{ OutputMode => StreamingMode }

sealed abstract class OutputMode extends Product with Serializable {
    def batchMode : SaveMode
    def streamMode : StreamingMode
}


object OutputMode {
    case object OVERWRITE extends OutputMode {
        override def batchMode: SaveMode = SaveMode.Overwrite
        override def streamMode : StreamingMode = StreamingMode.Complete()
    }
    case object OVERWRITE_DYNAMIC extends OutputMode {
        override def batchMode: SaveMode = SaveMode.Overwrite
        override def streamMode : StreamingMode = StreamingMode.Complete()
    }
    case object APPEND extends OutputMode {
        override def batchMode: SaveMode = SaveMode.Append
        override def streamMode : StreamingMode = StreamingMode.Append()
    }
    case object UPDATE extends OutputMode {
        override def batchMode: SaveMode = ???
        override def streamMode : StreamingMode = StreamingMode.Update()
    }
    case object IGNORE_IF_EXISTS extends OutputMode {
        override def batchMode: SaveMode = SaveMode.Ignore
        override def streamMode : StreamingMode = ???
    }
    case object ERROR_IF_EXISTS extends OutputMode {
        override def batchMode: SaveMode = SaveMode.ErrorIfExists
        override def streamMode : StreamingMode = ???
    }

    def ofString(mode:String) : OutputMode = {
        mode.toLowerCase(Locale.ROOT) match {
            case "overwrite" | "complete" => OutputMode.OVERWRITE
            case "overwrite_dynamic" | "dynamic_overwrite" => OutputMode.OVERWRITE_DYNAMIC
            case "append" => OutputMode.APPEND
            case "update"|"upsert" => OutputMode.UPDATE
            case "ignore" | "ignore_if_exists" | "ignoreifexists" => OutputMode.IGNORE_IF_EXISTS
            case "error" | "error_if_exists" | "errorifexists" | "default" => OutputMode.ERROR_IF_EXISTS
            case _ => throw new IllegalArgumentException(s"Unknown save mode: '$mode'. " +
                "Accepted save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists'.")
        }
    }
}
