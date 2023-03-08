/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.graph

import java.util.Locale


sealed abstract class Category extends Product with Serializable {
    def lower : String = toString.toLowerCase(Locale.ROOT)
    def upper : String = toString.toUpperCase(Locale.ROOT)
}

object Category {
    case object MAPPING extends Category
    case object MAPPING_OUTPUT extends Category
    case object COLUMN extends Category
    case object RELATION extends Category
    case object TARGET extends Category

    def ofString(category:String) : Category = {
        category.toLowerCase(Locale.ROOT) match {
            case "mapping" => MAPPING
            case "mapping_output" => MAPPING_OUTPUT
            case "column" => COLUMN
            case "relation" => RELATION
            case "target" => TARGET
            case _ => throw new IllegalArgumentException(s"No such category $category")
        }
    }
}
