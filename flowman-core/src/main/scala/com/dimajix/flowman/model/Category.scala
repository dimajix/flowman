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

package com.dimajix.flowman.model

import java.util.Locale


sealed abstract class Category extends Product with Serializable {
    def lower : String = toString.toLowerCase(Locale.ROOT)
    def upper : String = toString.toUpperCase(Locale.ROOT)
}

object Category {
    case object ASSERTION extends Category
    case object CONNECTION extends Category
    case object DATASET extends Category
    case object HOOK extends Category
    case object JOB extends Category
    case object MAPPING extends Category
    case object MEASURE extends Category
    case object RELATION extends Category
    case object SCHEMA extends Category
    case object TARGET extends Category
    case object TEMPLATE extends Category
    case object TEST extends Category
    case object ASSERTION_TEST extends Category
    case object METRIC_SINK extends Category
    case object METRIC_BOARD extends Category
    case object HISTORY_STORE extends Category
    case object EXTERNAL_CATALOG extends Category
    case object PROJECT_STORE extends Category
    case object DOCUMENTER extends Category
    case object DOCUMENTATION_COLLECTOR extends Category
    case object DOCUMENTATION_GENERATOR extends Category

    def ofString(category:String) : Category = {
        category.toLowerCase(Locale.ROOT) match {
            case "assertion" => ASSERTION
            case "assertion_test" => ASSERTION_TEST
            case "connection" => CONNECTION
            case "dataset" => DATASET
            case "hook" => HOOK
            case "job" => JOB
            case "mapping" => MAPPING
            case "measure" => MEASURE
            case "relation" => RELATION
            case "schema" => SCHEMA
            case "target" => TARGET
            case "template" => TEMPLATE
            case "test" => TEST
            case _ => throw new IllegalArgumentException(s"No such category $category")
        }
    }
}
