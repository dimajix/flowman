/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.transforms

import com.dimajix.common.text.CaseUtils
import com.dimajix.flowman.transforms.schema.ArrayNode
import com.dimajix.flowman.transforms.schema.LeafNode
import com.dimajix.flowman.transforms.schema.MapNode
import com.dimajix.flowman.transforms.schema.Node
import com.dimajix.flowman.transforms.schema.NodeOps
import com.dimajix.flowman.transforms.schema.StructNode
import com.dimajix.flowman.transforms.schema.TreeTransformer


object CaseFormatter {
    val CAMEL_CASE = "camelCase"
    val CAMEL_CASE_UPPER = "camelCaseUpper"
    val SNAKE_CASE = "snakeCase"
    val SNAKE_CASE_UPPER = "snakeCaseUpper"

    val ALL_CASES = Seq(CAMEL_CASE, CAMEL_CASE_UPPER, SNAKE_CASE, SNAKE_CASE_UPPER)
}


case class CaseFormatter(format:String) extends TreeTransformer {
    import CaseFormatter._

    private val caseFormat = CaseUtils.joinCamel(CaseUtils.splitGeneric(format))
    if (!ALL_CASES.contains(caseFormat))
        throw new IllegalArgumentException(s"Case format '$format' not supported, please use one of ${ALL_CASES.mkString(",")}")

    private def rename(str:String) : String = {
        val words = CaseUtils.splitGeneric(str)
        caseFormat match {
            case CAMEL_CASE => CaseUtils.joinCamel(words)
            case CAMEL_CASE_UPPER => CaseUtils.joinCamel(words, true)
            case SNAKE_CASE => CaseUtils.joinSnake(words)
            case SNAKE_CASE_UPPER => CaseUtils.joinSnake(words, true)
        }
    }

    override def transform[T](root:Node[T])(implicit ops:NodeOps[T]) : Node[T] = {
        root.transform(node => node.withName(rename(node.name)))
    }
}
