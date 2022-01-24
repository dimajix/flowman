/*
 * Copyright 2019-2022 Kaya Kupferschmidt
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

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.transforms.schema.ColumnTree
import com.dimajix.flowman.transforms.schema.Path
import com.dimajix.flowman.transforms.schema.SchemaTree
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.StructType


object ProjectTransformer {
    case class Column(
        column:Path,
        name:Option[String]=None,
        dtype:Option[FieldType]=None
    )
}
final case class ProjectTransformer(
    columns:Seq[ProjectTransformer.Column]
) extends Transformer {
    /**
      * Perform a projection with renaming and retyping for the given input DataFrame
      * @param df
      * @return
      */
    override def transform(df:DataFrame) : DataFrame = {
        import ColumnTree.implicits._

        val tree = ColumnTree.ofSchema(df.schema)

        def col(spec:ProjectTransformer.Column) = {
            val input = tree.find(spec.column).get.mkValue()
            val typed = spec.dtype match {
                case None => input
                case Some(ft) => input.cast(ft.sparkType)
            }
            spec.name match {
                case None => typed
                case Some(name) => typed.as(name)
            }
        }

        df.select(columns.map(col):_*)
    }

    /**
      * Perform a projection with renaming and retyping for the given input schema
      * @param schema
      * @return
      */
    override def transform(schema:StructType) : StructType = {
        import SchemaTree.implicits._

        val tree = SchemaTree.ofSchema(schema)

        def col(spec:ProjectTransformer.Column) = {
            val input = tree.find(spec.column).get.mkValue()
            val typed = spec.dtype match {
                case None => input
                case Some(ft) => input.copy(ftype = ft)
            }
            spec.name match {
                case None => typed
                case Some(name) => typed.copy(name=name)
            }
        }

        StructType(columns.map(col))
    }
}
