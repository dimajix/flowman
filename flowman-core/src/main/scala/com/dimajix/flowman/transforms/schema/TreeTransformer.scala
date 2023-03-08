/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.transforms.schema

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.transforms.Transformer
import com.dimajix.flowman.types.StructType


abstract class TreeTransformer extends Transformer {
    import com.dimajix.flowman.transforms.schema.ColumnTree.implicits._
    import com.dimajix.flowman.transforms.schema.SchemaTree.implicits._

    def transform[T](root:Node[T])(implicit ops:NodeOps[T]) : Node[T]

    /**
      * Transforms the field names of a dataframe to the desired case format
      * @param df
      * @return
      */
    override def transform(df:DataFrame) : DataFrame = {
        val tree = ColumnTree.ofSchema(df.schema)
        val newTree = transform(tree)
        val columns = newTree.children.map(_.mkValue())
        df.select(columns:_*)
    }

    /**
      * Transforms the field names of a schema to the desired case format
      * @param schema
      * @return
      */
    override def transform(schema:StructType) : StructType = {
        val tree = SchemaTree.ofSchema(schema)
        val newTree = transform(tree)
        val columns = newTree.children.map(_.mkValue())
        StructType(columns)
    }
}
