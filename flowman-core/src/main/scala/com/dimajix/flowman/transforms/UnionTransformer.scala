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
import org.apache.spark.sql.functions.lit

import com.dimajix.common.SetIgnoreCase
import com.dimajix.flowman.{types => ftypes}


final case class UnionTransformer() {
    /**
      * Transform a Spark DataFrame
      * @param input
      * @return
      */
    def transformDataFrames(input:Seq[DataFrame]) : DataFrame = {
        val schemas = input.map(df => ftypes.StructType(ftypes.Field.of(df.schema)))
        val allColumns = transformSchemas(schemas).fields.map(_.sparkField)

        val projectedDf = input.map { df =>
            // Get set of available field names
            val fields = SetIgnoreCase(df.schema.fields.map(_.name))
            // Either select corresponding field or NULL
            df.select(allColumns.map(col =>
                if (fields.contains(col.name))
                    df(col.name).cast(col.dataType)
                else
                    lit(null).cast(col.dataType).as(col.name, col.metadata)
            ):_*)
        }

        // Union all DataFrames into the result
        projectedDf.reduce(_.union(_))
    }

    /**
      * Transform a Flowman schema
      * @param input
      * @return
      */
    def transformSchemas(input:Seq[ftypes.StructType]) : ftypes.StructType = {
        ftypes.SchemaUtils.union(input)
    }
}
