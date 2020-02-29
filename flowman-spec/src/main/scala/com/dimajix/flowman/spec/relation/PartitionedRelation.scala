/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.relation

import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.PartitionFieldSpec
import com.dimajix.flowman.spec.schema.PartitionSchema
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue




trait PartitionedRelationSpec { this: RelationSpec =>
    @JsonProperty(value = "partitions", required = false) protected var partitions: Seq[PartitionFieldSpec] = Seq()
}
