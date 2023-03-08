/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.dsl.relation

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.RelationGen
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.relation


case class FileRelation(
    schema:Option[Prototype[Schema]] = None,
    partitions: Seq[PartitionField] = Seq(),
    location:Path,
    pattern:Option[String] = None,
    format:String = "csv"
) extends RelationGen {
    override def apply(props:Relation.Properties) : relation.FileRelation = {
        val context = props.context
        relation.FileRelation(
            props,
            schema.map(_.instantiate(context)),
            partitions,
            location,
            pattern,
            format
        )
    }
}
