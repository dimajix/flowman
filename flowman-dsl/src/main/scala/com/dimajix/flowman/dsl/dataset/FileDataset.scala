/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.dsl.dataset

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.DatasetGen
import com.dimajix.flowman.dsl.SchemaGen
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.spec.dataset


case class FileDataset (
    location:Path,
    format:String,
    options:Map[String,String] = Map(),
    schema:Option[SchemaGen] = None
) extends DatasetGen {
    override def instantiate(context: Context): dataset.FileDataset = {
        dataset.FileDataset(
            Dataset.Properties(context),
            location,
            format,
            options,
            schema.map(_.instantiate(context))
        )
    }
}
