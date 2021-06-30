/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.spark

import scala.util.Try

import org.apache.spark.sql.types.DataType


object features {
    lazy val hiveSupported: Boolean = Try {
        org.apache.hadoop.hive.shims.ShimLoader.getMajorVersion
        true
    }.getOrElse(false)

    lazy val hiveVarcharSupported: Boolean = Try {
        DataType.fromJson("""{"type":"struct","fields":[{"name":"vc","type":"varchar(10)","nullable":true}]}""")
        true
    }.getOrElse(false)
}
