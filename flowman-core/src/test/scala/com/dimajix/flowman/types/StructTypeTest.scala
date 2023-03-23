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

package com.dimajix.flowman.types

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class StructTypeTest extends AnyFlatSpec with Matchers {
    "A StructType" should "provide the correct SQL type (1)" in {
        val ftype = StructType(Seq())
        ftype.sqlType should be ("struct<>")
        ftype.sparkType should be (org.apache.spark.sql.types.StructType(Seq()))
    }

    it should "provide the correct SQL type (2)" in {
        val ftype = StructType(Seq(Field("f0", StringType)))
        ftype.sqlType should be ("struct<f0:string>")
    }

    it should "provide the correct SQL type (3)" in {
        val ftype = StructType(Seq(Field("f0", StringType), Field("f1", BooleanType)))
        ftype.sqlType should be ("struct<f0:string,f1:boolean>")
    }
}
