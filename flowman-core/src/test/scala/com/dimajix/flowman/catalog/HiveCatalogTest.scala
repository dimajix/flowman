/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.catalog

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Prototype
import com.dimajix.spark.testing.LocalSparkSession


class HiveCatalogTest extends AnyFlatSpec with Matchers with MockFactory with LocalSparkSession {
    "The HiveCatalog" should "call external catalogs" in {
        val externalCatalog = mock[ExternalCatalog]
        val namespace = Namespace(
            "default",
            catalogs = Seq(Prototype.of(externalCatalog))
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .withNamespace(namespace)
            .build()

        (externalCatalog.createView _).expects(*)
        session.catalog.createView(TableIdentifier("testview_2"), "SELECT 1", ignoreIfExists = false)
    }

    it should "not propagate errors of external catalogs" in {
        val externalCatalog = mock[ExternalCatalog]
        val namespace = Namespace(
            "default",
            catalogs = Seq(Prototype.of(externalCatalog))
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .withNamespace(namespace)
            .withConfig("flowman.externalCatalog.ignoreErrors", "true")
            .build()

        (externalCatalog.createView _).expects(*).throws(new RuntimeException("error"))
        (externalCatalog.name _).expects().returns("catalog")
        (externalCatalog.kind _).expects().returns("my_catalog")
        session.catalog.createView(TableIdentifier("testview_3"), "SELECT 1", ignoreIfExists = false)
    }

    it should "propagate errors of external catalogs" in {
        val externalCatalog = mock[ExternalCatalog]
        val namespace = Namespace(
            "default",
            catalogs = Seq(Prototype.of(externalCatalog))
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .withNamespace(namespace)
            .withConfig("flowman.externalCatalog.ignoreErrors", "false")
            .build()

        (externalCatalog.createView _).expects(*).throws(new RuntimeException("error"))
        a[RuntimeException] should be thrownBy(session.catalog.createView(TableIdentifier("testview_1"), "SELECT 1", ignoreIfExists = false))
    }
}
