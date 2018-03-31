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

package com.dimajix.flowman

import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.scalatest.mockito.MockitoSugar


trait MockedSparkSession extends BeforeAndAfterAll with MockitoSugar { this:Suite =>
    private var session: SparkSession = _
    var spark: SparkSession = _

    override def beforeAll() : Unit = {
        session = SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        session.sparkContext.setLogLevel("WARN")

        spark = mock[SparkSession]
        when(spark.sparkContext).thenReturn(session.sparkContext)
        when(spark.conf).thenReturn(session.conf)
    }
    override def afterAll() : Unit = {
        if (session != null) {
            session.stop()
            session = null
            spark = null
        }
    }
}
