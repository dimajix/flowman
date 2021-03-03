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

package org.apache.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.util.DateTimeUtils


class SparkShimTest extends AnyFlatSpec with Matchers with LocalSparkSession{
    "The SparkShim" should "return a new Hadoop configuration" in {
        val conf = SparkShim.getHadoopConf(spark.sparkContext.getConf)
        conf should not be (null)
    }

    it should "parse CalendarIntervals" in {
        SparkShim.parseCalendarInterval("interval 2 hours") should be(SparkShim.calendarInterval(0, 0, 2 * DateTimeUtils.MICROS_PER_HOUR))
        SparkShim.parseCalendarInterval("interval 1 day") should be(SparkShim.calendarInterval(0, 1))
    }

    it should "support static configs" in {
        SparkShim.isStaticConf("spark.sql.warehouse.dir") should be (true)
        SparkShim.isStaticConf("spark.sql.autoBroadcastJoinThreshold") should be (false)
    }

    it should "find out if a relation supports multiple paths" in {
        SparkShim.relationSupportsMultiplePaths(spark, "csv") should be (true)
        SparkShim.relationSupportsMultiplePaths(spark, "jdbc") should be (false)
    }
}
