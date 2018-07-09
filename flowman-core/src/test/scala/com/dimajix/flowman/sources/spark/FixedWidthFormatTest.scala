/*
 * Adapted for fixed width format 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.sources.spark

import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.LocalSparkSession


class FixedWidthFormatTest extends FlatSpec with Matchers with LocalSparkSession {
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.S]").withZone(ZoneOffset.UTC)
    def parseDateTime(value:String) = new Timestamp(LocalDateTime.parse(value, formatter).toEpochSecond(ZoneOffset.UTC) * 1000l)

    val rows = Seq(
        Row("00word1", 1, false, 1.0f, 1.0, BigDecimal(12), parseDateTime("2018-03-20 11:34:10"), Date.valueOf("2001-03-12")),
        Row("01word2", -1, true, -1.0f, -1.0, BigDecimal(-12.23), parseDateTime("2018-03-20 11:34:10"), Date.valueOf("2001-03-12")),
        Row("02verylongword1", 12345678, true, 12345678.0f, 12345678.0, BigDecimal(12345678), parseDateTime("2018-03-20 11:34:10"), Date.valueOf("2001-03-12")),
        Row("03verylongword2", -12345678, true, -12345678.0f, -12345678.0, BigDecimal(-12345678), parseDateTime("2018-03-20 11:34:10"), Date.valueOf("2001-03-12"))
    )
    val schema = StructType(
        StructField("s", StringType, metadata = new MetadataBuilder().putLong("size", 8).build()) ::
            StructField("i", IntegerType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("b", BooleanType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("flt", FloatType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("dbl", DoubleType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("d", DecimalType(12,2), metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("ts", TimestampType, metadata = new MetadataBuilder().putLong("size", 26).build()) ::
            StructField("dt", DateType, metadata = new MetadataBuilder().putLong("size", 10).build()) ::
            Nil
    )

    "The FixedWidthFormat" should "support writing different data types" in {
        val spark = this.spark
        import spark.implicits._

        val df = spark.createDataFrame(sc.parallelize(rows), schema)
        df.write
            .format("fixedwidth")
            .option("padding", " ")
            .option("timeZone", "UTC")
            .save(tempDir + "/fixedwidth-01")
        val gen = spark.read
            .format("text")
            .load(tempDir + "/fixedwidth-01")
            .orderBy("value")
            .as[String]
            .collect()
        gen(0) should be ("00word1 1     false 1     1     12.00 2018-03-20T11:34:10.000Z  2001-03-12")
        gen(1) should be ("01word2 -1    true  -1    -1    -12.232018-03-20T11:34:10.000Z  2001-03-12")
        gen(2) should be ("02verylo123456true  1234561234561234562018-03-20T11:34:10.000Z  2001-03-12")
        gen(3) should be ("03verylo-12345true  -12345-12345-123452018-03-20T11:34:10.000Z  2001-03-12")
    }

    it should "optionally pad numbers with zeros" in {
        val spark = this.spark
        import spark.implicits._

        val df = spark.createDataFrame(sc.parallelize(rows), schema)
        df.write
            .format("fixedwidth")
            .option("padding", " ")
            .option("timeZone", "UTC")
            .option("numbersLeadingZeros", true)
            .save(tempDir + "/fixedwidth-02")
        val gen = spark.read
            .format("text")
            .load(tempDir + "/fixedwidth-02")
            .orderBy("value")
            .as[String]
            .collect()
        gen(0) should be ("00word1 000001false 000001000001012.002018-03-20T11:34:10.000Z  2001-03-12")
        gen(1) should be ("01word2 -00001true  -00001-00001-12.232018-03-20T11:34:10.000Z  2001-03-12")
        gen(2) should be ("02verylo123456true  1234561234561234562018-03-20T11:34:10.000Z  2001-03-12")
        gen(3) should be ("03verylo-12345true  -12345-12345-123452018-03-20T11:34:10.000Z  2001-03-12")
    }

    it should "optionally add plus signs to numbers" in {
        val spark = this.spark
        import spark.implicits._

        val df = spark.createDataFrame(sc.parallelize(rows), schema)
        df.write
            .format("fixedwidth")
            .option("padding", " ")
            .option("timeZone", "UTC")
            .option("numbersLeadingZeros", false)
            .option("numbersPositiveSign", true)
            .save(tempDir + "/fixedwidth-03")
        val gen = spark.read
            .format("text")
            .load(tempDir + "/fixedwidth-03")
            .orderBy("value")
            .as[String]
            .collect()
        gen(0) should be ("00word1 +1    false +1    +1    +12.002018-03-20T11:34:10.000Z  2001-03-12")
        gen(1) should be ("01word2 -1    true  -1    -1    -12.232018-03-20T11:34:10.000Z  2001-03-12")
        gen(2) should be ("02verylo+12345true  +12345+12345+123452018-03-20T11:34:10.000Z  2001-03-12")
        gen(3) should be ("03verylo-12345true  -12345-12345-123452018-03-20T11:34:10.000Z  2001-03-12")
    }

    it should "optionally add plus signs to numbers and pad them with zeros" in {
        val spark = this.spark
        import spark.implicits._

        val df = spark.createDataFrame(sc.parallelize(rows), schema)
        df.write
            .format("fixedwidth")
            .option("padding", " ")
            .option("timeZone", "UTC")
            .option("numbersLeadingZeros", true)
            .option("numbersPositiveSign", true)
            .save(tempDir + "/fixedwidth-04")
        val gen = spark.read
            .format("text")
            .load(tempDir + "/fixedwidth-04")
            .orderBy("value")
            .as[String]
            .collect()
        gen(0) should be ("00word1 +00001false +00001+00001+12.002018-03-20T11:34:10.000Z  2001-03-12")
        gen(1) should be ("01word2 -00001true  -00001-00001-12.232018-03-20T11:34:10.000Z  2001-03-12")
        gen(2) should be ("02verylo+12345true  +12345+12345+123452018-03-20T11:34:10.000Z  2001-03-12")
        gen(3) should be ("03verylo-12345true  -12345-12345-123452018-03-20T11:34:10.000Z  2001-03-12")
    }

    it should "use format specifiers in schema" in {
        val spark = this.spark
        import spark.implicits._

        val schema = StructType(
            StructField("s", StringType, metadata = new MetadataBuilder().putLong("size", 8).build()) ::
            StructField("i", IntegerType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("b", BooleanType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("flt", FloatType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("dbl", DoubleType, metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("d", DecimalType(12,2), metadata = new MetadataBuilder().putLong("size", 6).build()) ::
            StructField("ts", TimestampType, metadata = new MetadataBuilder().putLong("size", 16).putString("format", "yyyyMMdd'T'hhmmss").build()) ::
            StructField("dt", DateType, metadata = new MetadataBuilder().putLong("size", 8).putString("format", "yyyyMMdd").build()) ::
            Nil
        )

        val df = spark.createDataFrame(sc.parallelize(rows), schema)
        df.write
            .format("fixedwidth")
            .option("padding", " ")
            .option("timeZone", "UTC")
            .save(tempDir + "/fixedwidth-05")
        val gen = spark.read
            .format("text")
            .load(tempDir + "/fixedwidth-05")
            .orderBy("value")
            .as[String]
            .collect()
        gen(0) should be ("00word1 1     false 1     1     12.00 20180320T113410 20010312")
        gen(1) should be ("01word2 -1    true  -1    -1    -12.2320180320T113410 20010312")
        gen(2) should be ("02verylo123456true  12345612345612345620180320T113410 20010312")
        gen(3) should be ("03verylo-12345true  -12345-12345-1234520180320T113410 20010312")
    }
}
