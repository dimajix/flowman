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

package com.dimajix.spark.sources.sequencefile

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.ShortWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.ShortType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import com.dimajix.flowman.hadoop.SerializableConfiguration


case class WritableConverter[W <: org.apache.hadoop.io.Writable,V](
    writable:Class[W],
    value:Class[V],
    converter:Writable => Any,
    writableExtractor:InternalRow => Writable,
    writableFactory:() => Writable
)


object WritableConverter {
    def of(fieldType:DataType, idx:Int) : WritableConverter[_,_] = {
        fieldType match {
            case ShortType => WritableConverter(
                classOf[ShortWritable],
                classOf[Short],
                (w:Writable) => w.asInstanceOf[ShortWritable].get(),
                (row:InternalRow) => if (row.isNullAt(idx)) new ShortWritable() else  new ShortWritable(row.getShort(idx)),
                () => new ShortWritable()
            )
            case IntegerType => WritableConverter(
                classOf[IntWritable],
                classOf[Int],
                (w:Writable) => w.asInstanceOf[IntWritable].get(),
                (row:InternalRow) => if (row.isNullAt(idx)) new IntWritable() else  new IntWritable(row.getInt(idx)),
                () => new IntWritable()
            )
            case LongType => WritableConverter(
                classOf[LongWritable],
                classOf[Long],
                (w:Writable) => w.asInstanceOf[LongWritable].get(),
                (row:InternalRow) => if (row.isNullAt(idx)) new LongWritable() else  new LongWritable(row.getLong(idx)),
                () => new LongWritable()
            )
            case FloatType => WritableConverter(
                classOf[FloatWritable],
                classOf[Float],
                (w:Writable) => w.asInstanceOf[FloatWritable].get(),
                (row:InternalRow) => if (row.isNullAt(idx)) new FloatWritable() else  new FloatWritable(row.getFloat(idx)),
                () => new FloatWritable()
            )
            case DoubleType => WritableConverter(
                classOf[DoubleWritable],
                classOf[Double],
                (w:Writable) => w.asInstanceOf[DoubleWritable].get(),
                (row:InternalRow) => if (row.isNullAt(idx)) new DoubleWritable() else  new DoubleWritable(row.getDouble(idx)),
                () => new DoubleWritable()
            )
            case StringType => WritableConverter(
                classOf[Text],
                classOf[String],
                (w:Writable) => UTF8String.fromBytes(w.asInstanceOf[Text].getBytes),
                (row:InternalRow) => if (row.isNullAt(idx)) new Text() else  new Text(row.getString(idx)),
                () => new Text()
            )
            case BinaryType => WritableConverter(
                classOf[BytesWritable],
                classOf[Array[Byte]],
                (w:Writable) => w.asInstanceOf[BytesWritable].copyBytes(),
                (row:InternalRow) => if (row.isNullAt(idx)) new BytesWritable() else new BytesWritable(row.getBinary(idx)),
                () => new BytesWritable()
            )
            case NullType => WritableConverter(
                classOf[NullWritable],
                classOf[Null],
                (_:Writable) => null,
                (_:InternalRow) => NullWritable.get(),
                () => NullWritable.get()
            )
            case _ => throw new UnsupportedOperationException(s"Data type $fieldType not supported")
        }
    }
}


class SequenceFileOptions(
        val config:SerializableConfiguration,
        @transient val parameters: CaseInsensitiveMap[String],
        val dataSchema:StructType,
        val requiredSchema:StructType) extends Serializable {

    require(config != null)
    require(parameters != null)
    require(dataSchema != null)
    require(requiredSchema != null)

    if (dataSchema.fields.length > 2)
        throw new IllegalArgumentException("SequenceFiles can only read or write up to two columns")
    if (requiredSchema.fields.length > 2)
        throw new IllegalArgumentException("SequenceFiles can only read or write up to two columns")

    def this(config:Configuration, parameters: Map[String, String], dataSchema:StructType, requiredSchema:StructType) = {
        this(new SerializableConfiguration(config), CaseInsensitiveMap(parameters), dataSchema, requiredSchema)
    }
    def this(config:Configuration, parameters: Map[String, String], dataSchema:StructType) = {
        this(new SerializableConfiguration(config), CaseInsensitiveMap(parameters), dataSchema, dataSchema)
    }

    def hadoopConf : Configuration = config.value

    def hasKey : Boolean = dataSchema.fields.length > 1
    def keyIdx : Int = if (hasKey) 0 else -1
    def valueIdx  : Int = if (hasKey) 1 else 0

    def keyName : String = if (hasKey) dataSchema.fields(0).name else ""
    def valueName : String = dataSchema.fields(valueIdx).name

    def keyType : DataType = if (hasKey) dataSchema.fields(0).dataType else NullType
    def valueType : DataType = dataSchema.fields(valueIdx).dataType

    def keyConverter : WritableConverter[_,_] = WritableConverter.of(keyType, keyIdx)
    def valueConverter : WritableConverter[_,_] = WritableConverter.of(valueType, valueIdx)

    def requireKeyIdx = if (hasKey) requiredSchema.fields.map(_.name).indexOf(keyName) else -1
    def requireValueIdx = requiredSchema.fields.map(_.name).indexOf(valueName)
}
