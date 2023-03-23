/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.kernel

import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

import scala.collection.JavaConverters._

import com.google.protobuf.ByteString
import org.apache.spark.sql.Row

import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.Measurement
import com.dimajix.flowman.history.MetricSeries
import com.dimajix.flowman.history.TargetOrder
import com.dimajix.flowman.kernel.{proto => p}
import com.dimajix.flowman.{execution => exec}
import com.dimajix.flowman.{model => m}
import com.dimajix.flowman.{types => ft}


object RpcConverters {
    def toProto(id: m.ResourceIdentifier): p.ResourceIdentifier = {
        p.ResourceIdentifier.newBuilder()
            .setName(id.name)
            .setCategory(id.category)
            .putAllPartition(id.partition.asJava)
            .build()
    }

    def toProto(id: m.MappingIdentifier): p.MappingIdentifier = {
        val builder = p.MappingIdentifier.newBuilder()
            .setName(id.name)
        id.project.foreach(builder.setProject)
        builder.build()
    }

    def toModel(id: p.MappingIdentifier): m.MappingIdentifier = {
        m.MappingIdentifier(id.getName, Option(id.getProject).filter(_.nonEmpty))
    }

    def toProto(id: m.MappingOutputIdentifier): p.MappingOutputIdentifier = {
        val builder = p.MappingOutputIdentifier.newBuilder()
            .setName(id.name)
        id.project.foreach(builder.setProject)
        builder.build()
    }

    def toModel(id: p.MappingOutputIdentifier): m.MappingOutputIdentifier = {
        m.MappingOutputIdentifier(id.getName, id.getOutput, Option(id.getProject).filter(_.nonEmpty))
    }

    def toProto(id: m.RelationIdentifier): p.RelationIdentifier = {
        val builder = p.RelationIdentifier.newBuilder()
            .setName(id.name)
        id.project.foreach(builder.setProject)
        builder.build()
    }

    def toModel(id: p.RelationIdentifier): m.RelationIdentifier = {
        m.RelationIdentifier(id.getName, Option(id.getProject).filter(_.nonEmpty))
    }


    def toProto(id: m.TestIdentifier): p.TestIdentifier = {
        val builder = p.TestIdentifier.newBuilder()
            .setName(id.name)
        id.project.foreach(builder.setProject)
        builder.build()
    }

    def toModel(id: p.TestIdentifier): m.TestIdentifier = {
        m.TestIdentifier(id.getName, Option(id.getProject).filter(_.nonEmpty))
    }


    def toProto(id: m.TargetIdentifier): p.TargetIdentifier = {
        val builder = p.TargetIdentifier.newBuilder()
            .setName(id.name)
        id.project.foreach(builder.setProject)
        builder.build()
    }

    def toModel(id: p.TargetIdentifier): m.TargetIdentifier = {
        m.TargetIdentifier(id.getName, Option(id.getProject).filter(_.nonEmpty))
    }


    def toProto(id: m.JobIdentifier): p.JobIdentifier = {
        val builder = p.JobIdentifier.newBuilder()
            .setName(id.name)
        id.project.foreach(builder.setProject)
        builder.build()
    }

    def toModel(id: p.JobIdentifier): m.JobIdentifier = {
        m.JobIdentifier(id.getName, Option(id.getProject).filter(_.nonEmpty))
    }

    def toProto(id: m.ConnectionIdentifier): p.ConnectionIdentifier = {
        val builder = p.ConnectionIdentifier.newBuilder()
            .setName(id.name)
        id.project.foreach(builder.setProject)
        builder.build()
    }

    def toModel(id: p.ConnectionIdentifier): m.ConnectionIdentifier = {
        m.ConnectionIdentifier(id.getName, Option(id.getProject).filter(_.nonEmpty))
    }

    def toModel(phase: p.ExecutionPhase) : exec.Phase = {
        phase match {
            case p.ExecutionPhase.VALIDATE => exec.Phase.VALIDATE
            case p.ExecutionPhase.CREATE => exec.Phase.CREATE
            case p.ExecutionPhase.BUILD => exec.Phase.BUILD
            case p.ExecutionPhase.VERIFY => exec.Phase.VERIFY
            case p.ExecutionPhase.TRUNCATE => exec.Phase.TRUNCATE
            case p.ExecutionPhase.DESTROY => exec.Phase.DESTROY
            case p => throw new IllegalArgumentException(s"Unknown execution phase ${p}")
        }
    }
    def toProto(phase: exec.Phase) : p.ExecutionPhase = {
        phase match {
            case exec.Phase.VALIDATE => p.ExecutionPhase.VALIDATE
            case exec.Phase.CREATE => p.ExecutionPhase.CREATE
            case exec.Phase.BUILD => p.ExecutionPhase.BUILD
            case exec.Phase.VERIFY => p.ExecutionPhase.VERIFY
            case exec.Phase.TRUNCATE => p.ExecutionPhase.TRUNCATE
            case exec.Phase.DESTROY => p.ExecutionPhase.DESTROY
        }
    }

    def toModel(status:p.ExecutionStatus) : exec.Status = {
        status match {
            case p.ExecutionStatus.UNKNOWN_STATUS => exec.Status.UNKNOWN
            case p.ExecutionStatus.RUNNING => exec.Status.RUNNING
            case p.ExecutionStatus.SUCCESS => exec.Status.SUCCESS
            case p.ExecutionStatus.SUCCESS_WITH_ERRORS => exec.Status.SUCCESS_WITH_ERRORS
            case p.ExecutionStatus.FAILED => exec.Status.FAILED
            case p.ExecutionStatus.ABORTED => exec.Status.ABORTED
            case p.ExecutionStatus.SKIPPED => exec.Status.SKIPPED
        }
    }
    def toProto(status: exec.Status) : p.ExecutionStatus = {
        status match {
            case exec.Status.UNKNOWN => p.ExecutionStatus.UNKNOWN_STATUS
            case exec.Status.RUNNING => p.ExecutionStatus.RUNNING
            case exec.Status.SUCCESS => p.ExecutionStatus.SUCCESS
            case exec.Status.SUCCESS_WITH_ERRORS => p.ExecutionStatus.SUCCESS_WITH_ERRORS
            case exec.Status.FAILED => p.ExecutionStatus.FAILED
            case exec.Status.ABORTED => p.ExecutionStatus.ABORTED
            case exec.Status.SKIPPED => p.ExecutionStatus.SKIPPED
        }
    }

    def toModel(order:p.history.JobOrder) : JobOrder = {
        order match {
            case p.history.JobOrder.JOB_BY_DATETIME => JobOrder.BY_DATETIME
            case p.history.JobOrder.JOB_BY_PROJECT => JobOrder.BY_PROJECT
            case p.history.JobOrder.JOB_BY_NAME => JobOrder.BY_NAME
            case p.history.JobOrder.JOB_BY_ID => JobOrder.BY_ID
            case p.history.JobOrder.JOB_BY_STATUS => JobOrder.BY_STATUS
            case p.history.JobOrder.JOB_BY_PHASE => JobOrder.BY_PHASE
        }
    }

    def toModel(order: p.history.TargetOrder): TargetOrder = {
        order match {
            case p.history.TargetOrder.TARGET_BY_DATETIME => TargetOrder.BY_DATETIME
            case p.history.TargetOrder.TARGET_BY_PROJECT => TargetOrder.BY_PROJECT
            case p.history.TargetOrder.TARGET_BY_NAME => TargetOrder.BY_NAME
            case p.history.TargetOrder.TARGET_BY_ID => TargetOrder.BY_ID
            case p.history.TargetOrder.TARGET_BY_STATUS => TargetOrder.BY_STATUS
            case p.history.TargetOrder.TARGET_BY_PHASE => TargetOrder.BY_PHASE
            case p.history.TargetOrder.TARGET_BY_PARENT_NAME => TargetOrder.BY_PARENT_NAME
            case p.history.TargetOrder.TARGET_BY_PARENT_ID => TargetOrder.BY_PARENT_ID
        }
    }


    def toProto(schema: ft.StructType) : p.StructType = {
        val fields = schema.fields.map(toProto)
        p.StructType.newBuilder()
            .setTypeName(schema.typeName)
            .addAllFields(fields.asJava)
            .build()
    }

    def toProto(array: ft.ArrayType): p.ArrayType = {
        val builder = p.ArrayType.newBuilder()
            .setTypeName(array.typeName)

        array.elementType match {
            case s: ft.StructType =>
                builder.setStruct(toProto(s))
            case a: ft.ArrayType =>
                builder.setArray(toProto(a))
            case m: ft.MapType =>
                builder.setMap(toProto(m))
            case t =>
                builder.setScalar(t.typeName)
        }

        builder.build()
    }

    def toProto(map: ft.MapType): p.MapType = {
        val builder = p.MapType.newBuilder()

        builder.setTypeName(map.typeName)
        builder.setKeyType(map.keyType.typeName)
        map.valueType match {
            case s: ft.StructType =>
                builder.setStruct(toProto(s))
            case a: ft.ArrayType =>
                builder.setArray(toProto(a))
            case m: ft.MapType =>
                builder.setMap(toProto(m))
            case t =>
                builder.setScalar(t.typeName)
        }

        builder.build()
    }

    def toProto(field: ft.Field): p.StructField = {
        val builder = p.StructField.newBuilder()
            .setName(field.name)
            .setNullable(field.nullable)
            .setSqlType(field.sqlType)
        field.description.foreach(builder.setDescription)
        field.format.foreach(builder.setFormat)
        field.collation.foreach(builder.setCollation)
        field.charset.foreach(builder.setCharset)

        field.ftype match {
            case s:ft.StructType =>
                builder.setStruct(toProto(s))
            case a:ft.ArrayType =>
                builder.setArray(toProto(a))
            case m:ft.MapType =>
                builder.setMap(toProto(m))
            case t =>
                builder.setScalar(p.ScalarType.newBuilder().setTypeName(t.typeName).build())
        }

        builder.build()
    }


    def toModel(ts:p.Timestamp) : ZonedDateTime = {
        val secs = ts.getSeconds
        val nanos = ts.getNanos
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(secs, nanos), ZoneId.systemDefault)
    }
    def toProto(ts:java.sql.Timestamp) : p.Timestamp = {
        val secs = ts.getTime
        val nanos = ts.getNanos
        p.Timestamp.newBuilder().setSeconds(secs).setNanos(nanos).build()
    }
    def toProto(ts: java.time.Instant): p.Timestamp = {
        val secs = ts.getEpochSecond
        val nanos = ts.getNano
        p.Timestamp.newBuilder().setSeconds(secs).setNanos(nanos).build()
    }
    def toProto(ts: ZonedDateTime): p.Timestamp = {
        val instant = ts.toInstant
        toProto(instant)
    }

    def toProto(ts: java.sql.Date): p.Date = {
        val days = ts.toLocalDate.toEpochDay
        p.Date.newBuilder().setDays(days).build()
    }

    def toProto(ts: java.time.LocalDate): p.Date = {
        val days = ts.toEpochDay
        p.Date.newBuilder().setDays(days).build()
    }

    def toProto(row:Row) : p.Row = {
        def convert(v:Any): p.Field = {
            v match {
                case b: Byte => p.Field.newBuilder().setLong(b).build()
                case b: Boolean => p.Field.newBuilder().setBool(b).build()
                case c: Char => p.Field.newBuilder().setString(c.toString).build()
                case i: Int => p.Field.newBuilder().setLong(i).build()
                case s: Short => p.Field.newBuilder().setLong(s).build()
                case l: Long => p.Field.newBuilder().setLong(l).build()
                case f: Float => p.Field.newBuilder().setDouble(f).build()
                case d: Double => p.Field.newBuilder().setDouble(d).build()
                case s: String => p.Field.newBuilder().setString(s).build()
                case b: java.math.BigDecimal => p.Field.newBuilder().setDouble(b.doubleValue()).build()
                case a: Array[Byte] => p.Field.newBuilder().setBytes(ByteString.copyFrom(a)).build()

                case ts: java.sql.Timestamp => p.Field.newBuilder().setTimestamp(toProto(ts)).build()
                case ts: java.time.Instant => p.Field.newBuilder().setTimestamp(toProto(ts)).build()
                case dt: java.sql.Date => p.Field.newBuilder().setDate(toProto(dt)).build()
                case dt: java.time.LocalDate => p.Field.newBuilder().setDate(toProto(dt)).build()

                case row: Row => p.Field.newBuilder().setRow(toProto(row)).build()
                case seq: Seq[_] =>
                    val entries = seq.map(convert)
                    val array = p.Array.newBuilder().addAllValues(entries.asJava).build()
                    p.Field.newBuilder().setArray(array).build()
                case map: Map[_, _] =>
                    val entries = map.map(kv => p.MapElement.newBuilder().setKey(convert(kv._1)).setValue(convert(kv._2)).build())
                    val elem = p.Map.newBuilder().addAllValues(entries.asJava)
                    p.Field.newBuilder().setMap(elem).build()
                case null => p.Field.newBuilder().setNull(p.Null.newBuilder().build()).build()
            }
        }
        val fields = row.toSeq.map(convert)
        p.Row.newBuilder().addAllField(fields.asJava).build()
    }

    def toProto(measurement:Measurement) : p.Measurement = {
        p.Measurement.newBuilder()
            .setName(measurement.name)
            .setJobId(measurement.jobId)
            .setTs(toProto(measurement.ts))
            .putAllLabels(measurement.labels.asJava)
            .setValue(measurement.value)
            .build()
    }
    def toProto(series:MetricSeries) : p.MetricSeries = {
        p.MetricSeries.newBuilder()
            .setMetric(series.metric)
            .setNamespace(series.namespace)
            .setProject(series.project)
            .setJob(series.job)
            .setPhase(toProto(series.phase))
            .putAllLabels(series.labels.asJava)
            .addAllMeasurements(series.measurements.map(toProto).asJava)
            .build()
    }
}
