package com.dimajix.flowman.dsl.example

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.ContextAware
import com.dimajix.flowman.dsl.Converters
import com.dimajix.flowman.dsl.Identifiers
import com.dimajix.flowman.dsl.Module
import com.dimajix.flowman.dsl.RelationWrapper
import com.dimajix.flowman.dsl.mapping.Historize
import com.dimajix.flowman.dsl.mapping.Latest
import com.dimajix.flowman.dsl.mapping.Read
import com.dimajix.flowman.dsl.relation.HiveUnionTable
import com.dimajix.flowman.dsl.relation.HiveView
import com.dimajix.flowman.dsl.schema.MappingSchema
import com.dimajix.flowman.dsl.target.RelationTarget
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.spec.mapping.InsertPosition
import com.dimajix.flowman.types.StringType


trait ModuleCommon extends Converters with Identifiers with ContextAware {
    val database = "dqm"

    def latestChild(child:String) : RelationWrapper = HiveView(
        database = database,
        view = child + "_latest",
        sql = s"""
                SELECT
                  child.*
                FROM ${database}.${child} child
                INNER JOIN ${database}.transaction_latest parent
                  ON parent.transaction_id = child.transaction_id AND parent.metadata_event_id = child.metadata_event_id AND parent.landing_date = child.landing_date
            """
    )

    def historyChild(child:String) : RelationWrapper = HiveView(
        database = database,
        view = child + "_history",
        sql =
            s"""
               |SELECT
               |  parent.edl_valid_from,
               |  parent.edl_valid_until,
               |  child.*
               |FROM ${database}.${child} child
               |INNER JOIN ${database}.transaction_history parent
               |  ON parent.transaction_id = child.transaction_id AND parent.metadata_event_id = child.metadata_event_id AND parent.landing_date = child.landing_date
               |""".stripMargin
    )

    def targetTable(table:String, mapping:String) : RelationWrapper = withEnvironment { env =>
        HiveUnionTable(
            viewDatabase = database,
            view = table,
            tableDatabase = database,
            tablePrefix = "zz_" + table,
            locationPrefix = new Path(env("hdfs_structured_dir"), s"$database/zz_$table"),
            external = true,
            format = "parquet",
            partitions = PartitionField("landing_date", StringType),
            schema = MappingSchema(output(mapping))
        )
    }
}

case class LatestAndHistory(entity:String) extends Module with ModuleCommon {
    val history = entity + "_history"
    val latest = entity + "_latest"
    val hive = "hive_" + entity

    mappings := (
        hive := Read(
            relation = relation(entity)
        ),
        latest := Latest(
            input = output(hive),
            keyColumns = "transaction_id",
            versionColumns = "metadata_occurred_at"
        ),
        history := Historize(
            input = output(hive),
            keyColumns = "transaction_id",
            timeColumn = "metadata_occurred_at",
            validFromColumn = "edl_valid_from",
            validToColumn = "edl_valid_until",
            columnInsertPosition = InsertPosition.BEGINNING
        )
    )

    relations := (
        entity := targetTable(entity, entity),
        latest := HiveView(
            database = database,
            view = latest,
            mapping = output(latest)
        ),
        history := HiveView(
            database = database,
            view = history,
            mapping = output(history)
        )
    )

    targets := (
        entity := withEnvironment { env =>
            RelationTarget(
                mapping = output(entity),
                relation = relation(entity),
                partition = Map("landing_date" -> env("processing_date"))
            )
        },
        latest := RelationTarget(
            relation = relation(latest)
        ),
        history := RelationTarget(
            relation = relation(history)
        )
    )
}
