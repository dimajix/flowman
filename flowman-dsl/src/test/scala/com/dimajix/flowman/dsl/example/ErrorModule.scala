package com.dimajix.flowman.dsl.example

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.Module
import com.dimajix.flowman.dsl.mapping.Union
import com.dimajix.flowman.dsl.relation.HiveTable
import com.dimajix.flowman.dsl.schema.EmbeddedSchema
import com.dimajix.flowman.dsl.target.DatabaseTarget
import com.dimajix.flowman.dsl.target.RelationTarget
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StringType


object ErrorModule extends Module {
    val database = "error_zone"

    relations := (
        "error" := withEnvironment { env =>
            HiveTable(
                database = database,
                table = "edl_dqm_provider",
                location = new Path(env.evaluate("$hdfs_basedir/error/${app_name}")),
                external = true,
                format = "parquet",
                partitions = PartitionField(
                    name = "run_id",
                    ftype = StringType
                ),
                schema = EmbeddedSchema(
                    fields = Seq(
                        Field(
                            name = "record",
                            ftype = StringType
                        ),
                        Field(
                            name = "landing_date",
                            ftype = StringType
                        ),
                        Field(
                            name = "app_name",
                            ftype = StringType
                        ),
                        Field(
                            name = "app_version",
                            ftype = StringType
                        ),
                        Field(
                            name = "run_date",
                            ftype = StringType
                        ),
                        Field(
                            name = "kafka_topic",
                            ftype = StringType
                        )
                    )
                )
            )
        }
    )

    mappings := (
        "error" := Union(
            inputs = Seq(
                output("qualityreport_events", "error")
            )
        )
    )

    targets := (
        "error_database" := DatabaseTarget(
            database = database
        ),
        "error" := withEnvironment { env =>
            RelationTarget(
                mapping = output("error"),
                relation = relation("error"),
                mode = OutputMode.APPEND,
                partition = "run_id" -> env("run_id")
            )
        }
    )
}
