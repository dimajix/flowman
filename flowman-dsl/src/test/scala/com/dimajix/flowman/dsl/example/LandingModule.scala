package com.dimajix.flowman.dsl.example

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.Module
import com.dimajix.flowman.dsl.mapping.Deduplicate
import com.dimajix.flowman.dsl.mapping.Extend
import com.dimajix.flowman.dsl.mapping.ExtractJson
import com.dimajix.flowman.dsl.mapping.Flow
import com.dimajix.flowman.dsl.mapping.Read
import com.dimajix.flowman.dsl.relation.FileRelation
import com.dimajix.flowman.dsl.schema.EmbeddedSchema
import com.dimajix.flowman.dsl.schema.SparkSchema
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType


case class LandingModule(kafkaTopic:String, entity:String, schema:String) extends Module {
    object LandingEvents extends Flow {
        mappings := (
            "events_raw" := withEnvironment { env =>
                Read(
                    relation = relation(s"landing_$entity"),
                    partitions = Map("processing_date" -> SingleValue(env("processing_date")))
                )
            },

            "events_extracted" := withEnvironment { env =>
                ExtractJson(
                    input = output("events_raw"),
                    column = "value",
                    schema = SparkSchema(
                        file = new Path(env.evaluate(s"$${project.basedir}/schema/${schema}"))
                    )
                )
            },

            "error" := withEnvironment { env =>
                Extend(
                    input = output("events_extracted", "error"),
                    columns = Map(
                        "landing_date" -> s"'${env("processing_date")}'",
                        "run_date" -> s"'${env("run_date")}'",
                        "app_name" -> s"'${env("app_name")}'",
                        "app_version" -> s"'${env("app_version")}'",
                        "kafka_topic" -> s"'$kafkaTopic'"
                    )
                )
            },

            "main" := Deduplicate(
                input = output("events_extracted"),
                columns = "metadata.eventId"
            )
        )
    }

    relations := (
        s"landing_$entity" := withEnvironment { env =>
            FileRelation(
                format = "sequencefile",
                location = new Path(env.evaluate(s"$$hdfs_landing_dir/kafka/topic=${kafkaTopic}.*.*")),
                pattern = "processing_date=${processing_date}",
                partitions = PartitionField("processing_date", StringType),
                schema = EmbeddedSchema(
                    fields = Seq(
                        Field("key", BinaryType),
                        Field("value", BinaryType)
                    )
                )
            )
        }
    )

    mappings := (
        s"${entity}_events" := LandingEvents
    )
}
