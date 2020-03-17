package com.dimajix.flowman.dsl

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.mapping.Deduplicate
import com.dimajix.flowman.dsl.mapping.Extend
import com.dimajix.flowman.dsl.mapping.ExtractJson
import com.dimajix.flowman.dsl.mapping.Flow
import com.dimajix.flowman.dsl.mapping.Read
import com.dimajix.flowman.dsl.relation.FileRelation
import com.dimajix.flowman.dsl.relation.HiveTable
import com.dimajix.flowman.dsl.relation.HiveUnionTable
import com.dimajix.flowman.dsl.relation.HiveView
import com.dimajix.flowman.dsl.schema.EmbeddedSchema
import com.dimajix.flowman.dsl.schema.MappingSchema
import com.dimajix.flowman.dsl.schema.SparkSchema
import com.dimajix.flowman.dsl.schema.SwaggerSchema
import com.dimajix.flowman.dsl.target.RelationTarget
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.relation.HiveTableRelation
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType



class DslExample {
    class CommonModule(suffix:String) extends Module {
        relations += (
            s"lala2_$suffix" := HiveTable(database=Some("default"), table="table"),
            s"lala_$suffix" := HiveTable(database=Some("default"), table="table") label "my" -> "abc",
            s"lala2_$suffix" := HiveTable(database=Some("default"), table="table")
        )
    }
    object Environment extends Module {
        environment += (
            "processing_date" -> "2019-03-01",
            "test_basedir" -> "/tmp/edl-crm-provider",
            "hdfs_landing_dir" -> "${hdfs_basedir}/landing",
            "hdfs_structured_dir" -> "${hdfs_basedir}/structured"
        )

        config += (
            "spark.sql.session.timeZone" -> "UTC",
            "spark.app.name" -> "@project.name@ (@project.version@)"
        )
    }

    class ModuleCommon extends WithWrapper {
        def landingRelation() : RelationGen = FileRelation(
            format = "sequencefile",
            location = env => new Path(env.evaluate("$hdfs_landing_dir/kafka/topic=${kafka_topic}.${version}.${kafka_suffix}")),
            pattern = "processing_date=${processing_date}",
            partitions = Seq(PartitionField("processing_date", StringType)),
            schema = EmbeddedSchema(
                fields = Seq(
                    Field("key", BinaryType),
                    Field("value", BinaryType)
                )
            )
        )

        case class LandingEvents(input:Environment => RelationIdentifier) extends Flow {
            mappings := (
                "events_raw" := Read(
                    relation = input,
                    partitions = env => Map("processing_date" -> SingleValue(env("processing_date")))
                ),

                "events_extracted" := ExtractJson(
                    input = mapping("events_raw"),
                    column = "value",
                    schema = SparkSchema(
                        file = env => new Path(env.evaluate("${project.basedir}/schema/${schema}"))
                    )
                ),

                "error" := Extend(),

                "main" := Deduplicate(
                    input = output("events_extracted"),
                    columns = Seq("metadata.eventId")
                )
            )
        }

        val landing_lala = "landing_lala" := LandingEvents(RelationIdentifier("xyz"))
        landing_lala.identifier

        def latestChild(child:String) : RelationGen = HiveView(
            database = "dqm",
            view = child + "_latest",
            sql = s"""
                SELECT
                  child.*
                FROM dqm.${child} child
                INNER JOIN dqm.transaction_latest parent
                  ON parent.transaction_id = child.transaction_id AND parent.metadata_event_id = child.metadata_event_id AND parent.landing_date = child.landing_date
            """
        )

        def historyChild(child:String) : RelationGen = HiveView(
            database = "dqm",
            view = child + "_history",
            sql =
                s"""
                   |SELECT
                   |  parent.edl_valid_from,
                   |  parent.edl_valid_until,
                   |  child.*
                   |FROM dqm.${child} child
                   |INNER JOIN dqm.transaction_history parent
                   |  ON parent.transaction_id = child.transaction_id AND parent.metadata_event_id = child.metadata_event_id AND parent.landing_date = child.landing_date
                   |""".stripMargin
        )

        def targetTable(table:String, mapping:String) : RelationGen = HiveUnionTable(
            viewDatabase = "dqm",
            view = table,
            tableDatabase = "dqm",
            tablePrefix = "zz_" + table,
            locationPrefix = env => new Path(env("hdfs_structured_dir"), s"dqm/zz_${table}"),
            external = true,
            format = "parquet",
            partitions = Seq(PartitionField("landing_date", StringType)),
            schema = MappingSchema(output(mapping))
        )
    }

    class QualityReport extends Module {

    }

    class ContractModule extends Module {
        targets := (
            "contract" := RelationTarget(
                mapping = output("contract"),
                relation = relation("contract"),
                partition = env => Map("laning_date" -> env("processing_date"))
            ),
            "contract_latest" := RelationTarget(
                relation = relation("contract_latest")
            ),
            "contract_history" := RelationTarget(
                relation = relation("contract_history")
            )
        )
    }


    object MyModule extends Module {
        val suffix = "xyz_"

        relations += (
            //s"lala_$suffix" := (HiveTableRelation(_, database=Some("default"), table="table")) label "my" -> "abc",
            //s"lala2_$suffix" := (HiveTableRelation(_, database=Some("default"), table="table")),
            s"lala3_$suffix" := HiveTable(database="default", table="table", location=new Path("xyz")) label "y" -> "abc",
            s"lala4_$suffix" := HiveTable(database="default", table="table")
        )

        relations += (
            "lala" := HiveTable(
                        database="default",
                        table="table",
                        location = env => new Path(env("lala")),
                        schema = SwaggerSchema(
                            file = new Path("lala")
                        )
                    ) label "my" -> "abc"
        )
        targets += (
            "lala" := RelationTarget(
                mapping = output("lala"),
                relation = relation("lala"),
                partition = env => Map("landing_date" -> env("landing_date"))
            )
        )
        jobs += (
            "main" := Job(
                parameters = Seq(),
                environment = Map(),
                targets = targets.identifiers
            )
        )

        modules += new CommonModule("lala")
        modules += (new CommonModule("lala"), new CommonModule("lala"))
    }


    object MyProject extends Project {
        name := "dqm"
        version := "1.0"
        description := "EDL DQM Provider"

        modules += (
            MyModule
            )
    }

}
