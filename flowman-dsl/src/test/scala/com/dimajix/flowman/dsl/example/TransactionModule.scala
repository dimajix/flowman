package com.dimajix.flowman.dsl.example

import com.dimajix.flowman.dsl.Module
import com.dimajix.flowman.dsl.mapping.Assemble
import com.dimajix.flowman.dsl.mapping.Conform
import com.dimajix.flowman.dsl.mapping.Explode
import com.dimajix.flowman.transforms.CaseFormat
import com.dimajix.flowman.types.TimestampType


object TransactionModule extends Module with ModuleCommon {
    mappings := (
        "transaction_array" := Explode(
            input = output("qualityreport_events"),
            array = col("Nested.Path"),
            outerColumns = Explode.Columns(
                keep = col("metadata"),
                drop = col("metadata.correlationIds")
            )
        ),
        "transaction_updates" := Assemble(
            input = output("transaction_array"),
            columns = Assemble.Flatten(
                drop = Seq(
                    col("ErrorInfo"),
                    col("Tax")
                )
            )
        ),
        "transaction" := Conform(
            input = output("transaction_updates"),
            naming = CaseFormat.SNAKE_CASE,
            types = Map("date" -> TimestampType)
        )
    )
    modules += (
        LatestAndHistory("transaction")
    )
}
