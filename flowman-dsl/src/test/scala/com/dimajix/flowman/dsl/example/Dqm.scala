package com.dimajix.flowman.dsl.example

import java.sql.Date

import com.dimajix.flowman.dsl.Job
import com.dimajix.flowman.dsl.Module
import com.dimajix.flowman.dsl.Project
import com.dimajix.flowman.dsl.target.DatabaseTarget
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.StringType


object DqmModule extends Module {
    val database = "dqm"

    modules += (
        EnvironmentModule,
        LandingModule("stream.events", "qualityreport", "example-schema"),
        TransactionModule,
        ErrorModule
    )

    targets += (
        "dqm_database" := DatabaseTarget(
            database = database
        )
    )

    jobs := (
        "main" := Job(
            parameters = Seq(
                Job.Parameter("processing_date", DateType, default=Date.valueOf("2019-01-01")),
                Job.Parameter("run_date", DateType, default=Date.valueOf("2019-01-01")),
                Job.Parameter("run_id", StringType, default="unknown_run_id")
            ),
            targets = modules.targets.identifiers ++ targets.identifiers
        ),
        "test" := Job(
            environment = Map(
                "hdfs_basedir" -> "${test_basedir}"
            ),
            parents = job("main")
        )
    )
}

object DqmProject extends Project {
    name := "dqm"
    version := "1.0"
    description := "Data Provider"

    modules += DqmModule
}
