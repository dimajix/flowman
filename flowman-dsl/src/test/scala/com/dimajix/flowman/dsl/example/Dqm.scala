package com.dimajix.flowman.dsl.example

import java.sql.Date

import com.dimajix.flowman.dsl.Job
import com.dimajix.flowman.dsl.Module
import com.dimajix.flowman.dsl.Project
import com.dimajix.flowman.types.DateType
import com.dimajix.flowman.types.StringType


object DqmModule extends Module {
    modules += (
        EnvironmentModule,
        LandingModule("stream.qualityreport", "qualityreport", "some_schema"),
        TransactionModule,
        ErrorModule
    )

    jobs := (
        "main" := Job(
            parameters = Seq(
                new Job.Parameter("processing_date", DateType, default=Date.valueOf("2019-01-01")),
                new Job.Parameter("run_date", DateType, default=Date.valueOf("2019-01-01")),
                new Job.Parameter("run_id", StringType, default="unknown_run_id")
            ),
            targets = modules.targets.identifiers ++ targets.identifiers
        ),
        "test" := Job(
            environment = Map(
                "hdfs_basedir" -> "/tmp/dqm"
            ),
            parents = job("main")
        )
    )
}

object DqmProject extends Project {
    name := "dqm"
    version := "1.0"
    description := "EDL DQM Provider"

    modules += DqmModule
}
