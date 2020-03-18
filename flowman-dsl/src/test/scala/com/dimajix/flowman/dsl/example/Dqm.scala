package com.dimajix.flowman.dsl.example

import java.sql.Date

import com.dimajix.flowman.dsl.Job
import com.dimajix.flowman.dsl.Module
import com.dimajix.flowman.dsl.Project
import com.dimajix.flowman.types.DateType


object DqmModule extends Module {
    jobs := (
        "main" := Job(
            parameters = new Job.Parameter("processing_date", DateType, default=Date.valueOf("2019-01-01")),
            targets = targets.identifiers
        )
        )
}

object DqmProject extends Project {
    name := "dqm"
    version := "1.0"
    description := "EDL DQM Provider"

    modules += (
        DqmModule,
        TransactionModule
    )
}
