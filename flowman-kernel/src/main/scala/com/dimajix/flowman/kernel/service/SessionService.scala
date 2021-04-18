package com.dimajix.flowman.kernel.service

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.kernel.model.Target
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Test


class SessionService(val id:String, val session:Session) {
    def project : Project = ???
    def namespace : Namespace = ???

    def getJob(name:String) : Job = ???
    def runJob(job:Job) = ???

    def runTarget(target:Target) = ???

    def runTest(test:Test) = ???

    def collectMapping(mapping:Mapping) = ???

    def collectRelation(relation:Relation) = ???
}
