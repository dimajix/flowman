package com.dimajix.flowman.state


abstract class StateStore {
    /**
      * Returns the state of a job, or None if no information is available
      * @param job
      * @return
      */
    def getJobState(job:JobInstance) : Option[JobState]

    /**
      * Performs some checkJob, if the run is required
      * @param job
      * @return
      */
    def checkJob(job:JobInstance) : Boolean

    /**
      * Starts the run and returns a token, which can be anything
      * @param job
      * @return
      */
    def startJob(job:JobInstance) : Object

    /**
      * Sets the status of a job after it has been started
      * @param token The token returned by startJob
      * @param status
      */
    def finishJob(token:Object, status:Status) : Unit

    /**
      * Returns the state of a specific target on its last run, or None if no information is available
      * @param target
      * @return
      */
    def getTargetState(target:TargetInstance) : Option[TargetState]

    def checkTarget(target:TargetInstance) : Boolean

    def startTarget(target:TargetInstance) : Object

    def finishTarget(token:Object, status:Status) : Unit
}
