package com.dimajix.flowman.state


abstract class StateStore {
    /**
      * Returns the state of a job
      * @param job
      * @return
      */
    def getState(job:JobInstance) : Option[JobState]

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
      * @param token
      * @param status
      */
    def finishJob(token:Object, status:Status) : Unit
}
