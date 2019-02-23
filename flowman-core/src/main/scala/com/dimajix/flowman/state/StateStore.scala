package com.dimajix.flowman.state


abstract class StateStore {
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
      * Marks a run as a success
      *
      * @param token
      */
    def success(token:Object) : Unit

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    def failure(token:Object) : Unit

    /**
      * Marks a run as a failure
      *
      * @param token
      */
    def aborted(token:Object) : Unit

    /**
      * Marks a run as being skipped
      *
      * @param token
      */
    def skipped(token:Object) : Unit

}
