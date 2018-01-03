package com.dimajix.dataflow.tools.dfexec

import java.util.Locale

import org.slf4j.LoggerFactory


object Driver {
    def main(args: Array[String]) : Unit = {
        // First create driver, so can already process arguments
        val options = new Arguments(args)
        val driver = new Driver(options)

        val result = driver.run()
        System.exit(if (result) 0 else 1)
    }
}


class Driver(options:Arguments) {
    private val logger = LoggerFactory.getLogger(classOf[Driver])

    /**
      * Main method for running this command
      * @return
      */
    def run() : Boolean = {
        // Adjust Spark loglevel
        if (options.sparkLogging != null) {
            val upperCased = options.sparkLogging.toUpperCase(Locale.ENGLISH)
            val l = org.apache.log4j.Level.toLevel(upperCased)
            org.apache.log4j.Logger.getLogger("org").setLevel(l)
            org.apache.log4j.Logger.getLogger("akka").setLevel(l)
        }

        options.execute(options)
    }
}
