/*
 * Copyright 2022 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.jdbc

import slick.jdbc.DerbyProfile
import slick.jdbc.H2Profile
import slick.jdbc.JdbcProfile
import slick.jdbc.MySQLProfile
import slick.jdbc.PostgresProfile
import slick.jdbc.SQLServerProfile
import slick.jdbc.SQLiteProfile


object SlickUtils {
    lazy val defaultExecutor =  slick.util.AsyncExecutor(
        name="Flowman.default",
        minThreads = 20,
        maxThreads = 20,
        queueSize = 1000,
        maxConnections = 20
    )

    def getProfile(driver:String) : JdbcProfile = {
        val derbyPattern = """.*\.derby\..*""".r
        val sqlitePattern = """.*\.sqlite\..*""".r
        val h2Pattern = """.*\.h2\..*""".r
        val mariadbPattern = """.*\.mariadb\..*""".r
        val mysqlPattern = """.*\.mysql\..*""".r
        val postgresqlPattern = """.*\.postgresql\..*""".r
        val sqlserverPattern = """.*\.sqlserver\..*""".r
        driver match {
            case derbyPattern() => DerbyProfile
            case sqlitePattern() => SQLiteProfile
            case h2Pattern() => H2Profile
            case mysqlPattern() => MySQLProfile
            case mariadbPattern() => MySQLProfile
            case postgresqlPattern() => PostgresProfile
            case sqlserverPattern() => SQLServerProfile
            case _ => throw new UnsupportedOperationException(s"Database with driver ${driver} is not supported")
        }
    }
}
