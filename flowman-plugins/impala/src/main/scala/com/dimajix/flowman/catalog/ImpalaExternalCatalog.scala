/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.catalog

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLRecoverableException
import java.sql.SQLTransientException
import java.sql.Statement
import java.util.Properties

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.slf4j.LoggerFactory

import com.dimajix.flowman.jdbc.HiveDialect


object ImpalaExternalCatalog {
    val IMPALA_DEFAULT_DRIVER = "com.cloudera.impala.jdbc.Driver"
    val IMPALA_DEFAULT_PORT = 21050

    case class Connection(
         url:String = "",
         host:String = "",
         port:Int = IMPALA_DEFAULT_PORT,
         driver:String = IMPALA_DEFAULT_DRIVER,
         user:Option[String] = None,
         password:Option[String] = None,
         properties: Map[String,String] = Map(),
         timeout: Int = 3000,
         retries: Int = 3
     )
}


final case class ImpalaExternalCatalog(
    connection:ImpalaExternalCatalog.Connection,
    computeStats:Boolean
) extends AbstractExternalCatalog {
    private val logger = LoggerFactory.getLogger(classOf[ImpalaExternalCatalog])
    private val connect = createConnectionFactory(connection)

    override def createTable(table: CatalogTable): Unit = {
        logger.info(s"INVALIDATE Impala metadata for newly created table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    override def alterTable(table: CatalogTable): Unit = {
        logger.info(s"REFRESH Impala metadata for altered table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"REFRESH $identifier")
        }
    }

    override def refreshTable(table: CatalogTable): Unit = {
        logger.info(s"REFRESH Impala metadata for modified table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"REFRESH $identifier")

            if (computeStats) {
                stmt.execute(s"COMPUTE STATS $identifier")
            }
        }
    }

    override def dropTable(table: CatalogTable): Unit = {
        logger.info(s"INVALIDATE Impala metadata for dropped table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    override def truncateTable(table: CatalogTable): Unit = {
        logger.info(s"REFRESH Impala metadata for truncated table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"REFRESH $identifier")
        }
    }

    override def addPartition(table: CatalogTable, partition: CatalogTablePartition): Unit = {
        logger.info(s"REFRESH Impala metadata for new partition ${partition.spec} of table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            val spec = HiveDialect.expr.partition(PartitionSpec(partition.spec))
            stmt.execute(s"REFRESH $identifier $spec")

            if (computeStats) {
                stmt.execute(s"COMPUTE INCREMENTAL STATS $identifier $spec")
            }
        }
    }

    override def alterPartition(table: CatalogTable, partition: CatalogTablePartition): Unit =  {
        logger.info(s"REFRESH Impala metadata for changed partition ${partition.spec} of table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            val spec = HiveDialect.expr.partition(PartitionSpec(partition.spec))
            stmt.execute(s"REFRESH $identifier $spec")

            if (computeStats) {
                stmt.execute(s"COMPUTE INCREMENTAL STATS $identifier $spec")
            }
        }
    }

    override def dropPartition(table: CatalogTable, partition: CatalogTablePartition): Unit = {
        logger.info(s"INVALIDATE Impala metadata for dropped partition ${partition.spec} of table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    override def truncatePartition(table: CatalogTable, partition: CatalogTablePartition): Unit =  {
        logger.info(s"REFRESH Impala metadata for truncated partition ${partition.spec} of table ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            val spec = HiveDialect.expr.partition(PartitionSpec(partition.spec))
            stmt.execute(s"REFRESH $identifier $spec")
        }
    }

    override def createView(table: CatalogTable): Unit = {
        logger.info(s"INVALIDATE Impala metadata for newly created view ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    override def alterView(table: CatalogTable): Unit = {
        logger.info(s"INVALIDATE Impala metadata for modified view ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    override def dropView(table: CatalogTable): Unit = {
        logger.info(s"INVALIDATE Impala metadata for dropped view ${table.identifier}")
        withStatement { stmt =>
            val identifier = HiveDialect.quote(table.identifier)
            stmt.execute(s"INVALIDATE METADATA $identifier")
        }
    }

    private def withConnection[T](fn:Connection => T) : T = {
        def retry[T](n:Int)(fn: => T) : T = {
            try {
                fn
            } catch {
                case e @(_:SQLRecoverableException|_:SQLTransientException) if n > 1 => {
                    logger.error("Retrying after error while executing SQL: {}", e.getMessage)
                    Thread.sleep(connection.timeout)
                    retry(n - 1)(fn)
                }
            }
        }

        retry(connection.retries) {
            val conn = connect()
            try {
                fn(conn)
            }
            finally {
                conn.close()
            }
        }
    }

    private def withStatement[T](fn:Statement => T) : T = {
        withConnection { con =>
            val statement = con.createStatement()
            try {
                // statement.setQueryTimeout(options.queryTimeout)
                fn(statement)
            }
            finally {
                statement.close()
            }
        }
    }

    private def createConnectionFactory(options: ImpalaExternalCatalog.Connection): () => Connection = {
        val driver = Option(options.driver)
            .filter(_.nonEmpty)
            .getOrElse(ImpalaExternalCatalog.IMPALA_DEFAULT_DRIVER)
        val url = Option(options.url)
            .filter(_.nonEmpty)
            .getOrElse("jdbc:impala://" + options.host + ":" + options.port)

        Class.forName(driver)

        val properties = new Properties()
        options.user.foreach(properties.setProperty("user", _))
        options.password.foreach(properties.setProperty("password", _))
        options.properties.foreach(kv => properties.setProperty(kv._1, kv._2))
        () => {
            DriverManager.getConnection(url, properties)
        }
    }
}
