package com.morillo.spark.delta

import org.apache.spark.sql.SparkSession
import com.morillo.spark.delta.model.User
import org.slf4j.LoggerFactory
import java.sql.Timestamp

object DeltaLakeApplication {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting Spark Delta Lake Application")

    val config = ConfigManager.getConfig
    val tablePath = config.getString("delta.table.path")

    implicit val spark: SparkSession = SparkSessionFactory.createSparkSession(config)

    try {
      val deltaOps = new DeltaTableOperations(spark, tablePath)

      logger.info("Creating Delta table...")
      deltaOps.createTable()

      logger.info("Inserting sample data...")
      val sampleUsers = generateSampleUsers()
      deltaOps.writeUsers(sampleUsers)

      logger.info("Reading all users...")
      val allUsers = deltaOps.readUsers()
      allUsers.show()

      logger.info("Getting users from Engineering department...")
      val engineeringUsers = deltaOps.getUsersByDepartment("Engineering")
      engineeringUsers.show()

      logger.info("Getting high earners (salary > 80000)...")
      val highEarners = deltaOps.getHighEarnersAbove(80000.0)
      highEarners.show()

      logger.info("Updating Alice's salary...")
      deltaOps.updateUserSalary(1L, 95000.0)

      logger.info("Reading users after salary update...")
      deltaOps.readUsers().show()

      logger.info("Merging new users...")
      val newUsers = Seq(
        User(6L, "Frank Miller", "frank.miller@example.com", 40, "HR", 65000.0, new Timestamp(System.currentTimeMillis())),
        User(7L, "Grace Kelly", "grace.kelly@example.com", 26, "Engineering", 82000.0, new Timestamp(System.currentTimeMillis()))
      )
      deltaOps.mergeUsers(newUsers)

      logger.info("Final user count:")
      deltaOps.readUsers().count()

      logger.info("Showing table history...")
      deltaOps.getTableHistory().show(truncate = false)

      logger.info("Showing table details...")
      deltaOps.showTableDetails()

      logger.info("Time travel example - reading version 0...")
      val version0 = deltaOps.timeTravel(0L)
      version0.show()

    } catch {
      case ex: Exception =>
        logger.error("Error in Delta Lake operations", ex)
        throw ex
    } finally {
      spark.stop()
      logger.info("Spark session stopped")
    }
  }

  private def generateSampleUsers(): Seq[User] = {
    val currentTime = new Timestamp(System.currentTimeMillis())

    Seq(
      User(1L, "Alice Johnson", "alice.johnson@example.com", 28, "Engineering", 85000.0, currentTime),
      User(2L, "Bob Smith", "bob.smith@example.com", 35, "Marketing", 72000.0, currentTime),
      User(3L, "Charlie Brown", "charlie.brown@example.com", 31, "Engineering", 92000.0, currentTime),
      User(4L, "Diana Prince", "diana.prince@example.com", 29, "Sales", 78000.0, currentTime),
      User(5L, "Edward Norton", "edward.norton@example.com", 33, "Engineering", 88000.0, currentTime)
    )
  }
}