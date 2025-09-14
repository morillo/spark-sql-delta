package com.morillo.spark.delta

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables.DeltaTable
import com.morillo.spark.delta.model.User
import org.slf4j.LoggerFactory
import java.sql.Timestamp

class DeltaTableOperations(spark: SparkSession, tablePath: String) {

  private val logger = LoggerFactory.getLogger(getClass)

  import spark.implicits._

  val userSchema: StructType = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("email", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false),
    StructField("department", StringType, nullable = false),
    StructField("salary", DoubleType, nullable = false),
    StructField("created_at", TimestampType, nullable = false),
    StructField("updated_at", TimestampType, nullable = true)
  ))

  def createTable(): Unit = {
    logger.info(s"Creating Delta table at path: $tablePath")

    val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], userSchema)
    emptyDF.write
      .format("delta")
      .mode("overwrite")
      .save(tablePath)

    logger.info("Delta table created successfully")
  }

  def writeUsers(users: Seq[User], mode: String = "append"): Unit = {
    logger.info(s"Writing ${users.length} users to Delta table in mode: $mode")

    val df = users.toDF()
    df.write
      .format("delta")
      .mode(mode)
      .save(tablePath)

    logger.info("Users written successfully")
  }

  def readUsers(): DataFrame = {
    logger.info("Reading users from Delta table")

    spark.read
      .format("delta")
      .load(tablePath)
  }

  def readUsersAsDataset(): Dataset[User] = {
    logger.info("Reading users from Delta table as Dataset")

    spark.read
      .format("delta")
      .load(tablePath)
      .as[User]
  }

  def updateUserSalary(userId: Long, newSalary: Double): Unit = {
    logger.info(s"Updating salary for user $userId to $newSalary")

    val deltaTable = DeltaTable.forPath(spark, tablePath)

    deltaTable.update(
      condition = col("id") === userId,
      set = Map(
        "salary" -> lit(newSalary),
        "updated_at" -> current_timestamp()
      )
    )

    logger.info("User salary updated successfully")
  }

  def deleteUser(userId: Long): Unit = {
    logger.info(s"Deleting user with id: $userId")

    val deltaTable = DeltaTable.forPath(spark, tablePath)
    deltaTable.delete(col("id") === userId)

    logger.info("User deleted successfully")
  }

  def mergeUsers(newUsers: Seq[User]): Unit = {
    logger.info(s"Merging ${newUsers.length} users into Delta table")

    val deltaTable = DeltaTable.forPath(spark, tablePath)
    val newUsersDF = newUsers.toDF()

    deltaTable
      .as("existing")
      .merge(
        newUsersDF.as("updates"),
        "existing.id = updates.id"
      )
      .whenMatched()
      .updateExpr(Map(
        "name" -> "updates.name",
        "email" -> "updates.email",
        "age" -> "updates.age",
        "department" -> "updates.department",
        "salary" -> "updates.salary",
        "updated_at" -> "current_timestamp()"
      ))
      .whenNotMatched()
      .insertAll()
      .execute()

    logger.info("Users merged successfully")
  }

  def getUsersByDepartment(department: String): DataFrame = {
    logger.info(s"Getting users from department: $department")

    readUsers()
      .filter(col("department") === department)
      .orderBy(col("name"))
  }

  def getHighEarnersAbove(salaryThreshold: Double): DataFrame = {
    logger.info(s"Getting users with salary above: $salaryThreshold")

    readUsers()
      .filter(col("salary") > salaryThreshold)
      .orderBy(col("salary").desc)
  }

  def timeTravel(versionNumber: Long): DataFrame = {
    logger.info(s"Reading Delta table at version: $versionNumber")

    spark.read
      .format("delta")
      .option("versionAsOf", versionNumber)
      .load(tablePath)
  }

  def timeTravelByTimestamp(timestamp: String): DataFrame = {
    logger.info(s"Reading Delta table as of timestamp: $timestamp")

    spark.read
      .format("delta")
      .option("timestampAsOf", timestamp)
      .load(tablePath)
  }

  def getTableHistory(): DataFrame = {
    logger.info("Getting Delta table history")

    val deltaTable = DeltaTable.forPath(spark, tablePath)
    deltaTable.history()
  }

  def vacuumTable(retentionHours: Int = 168): Unit = {
    logger.info(s"Vacuuming Delta table with retention of $retentionHours hours")

    val deltaTable = DeltaTable.forPath(spark, tablePath)
    deltaTable.vacuum(retentionHours)

    logger.info("Table vacuumed successfully")
  }

  def showTableDetails(): Unit = {
    logger.info("Showing Delta table details")

    val deltaTable = DeltaTable.forPath(spark, tablePath)
    deltaTable.detail().show(truncate = false)
  }

  def generateManifest(): Unit = {
    logger.info("Generating manifest for Delta table")

    val deltaTable = DeltaTable.forPath(spark, tablePath)
    deltaTable.generate("symlink_format_manifest")

    logger.info("Manifest generated successfully")
  }
}