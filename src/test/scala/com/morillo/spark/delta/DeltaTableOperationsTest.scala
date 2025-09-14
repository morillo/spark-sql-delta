package com.morillo.spark.delta

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import com.morillo.spark.delta.model.User
import java.sql.Timestamp
import java.nio.file.{Files, Paths}
import org.apache.commons.io.FileUtils
import java.io.File

class DeltaTableOperationsTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _
  var deltaOps: DeltaTableOperations = _
  val testTablePath = "./test-delta-table"

  override def beforeAll(): Unit = {
    spark = SparkSessionFactory.createLocalSparkSession("DeltaTableOperationsTest")
    deltaOps = new DeltaTableOperations(spark, testTablePath)

    cleanupTestDirectory()
    deltaOps.createTable()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    cleanupTestDirectory()
  }

  private def cleanupTestDirectory(): Unit = {
    val testDir = new File(testTablePath)
    if (testDir.exists()) {
      FileUtils.deleteDirectory(testDir)
    }
  }

  private def getSampleUsers: Seq[User] = {
    val currentTime = new Timestamp(System.currentTimeMillis())
    Seq(
      User(1L, "John Doe", "john.doe@example.com", 30, "Engineering", 75000.0, currentTime),
      User(2L, "Jane Smith", "jane.smith@example.com", 28, "Marketing", 68000.0, currentTime),
      User(3L, "Mike Johnson", "mike.johnson@example.com", 35, "Sales", 82000.0, currentTime)
    )
  }

  test("should create and write users to Delta table") {
    val users = getSampleUsers
    deltaOps.writeUsers(users, "overwrite")

    val result = deltaOps.readUsers()
    assert(result.count() == 3)
  }

  test("should read users as Dataset") {
    val users = getSampleUsers
    deltaOps.writeUsers(users, "overwrite")

    val dataset = deltaOps.readUsersAsDataset()
    val collectedUsers = dataset.collect()

    assert(collectedUsers.length == 3)
    assert(collectedUsers.exists(_.name == "John Doe"))
  }

  test("should filter users by department") {
    val users = getSampleUsers
    deltaOps.writeUsers(users, "overwrite")

    val engineeringUsers = deltaOps.getUsersByDepartment("Engineering")
    assert(engineeringUsers.count() == 1)

    val firstUser = engineeringUsers.collect()(0)
    assert(firstUser.getString(firstUser.fieldIndex("name")) == "John Doe")
  }

  test("should filter high earners") {
    val users = getSampleUsers
    deltaOps.writeUsers(users, "overwrite")

    val highEarners = deltaOps.getHighEarnersAbove(70000.0)
    assert(highEarners.count() == 2)
  }

  test("should update user salary") {
    val users = getSampleUsers
    deltaOps.writeUsers(users, "overwrite")

    deltaOps.updateUserSalary(1L, 85000.0)

    val updatedUsers = deltaOps.readUsers()
    val johnDoe = updatedUsers.filter(updatedUsers("id") === 1L).collect()(0)

    assert(johnDoe.getDouble(johnDoe.fieldIndex("salary")) == 85000.0)
    assert(johnDoe.get(johnDoe.fieldIndex("updated_at")) != null)
  }

  test("should delete user") {
    val users = getSampleUsers
    deltaOps.writeUsers(users, "overwrite")

    deltaOps.deleteUser(2L)

    val remainingUsers = deltaOps.readUsers()
    assert(remainingUsers.count() == 2)
    assert(!remainingUsers.collect().exists(row => row.getLong(row.fieldIndex("id")) == 2L))
  }

  test("should merge users") {
    val initialUsers = getSampleUsers
    deltaOps.writeUsers(initialUsers, "overwrite")

    val currentTime = new Timestamp(System.currentTimeMillis())
    val newUsers = Seq(
      User(1L, "John Doe Updated", "john.doe.updated@example.com", 31, "Engineering", 80000.0, currentTime),
      User(4L, "Alice Brown", "alice.brown@example.com", 27, "HR", 60000.0, currentTime)
    )

    deltaOps.mergeUsers(newUsers)

    val result = deltaOps.readUsers()
    assert(result.count() == 4)

    val johnDoe = result.filter(result("id") === 1L).collect()(0)
    assert(johnDoe.getString(johnDoe.fieldIndex("name")) == "John Doe Updated")
    assert(johnDoe.getString(johnDoe.fieldIndex("email")) == "john.doe.updated@example.com")
  }

  test("should perform time travel") {
    val users = getSampleUsers
    deltaOps.writeUsers(users, "overwrite")

    val version0 = deltaOps.timeTravel(0L)
    assert(version0.count() == 3)
  }

  test("should show table history") {
    val users = getSampleUsers
    deltaOps.writeUsers(users, "overwrite")

    val history = deltaOps.getTableHistory()
    assert(history.count() >= 1)
  }
}