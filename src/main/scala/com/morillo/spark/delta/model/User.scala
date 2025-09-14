package com.morillo.spark.delta.model

import java.sql.Timestamp

case class User(
  id: Long,
  name: String,
  email: String,
  age: Int,
  department: String,
  salary: Double,
  created_at: Timestamp,
  updated_at: Option[Timestamp] = None
)