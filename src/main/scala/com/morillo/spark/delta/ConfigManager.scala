package com.morillo.spark.delta

import com.typesafe.config.{Config, ConfigFactory}

object ConfigManager {

  def loadConfig(environment: String = "dev"): Config = {
    val configFile = environment match {
      case "prod" | "production" => "application-prod.conf"
      case _ => "application.conf"
    }

    ConfigFactory.load(configFile)
  }

  def getEnvironment: String = {
    sys.env.getOrElse("ENVIRONMENT", "dev")
  }

  def getConfig: Config = {
    loadConfig(getEnvironment)
  }
}