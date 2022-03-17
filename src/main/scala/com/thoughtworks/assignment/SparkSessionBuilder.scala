package com.thoughtworks.assignment

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {
  def build : SparkSession={
    SparkSession.builder
      .config(new SparkConf())
      .appName("Moving Average")
      .getOrCreate()
  }
}