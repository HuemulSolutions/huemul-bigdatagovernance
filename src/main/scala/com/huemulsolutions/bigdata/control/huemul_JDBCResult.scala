package com.huemulsolutions.bigdata.control

import org.apache.spark.sql._
import java.sql.DriverManager
import java.sql.Connection

class huemul_JDBCResult extends Serializable {
  var ResultSet: Array[Row] = null
  var ErrorDescription: String = ""
  var IsError: Boolean = false
}

class huemul_JDBCProperties(connectionString: String, driver: String) extends Serializable {
  val Driver = driver
  val ConnectionString = connectionString
  var connection: Connection = null
  
  def StartConnection() {
    
    Class.forName(driver)
    this.connection = DriverManager.getConnection(ConnectionString)
    
  }
}