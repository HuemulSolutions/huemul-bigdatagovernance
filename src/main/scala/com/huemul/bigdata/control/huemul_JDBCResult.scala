package com.huemul.bigdata.control

import org.apache.spark.sql._

class huemul_JDBCResult extends Serializable {
  var ResultSet: Array[Row] = null
  var ErrorDescription: String = ""
  var IsError: Boolean = false
}