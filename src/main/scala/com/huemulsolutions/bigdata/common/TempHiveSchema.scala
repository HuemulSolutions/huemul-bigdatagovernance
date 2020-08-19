package com.huemulsolutions.bigdata.common

import java.io.Serializable

case class TempHiveSchema(database_name: String, table_name: String, column_name: String, datetime_insert: String) extends Serializable {

}
