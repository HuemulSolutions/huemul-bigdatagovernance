package com.huemul.bigdata.datalake

import huemulType_Separator._

class huemul_DataLakeSchemaConf extends Serializable {
  /***
   * Type of fields separator: POSITION or CHARACTER
   */
  var ColSeparatorType: huemulType_Separator = huemulType_Separator.CHARACTER
  
  /***
   * RowSeparator for Type CHARACTER
   */
  var ColSeparator: String = ";"
  
  /***
   * List of fields with fixed position
   * Array("FieldsName","StartPosition","EndPosition")
   * Example: Array("ClientName","010","015")
   */
  var ColumnsPosition: Array[Array[String]] = null
  
  /***
   * List of fields header, ";" for Type CHARACTER
   */
  var HeaderColumnsString: String = ""
  
}