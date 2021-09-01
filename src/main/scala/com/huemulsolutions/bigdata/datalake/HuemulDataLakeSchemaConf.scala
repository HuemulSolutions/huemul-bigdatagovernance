package com.huemulsolutions.bigdata.datalake

import HuemulTypeSeparator._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

class HuemulDataLakeSchemaConf extends Serializable {
  /***
   * Type of fields separator: POSITION or CHARACTER
   */
  var ColSeparatorType: HuemulTypeSeparator = HuemulTypeSeparator.CHARACTER
  
  /***
   * RowSeparator for Type CHARACTER
   */
  var ColSeparator: String = ";"
  
  /***
   * List of fields with fixed position
   * Array("FieldsName","StartPosition","EndPosition")
   * Example: Array("ClientName","010","015")
   */
  //var ColumnsPosition: Array[Array[String]] = null
  
  /***
   * List of fields header, ";" for Type CHARACTER
   */
  def setHeaderColumnsString(List: String) {
    List.split(";").foreach { x => 
        AddColumns(x)
    }
  }
  //var HeaderColumnsString: String = ""
  
  
  var ColumnsDef: ArrayBuffer[HuemulDataLakeColumns] = new  ArrayBuffer[HuemulDataLakeColumns]()
  
  /**
   * Add DataLake Columns information
   */
  def AddColumns(columnName_Business: String
                , columnName_TI: String = null
                , DataType: DataType = StringType
                , Description: String = "[[missing description]]"
                , PosIni: Integer = null
                , PosFin: Integer = null
                , ApplyTrim: Boolean = false
                , ConvertToNull: Boolean = false
                ) {
    ColumnsDef.append(new HuemulDataLakeColumns( columnName_Business
                                                , if (columnName_TI == null) columnName_Business else columnName_TI
                                                , if (DataType == null) StringType else DataType
                                                , if (Description == null) "[[missing description]]" else Description
                                                , PosIni
                                                , PosFin
                                                , ApplyTrim
                                                , ConvertToNull
                                                ))
  }
  
  //new from 2.4
  private var customColumn: HuemulDataLakeColumns = _
  
  //new from 2.4
  /**
   * Add Custom column
   */
  def AddCustomColumn(columnName: String
                      , Description: String = "[[missing description]]") {
    
    customColumn = new HuemulDataLakeColumns( columnName
                                                , columnName
                                                , StringType 
                                                , if (Description == null) "[[missing description]]" else Description
                                                , 0
                                                , 0
                                                , false
                                                , false
                                                )
  }
  
  //new from 2.4
  def getCustomColumn: HuemulDataLakeColumns =  customColumn
}

