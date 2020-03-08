package com.huemulsolutions.bigdata.datalake

import huemulType_Separator._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

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
  
  
  var ColumnsDef: ArrayBuffer[huemul_DataLakeColumns] = new  ArrayBuffer[huemul_DataLakeColumns]()
  
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
    ColumnsDef.append(new huemul_DataLakeColumns( columnName_Business
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
  private var customColumn: huemul_DataLakeColumns = null
  
  //new from 2.4
  /**
   * Add Custom column
   */
  def AddCustomColumn(columnName: String
                      , Description: String = "[[missing description]]") {
    
    customColumn = new huemul_DataLakeColumns( columnName
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
  def getCustomColumn(): huemul_DataLakeColumns = {return customColumn}
}

class huemul_DataLakeColumns(columnName_Business: String
                          , columnName_TI: String 
                          , DataType: DataType = StringType
                          , Description: String = null
                          , PosIni: Integer = null
                          , PosFin: Integer = null
                          , ApplyTrim: Boolean = false
                          , ConvertToNull: Boolean = false) extends Serializable {
  def getcolumnName_Business: String = {return columnName_Business}
  def getcolumnName_TI: String  = {return columnName_TI}
  def getDataType: DataType = {return DataType}
  def getDescription: String = {return Description}
  def getPosIni: Integer  = {return PosIni}
  def getPosFin: Integer  = {return PosFin}
  def getApplyTrim: Boolean = {return ApplyTrim}
  def getConvertToNull: Boolean  = {return ConvertToNull}
  
}