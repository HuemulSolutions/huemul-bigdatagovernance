package com.huemulsolutions.bigdata.datalake

import org.apache.spark.sql.types.{DataType, StringType}

class HuemulDataLakeColumns(columnName_Business: String
                            , columnName_TI: String
                            , DataType: DataType = StringType
                            , Description: String = null
                            , PosIni: Integer = null
                            , PosFin: Integer = null
                            , ApplyTrim: Boolean = false
                            , ConvertToNull: Boolean = false) extends Serializable {
  def getcolumnName_Business: String =  columnName_Business
  def getcolumnName_TI: String  =  columnName_TI
  def getDataType: DataType =  DataType
  def getDescription: String =  Description
  def getPosIni: Integer  =  PosIni
  def getPosFin: Integer  =  PosFin
  def getApplyTrim: Boolean =  ApplyTrim
  def getConvertToNull: Boolean  =  ConvertToNull

}
