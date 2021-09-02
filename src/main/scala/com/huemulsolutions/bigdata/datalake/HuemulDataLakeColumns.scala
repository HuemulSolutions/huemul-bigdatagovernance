package com.huemulsolutions.bigdata.datalake

import org.apache.spark.sql.types.{DataType, StringType}

class HuemulDataLakeColumns(columnNameBusiness: String
                            , columnNameTi: String
                            , dataType: DataType = StringType
                            , description: String = null
                            , posIni: Integer = null
                            , posFin: Integer = null
                            , applyTrim: Boolean = false
                            , convertToNull: Boolean = false) extends Serializable {
  def getColumnNameBusiness: String =  columnNameBusiness
  def getColumnNameTi: String  =  columnNameTi
  def getDataType: DataType =  dataType
  def getDescription: String =  description
  def getPosIni: Integer  =  posIni
  def getPosFin: Integer  =  posFin
  def getApplyTrim: Boolean =  applyTrim
  def getConvertToNull: Boolean  =  convertToNull

}
