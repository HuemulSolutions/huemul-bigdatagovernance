package com.huemulsolutions.bigdata.datalake

import HuemulTypeSeparator._
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

class HuemulDataLakeSchemaConf extends Serializable {
  /***
   * Type of fields separator: POSITION or CHARACTER
   */
  var colSeparatorType: HuemulTypeSeparator = HuemulTypeSeparator.CHARACTER
  
  /***
   * RowSeparator for Type CHARACTER
   */
  var colSeparator: String = ";"
  
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
        addColumns(x)
    }
  }
  //var HeaderColumnsString: String = ""


  var columnsDef: ArrayBuffer[HuemulDataLakeColumns] = new  ArrayBuffer[HuemulDataLakeColumns]()

  /**
   * Add DataLake Columns information
   */
  def addColumns(columnNameBusiness: String
                 , columnNameTi: String = null
                 , dataType: DataType = StringType
                 , description: String = "[[missing description]]"
                 , posIni: Integer = null
                 , posFin: Integer = null
                 , applyTrim: Boolean = false
                 , convertToNull: Boolean = false
                ) {
    columnsDef.append(new HuemulDataLakeColumns( columnNameBusiness
                                                , if (columnNameTi == null) columnNameBusiness else columnNameTi
                                                , if (dataType == null) StringType else dataType
                                                , if (description == null) "[[missing description]]" else description
                                                , posIni
                                                , posFin
                                                , applyTrim
                                                , convertToNull
                                                ))
  }
  
  //new from 2.4
  private var customColumn: HuemulDataLakeColumns = _
  
  //new from 2.4
  /**
   * Add Custom column
   */
  def addCustomColumn(columnName: String
                      , description: String = "[[missing description]]") {

    customColumn = new HuemulDataLakeColumns( columnName
                                                , columnName
                                                , StringType
                                                , if (description == null) "[[missing description]]" else description
                                                , 0
                                                , 0
                                                , false
                                                , false
                                                )
  }
  
  //new from 2.4
  def getCustomColumn: HuemulDataLakeColumns =  customColumn
}

