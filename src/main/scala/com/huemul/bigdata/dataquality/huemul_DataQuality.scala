package com.huemul.bigdata.dataquality

import org.apache.spark.sql.types._
import com.huemul.bigdata.tables.huemul_Columns

class huemul_DataQuality(FieldName: huemul_Columns
            ,IsAggregated: Boolean
            ,RaiseError: Boolean
            ,Description: String            
            ,sqlformula: String
            ) extends Serializable {
  /**% of total for refuse validation. Example: 0.15 = 15% (null to not use)
   */
  var Error_Percent: Decimal = null
  /**N° of records for refuse validation. Example: 1000 = 1000 rows with error  (null to not use)
   */
  var Error_MaxNumRows: Long = 0
  /**SQL for validation, expressed in a positive way (boolean) . Example: Field1 < Field2 (field oK)
   */
  def getSQLFormula(): String = {return sqlformula} //= null
  
  private var _Id: Integer = null
  def setId(Id: Integer) {_Id = Id}
  def getId(): Integer = {return _Id}
  
  def getFieldName(): huemul_Columns = {return FieldName}
  def getIsAggregated(): Boolean  = {return IsAggregated}
  def getDescription(): String  ={return  Description}
  def getRaiseError(): Boolean = {return RaiseError}
  
  var NumRowsOK: java.lang.Long = null
  var NumRowsTotal: java.lang.Long = null
  
  
  var ResultDQ: String = null
  
  /**
   * MyName
   */
  private var MyName: String = null 
  def setMyName(name: String) {
    MyName = name
  }
  def getMyName(): String = {return MyName}
 
}