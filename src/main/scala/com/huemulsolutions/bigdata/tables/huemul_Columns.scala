package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql.types._
import huemulType_SecurityLevel._

/**
 Define Data Schema for master and dimensional models <br>
      param_DataType: DataType <br>
    , param_Required: Boolean <br> 
    , param_Description: String <br>
    , param_UsedForCheckSum: Boolean <br> 
 */
class huemul_Columns(param_DataType: DataType
                    , param_Required: Boolean
                    , param_Description: String
                    , param_UsedForCheckSum: Boolean = true) extends Serializable {
  val DataType: DataType = param_DataType
  val Description: String = param_Description
  val Required: Boolean = false
  val UsedForCheckSum: Boolean = param_UsedForCheckSum
  
  var IsPK: Boolean = false
  var IsUnique: Boolean = false
  var Nullable: Boolean = true  
  var DefaultValue: String = "null"
  /** Secret, Restricted, Confidential, Intern, Departamental, Public   
   */
  var SecurityLevel: huemulType_SecurityLevel = huemulType_SecurityLevel.Public //Secret, Restricted, Confidential, Intern, Departamental, Public
  /** none, sha2   
   */
  var EncryptedType: String = "none" //none, sha2,
  /** set true for customers data that can be used for identify a unique customer (name, address, phone number)   
   */
  var ARCO_Data: Boolean = false
  
  var DQ_MinLen: Integer = null
  var DQ_MaxLen: Integer = null
  
  var DQ_MinDecimalValue: Decimal = null
  var DQ_MaxDecimalValue: Decimal = null
  
  /**Format: YYYY-MM-DD HH:mm:ss or fieldName   
   */
  var DQ_MinDateTimeValue: String = null
  /**Format: YYYY-MM-DD HH:mm:ss or fieldName   
   */
  var DQ_MaxDateTimeValue: String = null
  
  /**
   save old value when new value arrive 
   */
  var MDM_EnableOldValue: Boolean = false
  /**
   create a new field with datetime log for change value
   */
  var MDM_EnableDTLog: Boolean = false
  /**
   create a new field with Process Log for change value
   */
  var MDM_EnableProcessLog: Boolean = false
  
  
   
  
  //user mapping attributes
  private var MappedName: String = null
  private var MyName: String = null
  private var ReplaceValueOnUpdate: Boolean = false
  private var SQLForUpdate: String = null
  private var SQLForInsert: String = null
  
  def Set_MyName(pname: String) {
    this.MyName = pname
  }
  
   def get_MyName(): String = {
    if (this.MyName == null || this.MyName == "")
      sys.error(s"DAPI ERROR: MUST call 'ApplyTableDefinition' in table definition, (field description: ${this.Description} )")

    return this.MyName
  }
  
  def get_MappedName(): String = {
    return this.MappedName
  }
  def get_ReplaceValueOnUpdate(): Boolean = {
    return this.ReplaceValueOnUpdate
  }
  def get_SQLForUpdate(): String = {
    return this.SQLForUpdate
  }
  def get_SQLForInsert(): String = {
    return this.SQLForInsert
  }
  
  /**
   mapping DataFrame fields to Table Fields for insert and update. <br>
   param_name: fields name in DataFrame <br>
   ReplaceOldValueOnUpdate: true (default) for replace values in table <br>
   SQLForUpdate: example: cast(case when old.[field] > new.[field] then old.[field] else new.[field] end as Integer)  <br>
   SQLForInsert: example: cast(case when old.[field] > new.[field] then old.[field] else new.[field] end as Integer)  <br>
  <b> in both case, don't iunclude "as Namefield". "old" is reference to table, new is reference to your DataFrame </b> 
   */
  def SetMapping(param_name: String, ReplaceValueOnUpdate: Boolean = true, SQLForUpdate: String = null, SQLForInsert: String = null) {
    this.MappedName = param_name
    this.ReplaceValueOnUpdate = ReplaceValueOnUpdate
    this.SQLForUpdate = SQLForUpdate
    this.SQLForInsert = SQLForInsert
  }
  
}
