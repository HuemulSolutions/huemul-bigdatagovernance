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
  val Required: Boolean = param_Required
  val UsedForCheckSum: Boolean = param_UsedForCheckSum
  
  private var DefinitionIsClose: Boolean = false
  def SetDefinitionIsClose() {DefinitionIsClose = true}
  
  
  private var IsPK: Boolean = false
  def getIsPK: Boolean = {return IsPK}
  def setIsPK(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setIsPK, definition is close")
    else
      IsPK = value
  }
  
  private var IsUnique: Boolean = false
  def getIsUnique: Boolean = {return IsUnique}
  def setIsUnique(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setIsUnique, definition is close")
    else
      IsUnique = value
  }
  
  private var Nullable: Boolean = false
  def getNullable: Boolean = {return Nullable}
  def setNullable(value: Boolean) {
    if (DefinitionIsClose && value)
      sys.error("You can't change value of setNullable, definition is close")
    else
      Nullable = value
  }
  
  private var DefaultValue: String = "null"
  def getDefaultValue: String = {return DefaultValue}
  def setDefaultValue(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDefaultValue, definition is close")
    else
      DefaultValue = value
  }
  /** Secret, Restricted, Confidential, Intern, Departamental, Public   
   */
  private var SecurityLevel: huemulType_SecurityLevel = huemulType_SecurityLevel.Public //Secret, Restricted, Confidential, Intern, Departamental, Public
  def getSecurityLevel: huemulType_SecurityLevel = {return SecurityLevel}
  def setSecurityLevel(value: huemulType_SecurityLevel) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setSecurityLevel, definition is close")
    else
      SecurityLevel = value
  }
  /** none, sha2   
   */
  private var EncryptedType: String = "none" //none, sha2,
  def getEncryptedType: String = {return EncryptedType}
  def setEncryptedType(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setEncryptedType, definition is close")
    else
      EncryptedType = value
  }
  
  /** set true for customers data that can be used for identify a unique customer (name, address, phone number)   
   */
  private var ARCO_Data: Boolean = false
  def getARCO_Data: Boolean = {return ARCO_Data}
  def setARCO_Data(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setARCO_Data, definition is close")
    else
      ARCO_Data = value
  }
  
  /** associate an external id to link with business glossary concepts 
   */
  private var BusinessGlossary_Id: String = ""
  def getBusinessGlossary_Id: String = {return BusinessGlossary_Id}
  def setBusinessGlossary_Id(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setBusinessGlossary_Id, definition is close")
    else
      BusinessGlossary_Id = value
  }
  
  private var DQ_MinLen: Integer = null
  def getDQ_MinLen: Integer = {return DQ_MinLen}
  def setDQ_MinLen(value: Integer) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MinLen, definition is close")
    else
      DQ_MinLen = value
  }
  
  private var DQ_MaxLen: Integer = null
  def getDQ_MaxLen: Integer = {return DQ_MaxLen}
  def setDQ_MaxLen(value: Integer) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MaxLen, definition is close")
    else
      DQ_MaxLen = value
  }
  
  private var DQ_MinDecimalValue: Decimal = null
  def getDQ_MinDecimalValue: Decimal = {return DQ_MinDecimalValue}
  def setDQ_MinDecimalValue(value: Decimal) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MinDecimalValue, definition is close")
    else
      DQ_MinDecimalValue = value
  }
  
  private var DQ_MaxDecimalValue: Decimal = null
  def getDQ_MaxDecimalValue: Decimal = {return DQ_MaxDecimalValue}
  def setDQ_MaxDecimalValue(value: Decimal) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MaxDecimalValue, definition is close")
    else
      DQ_MaxDecimalValue = value
  }
  
  /**Format: YYYY-MM-DD HH:mm:ss or fieldName   
   */
  private var DQ_MinDateTimeValue: String = null
  def getDQ_MinDateTimeValue: String = {return DQ_MinDateTimeValue}
  def setDQ_MinDateTimeValue(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MinDateTimeValue, definition is close")
    else
      DQ_MinDateTimeValue = value
  }
  
  /**Format: YYYY-MM-DD HH:mm:ss or fieldName   
   */
  private var DQ_MaxDateTimeValue: String = null
  def getDQ_MaxDateTimeValue: String = {return DQ_MaxDateTimeValue}
  def setDQ_MaxDateTimeValue(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MaxDateTimeValue, definition is close")
    else
      DQ_MaxDateTimeValue = value
  }
  
   /** Validate Regular Expression
   */
  private var DQ_RegExp: String = null //none, sha2,
  private var DQ_RegExp_externalCode: String = null
  def getDQ_RegExp: String = {return DQ_RegExp}
  def getDQ_RegExp_externalCode: String = {return DQ_RegExp_externalCode}
  def setDQ_RegExp(value: String, dq_externalCode: String = null) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_RegExp, definition is close")
    else {
      DQ_RegExp = value
      DQ_RegExp_externalCode = dq_externalCode
    }
  }
  
  /**
   save all old values trace in other table
   */
  private var MDM_EnableOldValue_FullTrace: Boolean = false
  def getMDM_EnableOldValue_FullTrace: Boolean = {return MDM_EnableOldValue_FullTrace}
  def setMDM_EnableOldValue_FullTrace(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setMDM_EnableOldValue_FullTrace, definition is close")
    else
      MDM_EnableOldValue_FullTrace = value
  }
  
  /**
   save old value when new value arrive 
   */
  private var MDM_EnableOldValue: Boolean = false
  def getMDM_EnableOldValue: Boolean = {return MDM_EnableOldValue}
  def setMDM_EnableOldValue(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setMDM_EnableOldValue, definition is close")
    else
      MDM_EnableOldValue = value
  }
  /**
   create a new field with datetime log for change value
   */
  private var MDM_EnableDTLog: Boolean = false
  def getMDM_EnableDTLog: Boolean = {return MDM_EnableDTLog}
  def setMDM_EnableDTLog(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setMDM_EnableDTLog, definition is close")
    else
      MDM_EnableDTLog = value
  }
  
  /**
   create a new field with Process Log for change value
   */
  private var MDM_EnableProcessLog: Boolean = false
  def getMDM_EnableProcessLog: Boolean = {return MDM_EnableProcessLog}
  def setMDM_EnableProcessLog(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setMDM_EnableProcessLog, definition is close")
    else
      MDM_EnableProcessLog = value
  }  
   
  
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
      sys.error(s"Huemul ERROR: MUST call 'ApplyTableDefinition' in table definition, (field description: ${this.Description} )")

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
  <b> in both cases, don't include "as Namefield". "old" is a reference to the table, new is reference to your DataFrame </b> 
   */
  def SetMapping(param_name: String, ReplaceValueOnUpdate: Boolean = true, SQLForUpdate: String = null, SQLForInsert: String = null) {
    this.MappedName = param_name
    this.ReplaceValueOnUpdate = ReplaceValueOnUpdate
    this.SQLForUpdate = SQLForUpdate
    this.SQLForInsert = SQLForInsert
  }
  
}
