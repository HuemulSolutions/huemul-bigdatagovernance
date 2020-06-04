package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql.types._
import huemulType_SecurityLevel._
import com.huemulsolutions.bigdata.tables.huemulType_StorageType._
import com.huemulsolutions.bigdata.common.huemulType_bigDataProvider._
import com.huemulsolutions.bigdata.common.huemulType_bigDataProvider

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
  
  
  private var _IsPK: Boolean = false
  def getIsPK: Boolean = {return _IsPK}
  @deprecated("this method will be removed, instead use setIsPK(): huemul_Columns", "3.0")
  def setIsPK(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setIsPK, definition is close")
    else
      _IsPK = value
  }
  
  //2.1: return this
  def setIsPK(): huemul_Columns = {
    setIsPK(true)
    this 
  }
  
  private var _IsUnique: Boolean = false
  private var _IsUnique_externalCode: String = "HUEMUL_DQ_003"
  def getIsUnique_externalCode: String = {return if (_IsUnique_externalCode == null) "HUEMUL_DQ_003" else _IsUnique_externalCode}
  def getIsUnique: Boolean = {return _IsUnique}
  @deprecated("this method will be removed, instead use setIsUnique(externalCode: String = null): huemul_Columns", "3.0")
  def setIsUnique(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setIsUnique, definition is close")
    else
      _IsUnique = value
  }
  
  //2.1: return this
  def setIsUnique(externalCode: String = null): huemul_Columns = {
    _IsUnique_externalCode = externalCode
    setIsUnique(true)
    this
  }
  
  private var _Nullable: Boolean = false
  private var _Nullable_externalCode: String = "HUEMUL_DQ_004"
  def getNullable_externalCode: String = {return if (_Nullable_externalCode == null) "HUEMUL_DQ_004" else _Nullable_externalCode}
  def getNullable: Boolean = {return _Nullable}
  @deprecated("this method will be removed, instead use setNullable(externalCode: String = null): huemul_Columns", "3.0")
  def setNullable(value: Boolean) {
    if (DefinitionIsClose && value)
      sys.error("You can't change value of setNullable, definition is close")
    else
      _Nullable = value
  }
  
  //2.1: return this
  def setNullable(externalCode: String = null): huemul_Columns = {
    _Nullable_externalCode = externalCode
    setNullable(true)
    this
  }
  
  private var _DefaultValue: String = "null"
  def getDefaultValue: String = {return _DefaultValue}
  @deprecated("this method will be removed, instead use setDefaultValues(value: String): huemul_Columns", "3.0")
  def setDefaultValue(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDefaultValue, definition is close")
    else
      _DefaultValue = value
  }
  
  //2.1: return this
  def setDefaultValues(value: String): huemul_Columns = {
    setDefaultValue(value)
    this
  }
  
  /** Secret, Restricted, Confidential, Intern, Departamental, Public   
   */
  private var _SecurityLevel: huemulType_SecurityLevel = huemulType_SecurityLevel.Public //Secret, Restricted, Confidential, Intern, Departamental, Public
  def getSecurityLevel: huemulType_SecurityLevel = {return _SecurityLevel}
  @deprecated("this method will be removed, instead use securityLevel(value: huemulType_SecurityLevel): huemul_Columns", "3.0")
  def setSecurityLevel(value: huemulType_SecurityLevel) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setSecurityLevel, definition is close")
    else
      _SecurityLevel = value
  }
  
  //2.1: return this
  /** Secret, Restricted, Confidential, Intern, Departamental, Public   
   */
  def securityLevel(value: huemulType_SecurityLevel): huemul_Columns = {
    setSecurityLevel(value)
    this
  }
  
  /** none, sha2   
   */
  private var _EncryptedType: String = "none" //none, sha2,
  def getEncryptedType: String = {return _EncryptedType}
  @deprecated("this method will be removed, instead use encryptedType(value: String): huemul_Columns", "3.0")
  def setEncryptedType(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setEncryptedType, definition is close")
    else
      _EncryptedType = value
  }
  
  //2.1: return this
  /** none, sha2   
   */
  def encryptedType(value: String): huemul_Columns = {
    setEncryptedType(value)
    this
  }
  
  /** set true for customers data that can be used for identify a unique customer (name, address, phone number)   
   */
  private var arco_Data: Boolean = false
  def getARCO_Data: Boolean = {return arco_Data}
  @deprecated("this method will be removed, instead use setARCO_Data(): huemul_Columns", "3.0")
  def setARCO_Data(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setARCO_Data, definition is close")
    else
      arco_Data = value
  }
  
  //2.1: return this
   /** set true for customers data that can be used for identify a unique customer (name, address, phone number)   
   */
  def setARCO_Data(): huemul_Columns = {
    setARCO_Data(true)
    this
  }
  
  /** associate an external id to link with business glossary concepts 
   */
  private var BusinessGlossary_Id: String = ""
  def getBusinessGlossary_Id: String = {return BusinessGlossary_Id}
  @deprecated("this method will be removed, instead use setBusinessGlossary(value: String): huemul_Columns", "3.0")
  def setBusinessGlossary_Id(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setBusinessGlossary_Id, definition is close")
    else
      BusinessGlossary_Id = value
  }
  
  //from 2.1
  /** associate an external id to link with business glossary concepts 
   */
  def setBusinessGlossary(value: String): huemul_Columns = {
    setBusinessGlossary_Id(value)
    this
  }
  
  private var _DQ_MinLen: Integer = null
  private var _DQ_MinLen_externalCode: String = "HUEMUL_DQ_006"
  def getDQ_MinLen_externalCode: String = {return if (_DQ_MinLen_externalCode == null) "HUEMUL_DQ_006" else _DQ_MinLen_externalCode}
  def getDQ_MinLen: Integer = {return _DQ_MinLen}
  @deprecated("this method will be removed, instead use setDQ_MinLen(value: Integer, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MinLen(value: Integer) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MinLen, definition is close")
    else
      _DQ_MinLen = value
  }
  
  //from 2.1
  def setDQ_MinLen(value: Integer, externalCode: String = null): huemul_Columns = {
    _DQ_MinLen_externalCode = externalCode
    setDQ_MinLen(value)
    this
  }
  
  private var _DQ_MaxLen: Integer = null
  private var _DQ_MaxLen_externalCode: String = "HUEMUL_DQ_006"
  def getDQ_MaxLen_externalCode: String = {return if (_DQ_MaxLen_externalCode == null) "HUEMUL_DQ_006" else _DQ_MaxLen_externalCode}
  def getDQ_MaxLen: Integer = {return _DQ_MaxLen}
  @deprecated("this method will be removed, instead use setDQ_MaxLen(value: Integer, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MaxLen(value: Integer) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MaxLen, definition is close")
    else
      _DQ_MaxLen = value
  }
  
  //from 2.1
  def setDQ_MaxLen(value: Integer, externalCode: String = null): huemul_Columns = {
    _DQ_MaxLen_externalCode = externalCode
    setDQ_MaxLen(value)
    this
  }
  
  private var _DQ_MinDecimalValue: Decimal = null
  private var _DQ_MinDecimalValue_externalCode: String = "HUEMUL_DQ_007"
  def getDQ_MinDecimalValue_externalCode: String = {return if (_DQ_MinDecimalValue_externalCode == null) "HUEMUL_DQ_007" else _DQ_MinDecimalValue_externalCode}
  def getDQ_MinDecimalValue: Decimal = {return _DQ_MinDecimalValue}
  @deprecated("this method will be removed, instead use setDQ_MinDecimalValue(value: Decimal, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MinDecimalValue(value: Decimal) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MinDecimalValue, definition is close")
    else
      _DQ_MinDecimalValue = value
  }
  
  //from 2.1
  def setDQ_MinDecimalValue(value: Decimal, externalCode: String = null): huemul_Columns = {
    _DQ_MinDecimalValue_externalCode = externalCode
    setDQ_MinDecimalValue(value)
    this
  }
  
  private var _DQ_MaxDecimalValue: Decimal = null
  private var _DQ_MaxDecimalValue_externalCode: String = "HUEMUL_DQ_007"
  def getDQ_MaxDecimalValue_externalCode: String = {return if (_DQ_MaxDecimalValue_externalCode == null) "HUEMUL_DQ_007" else _DQ_MaxDecimalValue_externalCode}
  def getDQ_MaxDecimalValue: Decimal = {return _DQ_MaxDecimalValue}
  @deprecated("this method will be removed, instead use setDQ_MaxDecimalValue(value: Decimal, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MaxDecimalValue(value: Decimal) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MaxDecimalValue, definition is close")
    else
      _DQ_MaxDecimalValue = value
  }
  
  //from 2.1
  def setDQ_MaxDecimalValue(value: Decimal, externalCode: String = null): huemul_Columns = {
    _DQ_MaxDecimalValue_externalCode = externalCode
    setDQ_MaxDecimalValue(value)
    this
  }
  
  /**Format: YYYY-MM-DD HH:mm:ss or fieldName   
   */
  private var _DQ_MinDateTimeValue: String = null
  private var _DQ_MinDateTimeValue_externalCode: String = "HUEMUL_DQ_008"
  def getDQ_MinDateTimeValue_externalCode: String = {return if (_DQ_MinDateTimeValue_externalCode==null) "HUEMUL_DQ_008" else _DQ_MinDateTimeValue_externalCode}
  def getDQ_MinDateTimeValue: String = {return _DQ_MinDateTimeValue}
  @deprecated("this method will be removed, instead use setDQ_MinDateTimeValue(value: String, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MinDateTimeValue(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MinDateTimeValue, definition is close")
    else
      _DQ_MinDateTimeValue = value
  }
  
  //from 2.1
  /**Format: YYYY-MM-DD HH:mm:ss or fieldName   
   */
  def setDQ_MinDateTimeValue(value: String, externalCode: String = null): huemul_Columns = {
    _DQ_MinDateTimeValue_externalCode = externalCode
    setDQ_MinDateTimeValue(value)
    this
  }
  
  /**Format: YYYY-MM-DD HH:mm:ss or fieldName   
   */
  private var _DQ_MaxDateTimeValue: String = null
  private var _DQ_MaxDateTimeValue_externalCode: String = "HUEMUL_DQ_008"
  def getDQ_MaxDateTimeValue_externalCode: String = {return if (_DQ_MaxDateTimeValue_externalCode==null) "HUEMUL_DQ_008" else _DQ_MaxDateTimeValue_externalCode}
  def getDQ_MaxDateTimeValue: String = {return _DQ_MaxDateTimeValue}
  @deprecated("this method will be removed, instead use setDQ_MaxDateTimeValue(value: String, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_MaxDateTimeValue(value: String) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_MaxDateTimeValue, definition is close")
    else
      _DQ_MaxDateTimeValue = value
  }
  
  //from 2.1
  /**Format: YYYY-MM-DD HH:mm:ss or fieldName   
   */
  def setDQ_MaxDateTimeValue(value: String, externalCode: String = null): huemul_Columns = {
    _DQ_MaxDateTimeValue_externalCode = externalCode
    setDQ_MaxDateTimeValue(value)
    this
  }
  
   /** Validate Regular Expression
   */
  private var DQ_RegExp: String = null 
  private var DQ_RegExp_externalCode: String = "HUEMUL_DQ_005"
  def getDQ_RegExp: String = {return DQ_RegExp}
  def getDQ_RegExp_externalCode: String = {return if (DQ_RegExp_externalCode == null) "HUEMUL_DQ_005" else DQ_RegExp_externalCode}
  @deprecated("this method will be removed, instead use setDQ_RegExpresion(value: String, externalCode: String = null): huemul_Columns", "3.0")
  def setDQ_RegExp(value: String, dq_externalCode: String = null) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setDQ_RegExp, definition is close")
    else {
      DQ_RegExp = value
      DQ_RegExp_externalCode = dq_externalCode
    }
  }
  
  //from 2.1
  /** Validate Regular Expression
   */
  def setDQ_RegExpresion(value: String, externalCode: String = null): huemul_Columns = {
    setDQ_RegExp(value, externalCode)
    this
  }
  
  /**
   save all old values trace in other table
   */
  private var _MDM_EnableOldValue_FullTrace: Boolean = false
  def getMDM_EnableOldValue_FullTrace: Boolean = {return _MDM_EnableOldValue_FullTrace}
  @deprecated("this method will be removed, instead use setMDM_EnableOldValue_FullTrace(): huemul_Columns", "3.0")
  def setMDM_EnableOldValue_FullTrace(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setMDM_EnableOldValue_FullTrace, definition is close")
    else
      _MDM_EnableOldValue_FullTrace = value
  }
  
  //from 2.1
  /**
   save all old values trace in other table
   */
  def setMDM_EnableOldValue_FullTrace(): huemul_Columns = {
    setMDM_EnableOldValue_FullTrace(true)
    this
  }
  
  /**
   save old value when new value arrive 
   */
  private var _MDM_EnableOldValue: Boolean = false
  def getMDM_EnableOldValue: Boolean = {return _MDM_EnableOldValue}
  @deprecated("this method will be removed, instead use setMDM_EnableOldValue(): huemul_Columns", "3.0")
  def setMDM_EnableOldValue(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setMDM_EnableOldValue, definition is close")
    else
      _MDM_EnableOldValue = value
  }
  
  //from 2.1
  /**
   save old value when new value arrive 
   */
  def setMDM_EnableOldValue(): huemul_Columns = {
    setMDM_EnableOldValue(true)
    this
  }
  
  /**
   create a new field with datetime log for change value
   */
  private var _MDM_EnableDTLog: Boolean = false
  def getMDM_EnableDTLog: Boolean = {return _MDM_EnableDTLog}
  @deprecated("this method will be removed, instead use setMDM_EnableDTLog(): huemul_Columns", "3.0")
  def setMDM_EnableDTLog(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setMDM_EnableDTLog, definition is close")
    else
      _MDM_EnableDTLog = value
  }
  
  //from 2.1
  /**
   create a new field with datetime log for change value
   */
  def setMDM_EnableDTLog(): huemul_Columns = {
    setMDM_EnableDTLog(true)
    this
  }
  
  /**
   create a new field with Process Log for change value
   */
  private var _MDM_EnableProcessLog: Boolean = false
  def getMDM_EnableProcessLog: Boolean = {return _MDM_EnableProcessLog}
  @deprecated("this method will be removed, instead use setMDM_EnableProcessLog(): huemul_Columns", "3.0")
  def setMDM_EnableProcessLog(value: Boolean) {
    if (DefinitionIsClose)
      sys.error("You can't change value of setMDM_EnableProcessLog, definition is close")
    else
      _MDM_EnableProcessLog = value
  }  
  
  //from 2.1
  /**
   create a new field with Process Log for change value
   */
  def setMDM_EnableProcessLog(): huemul_Columns = {
    setMDM_EnableProcessLog(true)
    this
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
  
  //replicated en huemul_BigDataGovernance
  def getCaseType(tableStorage: huemulType_StorageType, value: String): String = {
    return if (tableStorage == huemulType_StorageType.AVRO) value.toLowerCase() else value
  }
  
   def get_MyName(tableStorage: huemulType_StorageType): String = {
    if (this.MyName == null || this.MyName == "")
      sys.error(s"Huemul ERROR: MUST call 'ApplyTableDefinition' in table definition, (field description: ${this.Description} )")

    
    return getCaseType(tableStorage,this.MyName)
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
  
  //from 2.2 --> add setting to HBase
  private var _hive_df: String = "default"
  private var _hive_col: String = null
  
  /**
   * Define family and column name on HBase Tables 
   */
  def setHBaseCatalogMapping(df: String, col: String = null): huemul_Columns = {
    _hive_df = df
    _hive_col = col
    this
  }
  
  def getHBaseCatalogFamily(): String = {return _hive_df}
  def getHBaseCatalogColumn(): String = {return if (_hive_col == null) get_MyName(huemulType_StorageType.HBASE) else _hive_col}
  
  def getHBaseDataType(): String = {
    return if (DataType == DataTypes.StringType) "string"
      else if (DataType == DataTypes.IntegerType) "int"
      else if (DataType == DataTypes.ShortType) "smallint"
      else if (DataType == DataTypes.BooleanType) "boolean"
      else if (DataType == DataTypes.DoubleType) "double"
      else if (DataType == DataTypes.FloatType) "float"
      else if (DataType == DataTypes.LongType) "bigint"
      else if (DataType == DataTypes.ShortType) "tinyint"
      else if (DataType == DataTypes.BinaryType) "binary"
      else "string"
  }
  
  //from 2.5 --> return data type with avro and spark < 2.4 compatibility
  def getDataTypeDeploy(bigDataProvider: huemulType_bigDataProvider, storageType: huemulType_StorageType ): DataType = {
    var result = this.DataType
    if (storageType == huemulType_StorageType.AVRO && 
        bigDataProvider != huemulType_bigDataProvider.databricks &&
        ( result.sql.toUpperCase().contains("DATE") || result.sql.toUpperCase().contains("TIMESTAMP"))
       ) {
      result = StringType
    } else if (storageType == huemulType_StorageType.AVRO && 
        bigDataProvider == huemulType_bigDataProvider.databricks &&
        ( result.sql.toUpperCase().contains("SMALLINT") )
       ) {
      result = IntegerType
    } 
    
    return result
  }
  
  
  
  private var _partitionedPosition: Integer = null
  private var _partitionedDropBeforeSave: Boolean = true
  def setPartitionColumn(position: Integer, dropBeforeInsert: Boolean): huemul_Columns = {
    if (DefinitionIsClose) {
      sys.error("You can't change value of setPartitionColumn, definition is close")
      return this
    }
   
    if (position < 0) {
      sys.error("position value must be >= 0")
      return this
    }

    _partitionedPosition = position
    _partitionedDropBeforeSave = dropBeforeInsert
    
    return this;
  }
  
  
 
}
