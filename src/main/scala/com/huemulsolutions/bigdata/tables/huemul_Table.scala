package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import java.lang.Long
import scala.collection.mutable._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.permission.FsPermission
import huemulType_Tables._
import huemulType_StorageType._
//import com.huemulsolutions.bigdata._
import com.huemulsolutions.bigdata.dataquality.huemul_DataQuality
import com.huemulsolutions.bigdata.dataquality.huemul_DataQualityResult
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control.huemul_Control
import com.huemulsolutions.bigdata.control.huemulType_Frequency
import com.huemulsolutions.bigdata.control.huemulType_Frequency._

import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel._
//import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel.huemulType_DQQueryLevel
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification._
//import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification.huemulType_DQNotification
import com.huemulsolutions.bigdata.dataquality.huemul_DQRecord
//import com.huemulsolutions.bigdata.control.huemulType_Frequency
//import com.huemulsolutions.bigdata.control.huemulType_Frequency.huemulType_Frequency
//import com.huemulsolutions.bigdata.control.huemulType_Frequency.huemulType_Frequency
//import com.huemulsolutions.bigdata.tables.huemulType_Tables.huemulType_Tables
import com.huemulsolutions.bigdata.tables.huemulType_InternalTableType._
import com.huemulsolutions.bigdata.datalake.huemul_DataLake
//import com.huemulsolutions.bigdata.control.huemulType_Frequency.huemulType_Frequency



class huemul_Table(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_TableDQ with Serializable  {
  if (Control == null) 
    sys.error("Control is null in huemul_DataFrame")
  
  /*  ********************************************************************************
   *****   T A B L E   P R O P E R T I E S    **************************************** 
   ******************************************************************************** */
  /**
   Table Name
   */
  val TableName : String= this.getClass.getSimpleName.replace("$", "") // ""
  huemulBigDataGov.logMessageInfo(s"HuemulControl:    table instance: ${TableName}")
  
  def setDataBase(value: ArrayBuffer[huemul_KeyValuePath]) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of DataBase, definition is close", 1033)
    else
      _DataBase = value
  }
  private var _DataBase: ArrayBuffer[huemul_KeyValuePath] = null
    
  /**
   Type of Table (Reference; Master; Transaction
   */
  def setTableType(value: huemulType_Tables) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of TableType, definition is close", 1033)
    else
      _TableType = value
  }
  def getTableType: huemulType_Tables = {return _TableType}
  private var _TableType : huemulType_Tables = huemulType_Tables.Transaction
  
  private var _PK_externalCode: String = "HUEMUL_DQ_002"
  def getPK_externalCode: String = {return if (_PK_externalCode==null) "HUEMUL_DQ_002" else _PK_externalCode }
  def setPK_externalCode(value: String) {
    _PK_externalCode = value  
  }
  
  private def _storageType_default = huemulType_StorageType.PARQUET
  /**
   Save DQ Result to disk, only if DQ_SaveErrorDetails in GlobalPath is true
   */
  def setSaveDQResult(value: Boolean) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of SaveDQResult, definition is close", 1033)
    else
      _SaveDQResult = value
  }
  def getSaveDQResult: Boolean = {return _SaveDQResult}
  private var _SaveDQResult : Boolean = true
  
  /**
   Save Backup to disk, only if MDM_SaveBackup in GlobalPath is false
   */
  def setSaveBackup(value: Boolean) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of SaveBackup, definition is close", 1033)
    else
      _SaveBackup = value
  }
  def getSaveBackup: Boolean = {return _SaveBackup}
  private var _SaveBackup : Boolean = false
  def _getBackupPath(): String = {return _BackupPath }
  private var _BackupPath: String = ""
  
  /**
   Type of Persistent storage (parquet, csv, json)
   */
  def setStorageType(value: huemulType_StorageType) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of StorageType, definition is close", 1033)
    else
      _StorageType = value
  }
  def getStorageType: huemulType_StorageType = {return _StorageType}
  private var _StorageType : huemulType_StorageType = null
  
  
  /**
   Type of Persistent storage (parquet, csv, json) for DQ
   */
  def setStorageType_DQResult(value: huemulType_StorageType) {
    if (_StorageType_DQResult == huemulType_StorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
      
    if (DefinitionIsClose)
      this.raiseError("You can't change value of StorageType_DQResult, definition is close", 1033)
    else
      _StorageType_DQResult = value
  }
  def getStorageType_DQResult: huemulType_StorageType = {
    if (_StorageType_DQResult == huemulType_StorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
    return   if (_StorageType_DQResult != null) _StorageType_DQResult
        else if (_StorageType_DQResult == null && getStorageType == huemulType_StorageType.HBASE ) _storageType_default  
        else getStorageType
  }
  private var _StorageType_DQResult : huemulType_StorageType = null
  
  /**
   Type of Persistent storage (parquet, csv, json) for DQ
   */
  def setStorageType_OldValueTrace(value: huemulType_StorageType) {
    if (_StorageType_OldValueTrace == huemulType_StorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for OldValueTraceTable", 1063)
      
    if (DefinitionIsClose)
      this.raiseError("You can't change value of StorageType_OldValueTrace, definition is close", 1033)
    else
      _StorageType_OldValueTrace = value
  }
  def getStorageType_OldValueTrace: huemulType_StorageType = {
    if (_StorageType_OldValueTrace == huemulType_StorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for OldValueTraceTable", 1063)
    return   if (_StorageType_OldValueTrace != null) _StorageType_OldValueTrace
        else if (_StorageType_OldValueTrace == null && getStorageType == huemulType_StorageType.HBASE ) _storageType_default  
        else getStorageType    
  }
  private var _StorageType_OldValueTrace : huemulType_StorageType = null
  
  //val _StorageType_OldValueTrace: String = "parquet"  //csv, json, parquet, orc
  /**
    Table description
   */
  def setDescription(value: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of Description, definition is close", 1033)
    else
      _Description = value
  }
  def getDescription: String = {return _Description}
  private var _Description   : String= ""
  
  /**
    Responsible contact in IT
   */
  def setIT_ResponsibleName(value: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of IT_ResponsibleName, definition is close", 1033)
    else
      _IT_ResponsibleName = value
  }
  def getIT_ResponsibleName: String = {return _IT_ResponsibleName}
  private var _IT_ResponsibleName   : String= ""
  
  /**
    Responsible contact in Business (Name & Area)
   */
  def setBusiness_ResponsibleName(value: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of Business_ResponsibleName, definition is close", 1033)
    else
      _Business_ResponsibleName = value
  }
  def getBusiness_ResponsibleName: String = {return _Business_ResponsibleName}
  private var _Business_ResponsibleName   : String= ""
  
  /**
    Fields used to partition table 
   */
  def setPartitionField(value: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of PartitionField, definition is close", 1033)
    else
      _PartitionField = value
  }
  def getPartitionField: String = {return _PartitionField}
  private var _PartitionField   : String= null
  
  /**
    Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  def setLocalPath(value: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of LocalPath, definition is close", 1033)
    else
      _LocalPath = value
  }
  def getLocalPath: String = {return _LocalPath}
  private var _LocalPath   : String= ""
  
  def setGlobalPaths(value: ArrayBuffer[huemul_KeyValuePath]) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of GlobalPaths, definition is close", 1033)
    else
      _GlobalPaths = value
  }
  def getGlobalPaths: ArrayBuffer[huemul_KeyValuePath] = {return _GlobalPaths}
  private var _GlobalPaths: ArrayBuffer[huemul_KeyValuePath] = null
  
  def WhoCanRun_executeFull_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeFull, definition is close", 1033)
    else
      _WhoCanRun_executeFull.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeFull: huemul_Authorization = {return _WhoCanRun_executeFull}
  private var _WhoCanRun_executeFull: huemul_Authorization = new huemul_Authorization()
  
  def WhoCanRun_executeOnlyInsert_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeOnlyInsert, definition is close", 1033)
    else
      _WhoCanRun_executeOnlyInsert.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeOnlyInsert: huemul_Authorization = {return _WhoCanRun_executeOnlyInsert}
  private var _WhoCanRun_executeOnlyInsert: huemul_Authorization = new huemul_Authorization()
  
  def WhoCanRun_executeOnlyUpdate_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeOnlyUpdate, definition is close", 1033)
    else
      _WhoCanRun_executeOnlyUpdate.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeOnlyUpdate: huemul_Authorization = {return _WhoCanRun_executeOnlyUpdate}
  private var _WhoCanRun_executeOnlyUpdate: huemul_Authorization = new huemul_Authorization()
  
  /**
    Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  def setFrequency(value: huemulType_Frequency) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of Frequency, definition is close", 1033)
    else
      _Frequency = value
  }
  private var _Frequency: huemulType_Frequency = null
  def getFrequency: huemulType_Frequency = {return _Frequency}
  
  /**
   * SaveDQErrorOnce: false (default value) to save DQ result to disk only once (all together)
   * false to save DQ result to disk independently (each DQ error or warning)
  */
  def setSaveDQErrorOnce(value: Boolean) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of SaveDQErrorOnce, definition is close", 1033)
    else
      _SaveDQErrorOnce = value
  }
  private var _SaveDQErrorOnce: Boolean = false
  def getSaveDQErrorOnce: Boolean = {return _SaveDQErrorOnce}
  
  
  /**
   * Automatically map query names
   */
  def setMappingAuto() {
    getALLDeclaredFields(true).foreach { x =>  
      x.setAccessible(true)
          
      //Nombre de campos
      if (x.get(this).isInstanceOf[huemul_Columns]) {
        val DataField = x.get(this).asInstanceOf[huemul_Columns]
        DataField.SetMapping(DataField.get_MyName())
      }
    }
  }
    
  
  /**
   DataQuality: max N° records, null does'nt apply  DQ , 0 value doesn't accept new records (raiseError if new record found)
   If table is empty, DQ doesn't apply
   */
  def setDQ_MaxNewRecords_Num(value: Long) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of DQ_MaxNewRecords_Num, definition is close", 1033)
    else
      _DQ_MaxNewRecords_Num = value
  }
  def getDQ_MaxNewRecords_Num: Long = {return _DQ_MaxNewRecords_Num}
  private var _DQ_MaxNewRecords_Num: Long = null
  /**
   DataQuality: Max % new records vs old records, null does'n apply DQ, 0% doesn't accept new records (raiseError if new record found), 100%-> 0.1 accept double old records)
   */
  def setDQ_MaxNewRecords_Perc(value: Decimal) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of DQ_MaxNewRecords_Perc, definition is close", 1033)
    else
      _DQ_MaxNewRecords_Perc = value
  }
  def getDQ_MaxNewRecords_Perc: Decimal = {return _DQ_MaxNewRecords_Perc}
  private var _DQ_MaxNewRecords_Perc: Decimal = null
  
  //V1.3 --> set num partitions with repartition
  /**
   Set num of partitions (repartition function used). Value null for default behavior
   */
  def setNumPartitions(value: Integer) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of NumPartitions, definition is close", 1033)
    else
      _NumPartitions = value
  }
  def getNumPartitions: Integer = {return _NumPartitions}
  private var _NumPartitions : Integer = 0
  
  private val numPartitionsForDQFiles: Integer = 2
  
  /****** METODOS DEL LADO DEL "USUARIO" **************************/
  
  private var autoCast: Boolean = true
  def setAutoCast(value: Boolean) {autoCast = value}
  
  private var ApplyDistinct: Boolean = true
  def setApplyDistinct(value: Boolean) {ApplyDistinct = value}
  
  
  private var Table_id: String = ""
  
  
  
  private var CreateInHive: Boolean = true
  private var CreateTableScript: String = ""
  private var PartitionValue: String = null
  def getPartitionValue(): String = {return PartitionValue}
  var _tablesUseId: String = null
  private var DF_DQErrorDetails: DataFrame = null
  
  /*  ********************************************************************************
   *****   F I E L D   P R O P E R T I E S    **************************************** 
   ******************************************************************************** */
  
  
  val MDM_newValue = new huemul_Columns (StringType, true, "New value updated in table", false).setHBaseCatalogMapping("loginfo")
  val MDM_oldValue = new huemul_Columns (StringType, true, "Old value", false).setHBaseCatalogMapping("loginfo")
  val MDM_AutoInc = new huemul_Columns (LongType, true, "auto incremental for version control", false).setHBaseCatalogMapping("loginfo")
  val processExec_id = new huemul_Columns (StringType, true, "Process Execution Id (control model)", false).setHBaseCatalogMapping("loginfo")
  
  val MDM_fhNew = new huemul_Columns (TimestampType, true, "Fecha/hora cuando se insertaron los datos nuevos", false).setHBaseCatalogMapping("loginfo")
  val MDM_ProcessNew = new huemul_Columns (StringType, false, "Nombre del proceso que insertó los datos", false).setHBaseCatalogMapping("loginfo")
  val MDM_fhChange = new huemul_Columns (TimestampType, false, "fecha / hora de último cambio de valor en los campos de negocio", false).setHBaseCatalogMapping("loginfo")
  val MDM_ProcessChange = new huemul_Columns (StringType, false, "Nombre del proceso que cambió los datos", false).setHBaseCatalogMapping("loginfo")
  val MDM_StatusReg = new huemul_Columns (IntegerType, true, "indica si el registro fue insertado en forma automática por otro proceso (1), o fue insertado por el proceso formal (2), si está eliminado (-1)", false).setHBaseCatalogMapping("loginfo")
  val MDM_hash = new huemul_Columns (StringType, true, "Valor hash de los datos de la tabla", false).setHBaseCatalogMapping("loginfo")
  
  val MDM_columnName = new huemul_Columns (StringType, true, "Column Name", false).setHBaseCatalogMapping("loginfo")
  
  //from 2.2 --> add rowKey compatibility with HBase
  val hs_rowKey = new huemul_Columns (StringType, true, "Concatenated PK", false).setHBaseCatalogMapping("loginfo")
  
  var AdditionalRowsForDistint: String = ""
  private var DefinitionIsClose: Boolean = false
  
  /***
   * from Global path (small files, large files, DataBase, etc)
   */
  def globalPath()  : String= {
    return getPath(_GlobalPaths)
  }
  
  def globalPath(ManualEnvironment: String)  : String= {
    return getPath(_GlobalPaths, ManualEnvironment)
  }
  
  
  /**
   * Get Fullpath hdfs = GlobalPaths + LocalPaths + TableName  
   */
  def getFullNameWithPath() : String = {
    return globalPath + _LocalPath + TableName
  }
  
  /**
   * Get Fullpath hdfs for DQ results = GlobalPaths + DQError_Path + TableName + "_dq"
   */
  def getFullNameWithPath_DQ() : String = {
    return this.getPath(huemulBigDataGov.GlobalSettings.DQError_Path) + this.getDataBase(this._DataBase) + '/' + _LocalPath + TableName + "_dq"
  }
  
  /**
   * Get Fullpath hdfs for _oldvalue results = GlobalPaths + DQError_Path + TableName + "_oldvalue"
   */
  def getFullNameWithPath_OldValueTrace() : String = {
    return this.getPath(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_Path) + this.getDataBase(this._DataBase) + '/' + _LocalPath + TableName + "_oldvalue"
  }
  
   /**
   * Get Fullpath hdfs for backpu  = Backup_Path + database + TableName + "_backup"
   */
  def getFullNameWithPath_Backup(control_id: String) : String = {
    return this.getPath(huemulBigDataGov.GlobalSettings.MDM_Backup_Path) + this.getDataBase(this._DataBase) + '/' + _LocalPath + 'c' + control_id + '/' + TableName + "_backup"
  }
  
  def getFullNameWithPath2(ManualEnvironment: String) : String = {
    return globalPath(ManualEnvironment) + _LocalPath + TableName
  }
    
  def getFullPath() : String = {
    return globalPath + _LocalPath 
  }
  
  
  /**
   * Return DataBaseName.TableName
   */
  private var _TableWasRegistered: Boolean = false
  def getTable(): String = {
    if (!_TableWasRegistered) {
      Control.RegisterMASTER_USE(this)
      _TableWasRegistered = true
    }
    
    return internalGetTable(huemulType_InternalTableType.Normal)
  }
  
  //new from 2.1
  /**
   * Return DataBaseName.TableName for DQ
   */
  def getTable_DQ(): String = {
    return internalGetTable(huemulType_InternalTableType.DQ)
  }
  
  //new from 2.1
  /**
   * Return DataBaseName.TableName for OldValueTrace
   */
  def getTable_OldValueTrace(): String = {
    return internalGetTable(huemulType_InternalTableType.OldValueTrace)
  }
  
  //new from 2.2
  /**
   * Define HBase Table and namespace 
   */
  private var _hbase_namespace: String = null
  private var _hbase_tableName: String = null
  def setHBaseTableName(namespace: String, tableName: String = null) {
    _hbase_namespace = namespace
    _hbase_tableName = tableName
  }
  
  def getHBaseNamespace(internalTableType: huemulType_InternalTableType): String = {
    var valueName: String = null
    if (internalTableType == huemulType_InternalTableType.DQ) {
      valueName = getDataBase(huemulBigDataGov.GlobalSettings.DQError_DataBase)
    } else if (internalTableType == huemulType_InternalTableType.Normal) { 
      valueName = if ( _hbase_namespace == null) this.getDataBase(this._DataBase) else _hbase_namespace
    } else if (internalTableType == huemulType_InternalTableType.OldValueTrace) { 
      valueName = getDataBase(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_DataBase)
    } else {
      raiseError(s"huemul_Table Error: Type '${internalTableType}' doesn't exist in InternalGetTable method (getHBaseNamespace)", 1059)
    }
    
     return valueName
    
  }
  
  def getHBaseTableName(internalTableType: huemulType_InternalTableType): String = {
    var valueName: String = if (_hbase_tableName == null) this.TableName else _hbase_tableName
    if (internalTableType == huemulType_InternalTableType.DQ) {
      valueName =  valueName + "_dq"
    } else if (internalTableType == huemulType_InternalTableType.Normal) { 
      valueName = valueName
    } else if (internalTableType == huemulType_InternalTableType.OldValueTrace) { 
      valueName = valueName + "_oldvalue"
    } else {
      raiseError(s"huemul_Table Error: Type '${internalTableType}' doesn't exist in InternalGetTable method (getHBaseTableName)", 1060)
    }
     
    return valueName
  }
  
  /**
   * get Catalog for HBase mapping
   */
  
  def getHBaseCatalog(tableType: huemulType_InternalTableType): String = {

    //create fields
    var fields: String = s""""${if (_numPKColumns == 1) _HBase_PKColumn else "hs_rowkey"}":{"cf":"rowkey","col":"key","type":"string"} """
    getALLDeclaredFields().filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] 
    } foreach { x =>
      x.setAccessible(true)
      var dataField = x.get(this).asInstanceOf[huemul_Columns]
      
      if (!(dataField.getIsPK && _numPKColumns == 1)) {        
        //fields = fields + s""", \n "${x.getName}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}","type":"${dataField.getHBaseDataType()}"} """
        fields = fields + s""", \n "${x.getName}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}","type":"${dataField.getHBaseDataType()}"} """
      
        if (tableType != huemulType_InternalTableType.OldValueTrace) {
          if (dataField.getMDM_EnableOldValue)
            fields = fields + s""", \n "${x.getName}_old":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}_old","type":"${dataField.getHBaseDataType()}"} """
          if (dataField.getMDM_EnableDTLog)
            fields = fields + s""", \n "${x.getName}_fhChange":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}_fhChange","type":"string"} """
          if (dataField.getMDM_EnableProcessLog)
            fields = fields + s""", \n "${x.getName}_ProcessLog":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}_ProcessLog","type":"string"} """
        }
        
      }
    }
    
    //create struct
    var result = s"""{
        "table":{"namespace":"${getHBaseNamespace(tableType)}", "name":"${getHBaseNamespace(tableType)}:${getHBaseTableName(tableType)}"},
        "rowkey":"key",
        "columns":{${fields}
         }
      }""".stripMargin
    
    return result
  }
  
  
  /**
   * get Catalog for HBase mapping
   */
  def getHBaseCatalogForHIVE(tableType: huemulType_InternalTableType): String = {
   //WARNING!!! Any changes you make to this code, repeat it in getColumns_CreateTable
    //create fields
    var fields: String = s":key"
    getALLDeclaredFields().filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] 
    } foreach { x =>
      x.setAccessible(true)
      var dataField = x.get(this).asInstanceOf[huemul_Columns]
      
      if (!(dataField.getIsPK && _numPKColumns == 1)) {        
        
        if (tableType == huemulType_InternalTableType.DQ) {
          //create StructType
          if ("dq_control_id".toUpperCase() != x.getName.toUpperCase()) {
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}#b"""
          }
        }
        else if (tableType == huemulType_InternalTableType.OldValueTrace) {
          //create StructType MDM_columnName
          if ("MDM_columnName".toUpperCase() != x.getName.toUpperCase()) {
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}#b"""
          }
        }
        else {
          //create StructType
          fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}${if (dataField.DataType == TimestampType || dataField.DataType == DateType || dataField.DataType == DecimalType || dataField.DataType.typeName.toLowerCase().contains("decimal")) "" else "#b"}"""
          
        }
        
        if (tableType != huemulType_InternalTableType.OldValueTrace) {
          if (dataField.getMDM_EnableOldValue)
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}_old${if (dataField.DataType == TimestampType || dataField.DataType == DateType || dataField.DataType == DecimalType || dataField.DataType.typeName.toLowerCase().contains("decimal") ) "" else "#b"}"""
          if (dataField.getMDM_EnableDTLog) 
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}_fhChange"""
          if (dataField.getMDM_EnableProcessLog)
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}_ProcessLog#b"""
        }
        
        
      }
    }
    
    
    return fields
  }
 
  
  //get PK for HBase Tables rowKey 
  private var _HBase_rowKeyCalc: String = null
  private var _HBase_PKColumn: String = ""
  
  private var _numPKColumns: Int = 0
  
  
  
  
  
  private def internalGetTable(internalTableType: huemulType_InternalTableType, withDataBase: Boolean = true): String = {
    var _getTable: String = ""
    var _database: String = ""
    var _tableName: String = ""
    if (internalTableType == huemulType_InternalTableType.DQ) {
      _database = getDataBase(huemulBigDataGov.GlobalSettings.DQError_DataBase)
      _tableName = TableName + "_dq"
    } else if (internalTableType == huemulType_InternalTableType.Normal) { 
      _database = getDataBase(_DataBase)
      _tableName = TableName
    } else if (internalTableType == huemulType_InternalTableType.OldValueTrace) { 
      _database = getDataBase(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_DataBase)
      _tableName = TableName + "_oldvalue"
    } else {
      raiseError("huemul_Table DQ Error: Type '${Type}' doesn't exist in InternalGetTable method", 1051)
    }
      
    if (withDataBase)
      _getTable = s"${_database}.${_tableName}"
    else
      _getTable = s"${_tableName}"    
    
    return _getTable 
  }
   
  def getCurrentDataBase(): String = {
    return s"${getDataBase(_DataBase)}"
  }
  
  /**
   * Return DataBaseName.TableName
   */
  private def InternalGetTable(ManualEnvironment: String): String = {
    return s"${getDataBase(this._DataBase, ManualEnvironment)}.${TableName}"
  }
  
  
  def getDataBase(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    return huemulBigDataGov.GlobalSettings.GetDataBase(huemulBigDataGov, Division)  
  }
  
  def getDataBase(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    return huemulBigDataGov.GlobalSettings.GetDataBase(huemulBigDataGov, Division, ManualEnvironment)  
  }
  
  def getPath(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    return huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov, Division)  
  }
  
  def getPath(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    return huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov, Division, ManualEnvironment)  
  }
  
  
  
  /**
   get execution's info about rows and DF
   */
  var DataFramehuemul: huemul_DataFrame = new huemul_DataFrame(huemulBigDataGov, Control)  

   
  var Error_isError: Boolean = false
  var Error_Text: String = ""
  var Error_Code: Integer = null
  //var HasColumns: Boolean = false
  var HasPK: Boolean = false
  
  def ApplyTableDefinition(): Boolean = {
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: starting ApplyTableDefinition")
    if (this._PartitionField == null)
      _PartitionField = ""
      
    if (this._GlobalPaths == null)
      raiseError(s"huemul_Table Error: GlobalPaths must be defined",1000)
      
    if (this._LocalPath == null)
      raiseError(s"huemul_Table Error: LocalPath must be defined",1001)
      
    if (this.getStorageType == null)
      raiseError(s"huemul_Table Error: StorageType must be defined",1002)
      
    if (this.getStorageType_DQResult == null)
      raiseError(s"huemul_Table Error: HBase is not available for DQ Table",1061)
      
    if (this.getStorageType_OldValueTrace == null)
      raiseError(s"huemul_Table Error: HBase is not available for OldValueTraceTable",1063)
      
    if (this._TableType == null)
      raiseError(s"huemul_Table Error: TableType must be defined",1034)
    else if (this._TableType == huemulType_Tables.Transaction && _PartitionField == "")
      raiseError(s"huemul_Table Error: PartitionField should be defined if TableType is Transactional",1035)
    else if (this._TableType != huemulType_Tables.Transaction && _PartitionField != "")
      raiseError(s"huemul_Table Error: PartitionField shouldn't be defined if TableType is ${this._TableType}",1036)
      
    //from 2.2 --> validate tableType with Format
    if (this._TableType == huemulType_Tables.Transaction && !(this.getStorageType == huemulType_StorageType.PARQUET || this.getStorageType == huemulType_StorageType.ORC))
      raiseError(s"huemul_Table Error: Transaction Tables only available with PARQUET or ORC StorageType ",1057)
      
    //Fron 2.2 --> validate tableType HBASE and turn on globalSettings
    if (this.getStorageType == huemulType_StorageType.HBASE && !huemulBigDataGov.GlobalSettings.getHBase_available)
      raiseError(s"huemul_Table Error: StorageType is set to HBASE, requires invoke HBase_available in globalSettings  ",1058)
      
    if (this._DataBase == null)
      raiseError(s"huemul_Table Error: DataBase must be defined",1037)
    if (this._Frequency == null)
      raiseError(s"huemul_Table Error: Frequency must be defined",1047)
      
    if (this.getSaveBackup == true && this.getTableType == huemulType_Tables.Transaction)
      raiseError(s"huemul_Table Error: SaveBackup can't be true for transactional tables",1054)
        
      
    var PartitionFieldValid: Boolean = false
    var comaPKConcat = ""
    
    _numPKColumns = 0
      
    getALLDeclaredFields().filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] || x.get(this).isInstanceOf[huemul_DataQuality] || x.get(this).isInstanceOf[huemul_Table_Relationship]  
    } foreach { x =>
      x.setAccessible(true)
          
      //Nombre de campos
      if (x.get(this).isInstanceOf[huemul_Columns]) {
        val DataField = x.get(this).asInstanceOf[huemul_Columns]
        DataField.Set_MyName(x.getName)
        
        if (DataField.getDQ_MaxLen != null && DataField.getDQ_MaxLen < 0)
          raiseError(s"Error column ${x.getName}: DQ_MaxLen must be positive",1003)
        else if (DataField.getDQ_MaxLen != null && DataField.getDQ_MinLen != null && DataField.getDQ_MaxLen < DataField.getDQ_MinLen)
          raiseError(s"Error column ${x.getName}: DQ_MinLen(${DataField.getDQ_MinLen}) must be less than DQ_MaxLen(${DataField.getDQ_MaxLen})",1028)
        else if (DataField.getDQ_MaxDecimalValue != null && DataField.getDQ_MinDecimalValue != null && DataField.getDQ_MaxDecimalValue < DataField.getDQ_MinDecimalValue)
          raiseError(s"Error column ${x.getName}: DQ_MinDecimalValue(${DataField.getDQ_MinDecimalValue}) must be less than DQ_MaxDecimalValue(${DataField.getDQ_MaxDecimalValue})",1029)
        else if (DataField.getDQ_MaxDateTimeValue != null && DataField.getDQ_MinDateTimeValue != null && DataField.getDQ_MaxDateTimeValue < DataField.getDQ_MinDateTimeValue)
          raiseError(s"Error column ${x.getName}: DQ_MinDateTimeValue(${DataField.getDQ_MinDateTimeValue}) must be less than DQ_MaxDateTimeValue(${DataField.getDQ_MaxDateTimeValue})",1030)
        else if (DataField.getDefaultValue != null && DataField.DataType == StringType && DataField.getDefaultValue.toUpperCase() != "NULL" && !DataField.getDefaultValue.contains("'"))
          raiseError(s"Error column ${x.getName}: DefaultValue  must be like this: 'something', not something wihtout ')",1031)
          
        if (DataField.getIsPK) {
          DataField.setNullable(false)
          HasPK = true
          
          if (DataField.getMDM_EnableDTLog || DataField.getMDM_EnableOldValue || DataField.getMDM_EnableProcessLog || DataField.getMDM_EnableOldValue_FullTrace ) {
            raiseError(s"Error column ${x.getName}:, is PK, can't enabled MDM_EnableDTLog, MDM_EnableOldValue, MDM_EnableOldValue_FullTrace or MDM_EnableProcessLog",1040)
          }
        }
        
        DataField.SetDefinitionIsClose()
        
        if (this.getTableType == huemulType_Tables.Transaction && DataField.get_MyName().toUpperCase() == this.getPartitionField.toUpperCase())
          PartitionFieldValid = true
          
        //from 2.2 --> get concatenaded key for HBase
        if (DataField.getIsPK && getStorageType == huemulType_StorageType.HBASE) {
          _HBase_rowKeyCalc += s"${comaPKConcat}'[', ${if (DataField.DataType == StringType) DataField.get_MyName() else s"CAST(${DataField.get_MyName()} AS STRING)" },']'"
          comaPKConcat = ","
          _HBase_PKColumn = DataField.get_MyName()
          _numPKColumns += 1
        }
            
      }
      
      //Nombre de DQ      
      if (x.get(this).isInstanceOf[huemul_DataQuality]) {
        val DQField = x.get(this).asInstanceOf[huemul_DataQuality]
        DQField.setMyName(x.getName)     
        
        if (DQField.getNotification() == huemulType_DQNotification.WARNING_EXCLUDE && DQField.getSaveErrorDetails() == false)
          raiseError(s"huemul_Table Error: DQ ${x.getName}:, Notification is set to WARNING_EXCLUDE, but SaveErrorDetails is set to false. Use setSaveErrorDetails to set true ",1055)
        else if (DQField.getNotification() == huemulType_DQNotification.WARNING_EXCLUDE && DQField.getQueryLevel() != huemulType_DQQueryLevel.Row )
          raiseError(s"huemul_Table Error: DQ ${x.getName}:, Notification is set to WARNING_EXCLUDE, QueryLevel MUST set to huemulType_DQQueryLevel.Row",1056)
          
      }
      
      //Nombre de FK
      if (x.get(this).isInstanceOf[huemul_Table_Relationship]) {
        val FKField = x.get(this).asInstanceOf[huemul_Table_Relationship]
        FKField.MyName = x.getName
        
        //TODO: Validate FK Setting
        
      }
    }
    
    //from 2.2 --> set _HBase_rowKey for hBase Tables
    if (getStorageType == huemulType_StorageType.HBASE) {
      if (_numPKColumns == 1)
        _HBase_rowKeyCalc = _HBase_PKColumn
      else 
        _HBase_rowKeyCalc = s"concat(${_HBase_rowKeyCalc})"
    }
    
    
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: register metadata")
    //Register TableName and fields
    Control.RegisterMASTER_CREATE_Basic(this)
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: end ApplyTableDefinition")
    if (!HasPK) this.raiseError("huemul_Table Error: PK not defined", 1017)
    if (this.getTableType == huemulType_Tables.Transaction && !PartitionFieldValid)
      raiseError(s"huemul_Table Error: PartitionField should be defined if TableType is Transactional (invalid name ${this.getPartitionField} )",1035)
      
    DefinitionIsClose = true
    return true
  }
  
  def _setAutoIncUpate(value: Long) = {
    _MDM_AutoInc = value
  }
  
  def _setControlTableId(value: String) {
    _ControlTableId = value
  }
  
  def _getControlTableId(): String = {return _ControlTableId}
  
  /**
   * Get all declared fields from class
   */
  private def getALLDeclaredFields(OnlyUserDefined: Boolean = false, PartitionColumnToEnd: Boolean = false, tableType: huemulType_InternalTableType = huemulType_InternalTableType.Normal) : Array[java.lang.reflect.Field] = {
    val pClass = getClass()  
    
    val a = pClass.getDeclaredFields()  //huemul_table
    
    
    var c = a
    
    //only PK for save result to old value trace 
    if (tableType == huemulType_InternalTableType.OldValueTrace) {
        c = c.filter { x => x.setAccessible(true)
                            x.get(this).isInstanceOf[huemul_Columns] && 
                            x.get(this).asInstanceOf[huemul_Columns].getIsPK 
                            } 
    } 
    
    if (!OnlyUserDefined){ //all columns, including MDM
      var b = pClass.getSuperclass().getDeclaredFields()
      
       
      if (tableType == huemulType_InternalTableType.OldValueTrace) {
        b = b.filter { x => x.getName == "MDM_columnName" || x.getName == "MDM_newValue" || x.getName == "MDM_oldValue" || x.getName == "MDM_AutoInc" ||
                            x.getName == "MDM_fhChange" || x.getName == "MDM_ProcessChange" || x.getName == "processExec_id"  }
      } else {
        //exclude OldValuestrace columns
        b = b.filter { x => x.getName != "MDM_columnName" && x.getName != "MDM_newValue" && x.getName != "MDM_oldValue" && x.getName != "MDM_AutoInc" && x.getName != "processExec_id"  }
        
        if (this._TableType == huemulType_Tables.Transaction) 
          b = b.filter { x => x.getName != "MDM_ProcessChange" && x.getName != "MDM_fhChange" && x.getName != "MDM_StatusReg"   }
        
        //       EXCLUDE FOR PARQUET, ORC, ETC             OR            EXCLUDE FOR HBASE AND NUM PKs == 1
        if (getStorageType != huemulType_StorageType.HBASE || (getStorageType == huemulType_StorageType.HBASE && _numPKColumns == 1) )
          b = b.filter { x => x.getName != "hs_rowKey"  }
        
      }
      
      
      c = c.union(b)
    }
    
    if (tableType == huemulType_InternalTableType.DQ) {
        val DQClass = pClass.getSuperclass().getSuperclass() //huemul_TableDQ
        val d = DQClass.getDeclaredFields.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] } 
        
        c = d.union(c)
    } 
    
    if (PartitionColumnToEnd) {
      val partitionlast = c.filter { x => x.getName.toUpperCase() == this.getPartitionField.toUpperCase() }
      val rest_array = c.filter { x => x.getName.toUpperCase() != this.getPartitionField.toUpperCase() }
      c = rest_array.union(partitionlast)
    }

    
    return c
  }
  
  /**
   * Get all declared fields from class, transform to huemul_Columns
   */
  private def getALLDeclaredFields_forHBase(allDeclaredFields: Array[java.lang.reflect.Field]) : Array[(java.lang.reflect.Field, String, String, DataType)] = {
       
    val result = allDeclaredFields.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }.map { x =>     
      //Get field
      x.setAccessible(true)
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      (x,Field.getHBaseCatalogFamily(), Field.getHBaseCatalogColumn(), Field.DataType)
    }
    
    return result
  }
  
  //private var _SQL_OldValueFullTrace_DF: DataFrame = null 
  private var _MDM_AutoInc: Long = 0
  private var _ControlTableId: String = null
  def _getMDM_AutoInc(): Long = {return _MDM_AutoInc}
  private var _table_dq_isused: Int = 0
  def _getTable_dq_isused(): Int = {return _table_dq_isused}
  private var _table_ovt_isused: Int = 0
  def _getTable_ovt_isused(): Int = {return _table_ovt_isused}
  private var _NumRows_New: Long = null
  private var _NumRows_Update: Long = null
  private var _NumRows_Updatable: Long = null
  private var _NumRows_Delete: Long = null
  private var _NumRows_Total: Long = null
  private var _NumRows_NoChange: Long = null
  private var _NumRows_Excluded: Long = 0
  
  def NumRows_New(): Long = {
    return _NumRows_New
  }
  
  def NumRows_Update(): Long = {
    return _NumRows_Update
  }
  
  def NumRows_Updatable(): Long = {
    return _NumRows_Updatable
  }
  
  def NumRows_Delete(): Long = {
    return _NumRows_Delete
  }
  
  def NumRows_Total(): Long = {
    return _NumRows_Total
  }
  
  def NumRows_NoChange(): Long = {
    return _NumRows_NoChange
  }
  
  def NumRows_Excluded(): Long = {
    return _NumRows_Excluded
  }
   /*  ********************************************************************************
   *****   F I E L D   M E T H O D S    **************************************** 
   ******************************************************************************** */
  def getSchema(): StructType = {        
    val fieldsStruct = new ArrayBuffer[StructField]();
    
    var i:Integer = 0
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      //create StructType
      fieldsStruct.append( StructField(x.getName, Field.DataType , nullable = Field.getNullable , null))

      if (Field.getMDM_EnableOldValue) {
        fieldsStruct.append( StructField(x.getName + "_old", Field.DataType , nullable = true , null))
      }
      if (Field.getMDM_EnableDTLog) {
        fieldsStruct.append( StructField(x.getName + "_fhChange", TimestampType , nullable = true , null))
      }
      if (Field.getMDM_EnableProcessLog) {
        fieldsStruct.append( StructField(x.getName + "_ProcessLog", StringType , nullable = true , null))
      }
      
    }
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"N° Total: ${fieldsStruct.length}")
    return StructType.apply(fieldsStruct)
  }
  
  def getDataQuality(warning_exclude: Boolean): ArrayBuffer[huemul_DataQuality] = {
    val GetDeclaredfields = getALLDeclaredFields()
    val Result = new ArrayBuffer[huemul_DataQuality]()
     
    if (GetDeclaredfields != null ) {
        GetDeclaredfields.filter { x => x.setAccessible(true)
                             x.get(this).isInstanceOf[huemul_DataQuality] }
        .foreach { x =>
          //Get DQ
          var DQRule = x.get(this).asInstanceOf[huemul_DataQuality]
          
          if (warning_exclude == true && DQRule.getNotification() == huemulType_DQNotification.WARNING_EXCLUDE)
            Result.append(DQRule)
          else if (warning_exclude == false && DQRule.getNotification() != huemulType_DQNotification.WARNING_EXCLUDE)
            Result.append(DQRule)
        }
      }
    
    return Result
  }
  
  def getForeingKey(): ArrayBuffer[huemul_Table_Relationship] = {
    val GetDeclaredfields = getALLDeclaredFields()
    val Result = new ArrayBuffer[huemul_Table_Relationship]()
     
    if (GetDeclaredfields != null ) {
        GetDeclaredfields.filter { x => x.setAccessible(true)
                             x.get(this).isInstanceOf[huemul_Table_Relationship] }
        .foreach { x =>
          //Get DQ
          var FKRule = x.get(this).asInstanceOf[huemul_Table_Relationship]
          Result.append(FKRule)
        }
      }
    
    return Result
  }
  
  /**
   Create schema from DataDef Definition
   */
  def getColumns(): ArrayBuffer[huemul_Columns] = {    
    var Result: ArrayBuffer[huemul_Columns] = new ArrayBuffer[huemul_Columns]()
    val fieldList = getALLDeclaredFields()
    val NumFields = fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }.length
    
    var ColumnsCreateTable : String = ""
    var coma: String = ""
    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      //Field.Set_MyName(x.getName)
      Result.append(Field)
                      
      
      if (Field.getMDM_EnableOldValue) {
        val MDM_EnableOldValue = new huemul_Columns(Field.DataType, false, s"Old value for ${x.getName}")
        MDM_EnableOldValue.Set_MyName(s"${x.getName}_Old")
        Result.append(MDM_EnableOldValue)        
      } 
      if (Field.getMDM_EnableDTLog){
        val MDM_EnableDTLog = new huemul_Columns(TimestampType, false, s"Last change DT for ${x.getName}")
        MDM_EnableDTLog.Set_MyName(s"${x.getName}_fhChange")
        Result.append(MDM_EnableDTLog)                
      } 
      if (Field.getMDM_EnableProcessLog) {
        val MDM_EnableProcessLog = new huemul_Columns(StringType, false, s"System Name change for ${x.getName}")
        MDM_EnableProcessLog.Set_MyName(s"${x.getName}_ProcessLog")
        Result.append(MDM_EnableProcessLog) 
      }
              
    }
    
    return Result
  }
  
  
  /**
   Create schema from DataDef Definition
   */
  private def getColumns_CreateTable(ForHive: Boolean = false, tableType: huemulType_InternalTableType = huemulType_InternalTableType.Normal ): String = {
    //WARNING!!! Any changes you make to this code, repeat it in getHBaseCatalogForHIVE
    val fieldList = getALLDeclaredFields(false,true,tableType)
    val NumFields = fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }.length
    
    var ColumnsCreateTable : String = ""
    var coma: String = ""
    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      var DataTypeLocal = Field.DataType.sql
      
      if (tableType == huemulType_InternalTableType.DQ) {
        //create StructType
        if ("dq_control_id".toUpperCase() != x.getName.toUpperCase()) {
          ColumnsCreateTable += s"$coma${x.getName} ${DataTypeLocal} \n"
          coma = ","
        }
      }
      else if (tableType == huemulType_InternalTableType.OldValueTrace) {
        //create StructType MDM_columnName
        if ("MDM_columnName".toUpperCase() != x.getName.toUpperCase()) {
          ColumnsCreateTable += s"$coma${x.getName} ${DataTypeLocal} \n"
          coma = ","
        }
      }
      else {
        //create StructType
        if (_PartitionField != null && _PartitionField.toUpperCase() != x.getName.toUpperCase()) {
          ColumnsCreateTable += s"$coma${x.getName} ${DataTypeLocal} \n"
          coma = ","
        }
      }
      
      if (tableType != huemulType_InternalTableType.OldValueTrace) {
        if (Field.getMDM_EnableOldValue)
          ColumnsCreateTable += s"$coma${x.getName}_old ${DataTypeLocal} \n"  
        if (Field.getMDM_EnableDTLog) 
          ColumnsCreateTable += s"$coma${x.getName}_fhChange ${TimestampType.sql} \n"  
        if (Field.getMDM_EnableProcessLog) 
          ColumnsCreateTable += s"$coma${x.getName}_ProcessLog ${StringType.sql} \n"
      }
    }
    
    return ColumnsCreateTable
  }
  
  
 
  
  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE PRIMARY KEY
   */
  private def SQL_PrimaryKey_FinalTable(): String = {
    
    
    var StringSQL: String = ""
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] &&
                                      x.get(this).asInstanceOf[huemul_Columns].getIsPK }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      //All PK columns shouldn't be null
      Field.setNullable(false)
      
      if (!huemulBigDataGov.HasName(Field.get_MappedName()))
        sys.error(s"field ${x.getName} doesn't have an assigned name in 'name' attribute")
      StringSQL += coma + x.getName
      coma = ","
    }
    
       
    return StringSQL 
  }
  
  /**
  CREATE SQL SCRIPT FOR TRANSACTIONAL DATA
   */
  private def SQL_Step0_TXHash(OfficialAlias: String, processName: String): String = {
       
    var StringSQL: String = ""
    var StringSQl_PK: String = ""
    
    var StringSQL_partition: String = ""
    var StringSQL_hash: String = "sha2(concat( "
    var coma_hash: String = ""
    
    
    var coma: String = ""    
    
    getALLDeclaredFields(false,true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val NewColumnCast = ApplyAutoCast(if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"${Field.get_SQLForInsert()} " else s"New.${Field.get_MappedName()}"
                                        ,Field.DataType.sql)
        
     
      //New value (from field or compute column )
      if (huemulBigDataGov.HasName(Field.get_MappedName()) ){
        if (Field.get_MyName().toUpperCase() == this.getPartitionField.toUpperCase())
          StringSQL_partition += s",${NewColumnCast} as ${x.getName} \n"
        else {
          StringSQL += s"${coma}${NewColumnCast} as ${x.getName} \n"
          coma = ","
        }
      } else {
        if (x.getName == "MDM_fhNew") {
          if (Field.get_MyName().toUpperCase() == this.getPartitionField.toUpperCase())
            StringSQL_partition += s"${coma}CAST(now() AS ${Field.DataType.sql} ) as ${x.getName} \n"
          else {   
            StringSQL += s",CAST(now() AS ${Field.DataType.sql} ) as ${x.getName} \n"
            coma = ","
          }
            
        } else if (x.getName == "MDM_ProcessNew") {
          StringSQL += s"${coma}CAST('${processName}' AS ${Field.DataType.sql} ) as ${x.getName} \n"
          coma = ","
        }
      }
          
       
      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""${coma_hash}${if (Field.getNullable) s"coalesce(${NewColumnCast},'null')" else NewColumnCast}"""
        coma_hash = ","
      }
      
      
    }
    
    StringSQL_hash += "),256) "
    
    
    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL = s"""SELECT ${StringSQL}
                    ,${StringSQL_hash} as MDM_hash
                    ${StringSQL_partition}
                    FROM $OfficialAlias new
                          """
       
    return StringSQL 
  }
  
  /**
  CREATE SQL SCRIPT DISTINCT WITH NEW DATAFRAME
   */
  private def SQL_Step0_Distinct(NewAlias: String): String = {
    
    var StringSQL: String = ""
    var Distintos: ArrayBuffer[String] = new ArrayBuffer[String]()
    var coma: String = ""
    getALLDeclaredFields(true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns]
                                        }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val MappedField: String = Field.get_MappedName()
      if (huemulBigDataGov.HasName(MappedField)) {        
        if (Distintos.filter { x => x == MappedField }.length == 0){
          Distintos.append(MappedField)
          StringSQL += s"${coma}${MappedField}  \n"
          coma = "," 
        }
      }
    }
      
    //Aditional field from new table
    if (huemulBigDataGov.HasName(AdditionalRowsForDistint))
      StringSQL += s",${AdditionalRowsForDistint}"

    //set field New or Update action
    StringSQL = s"""SELECT DISTINCT ${StringSQL}
                     FROM $NewAlias"""
       
    return StringSQL 
  }
  
  
  private def ApplyAutoCast(ColumnName: String, DataType: String): String = {
    return if (autoCast) s"CAST($ColumnName AS $DataType)" else ColumnName
  }
  
  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step2_LeftJoin(OfficialAlias: String, NewAlias: String): String = {
    //Get fields in old table
    val OfficialColumns = huemulBigDataGov.spark.catalog.listColumns(OfficialAlias).collect()
       
    var StringSQL_LeftJoin: String = ""
    var StringSQl_PK: String = ""
    var StringSQL_ActionType: String = ""
    
    var coma: String = ""
    var sand: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val NewColumnCast = ApplyAutoCast(s"New.${Field.get_MappedName()}",Field.DataType.sql)
      //string for key on
      if (Field.getIsPK){
        val NewPKSentence = if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} )" else NewColumnCast
        StringSQL_LeftJoin += s"${coma}Old.${x.getName} AS ${x.getName} \n"
        StringSQl_PK += s" $sand Old.${x.getName} = ${NewPKSentence}  " 
        sand = " and "

        if (StringSQL_ActionType == "")
          StringSQL_ActionType = s" case when Old.${x.getName} is null then 'NEW' when ${NewColumnCast} is null then 'EQUAL' else 'UPDATE' END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = !(OfficialColumns.filter { y => y.name.toLowerCase() == x.getName.toLowerCase()}.length == 0)
        
        //String for new DataFrame fulljoin
        if (columnExist){
          StringSQL_LeftJoin += s"${coma}cast(old.${x.getName} as ${Field.DataType.sql} ) as old_${x.getName} \n"
          //StringSQL_LeftJoin += s"${coma}old.${x.getName} as old_${x.getName} \n"
        }
        else
          StringSQL_LeftJoin += s"${coma}cast(null as ${Field.DataType.sql} ) as old_${x.getName} \n"
        
        //New value (from field or compute column )
        if (huemulBigDataGov.HasName(Field.get_MappedName()))
          StringSQL_LeftJoin += s",${NewColumnCast} as new_${x.getName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForInsert()))
          StringSQL_LeftJoin += s",CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} ) as new_insert_${x.getName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForUpdate()))
          StringSQL_LeftJoin += s",CAST(${Field.get_SQLForUpdate()} as ${Field.DataType.sql} ) as new_update_${x.getName} \n"
          
        if (Field.getMDM_EnableOldValue || Field.getMDM_EnableDTLog || Field.getMDM_EnableProcessLog || Field.getMDM_EnableOldValue_FullTrace) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.HasName(Field.get_MappedName())) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) Field.get_SQLForUpdate() else "new.".concat(Field.get_MappedName()),Field.DataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(x.getName) else "null"
            StringSQL_LeftJoin += s",CAST(CASE WHEN ${NewFieldTXT} = ${OldFieldTXT} or (${NewFieldTXT} is null and ${OldFieldTXT} is null) THEN 0 ELSE 1 END as Integer ) as __Change_${x.getName}  \n"
          }
          else {
            StringSQL_LeftJoin += s",CAST(0 as Integer ) as __Change_${x.getName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae, 
            //la solución es poner el campo "tiene cambio" en falso
          }
              
        }
      
        if (Field.getMDM_EnableOldValue){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${x.getName}_old".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${x.getName}_old AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
        }
        if (Field.getMDM_EnableDTLog){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${x.getName}_fhChange".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS TimeStamp) as old_${x.getName}_fhChange \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${x.getName}_fhChange AS TimeStamp) as old_${x.getName}_fhChange \n"
        }
        if (Field.getMDM_EnableProcessLog){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${x.getName}_ProcessLog".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS STRING) as old_${x.getName}_ProcessLog \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${x.getName}_ProcessLog AS STRING) as old_${x.getName}_ProcessLog \n"
        }   
          
      }
       
      coma = ","
    }
    
    
    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL_LeftJoin = s"""SELECT ${StringSQL_LeftJoin}
                                   ,${StringSQL_ActionType}
                             FROM $OfficialAlias Old
                                LEFT JOIN ${NewAlias} New \n 
                                   on $StringSQl_PK  
                          """
       
    return StringSQL_LeftJoin 
  }
  
  
  /**
  CREATE SQL SCRIPT FOR OLD VALUE TRACE INSERT
   */
  private def SQL_Step_OldValueTrace(Alias: String, ProcessName: String): String = {
       
    //Get PK
    var StringSQl_PK: String = "SELECT "
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[huemul_Columns] &&
                                         x.get(this).asInstanceOf[huemul_Columns].getIsPK }
    .foreach { x =>
      StringSQl_PK += s" ${coma}${x.getName}"
      coma = ","
    }
    
    //Get SQL for get columns value change
    var StringSQl: String = ""
    var StringUnion: String = ""
    var count_fulltrace = 0
    
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] && 
                                      x.get(this).asInstanceOf[huemul_Columns].getMDM_EnableOldValue_FullTrace }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      StringSQl +=  s" ${StringUnion} ${StringSQl_PK}, CAST(new_${x.getName} as string) AS MDM_newValue, CAST(old_${x.getName} as string) AS MDM_oldValue, CAST(${_MDM_AutoInc} AS BIGINT) as MDM_AutoInc, '${Control.Control_Id}' as processExec_id, now() as MDM_fhChange, cast('$ProcessName' as string) as MDM_ProcessChange, cast('${x.getName.toLowerCase()}' as string) as MDM_columnName FROM $Alias WHERE ___ActionType__ = 'UPDATE' and __Change_${x.getName} = 1 "
      StringUnion = " \n UNION ALL "
      count_fulltrace += 1
    }
    
    if (count_fulltrace == 0)
      StringSQl = null
    
    
    return StringSQl 
  }

  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step1_FullJoin(OfficialAlias: String, NewAlias: String, isUpdate: Boolean, isDelete: Boolean): String = {
    //Get fields in old table
    val OfficialColumns = huemulBigDataGov.spark.catalog.listColumns(OfficialAlias).collect()
       
    var StringSQL_FullJoin: String = ""
    var StringSQl_PK: String = ""
    var StringSQL_ActionType: String = ""
    
    var coma: String = ""
    var sand: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val NewColumnCast = ApplyAutoCast(s"New.${Field.get_MappedName()}",Field.DataType.sql)
      //string for key on
      if (Field.getIsPK){
        val NewPKSentence = if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} )" else NewColumnCast
        StringSQL_FullJoin += s"${coma}CAST(coalesce(Old.${x.getName}, ${NewPKSentence}) as ${Field.DataType.sql}) AS ${x.getName} \n"
        StringSQl_PK += s" $sand Old.${x.getName} = ${NewPKSentence}  " 
        sand = " and "

        if (StringSQL_ActionType == "")
          StringSQL_ActionType = s" case when Old.${x.getName} is null then 'NEW' when ${NewColumnCast} is null then ${if (isDelete) "'DELETE'" else "'EQUAL'"} else ${if (isUpdate) "'UPDATE'" else "'EQUAL'"} END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = !(OfficialColumns.filter { y => y.name.toLowerCase() == x.getName.toLowerCase()}.length == 0)
        
        //String for new DataFrame fulljoin
        if (columnExist)
          StringSQL_FullJoin += s"${coma}cast(old.${x.getName} as ${Field.DataType.sql} ) as old_${x.getName} \n"
        else
          StringSQL_FullJoin += s"${coma}cast(null as ${Field.DataType.sql} ) as old_${x.getName} \n"
        
        //New value (from field or compute column )
        if (huemulBigDataGov.HasName(Field.get_MappedName()))
          StringSQL_FullJoin += s",${NewColumnCast} as new_${x.getName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForInsert()))
          StringSQL_FullJoin += s",CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} ) as new_insert_${x.getName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForUpdate()))
          StringSQL_FullJoin += s",CAST(${Field.get_SQLForUpdate()} as ${Field.DataType.sql} ) as new_update_${x.getName} \n"
          
        if (Field.getMDM_EnableOldValue || Field.getMDM_EnableDTLog || Field.getMDM_EnableProcessLog || Field.getMDM_EnableOldValue_FullTrace) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.HasName(Field.get_MappedName())) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) Field.get_SQLForUpdate() else "new.".concat(Field.get_MappedName()),Field.DataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(x.getName) else "null"
            StringSQL_FullJoin += s",CAST(CASE WHEN ${NewFieldTXT} = ${OldFieldTXT} or (${NewFieldTXT} is null and ${OldFieldTXT} is null) THEN 0 ELSE 1 END as Integer ) as __Change_${x.getName}  \n"
          }
          else {
            StringSQL_FullJoin += s",CAST(0 as Integer ) as __Change_${x.getName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae, 
            //la solución es poner el campo "tiene cambio" en falso
          }
              
        }
      
        if (Field.getMDM_EnableOldValue){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${x.getName}_old".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${x.getName}_old AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
        }
        if (Field.getMDM_EnableDTLog){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${x.getName}_fhChange".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS TimeStamp) as old_${x.getName}_fhChange \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${x.getName}_fhChange AS TimeStamp) as old_${x.getName}_fhChange \n"
        }
        if (Field.getMDM_EnableProcessLog){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${x.getName}_ProcessLog".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS STRING) as old_${x.getName}_ProcessLog \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${x.getName}_ProcessLog AS STRING) as old_${x.getName}_ProcessLog \n"
        }   
        
        
          
      }
       
      coma = ","
    }
    
    
    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL_FullJoin = s"""SELECT ${StringSQL_FullJoin}
                                   ,${StringSQL_ActionType}
                             FROM $OfficialAlias Old
                                FULL JOIN ${NewAlias} New \n 
                                   on $StringSQl_PK  
                          """
       
    return StringSQL_FullJoin 
  }
  
  /**
  CREATE SQL SCRIPT LEFT JOIN 
   */
  private def SQL_Step4_Update(NewAlias: String, ProcessName: String): String = {
    
    var StringSQL: String = "SELECT "
    
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      //string for key on
      if (Field.getIsPK){
        StringSQL += s" ${coma}${x.getName} as ${x.getName} \n"
        
      } else {
        if (   x.getName == "MDM_fhNew" || x.getName == "MDM_ProcessNew" || x.getName == "MDM_fhChange"
            || x.getName == "MDM_ProcessChange" || x.getName == "MDM_StatusReg" || x.getName == "hs_rowKey"
            || x.getName == "MDM_hash")
          StringSQL += s"${coma}old_${x.getName}  \n"
        else {          
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) s"new_update_${x.getName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.get_ReplaceValueOnUpdate() && this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${x.getName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${x.getName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${x.getName} END  as ${x.getName} \n"""
                
           if (Field.getMDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN old_${x.getName} ELSE old_${x.getName}_old END as ${x.getName}_old \n"""
           if (Field.getMDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN now() ELSE old_${x.getName}_fhChange END AS TimeStamp) as ${x.getName}_fhChange \n"""
           if (Field.getMDM_EnableProcessLog)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN '${ProcessName}' WHEN ___ActionType__ = 'NEW' THEN '${ProcessName}' ELSE old_${x.getName}_ProcessLog END as ${x.getName}_ProcessLog \n"""             
        }
      }
                         
      coma = ","
    }
                
    
    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" ,___ActionType__
                      FROM $NewAlias New 
                  """
    return StringSQL 
  }
   
  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step2_UpdateAndInsert(NewAlias: String, ProcessName: String, isInsert: Boolean): String = {
    
    var StringSQL: String = "SELECT "
    
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      //string for key on
      if (Field.getIsPK){
        StringSQL += s" ${coma}${x.getName} as ${x.getName} \n"
        
      } else {
        if (   x.getName == "MDM_fhNew" || x.getName == "MDM_ProcessNew" || x.getName == "MDM_fhChange"
            || x.getName == "MDM_ProcessChange" || x.getName == "MDM_StatusReg" || x.getName == "hs_rowKey" 
            || x.getName == "MDM_hash")
          StringSQL += s"${coma}old_${x.getName}  \n"
        else {          
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'NEW'    THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"new_insert_${x.getName}" //si tiene valor en SQL insert, lo usa
                                                                               else if (this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${x.getName}"     //si no, si tiene nombre de campo en DataFrame nuevo, lo usa 
                                                                               else ApplyAutoCast(Field.getDefaultValue,Field.DataType.sql)                        //si no tiene campo asignado, pone valor por default
                                                                           }
                                        WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) s"new_update_${x.getName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.get_ReplaceValueOnUpdate() && this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${x.getName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${x.getName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${x.getName} END  as ${x.getName} \n"""
                
           if (Field.getMDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN old_${x.getName} ELSE old_${x.getName}_old END as ${x.getName}_old \n"""
           if (Field.getMDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN now() ELSE old_${x.getName}_fhChange END AS TimeStamp) as ${x.getName}_fhChange \n"""
           if (Field.getMDM_EnableProcessLog)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN '${ProcessName}' WHEN ___ActionType__ = 'NEW' THEN '${ProcessName}' ELSE old_${x.getName}_ProcessLog END as ${x.getName}_ProcessLog \n"""             
        }
      }
                         
      coma = ","
    }
                
    
    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" ,___ActionType__
                      FROM $NewAlias New 
                      ${if (isInsert) "" else "WHERE ___ActionType__ <> 'NEW' "}
                  """
    
           
    return StringSQL 
  }
  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA and for Selective update
   */
  private def SQL_Step3_Hash_p1(NewAlias: String, isSelectiveUpdate: Boolean): String = {
    
    var StringSQL: String = "SELECT "
    var StringSQL_hash: String = "sha2(concat( "
  
    
    var coma: String = ""
    var coma_hash: String = ""

    //Obtiene todos los campos
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns]
                                    }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
           
      if (   x.getName == "MDM_fhNew" || x.getName == "MDM_ProcessNew" || x.getName == "MDM_fhChange"
            || x.getName == "MDM_ProcessChange" || x.getName == "MDM_StatusReg" || x.getName == "hs_rowKey" 
            || x.getName == "MDM_hash")
        StringSQL += s"${coma}old_${x.getName}  \n"
      else 
        StringSQL += s" ${coma}${x.getName}  \n"
        
      if (Field.getMDM_EnableOldValue)
        StringSQL += s""",${x.getName}_old \n"""
      if (Field.getMDM_EnableDTLog)
        StringSQL += s""",${x.getName}_fhChange \n"""
      if (Field.getMDM_EnableProcessLog)
        StringSQL += s""",${x.getName}_ProcessLog \n"""
       
      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""${coma_hash}${if (Field.getNullable) s"coalesce(${x.getName},'null')" else x.getName}"""
        coma_hash = ","
      }
        
      coma = ","
    }
    
    //Hash fields
    if (coma_hash == "")
      sys.error(s"must define one field as UsedForCheckSum")
    
    StringSQL_hash += "),256) "
                
    //Include HashFields to SQL
    StringSQL += s""",${StringSQL_hash} as MDM_hash \n"""
    
    if (this._TableType == huemulType_Tables.Reference || this._TableType == huemulType_Tables.Master || isSelectiveUpdate) {
      StringSQL += s""",case when old_MDM_hash = ${StringSQL_hash} THEN 1 ELSE 0 END AS SameHashKey  \n ,___ActionType__ \n"""
    }
    
    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" FROM $NewAlias New 
                  """
               
    return StringSQL 
  }
  
  
  private def SQL_Step4_Final(NewAlias: String, ProcessName: String, IncludeActionType: Boolean): String = {
    var StringSQL: String = "SELECT "
    
    var coma: String = ""
    
    //Obtiene todos los campos
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns]
                                    }
    .foreach { x =>     
      //Get field      
      var Field = x.get(this).asInstanceOf[huemul_Columns]
           
      if (x.getName == "MDM_fhNew")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN now() ELSE old_${x.getName} END as ${x.getName} \n"
      else if (x.getName == "MDM_ProcessNew")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN '$ProcessName' ELSE old_${x.getName} END as ${x.getName} \n"
      else if (x.getName == "MDM_fhChange")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN now() ELSE old_${x.getName} END as ${x.getName} \n"
      else if (x.getName == "MDM_ProcessChange")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN '$ProcessName' ELSE old_${x.getName} END as ${x.getName} \n"
      else if (x.getName == "MDM_StatusReg")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN CAST(2 as Int) WHEN ___ActionType__ = 'NEW' THEN CAST(2 as Int) WHEN ___ActionType__ = 'DELETE' THEN CAST(-1 AS Int)  ELSE CAST(old_${x.getName} AS Int) END as ${x.getName} \n"
      else if (x.getName == "MDM_hash")
        StringSQL += s"${coma}MDM_hash \n"
      else if (x.getName == "hs_rowKey"){
        if (getStorageType == huemulType_StorageType.HBASE && _numPKColumns > 1)
          StringSQL += s"${coma}${_HBase_rowKeyCalc} as ${x.getName} \n"
      }
      else 
        StringSQL += s" ${coma}${x.getName}  \n"
        
      if (Field.getMDM_EnableOldValue)
        StringSQL += s""",${x.getName}_old \n"""
      if (Field.getMDM_EnableDTLog)
        StringSQL += s""",${x.getName}_fhChange \n"""
      if (Field.getMDM_EnableProcessLog)
        StringSQL += s""",${x.getName}_ProcessLog \n"""
       
       
      coma = ","
    }
    
    if (IncludeActionType)
      StringSQL += s", ___ActionType__, SameHashKey \n" 
       
    StringSQL += s"FROM $NewAlias New\n" 
     
    return StringSQL
  }
  
  
  
  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE PRIMARY KEY
   */
  private def SQL_Unique_FinalTable(): ArrayBuffer[huemul_Columns] = {
    
    var StringSQL: ArrayBuffer[huemul_Columns] = new ArrayBuffer[huemul_Columns]()
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[huemul_Columns] &&
                                         x.get(this).asInstanceOf[huemul_Columns].getIsUnique && huemulBigDataGov.HasName(x.get(this).asInstanceOf[huemul_Columns].get_MappedName)}
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      StringSQL.append(Field)
    }
       
    return StringSQL 
  }
  
  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE NOT NULL ATTRIBUTES
   */
  private def SQL_NotNull_FinalTable(warning_exclude: Boolean): ArrayBuffer[huemul_Columns] = {
    var StringSQL: ArrayBuffer[huemul_Columns] = new ArrayBuffer[huemul_Columns]()
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                          x.get(this).isInstanceOf[huemul_Columns] &&
                                         !x.get(this).asInstanceOf[huemul_Columns].getNullable && 
                                         warning_exclude == false &&
                                         huemulBigDataGov.HasName(x.get(this).asInstanceOf[huemul_Columns].get_MappedName)}
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      StringSQL.append(Field)
    }
       
    return StringSQL 
  }
  
  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required 
   */
  private def missingRequiredFields(IsSelectiveUpdate: Boolean): ArrayBuffer[String] = {
    var StringSQL: ArrayBuffer[String] = new ArrayBuffer[String]()
    if (IsSelectiveUpdate) return StringSQL
    
    
    getALLDeclaredFields(true).filter { x => x.setAccessible(true)                                         
                                         x.get(this).isInstanceOf[huemul_Columns] &&
                                         x.get(this).asInstanceOf[huemul_Columns].Required  
                                          }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      //huemulBigDataGov.logMessageInfo(s"${Field.get_MyName()} Field.get_MappedName: ${Field.get_MappedName}, Field.get_SQLForUpdate(): ${Field.get_SQLForUpdate()}, Field.get_SQLForInsert(): ${Field.get_SQLForInsert()}")
      
      var isOK: Boolean = false
      if (huemulBigDataGov.HasName(Field.get_MappedName))
        isOK = true
      else if (!huemulBigDataGov.HasName(Field.get_MappedName) && 
              (huemulBigDataGov.HasName(Field.get_SQLForUpdate()) && huemulBigDataGov.HasName(Field.get_SQLForInsert())))
        isOK = true
      else
        isOK = false
        
      if (!isOK)
        StringSQL.append(x.getName)
    }
       
    return StringSQL 
  }
  
  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required 
   */
  private def missingRequiredFields_SelectiveUpdate(): String = {
    var PKNotMapped: String = ""
    var OneColumnMapped: Boolean = false
    //STEP 1: validate name setting
    getALLDeclaredFields(true).filter { x => x.setAccessible(true)
                                    x.get(this).isInstanceOf[huemul_Columns]}
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      if (Field.getIsPK) {
        
        var isOK: Boolean = false
        if (huemulBigDataGov.HasName(Field.get_MappedName))
          isOK = true
        else if (!huemulBigDataGov.HasName(Field.get_MappedName) && 
                (huemulBigDataGov.HasName(Field.get_SQLForUpdate()) && huemulBigDataGov.HasName(Field.get_SQLForInsert())))
          isOK = true
        else
          isOK = false
          
        if (!isOK) {
          PKNotMapped = s"${if (PKNotMapped == "") "" else ", "}${Field.get_MyName()} "
        }
        
      } else 
        OneColumnMapped = true
                
    }
    
    if (!OneColumnMapped)
        raiseError(s"huemul_Table Error: Only PK mapped, no columns mapped for update",1042)
    
    return PKNotMapped
  }
  
  
  /*  ********************************************************************************
   *****   T A B L E   M E T H O D S    **************************************** 
   ******************************************************************************** */
  def getSQLCreateTableScript(): String = {return DF_CreateTableScript}
  private def DF_CreateTableScript(): String = {
              
    var coma_partition = ""
    var PartitionForCreateTable = ""
    //Get SQL DataType for Partition Columns
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      if (_PartitionField.toUpperCase() == x.getName().toUpperCase() ) {
          PartitionForCreateTable += s"${coma_partition}${_PartitionField} ${Field.DataType.sql}"
          coma_partition = ","
      }
    }
    
    var lCreateTableScript: String = "" 
    if (getStorageType == huemulType_StorageType.PARQUET || getStorageType == huemulType_StorageType.ORC) {
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
      lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.Normal)} (${getColumns_CreateTable(true) })
                                   ${if (_PartitionField.length() > 0) s"PARTITIONED BY (${PartitionForCreateTable})" else "" }
                                   STORED AS ${getStorageType.toString()}                                  
                                   LOCATION '${getFullNameWithPath()}'"""
    } else if (getStorageType == huemulType_StorageType.HBASE)  {
      lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.Normal)} (${getColumns_CreateTable(true) })
                                   ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe' 
                                   STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                                   WITH SERDEPROPERTIES ("hbase.columns.mapping"="${getHBaseCatalogForHIVE(huemulType_InternalTableType.Normal)}")                                   
                                   TBLPROPERTIES ("hbase.table.name"="${getHBaseNamespace(huemulType_InternalTableType.Normal)}:${getHBaseTableName(huemulType_InternalTableType.Normal)}")"""
    }
    
    if (huemulBigDataGov.DebugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: ${lCreateTableScript} ")
      
    return lCreateTableScript    
  }
  
  /**
   * Create table script to save DQ Results
   */
  private def DF_CreateTable_DQ_Script(): String = {
              
    var coma_partition = ""
    var PartitionForCreateTable = s"dq_control_id STRING"
    
    var lCreateTableScript: String = "" 
    if (getStorageType_DQResult == huemulType_StorageType.PARQUET || getStorageType_DQResult == huemulType_StorageType.ORC) {
    //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
      lCreateTableScript = s"""
                                 CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.DQ)} (${getColumns_CreateTable(true, huemulType_InternalTableType.DQ) })
                                 PARTITIONED BY (${PartitionForCreateTable})
                                 STORED AS ${getStorageType_DQResult.toString()}                                  
                                 LOCATION '${getFullNameWithPath_DQ()}'"""
    } else if (getStorageType_DQResult == huemulType_StorageType.HBASE)  {
      raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
    }
                                 
    if (huemulBigDataGov.DebugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: ${lCreateTableScript} ")
      
    return lCreateTableScript    
  }
  
  
  
  /**
  CREATE TABLE SCRIPT TO SAVE OLD VALUE TRACE 
   */
  private def DF_CreateTable_OldValueTrace_Script(): String = {
    var coma_partition = ""
    
    //STORED AS ${_StorageType_OldValueTrace}
    //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
    val lCreateTableScript = s"""
                                 CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.OldValueTrace)} (${getColumns_CreateTable(true, huemulType_InternalTableType.OldValueTrace) })
                                  ${if (getStorageType_OldValueTrace == "csv") {s"""
                                  ROW FORMAT DELIMITED
                                  FIELDS TERMINATED BY '\t'
                                  STORED AS TEXTFILE """}
                                  else if (getStorageType_OldValueTrace == "json") {
                                    s"""
                                     ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
                                    """
                                  }
                                  else if (getStorageType_OldValueTrace == huemulType_StorageType.PARQUET) {
                                   """PARTITIONED BY (MDM_columnName STRING)
                                     STORED AS PARQUET""" 
                                  }
                                  else if (getStorageType_OldValueTrace == huemulType_StorageType.ORC) {
                                   """PARTITIONED BY (MDM_columnName STRING)
                                     STORED AS ORC""" 
                                  }
                                 }
                                 LOCATION '${getFullNameWithPath_OldValueTrace()}'"""
                                  //${if (_StorageType_OldValueTrace == "csv") {s"""
                                  //TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ")"""}}"""
     
    if (huemulBigDataGov.DebugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: ${lCreateTableScript} ")
      
    return lCreateTableScript    
  }
  
  
  
  /* //este metodo retorna el nombre de un objeto
  def GetFieldName[T0](ClassInstance: Object, Field: Object): String = {
    var ResultName: String = null
    
    if (ClassInstance == this){
      val abc_1 = ClassInstance.getClass
      abc_1.getDeclaredFields().filter { z => z.setAccessible(true)
          z.get(this).isInstanceOf[T0] }.foreach { zz =>
            if (ResultName ==  null) {
              zz.setAccessible(true)
              val zze = zz.get(this).asInstanceOf[T0]
              if (zze == Field) {
                ResultName = zz.getName
              }
            }
          }  
    } else {
      if (ClassInstance.isInstanceOf[huemul_Table]) {
        ResultName = ClassInstance.asInstanceOf[huemul_Table].GetFieldName[T0](ClassInstance, Field)
      }
    }
    
    
    return ResultName
  }
  */
  
  private def DF_ForeingKeyMasterAuto(warning_exclude: Boolean): huemul_DataQualityResult = {
    var Result: huemul_DataQualityResult = new huemul_DataQualityResult()
    val ArrayFK = this.getForeingKey()
    val DataBaseName = this.getDataBase(this._DataBase)
    //For Each Foreing Key Declared
    ArrayFK.filter { x => (warning_exclude == true && x.getNotification() == huemulType_DQNotification.WARNING_EXCLUDE) ||
                          (warning_exclude == false && x.getNotification() != huemulType_DQNotification.WARNING_EXCLUDE)
                   } foreach { x =>
      
      //Step1: Create distinct FROM NEW DF
      var SQLFields: String = ""
      var SQLLeftJoin: String = ""
      var sqlAnd: String = ""
      var sqlComa: String = ""
      var FirstRowPK: String = ""
      var FirstRowFK: String = ""
      x.Relationship.foreach { y =>
        //Get fields name
        FirstRowPK = s"PK.${y.PK.get_MyName()}"
        FirstRowFK = y.FK.get_MyName()
        SQLFields += s"${sqlComa}${y.FK.get_MyName()}"
        SQLLeftJoin += s"${sqlAnd}PK.${y.PK.get_MyName()} = FK.${y.FK.get_MyName()}"
        sqlAnd = " and "
        sqlComa = ","
      }
      
      //val FKRuleName: String = GetFieldName[DAPI_MASTER_FK](this, x)
      val AliasDistinct_B: String = s"___${x.MyName}_FKRuleDistB__"
      val DF_Distinct = huemulBigDataGov.DF_ExecuteQuery(AliasDistinct_B, s"SELECT DISTINCT ${SQLFields} FROM ${this.DataFramehuemul.Alias} ${if (x.AllowNull) s" WHERE ${FirstRowFK} is not null " else "" }")
        
      var broadcast_sql: String = ""
      if (x.getBroadcastJoin())
        broadcast_sql = "/*+ BROADCAST(PK) */"
        
      //Step2: left join with TABLE MASTER DATA
      val AliasLeft: String = s"___${x.MyName}_FKRuleLeft__"
      val InstanceTable = x._Class_TableName.asInstanceOf[huemul_Table]
      val fk_table_name = InstanceTable.internalGetTable(huemulType_InternalTableType.Normal)
      val SQLLeft: String = s"""SELECT $broadcast_sql FK.* 
                                 FROM ${AliasDistinct_B} FK 
                                   LEFT JOIN ${fk_table_name} PK
                                     ON ${SQLLeftJoin} 
                                 WHERE ${FirstRowPK} IS NULL
                              """
                                 
      val AliasDistinct: String = s"___${x.MyName}_FKRuleDist__"
      val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
      val DF_Left = huemulBigDataGov.DF_ExecuteQuery(AliasDistinct, SQLLeft)
      var TotalLeft = DF_Left.count()
      
      if (TotalLeft > 0) {
        Result.isError = true
        Result.Description = s"huemul_Table Error: Foreing Key DQ Error, ${TotalLeft} records not found"
        Result.Error_Code = 1024
        Result.dqDF = DF_Left
        Result.profilingResult.count_all_Col = TotalLeft
        DF_Left.show()
        
        val DF_leftDetail = huemulBigDataGov.DF_ExecuteQuery("__DF_leftDetail", s"""SELECT FK.* 
                                                                                    FROM ${this.DataFramehuemul.Alias} FK  
                                                                                      LEFT JOIN ${fk_table_name} PK
                                                                                         ON ${SQLLeftJoin} 
                                                                                    WHERE ${FirstRowPK} IS NULL""")
                                                                                    
        
        TotalLeft = DF_leftDetail.count()
     
      }
         
      //val NumTotalDistinct = DF_Distinct.count()
      val numTotal = this.DataFramehuemul.getNumRows()
      val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
      val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
      
      val Values = new huemul_DQRecord(huemulBigDataGov)
      Values.Table_Name =TableName
      Values.BBDD_Name =DataBaseName
      Values.DF_Alias = DataFramehuemul.Alias
      Values.ColumnName =FirstRowFK
      Values.DQ_Name =s"FK - ${SQLFields}"
      Values.DQ_Description =s"FK Validation: PK Table: ${InstanceTable.internalGetTable(huemulType_InternalTableType.Normal)} "
      Values.DQ_QueryLevel = huemulType_DQQueryLevel.Row // IsAggregate =false
      Values.DQ_Notification = x.getNotification() //from 2.1, before --> huemulType_DQNotification.ERROR// RaiseError =true
      Values.DQ_SQLFormula =SQLLeft
      Values.DQ_ErrorCode = Result.Error_Code
      Values.DQ_toleranceError_Rows =0
      Values.DQ_toleranceError_Percent =null
      Values.DQ_ResultDQ =Result.Description
      Values.DQ_NumRowsOK =numTotal - TotalLeft
      Values.DQ_NumRowsError =TotalLeft
      Values.DQ_NumRowsTotal =numTotal
      Values.DQ_IsError = if (x.getNotification() == huemulType_DQNotification.ERROR) Result.isError else false
      Values.DQ_IsWarning = if (x.getNotification() != huemulType_DQNotification.ERROR) Result.isError else false
      Values.DQ_ExternalCode = x.getExternalCode // "HUEMUL_DQ_001"
      Values.DQ_duration_hour = duration.hour.toInt
      Values.DQ_duration_minute = duration.minute.toInt
      Values.DQ_duration_second = duration.second.toInt

      this.DataFramehuemul.DQ_Register(Values) 
      
      //Step3: Return DQ Validation
      if (TotalLeft > 0) {
       
        
        DF_ProcessToDQ( "__DF_leftDetail"   //sqlfrom
                        , null           //sqlwhere
                        , true            //haveField
                        , FirstRowFK      //fieldname
                        , Values.DQ_Id
                        , x.getNotification() //from 2.1, before -->huemulType_DQNotification.ERROR //dq_error_notification 
                        , Result.Error_Code //error_code
                        , s"(${Result.Error_Code}) FK ERROR ON ${fk_table_name}"// dq_error_description
                        )
        
      }
      
      
      DF_Distinct.unpersist()
    }
    
    return Result
  }
  
  private def DF_ProcessToDQ(fromSQL: String
                  ,whereSQL: String
                  ,haveField: Boolean
                  ,fieldName: String
                  ,dq_id: String
                  ,dq_error_notification: huemulType_DQNotification.huemulType_DQNotification
                  ,error_code: Integer
                  ,dq_error_description: String
                  ) {
    //get SQL to save error details to DQ_Error_Table
        val SQL_ProcessToDQDetail = this.DataFramehuemul.DQ_GenQuery( fromSQL   
                                                              , whereSQL           
                                                              , haveField            
                                                              , fieldName      
                                                              , dq_id
                                                              , dq_error_notification  
                                                              , error_code 
                                                              , dq_error_description
                                                              )
        val DetailDF = huemulBigDataGov.DF_ExecuteQuery("__DetailDF", SQL_ProcessToDQDetail)
        
        //Save errors to disk
        if (huemulBigDataGov.GlobalSettings.DQ_SaveErrorDetails && DetailDF != null && this.getSaveDQResult) {
          Control.NewStep("Start Save DQ Error Details for FK ")                
          if (!savePersist_DQ(Control, DetailDF)){
            huemulBigDataGov.logMessageWarn("Warning: DQ error can't save to disk")
          }
        }
  }
  
  private def DF_DataQualityMasterAuto(IsSelectiveUpdate: Boolean, LocalControl: huemul_Control, warning_exclude: Boolean): huemul_DataQualityResult = {
    var Result: huemul_DataQualityResult = new huemul_DataQualityResult()
    val ArrayDQ: ArrayBuffer[huemul_DataQuality] = new ArrayBuffer[huemul_DataQuality]()
    
    //All required fields have been set
    val SQL_Missing = missingRequiredFields(IsSelectiveUpdate)
    if (SQL_Missing.length > 0) {
      Result.isError = true
      Result.Description += "\nhuemul_Table Error: requiered columns missing "
      Result.Error_Code = 1016
      SQL_Missing.foreach { x => Result.Description +=  s",$x " }
    }
    
    //*********************************
    //Primary Key Validation
    //*********************************
    val SQL_PK: String = SQL_PrimaryKey_FinalTable()
    if (SQL_PK == "" || SQL_PK == null) {
      Result.isError = true
      Result.Description += "\nhuemul_Table Error: PK not defined"
      Result.Error_Code = 1017
    }  
    
    if (!warning_exclude) { 
      val DQ_PK : huemul_DataQuality = new huemul_DataQuality(null, s"PK Validation",s"count(1) = count(distinct ${SQL_PK} )", 1018, huemulType_DQQueryLevel.Aggregate,huemulType_DQNotification.ERROR, true, getPK_externalCode )
      DQ_PK.setTolerance(0, null)
      ArrayDQ.append(DQ_PK)    
     
      //*********************************
      //Aplicar DQ según definición de campos en DataDefDQ: Unique Values   
      //*********************************
      SQL_Unique_FinalTable().foreach { x => 
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: VALIDATE UNIQUE FOR FIELD $x")
        
        val DQ_UNIQUE : huemul_DataQuality = new huemul_DataQuality(x, s"UNIQUE Validation ",s"count(1) = count(distinct ${x.get_MyName()} )", 2006, huemulType_DQQueryLevel.Aggregate,huemulType_DQNotification.ERROR, true, x.getIsUnique_externalCode )
        DQ_UNIQUE.setTolerance(0, null)
        ArrayDQ.append(DQ_UNIQUE) 
      }
    }
    
    
    //Aplicar DQ según definición de campos en DataDefDQ: Acepta nulos (nullable)
    SQL_NotNull_FinalTable(warning_exclude).foreach { x => 
      if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: VALIDATE NOT NULL FOR FIELD ${x.get_MyName()}")
      
        val NotNullDQ : huemul_DataQuality = new huemul_DataQuality(x, s"Not Null for field ${x.get_MyName()} ", s"${x.get_MyName()} IS NOT NULL",1023, huemulType_DQQueryLevel.Row,huemulType_DQNotification.ERROR, true, x.getNullable_externalCode)
        NotNullDQ.setTolerance(0, null)
        ArrayDQ.append(NotNullDQ)
    }
    
    //VAlidación DQ_RegExp
    getColumns().filter { x => x.getDQ_RegExp != null && warning_exclude == false}.foreach { x => 
        var SQLFormula : String = s"""${x.get_MyName()} rlike "${x.getDQ_RegExp}" """

        var tand = ""
                  
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val RegExp : huemul_DataQuality = new huemul_DataQuality(x, s"RegExp Column ${x.get_MyName()}",SQLFormula, 1041, huemulType_DQQueryLevel.Row,huemulType_DQNotification.ERROR, true, x.getDQ_RegExp_externalCode  )
        
        RegExp.setTolerance(0, null)
        ArrayDQ.append(RegExp)
    }
    
   
    //VAlidación DQ máximo y mínimo largo de texto
    getColumns().filter { x => (x.getDQ_MinLen != null || x.getDQ_MaxLen != null) && warning_exclude == false  }.foreach { x => 
        var SQLFormula : String = ""

        var tand = ""
        if (x.getDQ_MinLen != null){
          SQLFormula += s"length(${x.get_MyName()}) >= ${x.getDQ_MinLen}"
          tand = " and "
        }
        
        if (x.getDQ_MaxLen != null)
          SQLFormula += s" $tand length(${x.get_MyName()}) <= ${x.getDQ_MaxLen}"
                  
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxLen : huemul_DataQuality = new huemul_DataQuality(x, s"length DQ for Column ${x.get_MyName()}",SQLFormula, 1020, huemulType_DQQueryLevel.Row,huemulType_DQNotification.ERROR, true, if (x.getDQ_MinLen_externalCode == null) x.getDQ_MaxLen_externalCode else x.getDQ_MinLen_externalCode )
        MinMaxLen.setTolerance(0, null)
        ArrayDQ.append(MinMaxLen)
    }
    
    //VAlidación DQ máximo y mínimo de números
    getColumns().filter { x => (x.getDQ_MinDecimalValue != null || x.getDQ_MaxDecimalValue != null) && warning_exclude == false  }.foreach { x => 
        
        var SQLFormula : String = ""
        var tand = ""
        if (x.getDQ_MinDecimalValue != null){
          SQLFormula += s"${x.get_MyName()} >= ${x.getDQ_MinDecimalValue}"
          tand = " and "
        }
        
        if (x.getDQ_MaxDecimalValue != null)
          SQLFormula += s" $tand ${x.get_MyName()} <= ${x.getDQ_MaxDecimalValue}"
        
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxNumber : huemul_DataQuality = new huemul_DataQuality(x, s"Number range DQ for Column ${x.get_MyName()}", SQLFormula,1021, huemulType_DQQueryLevel.Row,huemulType_DQNotification.ERROR, true,if (x.getDQ_MinDecimalValue_externalCode == null) x.getDQ_MaxDecimalValue_externalCode else x.getDQ_MinDecimalValue_externalCode)
        MinMaxNumber.setTolerance(0, null)         
        ArrayDQ.append(MinMaxNumber)
    }
    
    //VAlidación DQ máximo y mínimo de fechas
    getColumns().filter { x => (x.getDQ_MinDateTimeValue != null || x.getDQ_MaxDateTimeValue != null) && warning_exclude == false  }.foreach { x => 
        
        var SQLFormula : String = ""
        var tand = ""
        if (x.getDQ_MinDateTimeValue != null){
          SQLFormula += s"${x.get_MyName()} >= '${x.getDQ_MinDateTimeValue}'"
          tand = " and "
        }
        
        if (x.getDQ_MaxDateTimeValue != null)
          SQLFormula += s" $tand ${x.get_MyName()} <= '${x.getDQ_MaxDateTimeValue}'"
          
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxDT : huemul_DataQuality = new huemul_DataQuality(x, s"DateTime range DQ Column ${x.get_MyName()} ", SQLFormula, 1022, huemulType_DQQueryLevel.Row,huemulType_DQNotification.ERROR, true, if (x.getDQ_MinDateTimeValue_externalCode == null) x.getDQ_MaxDateTimeValue_externalCode else x.getDQ_MinDateTimeValue_externalCode)
        MinMaxDT.setTolerance(0, null)
        ArrayDQ.append(MinMaxDT)
    }
    
    val ResultDQ = this.DataFramehuemul.DF_RunDataQuality(this.getDataQuality(warning_exclude), ArrayDQ, this.DataFramehuemul.Alias, this, this.getSaveDQErrorOnce)
    
    if (ResultDQ.isError){
      Result.isError = true
      Result.Description += s"\n${ResultDQ.Description}"
      Result.Error_Code = ResultDQ.Error_Code
      
      //version 2.0: add PK error detail
      val NumReg = ResultDQ.getDQResult().filter { dqres => dqres.DQ_ErrorCode == 1018 }.length
      if (NumReg == 1) {
        //get on sentence
        var SQL_PK_on: String = ""   
        var SQL_PK_firstCol: String = ""
        getColumns().filter { x_cols => x_cols.getIsPK == true }.foreach { x_cols => 
            if (SQL_PK_on.length() > 0) 
              SQL_PK_on = s"${SQL_PK_on} and "
            
            if (SQL_PK_firstCol.length() == 0)
              SQL_PK_firstCol = x_cols.get_MyName()
        
            SQL_PK_on = s"${SQL_PK_on} PK.${x_cols.get_MyName()} = dup.${x_cols.get_MyName()} "
        }    
        
        
        LocalControl.NewStep(s"Step: DQ Result: Get detail error for PK Error (step1)")
        //PK error found, get PK duplicated
        val df_detail_01 = huemulBigDataGov.DF_ExecuteQuery("___temp_pk_det_01", s""" SELECT ${SQL_PK}, COUNT(1) as Cantidad
                                                                                   FROM ${this.DataFramehuemul.Alias}
                                                                                   GROUP BY ${SQL_PK}
                                                                                   HAVING COUNT(1) > 1
                                                                                """)
        //val numReg_01 = df_detail_01.count()
        //get rows duplicated
        LocalControl.NewStep(s"Step: DQ Result: Get detail error for PK Error (step2)")
        val df_detail_02 = huemulBigDataGov.DF_ExecuteQuery("___temp_pk_det_02", s""" SELECT /*+ BROADCAST(dup) */ PK.*
                                                                                   FROM ${this.DataFramehuemul.Alias} PK
                                                                                     INNER JOIN ___temp_pk_det_01 dup
                                                                                        ON ${SQL_PK_on}
                                                                                """)   
        val numReg = df_detail_02.count()                                                                                 
                                                                              
        val DQ_Id_PK = ResultDQ.getDQResult().filter { dqres => dqres.DQ_ErrorCode == 1018 }(0).DQ_Id
        val SQL_Detail = this.DataFramehuemul.DQ_GenQuery( "___temp_pk_det_02"   //sqlfrom
                                                          , null           //sqlwhere
                                                          , true            //haveField
                                                          , SQL_PK_firstCol      //fieldname
                                                          , DQ_Id_PK
                                                          , huemulType_DQNotification.ERROR //dq_error_notification 
                                                          , 1018 //error_code
                                                          , s"(1018) PK ERROR ON ${SQL_PK_on} ($numReg reg)"// dq_error_description
                                                          )
                             
        //Execute query
        LocalControl.NewStep(s"Step: DQ Result: Get detail error for PK Error (step3, $numReg rows)")
        var DF_EDetail = huemulBigDataGov.DF_ExecuteQuery("temp_DQ_PK", SQL_Detail)
                             
        if (ResultDQ.DetailErrorsDF == null)
          ResultDQ.DetailErrorsDF = DF_EDetail
        else
          ResultDQ.DetailErrorsDF = ResultDQ.DetailErrorsDF.union(DF_EDetail)
      }
    }
    
    //Save errors to disk
    if (huemulBigDataGov.GlobalSettings.DQ_SaveErrorDetails && ResultDQ.DetailErrorsDF != null && this.getSaveDQResult) {
      Control.NewStep("Start Save DQ Error Details ")                
      if (!savePersist_DQ(Control, ResultDQ.DetailErrorsDF)){
        huemulBigDataGov.logMessageWarn("Warning: DQ error can't save to disk")
      }
    }
    
    
    
    return Result
  }
  
  def getOrderByColumn(): String = {
    var ColumnName: String = null
    
    
    val Result = this.getALLDeclaredFields(true, true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] &&
                                      x.get(this).asInstanceOf[huemul_Columns].getIsPK }.foreach {x => 
      x.setAccessible(true)
      if (ColumnName == null)
        ColumnName = x.get(this).asInstanceOf[huemul_Columns].get_MyName()
    }
    
    return ColumnName
  }
 
  /**
   Create final DataFrame with full join New DF with old DF
   */
  private def DF_MDM_Dohuemul(LocalControl: huemul_Control, AliasNewData: String, isInsert: Boolean, isUpdate: Boolean, isDelete: Boolean, isSelectiveUpdate: Boolean, PartitionValueForSelectiveUpdate: String = null, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null) {
    if (isSelectiveUpdate) {
      //Update some rows with some columns
      //Cant update PK fields
      
      //***********************************************************************/
      //STEP 1:    V A L I D A T E   M A P P E D   C O L U M N  S   *********/
      //***********************************************************************/
      
      LocalControl.NewStep("Selective Update: Validating fields ")
      
      val PKNotMapped = this.missingRequiredFields_SelectiveUpdate()
      
      if (PKNotMapped != "")
        raiseError(s"huemul_Table Error: PK not defined: ${PKNotMapped}",1017)
      
      //Get N° rows from user dataframe
      val NumRowsUserData: Long = this.DataFramehuemul.DataFrame.count()
      var NumRowsOldDataFrame: Long = 0
      //**************************************************//
      //STEP 2:   CREATE BACKUP
      //**************************************************//
      LocalControl.NewStep("Selective Update: Select Old Table")                                             
      val TempAlias: String = s"__${this.TableName}_old"
      val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      if (!huemulBigDataGov.HasName(PartitionValueForSelectiveUpdate) && _TableType == huemulType_Tables.Transaction)
        raiseError(s"huemul_Table Error: Partition Value not defined", 1044)
        
      
      val FullPathString = if (_TableType == huemulType_Tables.Transaction) 
                      s"${getFullNameWithPath()}/${_PartitionField.toLowerCase()}=${PartitionValueForSelectiveUpdate}"
                    else
                      this.getFullNameWithPath()
      
      val FullPath = new org.apache.hadoop.fs.Path(FullPathString)
      
      if (fs.exists(FullPath)){
        //Exist, copy for use
        val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
        //Open actual file
        val DFTempCopy = huemulBigDataGov.spark.read.format(this.getStorageType.toString()).load(FullPathString)
        val tempPath = huemulBigDataGov.GlobalSettings.GetDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, TempAlias)
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to temp dir: $tempPath ")
        if (this.getNumPartitions == null || this.getNumPartitions <= 0)
          DFTempCopy.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
        else
          DFTempCopy.repartition(this.getNumPartitions).write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
          
        DFTempCopy.unpersist()
       
        //Open temp file
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"open temp old df: $tempPath ")
        val DFTempOpen = if (_TableType == huemulType_Tables.Transaction) 
                            huemulBigDataGov.spark.read.parquet(tempPath).withColumn(_PartitionField.toLowerCase(), lit(PartitionValueForSelectiveUpdate))
                         else huemulBigDataGov.spark.read.parquet(tempPath)
        NumRowsOldDataFrame = DFTempOpen.count()
        DFTempOpen.createOrReplaceTempView(TempAlias)  
        
        val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
        huemulBigDataGov.DF_SaveLineage(TempAlias
                                    , s"""SELECT * FROM ${this.internalGetTable(huemulType_InternalTableType.Normal)} ${if (_TableType == huemulType_Tables.Transaction) 
                                                                                                                      s" WHERE ${_PartitionField.toLowerCase()}='${PartitionValueForSelectiveUpdate}'"}""" //sql
                                    , dt_start
                                    , dt_end
                                    , Control
                                    , null  //FinalTable
                                    , false //isQuery
                                    , true //isReferenced
                                    )
      } else 
        raiseError(s"huemul_Table Error: Table ${FullPathString} doesn't exists",1043)
        
      
      //**************************************************//
      //STEP 3: Create left Join updating columns
      //**************************************************//
      LocalControl.NewStep("Selective Update: Left Join")
      val SQLLeftJoin_DF = huemulBigDataGov.DF_ExecuteQuery("__LeftJoin"
                                              , SQL_Step2_LeftJoin(TempAlias, this.DataFramehuemul.Alias)
                                              , Control //parent control 
                                             )
                                             
                                             
      //**************************************************//
      //STEP 4: Create Table with Update and Insert result
      //**************************************************//

      LocalControl.NewStep("Selective Update: Update Logic")
      val SQLHash_p1_DF = huemulBigDataGov.DF_ExecuteQuery("__update_p1"
                                          , SQL_Step4_Update("__LeftJoin", huemulBigDataGov.ProcessNameCall)
                                          , Control //parent control 
                                         )
                                         
      //**************************************************//
      //STEP 5: Create Hash
      //**************************************************//
      LocalControl.NewStep("Selective Update: Hash Code")                                         
      val SQLHash_p2_DF = huemulBigDataGov.DF_ExecuteQuery("__Hash_p1"
                                          , SQL_Step3_Hash_p1("__update_p1", isSelectiveUpdate)
                                          , Control //parent control 
                                         )
                                         
      this.UpdateStatistics(LocalControl, "Selective Update", "__Hash_p1")
      
      //N° Rows Updated == NumRowsUserData
      if (NumRowsUserData != this._NumRows_Updatable) {
        raiseError(s"huemul_Table Error: it was expected to update ${NumRowsUserData} rows, only ${_NumRows_Updatable} was found", 1045)
      } else if (this._NumRows_Total != NumRowsOldDataFrame ) {
        raiseError(s"huemul_Table Error: Different number of rows in dataframes, original: ${NumRowsOldDataFrame}, new: ${this._NumRows_Total}, check your dataframe, maybe have duplicate keys", 1046)
      }
      
      LocalControl.NewStep("Selective Update: Final Table")
      val SQLFinalTable = SQL_Step4_Final("__Hash_p1", huemulBigDataGov.ProcessNameCall, true)

     
      //STEP 2: Execute final table  //Add this.getNumPartitions param in v1.3
      DataFramehuemul._CreateFinalQuery(AliasNewData , SQLFinalTable, huemulBigDataGov.DebugMode , this.getNumPartitions, this, storageLevelOfDF)
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
        
      //Unpersist first DF
      SQLHash_p2_DF.unpersist()
      SQLHash_p1_DF.unpersist()
      SQLLeftJoin_DF.unpersist()
    }
    else if (_TableType == huemulType_Tables.Transaction) {
      LocalControl.NewStep("Transaction: Validating fields ")
      //STEP 1: validate name setting
      getALLDeclaredFields(true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
      .foreach { x =>     
        //Get field
        var Field = x.get(this).asInstanceOf[huemul_Columns]
        
        //New value (from field or compute column )
        if (!huemulBigDataGov.HasName(Field.get_MappedName()) ){
          raiseError(s"${x.getName()} MUST have an assigned local name",1004)
        }                 
      }
      
      //STEP 2: Create Hash
      LocalControl.NewStep("Transaction: Create Hash Field")
      val SQLFinalTable = SQL_Step0_TXHash(this.DataFramehuemul.Alias, huemulBigDataGov.ProcessNameCall)
      if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery)
        huemulBigDataGov.logMessageDebug(SQLFinalTable)
      //STEP 2: Execute final table //Add debugmode and getnumpartitions in v1.3
      DataFramehuemul._CreateFinalQuery(AliasNewData , SQLFinalTable, huemulBigDataGov.DebugMode , this.getNumPartitions, this, storageLevelOfDF)
      
      //LocalControl.NewStep("Transaction: Get Statistics info")
      this.UpdateStatistics(LocalControl, "Transaction", null)
      
      
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
    } 
    else if (_TableType == huemulType_Tables.Reference || _TableType == huemulType_Tables.Master)
    {
      /*
       * isInsert: se aplica en SQL_Step2_UpdateAndInsert, si no permite insertar, filtra esos registros y no los inserta
       * isUpdate: se aplica en SQL_Step1_FullJoin: si no permite update, cambia el tipo ___ActionType__ de UPDATE a EQUAL
       */
      
      val OnlyInsert: Boolean = isInsert && !isUpdate
      
      //All required fields have been set
      val SQL_Missing = missingRequiredFields(isSelectiveUpdate)
      if (SQL_Missing.length > 0) {
        var ColumnsMissing: String = ""
        SQL_Missing.foreach { x => ColumnsMissing +=  s",$x " }
        this.raiseError(s"huemul_Table Error: requiered fields missing ${ColumnsMissing}", 1016)
         
      }
    
      //**************************************************//
      //STEP 0: Apply distinct to New DataFrame
      //**************************************************//
      var NextAlias = this.DataFramehuemul.Alias
      
      if (ApplyDistinct) {
        LocalControl.NewStep("Ref & Master: Select distinct")
        val SQLDistinct_DF = huemulBigDataGov.DF_ExecuteQuery("__Distinct"
                                                , SQL_Step0_Distinct(this.DataFramehuemul.Alias)
                                                , Control //parent control 
                                               )
        NextAlias = "__Distinct"
      }
      
      
      //**************************************************//
      //STEP 0.1: CREATE TEMP TABLE IF MASTER TABLE DOES NOT EXIST
      //**************************************************//
      LocalControl.NewStep("Ref & Master: Select Old Table")      
      val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
      val TempAlias: String = s"__${this.TableName}_old"
      val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      var oldDataFrameExists = false
      
      if (this.getStorageType == huemulType_StorageType.HBASE) {
        val hBaseConnector = new huemul_TableConnector(huemulBigDataGov, LocalControl)
        if (hBaseConnector.tableExistsHBase(getHBaseNamespace(huemulType_InternalTableType.Normal),getHBaseTableName(huemulType_InternalTableType.Normal))) {
          oldDataFrameExists = true
          //println(getHBaseCatalog(huemulType_InternalTableType.Normal))
          //val DFHBase = hBaseConnector.getDFFromHBase(TempAlias, getHBaseCatalog(huemulType_InternalTableType.Normal))
          
          //create external table if doesn't exists
          CreateTableScript = DF_CreateTableScript()          
          huemulBigDataGov.HIVE_connection.ExecuteJDBC_NoResulSet(CreateTableScript)
            
          val DFHBase = huemulBigDataGov.DF_ExecuteQuery(TempAlias, s"SELECT * FROM ${this.internalGetTable(huemulType_InternalTableType.Normal)}")
          
          /*
          var tempPath: String = null
          if (huemulBigDataGov.GlobalSettings.MDM_SaveBackup && this._SaveBackup){
            tempPath = this.getFullNameWithPath_Backup(Control.Control_Id )
            _BackupPath = tempPath
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to backup dir: $tempPath ")
          } else {
            tempPath = huemulBigDataGov.GlobalSettings.GetDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, TempAlias)
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to temp dir: $tempPath ")
          }
          
          if (this.getNumPartitions == null || this.getNumPartitions <= 0)
            DFHBase.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)     //2.2 -> this._StorageType.toString() instead of "parquet"
          else
            DFHBase.repartition(this.getNumPartitions).write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)   //2.2 -> this._StorageType.toString() instead of "parquet"
          */
          val numRows = DFHBase.count()
          //DFHBase.show()
        }
      } else {
        if (fs.exists(new org.apache.hadoop.fs.Path(this.getFullNameWithPath()))){
          //Exist, copy for use
          oldDataFrameExists = true
          
          //Open actual file
          val DFTempCopy = huemulBigDataGov.spark.read.format(this.getStorageType.toString()).load(this.getFullNameWithPath())
          
          //2.0: save previous to backup
          var tempPath: String = null
          if (huemulBigDataGov.GlobalSettings.MDM_SaveBackup && this._SaveBackup){
            tempPath = this.getFullNameWithPath_Backup(Control.Control_Id )
            _BackupPath = tempPath
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to backup dir: $tempPath ")
          } else {
            tempPath = huemulBigDataGov.GlobalSettings.GetDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, TempAlias)
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to temp dir: $tempPath ")
          }
          
          if (this.getNumPartitions == null || this.getNumPartitions <= 0)
            DFTempCopy.write.mode(SaveMode.Overwrite).format(this.getStorageType.toString()).save(tempPath)     //2.2 -> this._StorageType.toString() instead of "parquet"
          else
            DFTempCopy.repartition(this.getNumPartitions).write.mode(SaveMode.Overwrite).format(this.getStorageType.toString()).save(tempPath)   //2.2 -> this._StorageType.toString() instead of "parquet"
          DFTempCopy.unpersist()
         
          //Open temp file
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"open temp old df: $tempPath ")
          val DFTempOpen = huemulBigDataGov.spark.read.format(this.getStorageType.toString()).load(tempPath)  //2.2 --> read.format(this._StorageType.toString()).load(tempPath)    instead of  read.parquet(tempPath)   
          DFTempOpen.createOrReplaceTempView(TempAlias)        
        }
      }
      
      if (!oldDataFrameExists) {
          //huemulBigDataGov.logMessageInfo(s"create empty dataframe because ${this.internalGetTable(huemulType_InternalTableType.Normal)} does not exist")
          huemulBigDataGov.logMessageInfo(s"create empty dataframe because oldDF does not exist")
          val Schema = getSchema()
          val SchemaForEmpty = StructType(Schema.map { x => StructField(x.name, x.dataType, x.nullable) })
          val EmptyRDD = huemulBigDataGov.spark.sparkContext.emptyRDD[Row]
          val EmptyDF = huemulBigDataGov.spark.createDataFrame(EmptyRDD, SchemaForEmpty)
          EmptyDF.createOrReplaceTempView(TempAlias)
          if (huemulBigDataGov.DebugMode) EmptyDF.show()
      }
      val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
      
      huemulBigDataGov.DF_SaveLineage(TempAlias
                                    , s"SELECT * FROM ${this.internalGetTable(huemulType_InternalTableType.Normal)} " //sql
                                    , dt_start
                                    , dt_end
                                    , Control
                                    , null  //FinalTable
                                    , false //isQuery
                                    , true //isReferenced
                                    )
      
                                             
        
      //**************************************************//
      //STEP 1: Create Full Join with all fields
      //**************************************************//
      LocalControl.NewStep("Ref & Master: Full Join")
      val SQLFullJoin_DF = huemulBigDataGov.DF_ExecuteQuery("__FullJoin"
                                              , SQL_Step1_FullJoin(TempAlias, NextAlias, isUpdate, isDelete)
                                              , Control //parent control 
                                             )
                                             
                                             
      //STEP 2: Create Tabla with Update and Insert result
      LocalControl.NewStep("Ref & Master: Update & Insert Logic")
      val SQLHash_p1_DF = huemulBigDataGov.DF_ExecuteQuery("__logic_p1"
                                          , SQL_Step2_UpdateAndInsert("__FullJoin", huemulBigDataGov.ProcessNameCall, isInsert)
                                          , Control //parent control 
                                         )
     
      //STEP 3: Create Hash
      LocalControl.NewStep("Ref & Master: Hash Code")                                         
      val SQLHash_p2_DF = huemulBigDataGov.DF_ExecuteQuery("__Hash_p1"
                                          , SQL_Step3_Hash_p1("__logic_p1", false)
                                          , Control //parent control 
                                         )
                                         
                                        
      this.UpdateStatistics(LocalControl, "Ref & Master", "__Hash_p1")
      
                                         
      LocalControl.NewStep("Ref & Master: Final Table")
      //val SQLFinalTable = SQL_Step4_Final("__Hash_p1", huemulBigDataGov.ProcessNameCall, if (OnlyInsert) true else false)
      //from 2.1: incluye ActionType fro all master and transaction table types, to use on statistics 
      val SQLFinalTable = SQL_Step4_Final("__Hash_p1", huemulBigDataGov.ProcessNameCall, true )

     
      //STEP 2: Execute final table // Add debugmode and getnumpartitions in v1.3 
      DataFramehuemul._CreateFinalQuery(AliasNewData , SQLFinalTable, huemulBigDataGov.DebugMode , this.getNumPartitions, this, storageLevelOfDF)
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
      
      
      
      
    } else
      raiseError(s"huemul_Table Error: ${_TableType} found, Master o Reference required ", 1007)
  }
  
  private def UpdateStatistics(LocalControl: huemul_Control, TypeOfCall: String, Alias: String) {
    LocalControl.NewStep(s"${TypeOfCall}: Statistics")
    
    if (Alias == null) {
      this._NumRows_Total = this.DataFramehuemul.getNumRows
      this._NumRows_New = this._NumRows_Total
      this._NumRows_Update  = 0
      this._NumRows_Updatable = 0
      this._NumRows_Delete = 0
      this._NumRows_NoChange = 0
      //this._NumRows_Excluded = 0
    } else {
      //DQ for Reference and Master Data
      val DQ_ReferenceData: DataFrame = huemulBigDataGov.spark.sql(
                        s"""SELECT CAST(SUM(CASE WHEN ___ActionType__ = 'NEW' then 1 else 0 end) as Long) as __New
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'UPDATE' and SameHashKey = 0 then 1 else 0 end) as Long) as __Update
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'UPDATE' then 1 else 0 end) as Long) as __Updatable
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'DELETE' then 1 else 0 end) as Long) as __Delete
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'EQUAL' OR
                                                (___ActionType__ = 'UPDATE' and SameHashKey <> 0) then 1 else 0 end) as Long) as __NoChange
                                  ,CAST(count(1) AS Long) as __Total
                            FROM ${Alias} temp 
                         """)
      
      if (huemulBigDataGov.DebugMode) DQ_ReferenceData.show()
      
      val FirstRow = DQ_ReferenceData.collect()(0) // .first()
      this._NumRows_Total = FirstRow.getAs("__Total")
      this._NumRows_New = FirstRow.getAs("__New")
      this._NumRows_Update  = FirstRow.getAs("__Update")
      this._NumRows_Updatable  = FirstRow.getAs("__Updatable")
      this._NumRows_Delete = FirstRow.getAs("__Delete")
      this._NumRows_NoChange= FirstRow.getAs("__NoChange")
      
      
      
      LocalControl.NewStep(s"${TypeOfCall}: Validating Insert & Update")
      var DQ_Error: String = ""
      var Error_Number: Integer = null
      if (this._NumRows_Total == this._NumRows_New)
        DQ_Error = "" //Doesn't have error, first run
      else if (this._DQ_MaxNewRecords_Num != null && this._DQ_MaxNewRecords_Num > 0 && this._NumRows_New > this._DQ_MaxNewRecords_Num){
        DQ_Error = s"huemul_Table Error: DQ MDM Error: N° New Rows (${this._NumRows_New}) exceeds max defined (${this._DQ_MaxNewRecords_Num}) "
        Error_Number = 1005
      }
      else if (this._DQ_MaxNewRecords_Perc != null && this._DQ_MaxNewRecords_Perc > Decimal.apply(0) && (Decimal.apply(this._NumRows_New) / Decimal.apply(this._NumRows_Total)) > this._DQ_MaxNewRecords_Perc) {
        DQ_Error = s"huemul_Table Error: DQ MDM Error: % New Rows (${(Decimal.apply(this._NumRows_New) / Decimal.apply(this._NumRows_Total))}) exceeds % max defined (${this._DQ_MaxNewRecords_Perc}) "
        Error_Number = 1006
      }
  
      if (DQ_Error != "")
        raiseError(DQ_Error, Error_Number)
        
      DQ_ReferenceData.unpersist()
    }
  }
  
  private def getClassAndPackage(): huemul_AuthorizationPair = {
    val Invoker = new Exception().getStackTrace()(2)
    val InvokerName: String = Invoker.getClassName().replace("$", "")
    
    val ArrayResult = InvokerName.split('.')
    
    val ClassNameInvoker = ArrayResult(ArrayResult.length-1)
    val PackageNameInvoker: String = InvokerName.replace(".".concat(ClassNameInvoker), "")
    
    return new huemul_AuthorizationPair(ClassNameInvoker,PackageNameInvoker)
  }
  
  def executeFull(NewAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
    var Result: Boolean = false
    val whoExecute = getClassAndPackage()  
    if (this._WhoCanRun_executeFull.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.executeSave(NewAlias, true, true, true, false, null, storageLevelOfDF, false)      
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeFull in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}", 1008)
      
    }
    
    return Result
  }
  
  //2.1: compatibility with previous version
  def executeOnlyInsert(NewAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
    return executeOnlyInsert(NewAlias, storageLevelOfDF, false)
  }
  
  //2.1: add RegisterOnlyInsertInDQ params
  def executeOnlyInsert(NewAlias: String, RegisterOnlyInsertInDQ: Boolean): Boolean = {
    return executeOnlyInsert(NewAlias, null, RegisterOnlyInsertInDQ)
  }
  
  //2.1: introduce RegisterOnlyInsertInDQ
  def executeOnlyInsert(NewAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel, RegisterOnlyInsertInDQ: Boolean): Boolean = {
    var Result: Boolean = false
    if (this._TableType == huemulType_Tables.Transaction)
      raiseError("huemul_Table Error: DoOnlyInserthuemul is not available for Transaction Tables",1009)

    val whoExecute = getClassAndPackage()  
    if (this._WhoCanRun_executeOnlyInsert.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.executeSave(NewAlias, true, false, false, false, null, storageLevelOfDF, RegisterOnlyInsertInDQ) 
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeOnlyInsert in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}", 1010)
    }
         
    return Result
  }
  
  def executeOnlyUpdate(NewAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {   
    var Result: Boolean = false
    if (this._TableType == huemulType_Tables.Transaction)
      raiseError("huemul_Table Error: DoOnlyUpdatehuemul is not available for Transaction Tables", 1011)
      
    val whoExecute = getClassAndPackage()  
    if (this._WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.executeSave(NewAlias, false, true, false, false, null, storageLevelOfDF, false)  
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeOnlyUpdate in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}",1012)
    }
    
    return Result
        
  }
  
  def executeSelectiveUpdate(NewAlias: String, PartitionValueForSelectiveUpdate: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {   
    var Result: Boolean = false
      
    val whoExecute = getClassAndPackage()  
    if (this._WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.executeSave(NewAlias, false, false, false, true, PartitionValueForSelectiveUpdate, storageLevelOfDF, false)  
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeSelectiveUpdate in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}",1012)
    }
    
    return Result
        
  }
  
  private def CompareSchema(Columns: ArrayBuffer[huemul_Columns], Schema: StructType): String = {
    var Errores: String = ""
    Columns.foreach { x => 
      if (huemulBigDataGov.HasName(x.get_MappedName())) {
        //val ColumnNames = if (UseAliasColumnName) x.get_MappedName() else x.get_MyName()
        val ColumnNames = x.get_MyName()
        val ColumnInSchema = Schema.filter { y => y.name.toLowerCase() == ColumnNames.toLowerCase() }
        if (ColumnInSchema == null || ColumnInSchema.length == 0)
          raiseError(s"huemul_Table Error: column missing in Schema ${ColumnNames}", 1038)
        if (ColumnInSchema.length != 1)
          raiseError(s"huemul_Table Error: multiples columns found in Schema with name ${ColumnNames}", 1039)
        
          val dataType = ColumnInSchema(0).dataType
        if (dataType != x.DataType) {
          Errores = Errores.concat(s"Error Column ${ColumnNames}, Requiered: ${x.DataType}, actual: ${dataType}  \n")
        }
      }
    }
    
    return Errores
  }
  
  /**
   Save Full data: <br>
   Master & Reference: update and Insert
   Transactional: Delete and Insert new data
   */
  private def executeSave(AliasNewData: String, IsInsert: Boolean, IsUpdate: Boolean, IsDelete: Boolean, IsSelectiveUpdate: Boolean, PartitionValueForSelectiveUpdate: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel, RegisterOnlyInsertInDQ: Boolean): Boolean = {
    if (!this.DefinitionIsClose)
      this.raiseError(s"huemul_Table Error: MUST call ApplyTableDefinition ${this.TableName}", 1048)
    
    _NumRows_Excluded = 0  
      
    var LocalControl = new huemul_Control(huemulBigDataGov, Control ,huemulType_Frequency.ANY_MOMENT, false )
    LocalControl.AddParamInformation("AliasNewData", AliasNewData)
    LocalControl.AddParamInformation("IsInsert", IsInsert.toString())
    LocalControl.AddParamInformation("IsUpdate", IsUpdate.toString())
    LocalControl.AddParamInformation("IsDelete", IsDelete.toString())
    LocalControl.AddParamInformation("IsSelectiveUpdate", IsSelectiveUpdate.toString())
    LocalControl.AddParamInformation("PartitionValueForSelectiveUpdate", PartitionValueForSelectiveUpdate)
    
    var result : Boolean = true
    var ErrorCode: Integer = null
    
    try {
      val OnlyInsert: Boolean = IsInsert && !IsUpdate
      
      //Compare schemas
      if (!autoCast) {
        LocalControl.NewStep("Compare Schema")
        val ResultCompareSchema = CompareSchema(this.getColumns(), this.DataFramehuemul.DataFrame.schema) 
        if (ResultCompareSchema != "") {
          result = false
          ErrorCode = 1013
          raiseError(s"huemul_Table Error: User Error: incorrect DataType: \n${ResultCompareSchema}", ErrorCode)
        }
      }
    
      //do work
      DF_MDM_Dohuemul(LocalControl, AliasNewData,IsInsert, IsUpdate, IsDelete, IsSelectiveUpdate, PartitionValueForSelectiveUpdate, storageLevelOfDF)
  
      
   //WARNING_EXCLUDE (starting 2.1)
      //DataQuality by Columns
      LocalControl.NewStep("Start DataQuality WARNING_EXCLUDE")
      val DQResult_EXCLUDE = DF_DataQualityMasterAuto(IsSelectiveUpdate, LocalControl, true)
      //Foreing Keys by Columns
      LocalControl.NewStep("Start ForeingKey WARNING_EXCLUDE ")
      val FKResult_EXCLUDE = DF_ForeingKeyMasterAuto(true)
      
      //from 2.1: exclude_warnings, update DataFramehuemul 
      excludeRows(LocalControl)
   
      //from 2.2 --> raiserror if DF is empty
      if (this.DataFramehuemul.getNumRows() == 0) {
        this.raiseError(s"huemul_Table Error: Dataframe is empty, nothing to save", 1062)
      }
      
    //REST OF RULES (DIFFERENT FROM WARNING_EXCLUDE)
      //DataQuality by Columns
      LocalControl.NewStep("Start DataQuality ERROR AND WARNING")
      val DQResult = DF_DataQualityMasterAuto(IsSelectiveUpdate, LocalControl, false)
      //Foreing Keys by Columns
      LocalControl.NewStep("Start ForeingKey ERROR AND WARNING ")
      val FKResult = DF_ForeingKeyMasterAuto(false)
      
      
      LocalControl.NewStep("Validating errors ")
      var localErrorCode: Integer = null
      if (DQResult.isError || FKResult.isError ) {
        result = false
        var ErrorDetail: String = ""
                
        if (DQResult.isError) {
          ErrorDetail = s"DataQuality Error: \n${DQResult.Description}"
          localErrorCode = DQResult.Error_Code
        }
        
        if (FKResult.isError) {
          ErrorDetail += s"\nForeing Key Validation Error: \n${FKResult.Description}"
          localErrorCode = DQResult.Error_Code
        }
         
        
        raiseError(ErrorDetail, localErrorCode)
      }
      
      //Compare schemas final table
      LocalControl.NewStep("Compare Schema Final DF")
      val ResultCompareSchemaFinal = CompareSchema(this.getColumns(), this.DataFramehuemul.DataFrame.schema) 
      if (ResultCompareSchemaFinal != "") {
        ErrorCode = 1014
        raiseError(s"huemul_Table Error: User Error: incorrect DataType: \n${ResultCompareSchemaFinal}",ErrorCode)
      }
      
      //Create table persistent
      if (huemulBigDataGov.DebugMode){
        huemulBigDataGov.logMessageDebug(s"Saving ${internalGetTable(huemulType_InternalTableType.Normal)} Table with params: ") 
        huemulBigDataGov.logMessageDebug(s"${_PartitionField} field for partitioning table")
        huemulBigDataGov.logMessageDebug(s"${getFullNameWithPath()} path")
      }
      
      LocalControl.NewStep("Start Save ")                
      if (savePersist(LocalControl, DataFramehuemul, OnlyInsert, IsSelectiveUpdate, RegisterOnlyInsertInDQ )){
        LocalControl.NewStep("Register Master Information ")
        Control.RegisterMASTER_CREATE_Use(this)
      
        LocalControl.FinishProcessOK
      }
      else {
        result = false
        LocalControl.FinishProcessError()
      }
      
    } catch {
      case e: Exception => 
        result = false
        if (ErrorCode == null) {
          if (this.Error_Code != null && this.Error_Code != 0)
            ErrorCode  = this.Error_Code
          else if (LocalControl.Control_Error.ControlError_ErrorCode == null)
            ErrorCode = 1027
          else
            ErrorCode = LocalControl.Control_Error.ControlError_ErrorCode
        }
        
        LocalControl.Control_Error.GetError(e, getClass().getSimpleName, ErrorCode)
        LocalControl.FinishProcessError()
    }
    
    return result
  }
  
  def copyToDest(PartitionValue: String, DestEnvironment: String) {
    if (huemulBigDataGov.HasName(_PartitionField)) {
       val ProdFullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath()}/${_PartitionField.toLowerCase()}=${PartitionValue}")
       val ManualFullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath2(DestEnvironment)}/${_PartitionField.toLowerCase()}=${PartitionValue}")
       
       val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       org.apache.hadoop.fs.FileUtil.copy(fs, ProdFullPath, fs, ManualFullPath, false, true, huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       
       val DestTableName: String = InternalGetTable(DestEnvironment)
       huemulBigDataGov.logMessageInfo(s"MSCK REPAIR TABLE ${DestTableName}")
       huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${DestTableName}")
              
    } else {
       val ProdFullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath()}/${_PartitionField.toLowerCase()}")
       val ManualFullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath2(DestEnvironment)}/${_PartitionField.toLowerCase()}")
       
       val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       org.apache.hadoop.fs.FileUtil.copy(fs, ProdFullPath, fs, ManualFullPath, false, true, huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
    }
    
  }
  
  private def excludeRows(LocalControl: huemul_Control): Unit = {
    //Add from 2.1: exclude DQ from WARNING_EXCLUDE
    val warning_Exclude_detail = Control.control_getDQResultForWarningExclude()
    if (warning_Exclude_detail.length > 0) {
      //get PK
      var dq_StringSQL_PK: String = ""
      var dq_StringSQL_PK_join: String = ""
      var dq_firstPK: String = null
      var coma: String = ""
      var _and: String = ""
      getALLDeclaredFields().filter { x => x.setAccessible(true)
                                           x.get(this).isInstanceOf[huemul_Columns] &&
                                           x.get(this).asInstanceOf[huemul_Columns].getIsPK }
      .foreach { x =>
        dq_StringSQL_PK += s" ${coma}${x.getName}"
        dq_StringSQL_PK_join += s" ${_and} PK.${x.getName} = EXCLUDE.${x.getName}" 
        if (dq_firstPK == null)
          dq_firstPK = x.getName
        coma = ","
        _and = " and "
      }
      
      
      //get DQ with WARNING_EXCLUDE > 0 rows
      var dq_id_list: String = ""
      coma = ""
      warning_Exclude_detail.foreach { x => 
        dq_id_list += s"""${coma}"${x}"""" 
        coma = ","
      }
      val _tableNameDQ: String = internalGetTable(huemulType_InternalTableType.DQ)
      
      //get PK Details
      LocalControl.NewStep("WARNING_EXCLUDE: Get details")
      val DQ_Det = huemulBigDataGov.DF_ExecuteQuery("__DQ_Det", s"""SELECT DISTINCT ${dq_StringSQL_PK} FROM ${_tableNameDQ} WHERE dq_control_id="${Control.Control_Id}" AND dq_dq_id in ($dq_id_list)""")
      //Broadcast
      _NumRows_Excluded = DQ_Det.count()
      var apply_broadcast: String = ""
      if (_NumRows_Excluded < 50000)
        apply_broadcast = "/*+ BROADCAST(EXCLUDE) */"
        
      //exclude
      LocalControl.NewStep(s"WARNING_EXCLUDE: EXCLUDE rows")
      huemulBigDataGov.logMessageInfo(s"WARNING_EXCLUDE: ${_NumRows_Excluded} rows excluded")
      DataFramehuemul.DF_from_SQL(DataFramehuemul.Alias, s"SELECT $apply_broadcast PK.* FROM ${DataFramehuemul.Alias} PK LEFT JOIN __DQ_Det EXCLUDE ON ${dq_StringSQL_PK_join} WHERE EXCLUDE.${dq_firstPK} IS NULL ")
      
      if (_TableType == huemulType_Tables.Transaction)
        this.UpdateStatistics(LocalControl, "WARNING_EXCLUDE", null)
      else 
        this.UpdateStatistics(LocalControl, "WARNING_EXCLUDE", DataFramehuemul.Alias)
    }
    
    
  }
  
  /**
   * Save data to disk
   */
  private def savePersist(LocalControl: huemul_Control, DF_huemul: huemul_DataFrame , OnlyInsert: Boolean, IsSelectiveUpdate: Boolean, RegisterOnlyInsertInDQ:Boolean): Boolean = {
    var DF_Final = DF_huemul.DataFrame
    var Result: Boolean = true
    
    //from 2.1 --> change position of this code, to get after WARNING_EXCLUDE
    //Add from 2.0: save Old Value Trace
    //CREATE NEW DATAFRAME WITH MDM OLD VALUE FULL TRACE
    if (huemulBigDataGov.GlobalSettings.MDM_SaveOldValueTrace) {
      LocalControl.NewStep("Ref & Master: MDM Old Value Full Trace")
      val SQL_FullTrace = SQL_Step_OldValueTrace("__FullJoin", huemulBigDataGov.ProcessNameCall)
      
      var tempSQL_OldValueFullTrace_DF : DataFrame = null 
      if (SQL_FullTrace != null){ //if null, doesn't have the mdm old "value full trace" to get          
        tempSQL_OldValueFullTrace_DF = huemulBigDataGov.DF_ExecuteQuery("__SQL_OldValueFullTrace_DF",SQL_FullTrace) 
      
        if (tempSQL_OldValueFullTrace_DF != null) {
          Result = savePersist_OldValueTrace(LocalControl,tempSQL_OldValueFullTrace_DF)
          
          if (!Result)
            return Result 
        }
      }
    }

    
    if (this._TableType == huemulType_Tables.Reference || this._TableType == huemulType_Tables.Master || IsSelectiveUpdate) {
      LocalControl.NewStep("Save: Drop ActionType column")
   
      if (OnlyInsert && !IsSelectiveUpdate) {
        DF_Final = DF_Final.where("___ActionType__ = 'NEW'") 
      }
      
      DF_Final = DF_Final.drop("___ActionType__").drop("SameHashKey") //from 2.1: always this columns exist on reference and master tables
      if (OnlyInsert) {
      
        if (RegisterOnlyInsertInDQ) {
          val dt_start = huemulBigDataGov.getCurrentDateTimeJava()  
          DF_Final.createOrReplaceTempView("__tempnewtodq")
          val numRowsDQ = DF_Final.count()
          val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
          val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
          
          //from 2.1: insertOnlyNew as DQ issue 
          val Values = new huemul_DQRecord(huemulBigDataGov)
          Values.Table_Name =TableName
          Values.BBDD_Name =this.getDataBase(this._DataBase)
          Values.DF_Alias = ""
          Values.ColumnName = null
          Values.DQ_Name =s"OnlyInsert"
          Values.DQ_Description =s"new values inserted to master or reference table"
          Values.DQ_QueryLevel = huemulType_DQQueryLevel.Row // IsAggregate =false
          Values.DQ_Notification = huemulType_DQNotification.WARNING// RaiseError =true
          Values.DQ_SQLFormula =""
          Values.DQ_ErrorCode = 1055
          Values.DQ_toleranceError_Rows = 0
          Values.DQ_toleranceError_Percent = null
          Values.DQ_ResultDQ ="new values inserted to master or reference table"
          Values.DQ_NumRowsOK = 0
          Values.DQ_NumRowsError = numRowsDQ
          Values.DQ_NumRowsTotal =numRowsDQ
          Values.DQ_IsError = false
          Values.DQ_IsWarning = true
          Values.DQ_ExternalCode = "HUEMUL_DQ_009"
          Values.DQ_duration_hour = duration.hour.toInt
          Values.DQ_duration_minute = duration.minute.toInt
          Values.DQ_duration_second = duration.second.toInt
          
          this.DataFramehuemul.DQ_Register(Values)
          
          //from 2.1: add only insert to DQ record
          DF_ProcessToDQ(   "__tempnewtodq"   //sqlfrom
                          , null              //sqlwhere
                          , false             //haveField
                          , null              //fieldname
                          , Values.DQ_Id
                          , huemulType_DQNotification.WARNING //dq_error_notification 
                          , Values.DQ_ErrorCode //error_code
                          , s"(1055) huemul_Table Warning: new values inserted to master or reference table"// dq_error_description
                          )
        }
      }
      
    }
      
    
    try {
      if (_PartitionField == null || _PartitionField == ""){
        
        if (this.getNumPartitions > 0) {
          LocalControl.NewStep("Save: Set num FileParts")
          DF_Final = DF_Final.repartition(this.getNumPartitions)
        }
                
        var localSaveMode: SaveMode = null
        if (OnlyInsert) {
          LocalControl.NewStep("Save: Append Master & Ref Data")
          localSaveMode = SaveMode.Append
        }
        else {
          LocalControl.NewStep("Save: Overwrite Master & Ref Data")
          localSaveMode = SaveMode.Overwrite
        }
        
       
        if (this.getStorageType == huemulType_StorageType.HBASE){
          if (DF_Final.count() > 0) {
            val huemulDriver = new huemul_TableConnector(huemulBigDataGov, LocalControl)
            huemulDriver.saveToHBase(DF_Final
                                    ,getHBaseNamespace(huemulType_InternalTableType.Normal)
                                    ,getHBaseTableName(huemulType_InternalTableType.Normal)
                                    ,this.getNumPartitions //numPartitions
                                    ,OnlyInsert
                                    ,if (_numPKColumns == 1) _HBase_PKColumn else "hs_rowkey" //PKName
                                    ,getALLDeclaredFields_forHBase(getALLDeclaredFields(false))
                                    )
          }
        }
        else 
          DF_Final.write.mode(localSaveMode).format(this.getStorageType.toString()).save(getFullNameWithPath())
        
        
        //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)       
        //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath()), new FsPermission("770"))
      }
      else{
        //Get Partition_Id Values
        LocalControl.NewStep("Save: Validating N° partitions")
        val DFDistinct = DF_Final.select(_PartitionField).distinct().withColumn(_PartitionField, DF_Final.col(_PartitionField).cast(StringType)).collect()
        if (DFDistinct.length != 1){
          raiseError(s"huemul_Table Error: N° values in partition wrong!, expected: 1, real: ${DFDistinct.length}",1015)
        } else {
          this.PartitionValue = DFDistinct(0).getAs[String](_PartitionField)
          val FullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath()}/${_PartitionField.toLowerCase()}=${this.PartitionValue}")
          
          LocalControl.NewStep("Save: Drop old partition")
          val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)       
          fs.delete(FullPath, true)
          
          if (this.getNumPartitions > 0) {
            LocalControl.NewStep("Save: Set num FileParts")
            DF_Final = DF_Final.repartition(this.getNumPartitions)
          }
          
          LocalControl.NewStep("Save: OverWrite partition with new data")
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: ${FullPath} ")     
          
          DF_Final.write.mode(SaveMode.Append).format(this.getStorageType.toString()).partitionBy(_PartitionField).save(getFullNameWithPath())
                
          //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath()), new FsPermission("770"))
  
        }                       
      }
    } catch {
      case e: Exception => 
        
        this.Error_isError = true
        this.Error_Text = s"huemul_Table Error: write in disk failed. ${e.getMessage}"
        this.Error_Code = 1026
        Result = false
        LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
    }
    
    if (Result) {
      if (CreateInHive ) {
        val sqlDrop01 = s"drop table if exists ${internalGetTable(huemulType_InternalTableType.Normal)}"
        LocalControl.NewStep("Save: Drop Hive table Def")
        if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
        try {
          val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(getCurrentDataBase()).collect()
          if (TablesListFromHive.filter { x => x.name.toUpperCase() == TableName.toUpperCase()  }.length > 0) 
            huemulBigDataGov.spark.sql(sqlDrop01)
            
        } catch {
          case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
        }
       
      }
        
      try {
        //create table
        if (CreateInHive ) {
          LocalControl.NewStep("Save: Create Table in Hive Metadata")
          CreateTableScript = DF_CreateTableScript() 
         
          //from 2.2 --> create HBAse table using JDBC Hive connection
          if (getStorageType == huemulType_StorageType.HBASE)
            huemulBigDataGov.HIVE_connection.ExecuteJDBC_NoResulSet(CreateTableScript)
          else 
            huemulBigDataGov.spark.sql(CreateTableScript)
        }
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableName : String = internalGetTable(huemulType_InternalTableType.Normal)
        if (CreateInHive && (_PartitionField != null && _PartitionField != "")) {
          LocalControl.NewStep("Save: Repair Hive Metadata")
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"MSCK REPAIR TABLE ${_tableName}")
          huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${_tableName}")
        }
        
        if (huemulBigDataGov.ImpalaEnabled) {
          LocalControl.NewStep("Save: refresh Impala Metadata")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"invalidate metadata ${_tableName}")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"refresh ${_tableName}")
        }
      } catch {
        case e: Exception => 
          
          this.Error_isError = true
          this.Error_Text = s"huemul_Table Error: create external table failed. ${e.getMessage}"
          this.Error_Code = 1025
          Result = false
          LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
      }
    }
    
    //from 2.1: assing final DF to huemulDataFrame
    DF_huemul.setDataFrame(DF_Final, DF_huemul.Alias, false) 
    
    //from 2.0: update dq and ovt used
    LocalControl.RegisterMASTER_UPDATE_isused(this)
    
      
      
    return Result
    
  }
    
  
  
  /**
   * Save DQ Result data to disk
   */
  private def savePersist_OldValueTrace(LocalControl: huemul_Control, DF: DataFrame): Boolean = {
    var DF_Final = DF
    var Result: Boolean = true
    this._table_ovt_isused = 1
      
    try {      
      LocalControl.NewStep("Save: OldVT Result: Saving Old Value Trace result")
      if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: ${getFullNameWithPath_OldValueTrace()} ")
      if (getStorageType_OldValueTrace == huemulType_StorageType.PARQUET){
        DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).partitionBy("MDM_columnName").format(getStorageType_OldValueTrace.toString()).save(getFullNameWithPath_OldValueTrace())
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).format(_StorageType_OldValueTrace).save(GetFullNameWithPath_OldValueTrace())
      }
      else
        DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).option("delimiter", "\t").option("emptyValue", "").option("treatEmptyValuesAsNulls", "false").option("nullValue", "null").format(getStorageType_OldValueTrace.toString()).save(getFullNameWithPath_OldValueTrace())
      
    } catch {
      case e: Exception => 
        this.Error_isError = true
        this.Error_Text = s"huemul_Table OldVT Error: write in disk failed for Old Value Trace result. ${e.getMessage}"
        this.Error_Code = 1052
        Result = false
        LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
    }
    
    if (Result) {
      if (CreateInHive ) {
        val sqlDrop01 = s"drop table if exists ${internalGetTable(huemulType_InternalTableType.OldValueTrace)}"
        LocalControl.NewStep("Save: OldVT Result: Drop Hive table Def")
        if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
        try {
          val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(getDataBase(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_DataBase)).collect()
          if (TablesListFromHive.filter { x => x.name.toUpperCase() == internalGetTable(huemulType_InternalTableType.OldValueTrace,false).toUpperCase() }.length > 0) 
            huemulBigDataGov.spark.sql(sqlDrop01)
            
        } catch {
          case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
        }
       
      }
        
      try {
        //create table
        if (CreateInHive ) {
          LocalControl.NewStep("Save: OldVT Result: Create Table in Hive Metadata")
          val lscript = DF_CreateTable_OldValueTrace_Script()       
          huemulBigDataGov.spark.sql(lscript)
        }
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableNameOldValueTrace: String = internalGetTable(huemulType_InternalTableType.OldValueTrace)
        
        LocalControl.NewStep("Save: OldVT Result: Repair Hive Metadata")
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"REFRESH TABLE ${_tableNameOldValueTrace}")
        huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${_tableNameOldValueTrace}")
        
        if (huemulBigDataGov.ImpalaEnabled) {
          LocalControl.NewStep("Save: OldVT Result: refresh Impala Metadata")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"invalidate metadata ${_tableNameOldValueTrace}")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"refresh ${_tableNameOldValueTrace}")
        }
      } catch {
        case e: Exception => 
          
          this.Error_isError = true
          this.Error_Text = s"huemul_Table OldVT Error: create external table OldValueTrace output failed. ${e.getMessage}"
          this.Error_Code = 1053
          Result = false
          LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
      }
    }
      
    return Result
    
  }
  
  /**
   * Save DQ Result data to disk
   */
  def savePersist_DQ(LocalControl: huemul_Control, DF: DataFrame): Boolean = {
    var DF_Final = DF
    var Result: Boolean = true
    this._table_dq_isused = 1
      
    try {      
      LocalControl.NewStep("Save DQ Result: Saving new DQ result")
      //from 2.2 --> add HBase storage (from table definition) --> NOT SUPPORTED
      if (this.getStorageType_DQResult == huemulType_StorageType.HBASE) {
        val huemulDriver = new huemul_TableConnector(huemulBigDataGov, LocalControl)
        huemulDriver.saveToHBase(DF_Final
                                  ,getHBaseNamespace(huemulType_InternalTableType.DQ)
                                  ,getHBaseTableName(huemulType_InternalTableType.DQ)
                                  ,this.getNumPartitions //numPartitions
                                  ,true
                                  ,if (_numPKColumns == 1) _HBase_PKColumn else "hs_rowkey" //PKName
                                  ,getALLDeclaredFields_forHBase(getALLDeclaredFields(false))
                                  )      
      } else {
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: ${getFullNameWithPath_DQ()} ")        
        DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).format(this.getStorageType_DQResult.toString()).partitionBy("dq_control_id").save(getFullNameWithPath_DQ())
      }
      
    } catch {
      case e: Exception => 
        this.Error_isError = true
        this.Error_Text = s"huemul_Table DQ Error: write in disk failed for DQ result. ${e.getMessage}"
        this.Error_Code = 1049
        Result = false
        LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
    }
    
    if (Result) {
      if (CreateInHive ) {
        val sqlDrop01 = s"drop table if exists ${internalGetTable(huemulType_InternalTableType.DQ)}"
        LocalControl.NewStep("Save: Drop Hive table Def")
        if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
        try {
          val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(getDataBase(huemulBigDataGov.GlobalSettings.DQError_DataBase)).collect()
          if (TablesListFromHive.filter { x => x.name.toUpperCase() == internalGetTable(huemulType_InternalTableType.DQ,false).toUpperCase() }.length > 0) 
            huemulBigDataGov.spark.sql(sqlDrop01)
            
        } catch {
          case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
        }
       
      }
        
      try {
        //create table
        if (CreateInHive ) {
          LocalControl.NewStep("Save: Create Table in Hive Metadata")
          val lscript = DF_CreateTable_DQ_Script() 
         
          huemulBigDataGov.spark.sql(lscript)
        }
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableNameDQ: String = internalGetTable(huemulType_InternalTableType.DQ)
        LocalControl.NewStep("Save: Repair Hive Metadata")
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"MSCK REPAIR TABLE ${_tableNameDQ}")
        huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${_tableNameDQ}")
        
        if (huemulBigDataGov.ImpalaEnabled) {
          LocalControl.NewStep("Save: refresh Impala Metadata")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"invalidate metadata ${_tableNameDQ}")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"refresh ${_tableNameDQ}")
        }
      } catch {
        case e: Exception => 
          
          this.Error_isError = true
          this.Error_Text = s"huemul_Table DQ Error: create external table DQ output failed. ${e.getMessage}"
          this.Error_Code = 1050
          Result = false
          LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
      }
    }
      
    return Result
    
  }
  
  /**
   * Return an Empty table with all columns
   */
  def getEmptyTable(Alias: String): DataFrame = {
    val Schema = getSchema()
    val SchemaForEmpty = StructType(Schema.map { x => StructField(x.name, x.dataType, x.nullable) })
    val EmptyRDD = huemulBigDataGov.spark.sparkContext.emptyRDD[Row]
    val EmptyDF = huemulBigDataGov.spark.createDataFrame(EmptyRDD, SchemaForEmpty)
    EmptyDF.createOrReplaceTempView(Alias)
    
    return EmptyDF
  }
  
  def DF_from_SQL(Alias: String, sql: String, SaveInTemp: Boolean = true, NumPartitions: Integer = null) {
    this.DataFramehuemul.DF_from_SQL(Alias, sql, SaveInTemp, NumPartitions) 
  }
  
  
  def DF_from_DF(DFFrom: DataFrame, AliasFrom: String, AliasTo: String, SaveInTemp: Boolean = true) {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
    this.DataFramehuemul.setDataFrame(DFFrom, AliasTo, SaveInTemp)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    
    huemulBigDataGov.DF_SaveLineage(AliasTo
                                 , s"SELECT * FROM ${AliasFrom}" //sql
                                 , dt_start
                                 , dt_end
                                 , Control
                                 , null //FinalTable
                                 , false //isQuery
                                 , true //isReferenced
                                 )
  }
  
  def DF_from_RAW(dataLake_RAW: huemul_DataLake, AliasTo: String) {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
    this.DataFramehuemul.setDataFrame(dataLake_RAW.DataFramehuemul.DataFrame , AliasTo, huemulBigDataGov.DebugMode)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    
    huemulBigDataGov.DF_SaveLineage(AliasTo
                                 , s"SELECT * FROM ${dataLake_RAW.DataFramehuemul.Alias}" //sql
                                 , dt_start
                                 , dt_end
                                 , Control
                                 , null //FinalTable
                                 , false //isQuery
                                 , true //isReferenced
                                 )
  }
  
  
  /**
   * Raise Error
   */
  def raiseError(txt: String, code: Integer) {
    Error_Text = txt
    Error_isError = true
    Error_Code = code
    Control.Control_Error.ControlError_ErrorCode = code
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageError(txt)
    Control.RaiseError(txt)
    //sys.error(txt)
  }  
  
  

}
