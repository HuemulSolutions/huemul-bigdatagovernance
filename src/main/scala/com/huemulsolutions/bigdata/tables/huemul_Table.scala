package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import java.lang.Long

import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification.huemulType_DQNotification

import scala.collection.mutable._
import huemulType_Tables._
import huemulType_StorageType._
import org.apache.log4j
import org.apache.log4j.Level
//import com.huemulsolutions.bigdata._


import com.huemulsolutions.bigdata.dataquality.huemul_DataQuality
import com.huemulsolutions.bigdata.dataquality.huemul_DataQualityResult
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control.huemul_Control
import com.huemulsolutions.bigdata.control.huemulType_Frequency
import com.huemulsolutions.bigdata.control.huemulType_Frequency._

import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification
import com.huemulsolutions.bigdata.dataquality.huemul_DQRecord
import com.huemulsolutions.bigdata.datalake.huemul_DataLake
import com.huemulsolutions.bigdata.tables.huemulType_InternalTableType.huemulType_InternalTableType

//import com.huemulsolutions.bigdata.control.huemulType_Frequency.huemulType_Frequency



class huemul_Table(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_TableDQ with Serializable  {
  @transient private lazy val log = log4j.LogManager.getLogger("com.huemulsolutions")

  if (huemulBigDataGov.DebugMode) log.setLevel(Level.TRACE)  else log.setLevel(Level.INFO)

  if (Control == null) sys.error("Control is null in huemul_DataFrame")
  
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

  private var _PkNotification:huemulType_DQNotification = huemulType_DQNotification.ERROR

  /** Set Primary Key notification level: ERROR(default),WARNING, WARNING_EXCLUDE
   * @author  christian.sattler@gmail.com; update: sebas_rod@hotmail.com
   * @since   2.[6]
   * @group   column_attribute
   *
   * @param value   huemulType_DQNotification ERROR,WARNING, WARNING_EXCLUDE
   * @return        huemul_Columns
   * @note 2020-06-17 by sebas_rod@hotmail.com: add huemul_Table as return
   */
  def setPkNotification(value: huemulType_DQNotification ): huemul_Table = {
    _PkNotification = value
    this
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
  
  
  private var _partitionFieldValueTemp: String = ""
  /**
    Fields used to partition table 
   */
  @deprecated("this method will be removed, instead use huemul_Columns.setPartitionColumn(position: Integer, dropBeforeInsert: Boolean)", "3.0")
  def setPartitionField(value: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of PartitionField, definition is close", 1033)
    else {
      _partitionFieldValueTemp = value
    }
  }
  
  //from 2.6 --> return array with partitionColumns defined (ordered)
  def getPartitionList: Array[huemul_Columns] = {
     val partListOrder = getALLDeclaredFields(true).filter { x => x.setAccessible(true) 
                                                     x.get(this).isInstanceOf[huemul_Columns] && 
                                                     x.get(this).asInstanceOf[huemul_Columns].getPartitionColumnPosition >= 1
                                             }.sortBy { x => x.setAccessible(true)
                                                             x.get(this).asInstanceOf[huemul_Columns].getPartitionColumnPosition}
                                             
     //val b2 = partListOrder.ma
     //                                        }.map { x => x.setAccessible(true) 
     //                                                        x.get(this).asInstanceOf[huemul_Columns]}
     val result = partListOrder.map { x => x.setAccessible(true) 
       x.get(this).asInstanceOf[huemul_Columns]  
     }                                         
    
                                         
    return result
     
     //return getColumns().filter { x => x.getPartitionColumnPosition >= 1 }
     //                                     .sortBy { x => x.getPartitionColumnPosition }
     
  }
  
  //from 2.6 --> change code, get results from huemul_Columns.getPartitionColumnPosition
  def getPartitionField: String = {
     return getPartitionList.map { x => x.get_MyName(this.getStorageType) }.mkString(",")
     
  }

  //from 2.6 --> method to save data
  private def getPartitionListForSave: Array[String] = {
    return getPartitionList.map { x => x.get_MyName(this.getStorageType) }

  }
  
  //From 2.6 --> drop this attribute
  //private var _PartitionField   : String= null
  
  /**
    Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  def setLocalPath(value: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of LocalPath, definition is close", 1033)
    else
      _LocalPath = value
  }
  private var _LocalPath   : String= ""
  def getLocalPath: String = {return if (_LocalPath == null) "null/"
                                     else if (_LocalPath.takeRight(1) != "/") _LocalPath.concat("/")
                                     else _LocalPath }
  
  
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
  
  //from 2.3 --> #85 add new options to determine whether create External Table
  //var createExternalTable_Normal: Boolean = true
  //var createExternalTable_DQ: Boolean = true
  //var createExternalTable_OVT: Boolean = true
  
  
  /**
   * Automatically map query names
   */
  def setMappingAuto() {
    getALLDeclaredFields(true).foreach { x =>  
      x.setAccessible(true)
          
      //Nombre de campos
      if (x.get(this).isInstanceOf[huemul_Columns]) {
        val DataField = x.get(this).asInstanceOf[huemul_Columns]
        DataField.SetMapping(DataField.get_MyName(this.getStorageType))
      }
    }
  }
    
  
  /**
   DataQuality: max N° records, null does'nt apply  DQ , 0 value doesn't accept new records (raiseError if new record found)
   If table is empty, DQ doesn't apply
   */
  def setDQ_MaxNewRecords_Num(value: java.lang.Long) {
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
  
  //FROM 2.5 
  //ADD AVRO SUPPORT
  private var _avro_format: String = huemulBigDataGov.GlobalSettings.getAVRO_format()
  def getAVRO_format(): String = {return  _avro_format}
  def setAVRO_format(value: String) {_avro_format = value} 
  
  /*
  private var _Tablecodec_compression: String = null
  def getTableCodec_compression(): String = {return  _Tablecodec_compression}
  def setTableCodec_compression(value: String) {_Tablecodec_compression = value} 
  */
  
  private def _getSaveFormat(storageType: huemulType_StorageType): String = {
    return if (storageType == huemulType_StorageType.AVRO)
      this.getAVRO_format()
    else 
      storageType.toString()
     
  }
  
  /****** METODOS DEL LADO DEL "USUARIO" **************************/
  
  private var autoCast: Boolean = true
  def setAutoCast(value: Boolean) {autoCast = value}
  
  private var ApplyDistinct: Boolean = true
  def setApplyDistinct(value: Boolean) {ApplyDistinct = value}
  
  
  private var Table_id: String = ""
  
  
  
  //private var CreateInHive: Boolean = true
  private var CreateTableScript: String = ""
  private var PartitionValue: ArrayBuffer[String] = new ArrayBuffer[String]  //column,value
  def getPartitionValue(): String = {return if (PartitionValue.length > 0) PartitionValue(0) else ""}
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
  
  val mdm_columnname = new huemul_Columns (StringType, true, "Column Name", false).setHBaseCatalogMapping("loginfo")
  
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
    return globalPath + getLocalPath + TableName
  }
  
  /**
   * Get Fullpath hdfs for DQ results = GlobalPaths + DQError_Path + TableName + "_dq"
   */
  def getFullNameWithPath_DQ() : String = {
    return this.getPath(huemulBigDataGov.GlobalSettings.DQError_Path) + this.getDataBase(this._DataBase) + '/' + getLocalPath + TableName + "_dq"
  }
  
  /**
   * Get Fullpath hdfs for _oldvalue results = GlobalPaths + DQError_Path + TableName + "_oldvalue"
   */
  def getFullNameWithPath_OldValueTrace() : String = {
    return this.getPath(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_Path) + this.getDataBase(this._DataBase) + '/' + getLocalPath + TableName + "_oldvalue"
  }
  
   /**
   * Get Fullpath hdfs for backpu  = Backup_Path + database + TableName + "_backup"
   */
  def getFullNameWithPath_Backup(control_id: String) : String = {
    return this.getPath(huemulBigDataGov.GlobalSettings.MDM_Backup_Path) + this.getDataBase(this._DataBase) + '/' + getLocalPath + 'c' + control_id + '/' + TableName + "_backup"
  }
  
  def getFullNameWithPath2(ManualEnvironment: String) : String = {
    return globalPath(ManualEnvironment) + getLocalPath + TableName
  }
    
  def getFullPath() : String = {
    return globalPath + getLocalPath 
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

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    
    //create fields
    var fields: String = s""""${if (_numPKColumns == 1) _HBase_PKColumn else "hs_rowkey"}":{"cf":"rowkey","col":"key","type":"string"} """
    getALLDeclaredFields().filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] 
    } foreach { x =>
      x.setAccessible(true)
      var dataField = x.get(this).asInstanceOf[huemul_Columns]
      val colMyName = dataField.get_MyName(this.getStorageType)
      
      if (!(dataField.getIsPK && _numPKColumns == 1)) {        
        //fields = fields + s""", \n "${x.getName}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}","type":"${dataField.getHBaseDataType()}"} """
        fields = fields + s""", \n "${colMyName}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}","type":"${dataField.getHBaseDataType()}"} """
      
        if (tableType != huemulType_InternalTableType.OldValueTrace) {
          if (dataField.getMDM_EnableOldValue)
            fields = fields + s""", \n "${colMyName}${__old}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}${__old}","type":"${dataField.getHBaseDataType()}"} """
          if (dataField.getMDM_EnableDTLog)
            fields = fields + s""", \n "${colMyName}${__fhChange}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}${__fhChange}","type":"string"} """
          if (dataField.getMDM_EnableProcessLog)
            fields = fields + s""", \n "${colMyName}${__ProcessLog}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}${__ProcessLog}","type":"string"} """
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
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    
   //WARNING!!! Any changes you make to this code, repeat it in getColumns_CreateTable
    //create fields
    var fields: String = s":key"
    getALLDeclaredFields().filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] 
    } foreach { x =>
      x.setAccessible(true)
      var dataField = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = dataField.get_MyName(this.getStorageType).toLowerCase()
      val _dataType  = dataField.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
      
      if (!(dataField.getIsPK && _numPKColumns == 1) && (_colMyName != "hs_rowKey".toLowerCase())) {        
        
        if (tableType == huemulType_InternalTableType.DQ) {
          //create StructType
          if ("dq_control_id".toLowerCase() != _colMyName) {
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}#b"""
          }
        }
        else if (tableType == huemulType_InternalTableType.OldValueTrace) {
          //create StructType mdm_columnname
          if ("mdm_columnname".toLowerCase() != _colMyName) {
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}#b"""
          }
        }
        else {
          //create StructType
          fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}${if (_dataType == TimestampType || _dataType == DateType || _dataType == DecimalType || _dataType.typeName.toLowerCase().contains("decimal")) "" else "#b"}"""
          
        }
        
        if (tableType != huemulType_InternalTableType.OldValueTrace) {
          if (dataField.getMDM_EnableOldValue)
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}${__old}${if (_dataType == TimestampType || _dataType == DateType || _dataType == DecimalType || _dataType.typeName.toLowerCase().contains("decimal") ) "" else "#b"}"""
          if (dataField.getMDM_EnableDTLog) 
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}${__fhChange}"""
          if (dataField.getMDM_EnableProcessLog)
            fields = fields + s""",${dataField.getHBaseCatalogFamily()}:${dataField.getHBaseCatalogColumn()}${__ProcessLog}#b"""
        }
        
        
      }
    }
    
    
    return fields
  }
 
  
  //get PK for HBase Tables rowKey 
  private var _HBase_rowKeyCalc: String = ""
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
    
    //if (this.getPartitionField == null)
    //  _PartitionField = ""
      
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
      
    if (_partitionFieldValueTemp != null && _partitionFieldValueTemp != "") {
      val partitionCol2 =  getALLDeclaredFields().filter { x => x.setAccessible(true) 
                                                               x.get(this).isInstanceOf[huemul_Columns] && x.getName.toUpperCase() == _partitionFieldValueTemp.toUpperCase() }
    
     
      if (partitionCol2.length == 1) {
        partitionCol2(0).setAccessible(true)
        val DataField = partitionCol2(0).get(this).asInstanceOf[huemul_Columns]
        DataField.setPartitionColumn(1, true, true)
      } else 
        this.raiseError(s"Partition Column name '${_partitionFieldValueTemp}' not found", 1064)
    }
    
      /*
    val partitionCol =  getALLDeclaredFields().filter { x => x.setAccessible(true) 
                                                             x.get(this).isInstanceOf[huemul_Columns] }
    &&
                                                             x.getName.toUpperCase() == _partitionFieldValueTemp.toUpperCase()
      //getColumns().filter { x => x.get_MyName(this.getStorageType).toUpperCase == value.toUpperCase  }
    if (partitionCol.length == 1) {
          //from 2.6 --> add column to partitionColumn list
          partitionCol(0).setPartitionColumn(1, true)
        } else {
          this.raiseError(s"Partition Column name '$value' not found", 1064)
        }
      }
    }
    * 
    */
        
      
    if (this._TableType == null)
      raiseError(s"huemul_Table Error: TableType must be defined",1034)
    
    //from 2.6 --> allow partitionColumns in all tables, this check was disabled
    //else if (this._TableType != huemulType_Tables.Transaction && getPartitionField != "")
    //  raiseError(s"huemul_Table Error: PartitionField shouldn't be defined if TableType is ${this._TableType}",1036)
      
    //from 2.2 --> validate tableType with Format
    if (this._TableType == huemulType_Tables.Transaction && !(this.getStorageType == huemulType_StorageType.PARQUET || 
                                                              this.getStorageType == huemulType_StorageType.ORC || 
                                                              this.getStorageType == huemulType_StorageType.DELTA ||
                                                              this.getStorageType == huemulType_StorageType.AVRO
                                                              ))
      raiseError(s"huemul_Table Error: Transaction Tables only available with PARQUET, DELTA, AVRO or ORC StorageType ",1057)
      
      
    //Fron 2.2 --> validate tableType HBASE and turn on globalSettings
    if (this.getStorageType == huemulType_StorageType.HBASE && !huemulBigDataGov.GlobalSettings.getHBase_available)
      raiseError(s"huemul_Table Error: StorageType is set to HBASE, requires invoke HBase_available in globalSettings  ",1058)
      
    if (this._DataBase == null)
      raiseError(s"huemul_Table Error: DataBase must be defined",1037)
    if (this._Frequency == null)
      raiseError(s"huemul_Table Error: Frequency must be defined",1047)
      
    if (this.getSaveBackup == true && this.getTableType == huemulType_Tables.Transaction)
      raiseError(s"huemul_Table Error: SaveBackup can't be true for transactional tables",1054)
        
      
    //var PartitionFieldValid: Boolean = false
    var comaPKConcat = ""
    
    _numPKColumns = 0
    _HBase_rowKeyCalc = ""
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
        val _colMyName = DataField.get_MyName(this.getStorageType)
        //from 2.6 --> disabled this code, #98
        //if (this.getTableType == huemulType_Tables.Transaction && _colMyName.toLowerCase() == this.getPartitionField.toLowerCase())
        //  PartitionFieldValid = true
          
        //from 2.2 --> get concatenaded key for HBase
        if (DataField.getIsPK && getStorageType == huemulType_StorageType.HBASE) {
          _HBase_rowKeyCalc += s"${comaPKConcat}'[', ${if (DataField.DataType == StringType) _colMyName else s"CAST(${_colMyName} AS STRING)" },']'"
          comaPKConcat = ","
          _HBase_PKColumn = _colMyName
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
    
    //add OVT
    getALLDeclaredFields(false,false,huemulType_InternalTableType.OldValueTrace).filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] } foreach { x =>
      x.setAccessible(true)
          
      val DataField = x.get(this).asInstanceOf[huemul_Columns]
      DataField.Set_MyName(x.getName)
      DataField.SetDefinitionIsClose()
    }
                
    //add DQ and OVT
    getALLDeclaredFields(false,false,huemulType_InternalTableType.DQ).filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] } foreach { x =>
      x.setAccessible(true)
          
      val DataField = x.get(this).asInstanceOf[huemul_Columns]
      DataField.Set_MyName(x.getName)
      DataField.SetDefinitionIsClose()
    }
    
    
    //from 2.2 --> set _HBase_rowKey for hBase Tables
    if (getStorageType == huemulType_StorageType.HBASE) {
      if (_numPKColumns == 1)
        _HBase_rowKeyCalc = _HBase_PKColumn
      else 
        _HBase_rowKeyCalc = s"concat(${_HBase_rowKeyCalc})"
    }
    
    if (this._TableType == huemulType_Tables.Transaction && getPartitionField == "")
      raiseError(s"huemul_Table Error: Partitions should be defined if TableType is Transactional, use column.setPartitionColumn",1035)
    
    
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: register metadata")
    //Register TableName and fields
    Control.RegisterMASTER_CREATE_Basic(this)
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: end ApplyTableDefinition")
    if (!HasPK) this.raiseError("huemul_Table Error: PK not defined", 1017)
    //from 2.6 --> disabled this code, is controlled before
    //if (this.getTableType == huemulType_Tables.Transaction && !PartitionFieldValid)
    //  raiseError(s"huemul_Table Error: PartitionField should be defined if TableType is Transactional (invalid name ${this.getPartitionField} )",1035)
      
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
        b = b.filter { x => x.getName == "mdm_columnname" || x.getName == "MDM_newValue" || x.getName == "MDM_oldValue" || x.getName == "MDM_AutoInc" ||
                            x.getName == "MDM_fhChange" || x.getName == "MDM_ProcessChange" || x.getName == "processExec_id"  }
      } else {
        //exclude OldValuestrace columns
        b = b.filter { x => x.getName != "mdm_columnname" && x.getName != "MDM_newValue" && x.getName != "MDM_oldValue" && x.getName != "MDM_AutoInc" && x.getName != "processExec_id"  }
        
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
        //from 2.5 --> add Set_MyName
        d.foreach { x => 
          x.setAccessible(true) 
          val DataField = x.get(this).asInstanceOf[huemul_Columns]
          DataField.Set_MyName(x.getName)
        }
        
        c = d.union(c)
    } 
    
    if (PartitionColumnToEnd) {
        //from 2.6 --> old logic
        //val partitionlast = c.filter { x => x.getName.toLowerCase() == DataField.getm this.getPartitionField.toLowerCase() }
        //val rest_array = c.filter { x => x.getName.toLowerCase() != this.getPartitionField.toLowerCase() }
        //c = rest_array.union(partitionlast)

      
      
      //from 2.6 --> change logic
      //exclude partitioned columns
      val rest_array = c.filter { x => x.setAccessible(true) 
                                     x.get(this).isInstanceOf[huemul_Columns] &&
                                     x.get(this).asInstanceOf[huemul_Columns].getPartitionColumnPosition == 0}
      
      //from 2.6
      //get partitioned columns ordered by getPartitionColumnPosition
      val partitionlast = pClass.getDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] &&
                                      x.get(this).asInstanceOf[huemul_Columns].getPartitionColumnPosition >= 1 }
                                .sortBy { x => x.setAccessible(true)
                                          x.get(this).asInstanceOf[huemul_Columns].getPartitionColumnPosition }
        
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
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
      (x,Field.getHBaseCatalogFamily(), Field.getHBaseCatalogColumn(), _dataType)
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
    
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    
    var i:Integer = 0
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.get_MyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
      //create StructType
      fieldsStruct.append( StructField(_colMyName, _dataType , nullable = Field.getNullable , null))

      if (Field.getMDM_EnableOldValue) {
        fieldsStruct.append( StructField(_colMyName.concat(__old), _dataType , nullable = true , null))
      }
      if (Field.getMDM_EnableDTLog) {
        fieldsStruct.append( StructField(_colMyName.concat(__fhChange), TimestampType , nullable = true , null))
      }
      if (Field.getMDM_EnableProcessLog) {
        fieldsStruct.append( StructField(_colMyName.concat(__ProcessLog), StringType , nullable = true , null))
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
    
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    
    var ColumnsCreateTable : String = ""
    var coma: String = ""
    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.get_MyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
      //Field.Set_MyName(x.getName)
      Result.append(Field)
                      
      
      if (Field.getMDM_EnableOldValue) {
        val MDM_EnableOldValue = new huemul_Columns(_dataType, false, s"Old value for ${_colMyName}")
        MDM_EnableOldValue.Set_MyName(s"${_colMyName}${__old}")
        Result.append(MDM_EnableOldValue)        
      } 
      if (Field.getMDM_EnableDTLog){
        val MDM_EnableDTLog = new huemul_Columns(TimestampType, false, s"Last change DT for ${_colMyName}")
        MDM_EnableDTLog.Set_MyName(s"${_colMyName}${__fhChange}")
        Result.append(MDM_EnableDTLog)                
      } 
      if (Field.getMDM_EnableProcessLog) {
        val MDM_EnableProcessLog = new huemul_Columns(StringType, false, s"System Name change for ${_colMyName}")
        MDM_EnableProcessLog.Set_MyName(s"${_colMyName}${__ProcessLog}")
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
    val partitionColumnToEnd = if (getStorageType == huemulType_StorageType.HBASE) false else true
    val fieldList = getALLDeclaredFields(false,partitionColumnToEnd,tableType)
    val NumFields = fieldList.filter { x => x.setAccessible(true)
      x.get(this).isInstanceOf[huemul_Columns] }.length
    
    //set name according to getStorageType (AVRO IS LowerCase)
    val __storageType =  if (tableType == huemulType_InternalTableType.Normal) this.getStorageType
                    else if (tableType == huemulType_InternalTableType.DQ) this.getStorageType_DQResult 
                    else if (tableType == huemulType_InternalTableType.OldValueTrace) this.getStorageType_OldValueTrace
                    else this.getStorageType      
                    
    val __old = huemulBigDataGov.getCaseType( __storageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( __storageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( __storageType, "_ProcessLog")
    
    var ColumnsCreateTable : String = ""
    var coma: String = ""
    
    //for HBase, add hs_rowKey as Key column
    if (getStorageType == huemulType_StorageType.HBASE && _numPKColumns > 1) {
      fieldList.filter { x => x.getName == "hs_rowKey" }.foreach { x =>
        var Field = x.get(this).asInstanceOf[huemul_Columns]
        val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), __storageType)
        var DataTypeLocal = _dataType.sql
      
        ColumnsCreateTable += s"$coma${x.getName} ${DataTypeLocal} \n"
        coma = ","
      }
    }                                      
    
    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] && x.getName != "hs_rowKey" }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), __storageType)
      var DataTypeLocal = _dataType.sql
      //from 2.5 --> replace DECIMAL to STRING when storage type = AVRO. this happen because AVRO outside DATABRICKS doesn't support DECIMAL, DATE AND TIMESTAMP TYPE.
      if (    huemulBigDataGov.GlobalSettings.getBigDataProvider() != huemulType_bigDataProvider.databricks 
          && __storageType == huemulType_StorageType.AVRO 
          && (  DataTypeLocal.toUpperCase().contains("DECIMAL") || 
                DataTypeLocal.toUpperCase().contains("DATE") || 
                DataTypeLocal.toUpperCase().contains("TIMESTAMP")) 
          ) {
        DataTypeLocal = StringType.sql
      }
      val _colMyName = Field.get_MyName(__storageType)
      
      if (tableType == huemulType_InternalTableType.DQ) {
        //create StructType
        //FROM 2.4 --> INCLUDE PARTITIONED COLUMN IN CREATE TABLE ONLY FOR databricks COMPATIBILITY
        if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) { 
          ColumnsCreateTable += s"$coma${_colMyName} ${DataTypeLocal} \n"
          coma = ","
        } else if ("dq_control_id".toLowerCase() != _colMyName.toLowerCase()) {
          ColumnsCreateTable += s"$coma${_colMyName} ${DataTypeLocal} \n"
          coma = ","
        }
      }
      else if (tableType == huemulType_InternalTableType.OldValueTrace) {
        //create StructType mdm_columnname
        //FROM 2.4 --> INCLUDE PARTITIONED COLUMN IN CREATE TABLE ONLY FOR databricks COMPATIBILITY
        if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
          ColumnsCreateTable += s"$coma${_colMyName} ${DataTypeLocal} \n"
          coma = ","
        } else if ("mdm_columnname".toLowerCase() != _colMyName.toLowerCase()) {
          ColumnsCreateTable += s"$coma${_colMyName} ${DataTypeLocal} \n"
          coma = ","
        }
      }
      else {
        //create StructType
        //if (getPartitionField != null && getPartitionField.toLowerCase() != _colMyName.toLowerCase()) {
        if (Field.getPartitionColumnPosition == 0 || getStorageType == huemulType_StorageType.HBASE) {
          ColumnsCreateTable += s"$coma${_colMyName} ${DataTypeLocal} \n"
          coma = ","
        } else if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
          //from 2.4 --> add partitioned field
          ColumnsCreateTable += s"$coma${_colMyName} ${DataTypeLocal} \n"
          coma = ","
        }
      }
      
      if (tableType != huemulType_InternalTableType.OldValueTrace) {
        if (Field.getMDM_EnableOldValue)
          ColumnsCreateTable += s"$coma${_colMyName}${__old} ${DataTypeLocal} \n"  
        if (Field.getMDM_EnableDTLog) 
          ColumnsCreateTable += s"$coma${_colMyName}${__fhChange} ${TimestampType.sql} \n"  
        if (Field.getMDM_EnableProcessLog) 
          ColumnsCreateTable += s"$coma${_colMyName}${__ProcessLog} ${StringType.sql} \n"
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
      val _colMyName = Field.get_MyName(this.getStorageType)
      
      //All PK columns shouldn't be null
      Field.setNullable(false)
      
      if (!huemulBigDataGov.HasName(Field.get_MappedName()))
        sys.error(s"field ${_colMyName} doesn't have an assigned name in 'name' attribute")
      StringSQL += coma + _colMyName
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
    
    //var StringSQL_partition: String = ""
    var StringSQL_hash: String = "sha2(concat( "
    var coma_hash: String = ""
    var coma: String = ""    
    
    val partitionList : ArrayBuffer[huemul_Columns] = new ArrayBuffer[huemul_Columns]
    
    getALLDeclaredFields(false,true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.get_MyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)

      val NewColumnCast = ApplyAutoCast(if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"${Field.get_SQLForInsert()} " else s"New.${Field.get_MappedName()}"
                                        ,_dataType.sql)
        
     
      //New value (from field or compute column )
      if (huemulBigDataGov.HasName(Field.get_MappedName()) ){
        if (Field.getPartitionColumnPosition == 0){
          StringSQL += s"${coma}${NewColumnCast} as ${_colMyName} \n"
          coma = ","
        } else {
          partitionList.append(Field)
        }
        /*
        if (_colMyName.toLowerCase() == this.getPartitionField.toLowerCase())
          StringSQL_partition += s",${NewColumnCast} as ${_colMyName} \n"
        else {
          StringSQL += s"${coma}${NewColumnCast} as ${_colMyName} \n"
          coma = ","
        }
        * 
        */
      } else {
        if (_colMyName.toLowerCase() == "MDM_fhNew".toLowerCase()) {
          //if (_colMyName.toLowerCase() == this.getPartitionField.toLowerCase())
          //  StringSQL_partition += s"${coma}CAST(now() AS ${_dataType.sql} ) as ${_colMyName} \n"
          //else {   
          StringSQL += s",CAST(now() AS ${_dataType.sql} ) as ${_colMyName} \n"
          coma = ","
          //}
            
        } else if (_colMyName.toLowerCase() == "MDM_ProcessNew".toLowerCase()) {
          StringSQL += s"${coma}CAST('${processName}' AS ${_dataType.sql} ) as ${_colMyName} \n"
          coma = ","
        }
      }
          
       
      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""${coma_hash}${if (Field.getNullable) s"coalesce(${NewColumnCast},'null')" else NewColumnCast}"""
        coma_hash = ","
      }
      
      
    }
    
    StringSQL_hash += "),256) "
    
    //set name according to getStorageType (AVRO IS LowerCase)
    val __MDM_hash = huemulBigDataGov.getCaseType( this.getStorageType, "MDM_hash")
    
    //from 2.6 --> create partitions sql (replace commented text)
    val StringSQL_partition = partitionList.sortBy { x => x.getPartitionColumnPosition}.map{x =>
      val _colMyName = x.get_MyName(this.getStorageType)
      val _dataType  = x.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)

      val NewColumnCast = ApplyAutoCast(if (huemulBigDataGov.HasName(x.get_SQLForInsert())) s"${x.get_SQLForInsert()} " else s"New.${x.get_MappedName()}"
                                        ,_dataType.sql)

      val result = s"${NewColumnCast} as ${_colMyName} \n"

      result
    }.mkString(",")
    
    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL = s"""SELECT ${StringSQL}
                    ,${StringSQL_hash} as ${__MDM_hash}
                    ,${StringSQL_partition}
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
    
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
       
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
      val _colMyName = Field.get_MyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
      
      val NewColumnCast = ApplyAutoCast(s"New.${Field.get_MappedName()}",_dataType.sql)
      //string for key on
      if (Field.getIsPK){
        val NewPKSentence = if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"CAST(${Field.get_SQLForInsert()} as ${_dataType.sql} )" else NewColumnCast
        StringSQL_LeftJoin += s"${coma}Old.${_colMyName} AS ${_colMyName} \n"
        StringSQl_PK += s" $sand Old.${_colMyName} = ${NewPKSentence}  " 
        sand = " and "

        if (StringSQL_ActionType == "")
          StringSQL_ActionType = s" case when Old.${_colMyName} is null then 'NEW' when ${NewColumnCast} is null then 'EQUAL' else 'UPDATE' END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = !(OfficialColumns.filter { y => y.name.toLowerCase() == _colMyName.toLowerCase()}.length == 0)
        
        //String for new DataFrame fulljoin
        if (columnExist){
          StringSQL_LeftJoin += s"${coma}cast(old.${_colMyName} as ${_dataType.sql} ) as old_${_colMyName} \n"
          //StringSQL_LeftJoin += s"${coma}old.${_colMyName} as old_${_colMyName} \n"
        }
        else
          StringSQL_LeftJoin += s"${coma}cast(null as ${_dataType.sql} ) as old_${_colMyName} \n"
        
        //New value (from field or compute column )
        if (huemulBigDataGov.HasName(Field.get_MappedName()))
          StringSQL_LeftJoin += s",${NewColumnCast} as new_${_colMyName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForInsert()))
          StringSQL_LeftJoin += s",CAST(${Field.get_SQLForInsert()} as ${_dataType.sql} ) as new_insert_${_colMyName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForUpdate()))
          StringSQL_LeftJoin += s",CAST(${Field.get_SQLForUpdate()} as ${_dataType.sql} ) as new_update_${_colMyName} \n"
          
        if (Field.getMDM_EnableOldValue || Field.getMDM_EnableDTLog || Field.getMDM_EnableProcessLog || Field.getMDM_EnableOldValue_FullTrace) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.HasName(Field.get_MappedName())) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) Field.get_SQLForUpdate() else "new.".concat(Field.get_MappedName()),_dataType.sql)
            var OldFieldTXT = if (columnExist) "old.".concat(_colMyName) else "null"
            //from 2.5 --> add cast to avro timestamp dataType
            //if (this.getStorageType == huemulType_StorageType.AVRO && (Field._DataType.sql.toUpperCase().contains("TIMESTAMP") || Field._DataType.sql.toUpperCase().contains("DATE")))
            //  OldFieldTXT =   ApplyAutoCast(OldFieldTXT,Field._DataType.sql)
            StringSQL_LeftJoin += s",CAST(CASE WHEN ${NewFieldTXT} = ${OldFieldTXT} or (${NewFieldTXT} is null and ${OldFieldTXT} is null) THEN 0 ELSE 1 END as Integer ) as __Change_${_colMyName}  \n"
          }
          else {
            StringSQL_LeftJoin += s",CAST(0 as Integer ) as __Change_${_colMyName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae, 
            //la solución es poner el campo "tiene cambio" en falso
          }
              
        }
      
        if (Field.getMDM_EnableOldValue){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${_colMyName}${__old}".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${_colMyName}${__old} AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
        }
        if (Field.getMDM_EnableDTLog){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${_colMyName}${__fhChange}".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${_colMyName}${__fhChange} AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
        }
        if (Field.getMDM_EnableProcessLog){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${_colMyName}${__ProcessLog}".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS STRING) as old_${_colMyName}${__ProcessLog} \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${_colMyName}${__ProcessLog} AS STRING) as old_${_colMyName}${__ProcessLog} \n"
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
  SCRIPT FOR OLD VALUE TRACE INSERT
   */
  private def OldValueTrace_save(Alias: String, ProcessName: String, LocalControl: huemul_Control) = {
       
    //set name according to getStorageType (AVRO IS LowerCase)
    val __MDM_newValue = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, "MDM_newValue")
    val __MDM_oldValue = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, "MDM_oldValue")
    val __MDM_AutoInc = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, "MDM_AutoInc")
    val __MDM_ProcessChange = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, "MDM_ProcessChange")
    val __mdm_columnname = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, "mdm_columnname").toLowerCase() //because it's partitioned column
    val __MDM_fhChange = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, "MDM_fhChange")
    val __processExec_id = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, "processExec_id")
    
    //Get PK
    var StringSQl_PK_base: String = "SELECT "
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[huemul_Columns] &&
                                         x.get(this).asInstanceOf[huemul_Columns].getIsPK }
    .foreach { x =>
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      StringSQl_PK_base += s" ${coma}${Field.get_MyName(this.getStorageType_OldValueTrace)}"
      coma = ","
    }
    
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] && 
                                      x.get(this).asInstanceOf[huemul_Columns].getMDM_EnableOldValue_FullTrace }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.get_MyName(this.getStorageType_OldValueTrace)
      val _dataType_MDM_fhChange = MDM_fhChange.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType_OldValueTrace)
      var __MDM_fhChange_cast: String = if (_dataType_MDM_fhChange == MDM_fhChange.DataType) s"now() as ${__MDM_fhChange}" else s"CAST(now() as STRING) as ${__MDM_fhChange}"
      val StringSQL =  s"${StringSQl_PK_base}, CAST(new_${_colMyName} as string) AS ${__MDM_newValue}, CAST(old_${_colMyName} as string) AS ${__MDM_oldValue}, CAST(${_MDM_AutoInc} AS BIGINT) as ${__MDM_AutoInc}, cast('${Control.Control_Id}' as string) as ${__processExec_id}, ${__MDM_fhChange_cast}, cast('$ProcessName' as string) as ${__MDM_ProcessChange}, cast('${_colMyName.toLowerCase()}' as string) as ${__mdm_columnname} FROM $Alias WHERE ___ActionType__ = 'UPDATE' and __Change_${_colMyName} = 1 "
      val aliasFullTrace: String = s"__SQL_ovt_full_${_colMyName}"
      
      val tempSQL_OldValueFullTrace_DF = huemulBigDataGov.DF_ExecuteQuery(aliasFullTrace,StringSQL) 
      
      val numRowsAffected = tempSQL_OldValueFullTrace_DF.count()
      if (numRowsAffected > 0) {
        val Result = savePersist_OldValueTrace(LocalControl,tempSQL_OldValueFullTrace_DF)
        if (!Result)
            huemulBigDataGov.logMessageWarn(s"Old value trace full trace can't save to disk, column ${_colMyName}")
      }
      
      LocalControl.NewStep(s"Ref & Master: ovt full trace finished, ${numRowsAffected} rows changed for ${_colMyName} column ")
    }
  }

  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step1_FullJoin(OfficialAlias: String, NewAlias: String, isUpdate: Boolean, isDelete: Boolean): String = {
    //Get fields in old table
    val OfficialColumns = huemulBigDataGov.spark.catalog.listColumns(OfficialAlias).collect()
    
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
       
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
      val _colMyName = Field.get_MyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
      
      val NewColumnCast = ApplyAutoCast(s"New.${Field.get_MappedName()}",_dataType.sql)
      //string for key on
      if (Field.getIsPK){
        val NewPKSentence = if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"CAST(${Field.get_SQLForInsert()} as ${_dataType.sql} )" else NewColumnCast
        StringSQL_FullJoin += s"${coma}CAST(coalesce(Old.${_colMyName}, ${NewPKSentence}) as ${_dataType.sql}) AS ${_colMyName} \n"
        StringSQl_PK += s" $sand Old.${_colMyName} = ${NewPKSentence}  " 
        sand = " and "

        if (StringSQL_ActionType == "")
          StringSQL_ActionType = s" case when Old.${_colMyName} is null then 'NEW' when ${NewColumnCast} is null then ${if (isDelete) "'DELETE'" else "'EQUAL'"} else ${if (isUpdate) "'UPDATE'" else "'EQUAL'"} END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = !(OfficialColumns.filter { y => y.name.toLowerCase() == _colMyName.toLowerCase()}.length == 0)
        
        //String for new DataFrame fulljoin
        if (columnExist)
          StringSQL_FullJoin += s"${coma}cast(old.${_colMyName} as ${_dataType.sql} ) as old_${_colMyName} \n"
        else
          StringSQL_FullJoin += s"${coma}cast(null as ${_dataType.sql} ) as old_${_colMyName} \n"
        
        //New value (from field or compute column )
        if (huemulBigDataGov.HasName(Field.get_MappedName()))
          StringSQL_FullJoin += s",${NewColumnCast} as new_${_colMyName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForInsert()))
          StringSQL_FullJoin += s",CAST(${Field.get_SQLForInsert()} as ${_dataType.sql} ) as new_insert_${_colMyName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForUpdate()))
          StringSQL_FullJoin += s",CAST(${Field.get_SQLForUpdate()} as ${_dataType.sql} ) as new_update_${_colMyName} \n"
          
        if (Field.getMDM_EnableOldValue || Field.getMDM_EnableDTLog || Field.getMDM_EnableProcessLog || Field.getMDM_EnableOldValue_FullTrace) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.HasName(Field.get_MappedName())) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) Field.get_SQLForUpdate() else "new.".concat(Field.get_MappedName()),_dataType.sql)
            var OldFieldTXT = if (columnExist) "old.".concat(_colMyName) else "null"
            //from 2.5 --> add cast to avro timestamp dataType
            //if (this.getStorageType == huemulType_StorageType.AVRO && (Field._DataType.sql.toUpperCase().contains("TIMESTAMP") || Field._DataType.sql.toUpperCase().contains("DATE")) )
            //  OldFieldTXT =   ApplyAutoCast(OldFieldTXT,Field._DataType.sql)
            StringSQL_FullJoin += s",CAST(CASE WHEN ${NewFieldTXT} = ${OldFieldTXT} or (${NewFieldTXT} is null and ${OldFieldTXT} is null) THEN 0 ELSE 1 END as Integer ) as __Change_${_colMyName}  \n"
          }
          else {
            StringSQL_FullJoin += s",CAST(0 as Integer ) as __Change_${_colMyName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae, 
            //la solución es poner el campo "tiene cambio" en falso
          }
              
        }
      
        if (Field.getMDM_EnableOldValue){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${_colMyName}${__old}".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${_colMyName}${__old} AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
        }
        if (Field.getMDM_EnableDTLog){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${_colMyName}${__fhChange}".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${_colMyName}${__fhChange} AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
        }
        if (Field.getMDM_EnableProcessLog){
          if (OfficialColumns.filter { y => y.name.toLowerCase() == s"${_colMyName}${__ProcessLog}".toLowerCase()}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS STRING) as old_${_colMyName}${__ProcessLog} \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${_colMyName}${__ProcessLog} AS STRING) as old_${_colMyName}${__ProcessLog} \n"
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
    
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    
    var StringSQL: String = "SELECT "
    
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.get_MyName(this.getStorageType)
      
      //string for key on
      if (Field.getIsPK){
        StringSQL += s" ${coma}${_colMyName} as ${_colMyName} \n"
        
      } else {
        if (   _colMyName.toLowerCase() == "MDM_fhNew".toLowerCase() || _colMyName.toLowerCase() == "MDM_ProcessNew".toLowerCase() || _colMyName.toLowerCase() == "MDM_fhChange".toLowerCase()
            || _colMyName.toLowerCase() == "MDM_ProcessChange".toLowerCase() || _colMyName.toLowerCase() == "MDM_StatusReg".toLowerCase() || _colMyName.toLowerCase() == "hs_rowKey".toLowerCase()
            || _colMyName.toLowerCase() == "MDM_hash".toLowerCase())
          StringSQL += s"${coma}old_${_colMyName}  \n"
        else {          
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) s"new_update_${_colMyName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.get_ReplaceValueOnUpdate() && this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${_colMyName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${_colMyName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${_colMyName} END  as ${_colMyName} \n"""
                
           if (Field.getMDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN old_${_colMyName} ELSE old_${_colMyName}${__old} END as ${_colMyName}${__old} \n"""
           if (Field.getMDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN now() ELSE old_${_colMyName}${__fhChange} END AS TimeStamp) as ${_colMyName}${__fhChange} \n"""
           if (Field.getMDM_EnableProcessLog)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN '${ProcessName}' WHEN ___ActionType__ = 'NEW' THEN '${ProcessName}' ELSE old_${_colMyName}${__ProcessLog} END as ${_colMyName}${__ProcessLog} \n"""             
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
    
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    
    var StringSQL: String = "SELECT "
    
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.get_MyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
      
      //string for key on
      if (Field.getIsPK){
        StringSQL += s" ${coma}${_colMyName} as ${_colMyName} \n"
        
      } else {
        if (   _colMyName.toLowerCase() == "MDM_fhNew".toLowerCase()         || _colMyName.toLowerCase() == "MDM_ProcessNew".toLowerCase() || _colMyName.toLowerCase() == "MDM_fhChange".toLowerCase()
            || _colMyName.toLowerCase() == "MDM_ProcessChange".toLowerCase() || _colMyName.toLowerCase() == "MDM_StatusReg".toLowerCase()  || _colMyName.toLowerCase() == "hs_rowKey".toLowerCase() 
            || _colMyName.toLowerCase() == "MDM_hash".toLowerCase())
          StringSQL += s"${coma}old_${_colMyName}  \n"
        else {          
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'NEW'    THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"new_insert_${_colMyName}" //si tiene valor en SQL insert, lo usa
                                                                               else if (this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${_colMyName}"     //si no, si tiene nombre de campo en DataFrame nuevo, lo usa 
                                                                               else ApplyAutoCast(Field.getDefaultValue,_dataType.sql)                        //si no tiene campo asignado, pone valor por default
                                                                           }
                                        WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) s"new_update_${_colMyName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.get_ReplaceValueOnUpdate() && this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${_colMyName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${_colMyName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${_colMyName} END  as ${_colMyName} \n"""
                
           if (Field.getMDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN old_${_colMyName} ELSE old_${_colMyName}${__old} END as ${_colMyName}${__old} \n"""
           if (Field.getMDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN now() ELSE old_${_colMyName}${__fhChange} END AS TimeStamp) as ${_colMyName}${__fhChange} \n"""
           if (Field.getMDM_EnableProcessLog)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN '${ProcessName}' WHEN ___ActionType__ = 'NEW' THEN '${ProcessName}' ELSE old_${_colMyName}${__ProcessLog} END as ${_colMyName}${__ProcessLog} \n"""             
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
    
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    
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
      val _colMyName = Field.get_MyName(this.getStorageType)
      
      if (     _colMyName.toLowerCase() == "MDM_fhNew".toLowerCase()         || _colMyName.toLowerCase() == "MDM_ProcessNew".toLowerCase() || _colMyName.toLowerCase() == "MDM_fhChange".toLowerCase()
            || _colMyName.toLowerCase() == "MDM_ProcessChange".toLowerCase() || _colMyName.toLowerCase() == "MDM_StatusReg".toLowerCase()  || _colMyName.toLowerCase() == "hs_rowKey".toLowerCase() 
            || _colMyName.toLowerCase() == "MDM_hash".toLowerCase())
        StringSQL += s"${coma}old_${_colMyName}  \n"
      else 
        StringSQL += s" ${coma}${_colMyName}  \n"
        
      if (Field.getMDM_EnableOldValue)
        StringSQL += s""",${_colMyName}${__old} \n"""
      if (Field.getMDM_EnableDTLog)
        StringSQL += s""",${_colMyName}${__fhChange} \n"""
      if (Field.getMDM_EnableProcessLog)
        StringSQL += s""",${_colMyName}${__ProcessLog} \n"""
       
      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""${coma_hash}${if (Field.getNullable) s"coalesce(${_colMyName},'null')" else _colMyName}"""
        coma_hash = ","
      }
        
      coma = ","
    }
    
    //Hash fields
    if (coma_hash == "")
      sys.error(s"must define one field as UsedForCheckSum")
    
    StringSQL_hash += "),256) "
                
    //set name according to getStorageType (AVRO IS LowerCase)
    val __MDM_hash = huemulBigDataGov.getCaseType( this.getStorageType, "MDM_hash")
    
    //Include HashFields to SQL
    StringSQL += s""",${StringSQL_hash} as ${__MDM_hash} \n"""
    
    if (this._TableType == huemulType_Tables.Reference || this._TableType == huemulType_Tables.Master || isSelectiveUpdate) {
      StringSQL += s""",case when old_${__MDM_hash} = ${StringSQL_hash} THEN 1 ELSE 0 END AS SameHashKey  \n ,___ActionType__ \n"""
    }
    
    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" FROM $NewAlias New 
                  """
               
    return StringSQL 
  }
  
  
  private def SQL_Step4_Final(NewAlias: String, ProcessName: String, IncludeActionType: Boolean): String = {
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    val __MDM_hash = huemulBigDataGov.getCaseType( this.getStorageType, "MDM_hash")
    
    var StringSQL: String = "SELECT "
    
    var coma: String = ""
    
    //Obtiene todos los campos
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns]
                                    }
    .foreach { x =>     
      //Get field      
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.get_MyName(this.getStorageType)
      
      if (_colMyName.toLowerCase() == "MDM_fhNew".toLowerCase())
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN now() ELSE old_${_colMyName} END as ${_colMyName} \n"
      else if (_colMyName.toLowerCase() == "MDM_ProcessNew".toLowerCase())
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN '$ProcessName' ELSE old_${_colMyName} END as ${_colMyName} \n"
      else if (_colMyName.toLowerCase() == "MDM_fhChange".toLowerCase())
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN now() ELSE old_${_colMyName} END as ${_colMyName} \n"
      else if (_colMyName.toLowerCase() == "MDM_ProcessChange".toLowerCase())
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN '$ProcessName' ELSE old_${_colMyName} END as ${_colMyName} \n"
      else if (_colMyName.toLowerCase() == "MDM_StatusReg".toLowerCase())
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN CAST(2 as Int) WHEN ___ActionType__ = 'NEW' THEN CAST(2 as Int) WHEN ___ActionType__ = 'DELETE' THEN CAST(-1 AS Int)  ELSE CAST(old_${_colMyName} AS Int) END as ${_colMyName} \n"
      else if (_colMyName.toLowerCase() == "MDM_hash".toLowerCase())
        StringSQL += s"${coma}${__MDM_hash} \n"
      else if (_colMyName.toLowerCase() == "hs_rowKey".toLowerCase()){
        if (getStorageType == huemulType_StorageType.HBASE && _numPKColumns > 1)
          StringSQL += s"${coma}${_HBase_rowKeyCalc} as ${_colMyName} \n"
      }
      else 
        StringSQL += s" ${coma}${_colMyName}  \n"
        
      if (Field.getMDM_EnableOldValue)
        StringSQL += s""",${_colMyName}${__old} \n"""
      if (Field.getMDM_EnableDTLog)
        StringSQL += s""",${_colMyName}${__fhChange} \n"""
      if (Field.getMDM_EnableProcessLog)
        StringSQL += s""",${_colMyName}${__ProcessLog} \n"""
       
       
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
      //huemulBigDataGov.logMessageInfo(s"${_colMyName} Field.get_MappedName: ${Field.get_MappedName}, Field.get_SQLForUpdate(): ${Field.get_SQLForUpdate()}, Field.get_SQLForInsert(): ${Field.get_SQLForInsert()}")
      
      var isOK: Boolean = false
      if (huemulBigDataGov.HasName(Field.get_MappedName))
        isOK = true
      else if (!huemulBigDataGov.HasName(Field.get_MappedName) && 
              (huemulBigDataGov.HasName(Field.get_SQLForUpdate()) && huemulBigDataGov.HasName(Field.get_SQLForInsert())))
        isOK = true
      else
        isOK = false
        
      if (!isOK)
        StringSQL.append(Field.get_MyName(this.getStorageType))
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
          PKNotMapped = s"${if (PKNotMapped == "") "" else ", "}${Field.get_MyName(this.getStorageType)} "
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
              
    //var coma_partition = ""
    
    //Get SQL DataType for Partition Columns
    val PartitionForCreateTable = getALLDeclaredFields().filter { x => x.setAccessible(true)
                                                                  x.get(this).isInstanceOf[huemul_Columns] &&
                                                                  x.get(this).asInstanceOf[huemul_Columns].getPartitionColumnPosition >= 1 }
                          .sortBy { x => x.setAccessible(true)
                                         x.get(this).asInstanceOf[huemul_Columns].getPartitionColumnPosition}
                          .map { x => 
              var Field = x.get(this).asInstanceOf[huemul_Columns]
              val _colMyName = Field.get_MyName(this.getStorageType)
              if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
                 s"${_colMyName}" //without datatype
              } else {
                val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
                s"${_colMyName} ${_dataType.sql}"
              } 
          }.mkString(",")
          
    /*
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.get_MyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
      
      if (getPartitionField.toLowerCase() == _colMyName.toLowerCase() ) {
        if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
          PartitionForCreateTable += s"${coma_partition}${getPartitionField}" //without datatype
        } else {
          PartitionForCreateTable += s"${coma_partition}${getPartitionField} ${_dataType.sql}"
        }
          coma_partition = ","
      }
    }
    * 
    */
    
    var lCreateTableScript: String = "" 
    //FROM 2.4 --> INCLUDE SPECIAL OPTIONS FOR DATABRICKS
    if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
      if (getStorageType == huemulType_StorageType.PARQUET || getStorageType == huemulType_StorageType.ORC || 
          getStorageType == huemulType_StorageType.DELTA   || getStorageType == huemulType_StorageType.AVRO) {
        //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                     CREATE TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.Normal)} (${getColumns_CreateTable(true) })
                                     USING ${getStorageType.toString()} 
                                     ${if (PartitionForCreateTable.length() > 0) s"PARTITIONED BY (${PartitionForCreateTable})" else "" }                                 
                                     LOCATION '${getFullNameWithPath()}'"""
      } else if (getStorageType == huemulType_StorageType.HBASE)  {
        lCreateTableScript = s"""
                                     CREATE TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.Normal)} (${getColumns_CreateTable(true) })
                                     USING 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                                     WITH SERDEPROPERTIES ("hbase.columns.mapping"="${getHBaseCatalogForHIVE(huemulType_InternalTableType.Normal)}")                                   
                                     TBLPROPERTIES ("hbase.table.name"="${getHBaseNamespace(huemulType_InternalTableType.Normal)}:${getHBaseTableName(huemulType_InternalTableType.Normal)}")"""
      }
    } else {
      if (getStorageType == huemulType_StorageType.PARQUET || getStorageType == huemulType_StorageType.ORC || 
          getStorageType == huemulType_StorageType.DELTA   || getStorageType == huemulType_StorageType.AVRO) {
        //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                     CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.Normal)} (${getColumns_CreateTable(true) })
                                     ${if (PartitionForCreateTable.length() > 0) s"PARTITIONED BY (${PartitionForCreateTable})" else "" }
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
    var PartitionForCreateTable = if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) s"dq_control_id" else s"dq_control_id STRING"
    
    var lCreateTableScript: String = "" 
    //FROM 2.4 --> INCLUDE SPECIAL OPTIONS FOR DATABRICKS
    if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
      if (getStorageType_DQResult == huemulType_StorageType.PARQUET || getStorageType_DQResult == huemulType_StorageType.ORC || getStorageType_DQResult == huemulType_StorageType.AVRO ) {
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.DQ)} (${getColumns_CreateTable(true, huemulType_InternalTableType.DQ) })
                                   USING ${getStorageType_DQResult.toString()}
                                   PARTITIONED BY (${PartitionForCreateTable})                                  
                                   LOCATION '${getFullNameWithPath_DQ()}'"""
      } else if (getStorageType_DQResult == huemulType_StorageType.HBASE)  {
        raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
      } else if (getStorageType_DQResult == huemulType_StorageType.DELTA) {
        //for delta, databricks get all columns and partition columns
        //see https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html
        lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.DQ)} 
                                   USING ${getStorageType_DQResult.toString()}                             
                                   LOCATION '${getFullNameWithPath_DQ()}'"""
      }
    }  else {
      if (getStorageType_DQResult == huemulType_StorageType.PARQUET || getStorageType_DQResult == huemulType_StorageType.ORC || getStorageType_DQResult == huemulType_StorageType.AVRO) {
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.DQ)} (${getColumns_CreateTable(true, huemulType_InternalTableType.DQ) })
                                   PARTITIONED BY (${PartitionForCreateTable})
                                   STORED AS ${getStorageType_DQResult.toString()}                                  
                                   LOCATION '${getFullNameWithPath_DQ()}'"""
      } else if (getStorageType_DQResult == huemulType_StorageType.HBASE)  {
        raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
      } else if (getStorageType_DQResult == huemulType_StorageType.DELTA) {
        lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.DQ)}
                                   STORED AS ${getStorageType_DQResult.toString()}                                  
                                   LOCATION '${getFullNameWithPath_DQ()}'"""
      }
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
    
    var lCreateTableScript: String = "" 
    //FROM 2.4 --> INCLUDE SPECIAL OPTIONS FOR DATABRICKS
    if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
      lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(huemulType_InternalTableType.OldValueTrace)} (${getColumns_CreateTable(true, huemulType_InternalTableType.OldValueTrace) })
                                    ${if (getStorageType_OldValueTrace == huemulType_StorageType.PARQUET) {
                                     """USING PARQUET
                                        PARTITIONED BY (mdm_columnname)""" 
                                    }
                                    else if (getStorageType_OldValueTrace == huemulType_StorageType.ORC) {
                                     """USING ORC
                                        PARTITIONED BY (mdm_columnname)""" 
                                    }
                                    else if (getStorageType_OldValueTrace == huemulType_StorageType.DELTA) {
                                     """USING DELTA
                                        PARTITIONED BY (mdm_columnname)""" 
                                    }
                                    else if (getStorageType_OldValueTrace == huemulType_StorageType.AVRO) {
                                     s"""USING AVRO
                                        PARTITIONED BY (mdm_columnname)""" 
                                    }
                                   }
                                   LOCATION '${getFullNameWithPath_OldValueTrace()}'"""
                                    //${if (_StorageType_OldValueTrace == "csv") {s"""
                                    //TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ")"""}}"""
    } else {
      //STORED AS ${_StorageType_OldValueTrace}
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
      lCreateTableScript = s"""
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
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS PARQUET""" 
                                    }
                                    else if (getStorageType_OldValueTrace == huemulType_StorageType.ORC) {
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS ORC""" 
                                    }
                                    else if (getStorageType_OldValueTrace == huemulType_StorageType.DELTA) {
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS DELTA""" 
                                    }
                                    else if (getStorageType_OldValueTrace == huemulType_StorageType.AVRO) {
                                     s"""PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS AVRO""" 
                                    }
                                   }
                                   LOCATION '${getFullNameWithPath_OldValueTrace()}'"""
                                    //${if (_StorageType_OldValueTrace == "csv") {s"""
                                    //TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ")"""}}"""
    }
    
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
  
  private def DF_ForeingKeyMasterAuto(warning_exclude: Boolean, LocalControl: huemul_Control): huemul_DataQualityResult = {
    var Result: huemul_DataQualityResult = new huemul_DataQualityResult()
    val ArrayFK = this.getForeingKey()
    val DataBaseName = this.getDataBase(this._DataBase)
    //For Each Foreing Key Declared
    ArrayFK.filter { x => (warning_exclude == true && x.getNotification() == huemulType_DQNotification.WARNING_EXCLUDE) ||
                          (warning_exclude == false && x.getNotification() != huemulType_DQNotification.WARNING_EXCLUDE)
                   } foreach { x =>
      val pk_InstanceTable = x._Class_TableName.asInstanceOf[huemul_Table]
      
      //Step1: Create distinct FROM NEW DF
      var SQLFields: String = ""
      var SQLLeftJoin: String = ""
      var sqlAnd: String = ""
      var sqlComa: String = ""
      var FirstRowPK: String = ""
      var FirstRowFK: String = ""
      x.Relationship.foreach { y =>
        //Get fields name
        FirstRowPK = s"PK.${y.PK.get_MyName(pk_InstanceTable.getStorageType)}"
        FirstRowFK = y.FK.get_MyName(this.getStorageType)
        SQLFields += s"${sqlComa}${y.FK.get_MyName(this.getStorageType)}"
        SQLLeftJoin += s"${sqlAnd}PK.${y.PK.get_MyName(pk_InstanceTable.getStorageType)} = FK.${y.FK.get_MyName(this.getStorageType)}"
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
      
      val pk_table_name = pk_InstanceTable.internalGetTable(huemulType_InternalTableType.Normal)
      val SQLLeft: String = s"""SELECT $broadcast_sql FK.* 
                                 FROM ${AliasDistinct_B} FK 
                                   LEFT JOIN ${pk_table_name} PK
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
                                                                                      LEFT JOIN ${pk_table_name} PK
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
      Values.DQ_Description =s"FK Validation: PK Table: ${pk_InstanceTable.internalGetTable(huemulType_InternalTableType.Normal)} "
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
                        , s"(${Result.Error_Code}) FK ERROR ON ${pk_table_name}"// dq_error_description
                        , LocalControl
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
                  ,LocalControl: huemul_Control
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
          LocalControl.NewStep("Start Save DQ Error Details for FK ")                
          if (!savePersist_DQ(Control, DetailDF)){
            huemulBigDataGov.logMessageWarn("Warning: DQ error can't save to disk")
          }
        }
  }

  /** DQ validation helper, if a DQ rule exists, ensures that it only runs once for WARNING_EXCLUDE or (ERROR/WARNING)
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   *
   * @param warningExclude          indicate if its processing WARNING_EXCLUDE rule notification
   * @param dqNotification          Data quality rule on column/field
   * @return                        [Boolean] true if its apply for the given parameters
   */
  private def validateDQRun(warningExclude:Boolean
                            , dqNotification:huemulType_DQNotification): Boolean = (
    (warningExclude && dqNotification == huemulType_DQNotification.WARNING_EXCLUDE)
      || (!warningExclude && dqNotification != huemulType_DQNotification.WARNING_EXCLUDE)
    )


  /** Generate Data Quality Rules (huemul_DataQuality] for min/max rules at attributes
   *
   * TODO Documentar
   *  1) Warning Exclude -> min, max or minmax -> 1 rule
   *  2) Error or Warning -> it separete if one is warning and the other is error -> 2 rules
   *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   *
   * @param dqWarningExclude        true is its es WARNING_EXCLUDE notrification
   * @param columnDefinition        huemul_Columns column definition
   * @param dqRuleDescription       Data quality rule description
   * @param dqErrorCode             Data quality error code
   * @param minValue                Min value (length, decimal, datetime)
   * @param maxValue                Max value (length, decimal, datetime)
   * @param minValueNotification    Min notification level
   * @param maxValueNotification    Max Notification level
   * @param sqlMinSentence          SQL Sentences/rule for min rule
   * @param sqlMaxSentence          SQL Sentences/rule for max rule
   * @param minExternalCode         Min rule external error code
   * @param maxExternalCode         Max rule external error code
   * @return                        List[huemul_DataQuality] list de data quality rules to apply
   */
  private def dqRuleMinMax(dqWarningExclude:Boolean
                           , columnDefinition:huemul_Columns
                           , dqRuleDescription:String
                           , dqErrorCode:Integer
                           , minValue:String
                           , maxValue:String
                           , minValueNotification: huemulType_DQNotification
                           , maxValueNotification: huemulType_DQNotification
                           , sqlMinSentence:String
                           , sqlMaxSentence:String
                           , minExternalCode:String
                           , maxExternalCode:String):List[huemul_DataQuality] = {
    var listDqRule:List[huemul_DataQuality] = List[huemul_DataQuality]()
    val colMyName = columnDefinition.get_MyName(this.getStorageType)
    var SQLFormula : HashMap[String,String] = new HashMap[String,String]
    var notificationMin:huemulType_DQNotification = null
    var notificationMax:huemulType_DQNotification = null
    var externalCode:String = null

    if (minValue != null && validateDQRun(dqWarningExclude,minValueNotification) ){
      SQLFormula += ( "Min" -> s" ${sqlMinSentence} ")
      notificationMin = minValueNotification
      externalCode = minExternalCode
    }

    if (maxValue != null && validateDQRun(dqWarningExclude,maxValueNotification)) {
      SQLFormula += ("Max" -> s" ${sqlMaxSentence} ")
      notificationMax = maxValueNotification
      externalCode = if (externalCode == null) maxExternalCode else externalCode
    }

    if (notificationMin == notificationMax || notificationMin == null || notificationMax == null ) {
      val ruleMinMax = new huemul_DataQuality(
        columnDefinition
        , s"${SQLFormula.keys.mkString("/")} ${dqRuleDescription} ${colMyName}"
        , s" (${SQLFormula.values.mkString(" and ")}) or (${colMyName} is null) "
        , dqErrorCode
        , huemulType_DQQueryLevel.Row
        , if (notificationMin == null ) notificationMax else notificationMin
        , true
        , externalCode)
      ruleMinMax.setTolerance(0L, null)
      listDqRule  = listDqRule :+ ruleMinMax
    } else {
      val ruleMin: huemul_DataQuality = new huemul_DataQuality(
        columnDefinition
        , s"Min ${dqRuleDescription} ${colMyName}"
        , s" (${SQLFormula("Min")}) or (${colMyName} is null) "
        , dqErrorCode
        , huemulType_DQQueryLevel.Row
        , notificationMin
        , true
        , if (minExternalCode != null && minExternalCode.nonEmpty)
          minExternalCode else maxExternalCode)
      ruleMin.setTolerance(0L, null)
      listDqRule = listDqRule :+ ruleMin

      val ruleMax: huemul_DataQuality = new huemul_DataQuality(
        columnDefinition
        , s"Max ${dqRuleDescription} ${colMyName}"
        , s" (${SQLFormula("Max")}) or (${colMyName} is null) "
        , dqErrorCode
        , huemulType_DQQueryLevel.Row
        , notificationMax
        , true
        , if (maxExternalCode != null && maxExternalCode.nonEmpty)
          maxExternalCode else minExternalCode)
      ruleMax.setTolerance(0L, null)
      listDqRule = listDqRule :+ ruleMax

    }
    listDqRule
  }

  /** Save the records from the base table records with WARNING_EXCLUDE, WARNING & ERROR for rules PK & UK
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   *
   * @param warningExclude  It separates WARNING_EXCLUDE (true) from ERROR/WARNING (false)
   * @param ResultDQ        DQ Rules execution result
   * @param LocalControl    huemul_control refernce for logging
   * @return                [[com.huemulsolutions.bigdata.dataquality.huemul_DataQualityResult]] with new records
   */
  private def executeRuleTypeAggSaveData(warningExclude:Boolean
                                 , ResultDQ:huemul_DataQualityResult
                                 , LocalControl: huemul_Control): Unit = {

    //.filter( f => (typeOpr == "pk" && f.DQ_ErrorCode == 1018) || (typeOpr == "uk" && f.DQ_ErrorCode == 2006) )
    //get on sentence, until now it only process pk and unique rules
    val dqResultData:List[huemul_DQRecord] = ResultDQ
      .getDQResult()
      .filter( f => (
        ((warningExclude && f.DQ_Notification == huemulType_DQNotification.WARNING_EXCLUDE)
          || ((!warningExclude && f.DQ_Notification != huemulType_DQNotification.WARNING_EXCLUDE)))
          && (f.DQ_ErrorCode == 1018 || f.DQ_ErrorCode == 2006)) )
      .toList

    //Start loop each pk, uk data rule with error, warning or warning_exclude
    for(dqKeyRule <- dqResultData ) {
      val typeOpr:String = if (dqKeyRule.DQ_ErrorCode==1018) "pk" else "uk"
      val dqId = dqKeyRule.DQ_Id
      val notification:huemulType_DQNotification = dqKeyRule.DQ_Notification
      val stepRunType = notification match {
        case huemulType_DQNotification.WARNING_EXCLUDE => "WARNING_EXCLUDE"
        case huemulType_DQNotification.WARNING => "WARNING"
        case _ => "ERROR"
      }
      val colData:List[huemul_Columns] = getColumns()
        .filter(f => (typeOpr == "pk" && f.getIsPK)
          || (typeOpr == "uk" && f.getIsUnique && f.getMyName(this.getStorageType).toLowerCase() == dqKeyRule.ColumnName.toLowerCase()))
        .toList
      val keyColumns:String = colData.head.getMyName(this.getStorageType)
      val joinColList:String = colData.map(m => m.getMyName(this.getStorageType)).mkString(",")
      val sourceAlias:String = this.DataFramehuemul.Alias
      val keyJoinSql = colData
        .map(m => s" $typeOpr.${m.getMyName(this.getStorageType)} = dup.${m.getMyName(this.getStorageType)}").mkString(" and ")

      LocalControl.NewStep(s"Step: DQ Result: Get detail for ${typeOpr.toUpperCase} $stepRunType (step1)")
      val step1SQL = s"""SELECT $joinColList, COUNT(1) as Cantidad
                        | FROM $sourceAlias
                        | GROUP BY $joinColList
                        | HAVING COUNT(1) > 1 """.stripMargin
      log.trace(s"Step: DQ Result: Step1 Sql : $step1SQL")
      val dfDupRecords:DataFrame = huemulBigDataGov.DF_ExecuteQuery(s"___temp_${typeOpr}_det_01", step1SQL)
      dfDupRecords.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

      //Broadcast Hint
      var broadcastHint : String = if (dfDupRecords.count() < 50000) "/*+ BROADCAST(dup) */" else ""

      //get rows duplicated
      LocalControl.NewStep(s"Step: DQ Result: Get detail for ${typeOpr.toUpperCase} $stepRunType (step2)")
      val step2SQL =s"""SELECT $broadcastHint $typeOpr.*
                       | FROM $sourceAlias $typeOpr
                       | INNER JOIN ___temp_${typeOpr}_det_01 dup
                       |  ON $keyJoinSql""".stripMargin
      log.trace(s"Step: DQ Result: Step2 Sql : $step2SQL")
      val dfDupRecordsDetail:DataFrame = huemulBigDataGov.DF_ExecuteQuery(s"___temp_${typeOpr}_det_02", step2SQL)
      dfDupRecordsDetail.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
      val numReg = dfDupRecordsDetail.count()

      val dqSqlTableSentence = this.DataFramehuemul.DQ_GenQuery(
        s"___temp_${typeOpr}_det_02"
        , null
        , true
        , keyColumns
        , dqId
        , notification
        , dqKeyRule.DQ_ErrorCode
        , s"(${dqKeyRule.DQ_ErrorCode}) ${typeOpr.toUpperCase} $stepRunType ON ${keyJoinSql} ($numReg reg)"// dq_error_description
      )
      log.trace(s"Step: DQ Result: Step3 Sql : $dqSqlTableSentence")

      //Execute query
      LocalControl.NewStep(s"Step: DQ Result: Get detail for ${typeOpr.toUpperCase} $stepRunType (step3, $numReg rows)")
      var dfErrorRecords = huemulBigDataGov.DF_ExecuteQuery(s"temp_DQ_KEY", dqSqlTableSentence)
      dfErrorRecords.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
      val numCount = dfErrorRecords.count()
      if (ResultDQ.DetailErrorsDF == null)
        ResultDQ.DetailErrorsDF = dfErrorRecords
      else
        ResultDQ.DetailErrorsDF = ResultDQ.DetailErrorsDF.union(dfErrorRecords)

    }
  }

  /**
   *
   * @param IsSelectiveUpdate
   * @param LocalControl
   * @param warning_exclude
   * @return
   */
  private def DF_DataQualityMasterAuto(IsSelectiveUpdate: Boolean
                                       , LocalControl: huemul_Control
                                       , warning_exclude: Boolean): huemul_DataQualityResult = {

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

    // Validate Primary Key Rule
    if (SQL_PK != null && validateDQRun(warning_exclude, _PkNotification )) {
      log.info(s"DQ Rule: Validate PK for columns ${SQL_PK}")
      val dqPk: huemul_DataQuality = new huemul_DataQuality(
        null
        , s"PK Validation"
        , s"count(1) = count(distinct ${SQL_PK} )"
        , 1018
        , huemulType_DQQueryLevel.Aggregate
        , _PkNotification
        , true
        , getPK_externalCode)
      dqPk.setTolerance(0L, null)
      ArrayDQ.append(dqPk)
    }

    //Apply Data Quality according to field definition in DataDefDQ: Unique Values
    //Note: It doesn't apply getDQ_Notification, it must be explicit se the notification
    SQL_Unique_FinalTable()
      .filter(f => validateDQRun(warning_exclude, f.getIsUnique_Notification ))
      .foreach { x =>
        log.trace(s"DF_SAVE DQ: VALIDATE UNIQUE FOR FIELD ${x.getMyName(this.getStorageType)}")

        val DQ_UNIQUE : huemul_DataQuality = new huemul_DataQuality(
          x
          , s"UNIQUE Validation ${x.getMyName(this.getStorageType)}"
          ,s"count(1) = count(distinct ${x.getMyName(this.getStorageType)} )"
          , 2006
          , huemulType_DQQueryLevel.Aggregate
          , x.getIsUnique_Notification
          , true
          , x.getIsUnique_externalCode )
        DQ_UNIQUE.setTolerance(0L, null)
        ArrayDQ.append(DQ_UNIQUE)
      }

    //Apply Data Quality according to field definition in DataDefDQ: Accept nulls (nullable)
    //Note: If it has default value and it is NOT NULL it will not be evaluated
    //ToDO: Checks te notes for default values
    SQL_NotNull_FinalTable(warning_exclude)
      .filter( f => (!f.getNullable  && (f.getDefaultValue == null || f.getDefaultValue == "null"))
        && validateDQRun(warning_exclude, f.getDQ_Nullable_Notification ) )
      .foreach { x =>
        val colMyName = x.get_MyName(this.getStorageType)
        log.trace(s"DF_SAVE DQ: VALIDATE NOT NULL FOR FIELD $colMyName")

        val NotNullDQ : huemul_DataQuality = new huemul_DataQuality(
          x
          , s"Not Null for field ${colMyName} "
          , s"${colMyName} IS NOT NULL"
          , 1023
          , huemulType_DQQueryLevel.Row
          , x.getDQ_Nullable_Notification
          , true
          , x.getNullable_externalCode)
        NotNullDQ.setTolerance(0L, null)
        ArrayDQ.append(NotNullDQ)
      }

    //Validación DQ_RegExp
    getColumns().filter {
      x => x.getDQ_RegExpression != null && validateDQRun(warning_exclude, x.getDQ_RegExpression_Notification)
    }.foreach { x =>
      val _colMyName = x.getMyName(this.getStorageType)
      val SQLFormula : String = s"""${_colMyName} rlike "${x.getDQ_RegExpression}" """

      val RegExp : huemul_DataQuality = new huemul_DataQuality(
        x
        , s"RegExp Column ${_colMyName}"
        , s" (${SQLFormula}) or (${_colMyName} is null) "
        , 1041
        , huemulType_DQQueryLevel.Row
        , x.getDQ_RegExpression_Notification
        , true
        , x.getDQ_RegExpression_externalCode  )

      RegExp.setTolerance(0L, null)
      ArrayDQ.append(RegExp)
    }


    //Validate DQ rule max/mín length the field
    getColumns()
      .filter { x =>(
        (x.getDQ_MinLen != null && validateDQRun(warning_exclude, x.getDQ_MinLen_Notification) )
          || (x.getDQ_MaxLen != null && validateDQRun(warning_exclude,x.getDQ_MaxLen_Notification) )
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[huemul_DataQuality] = dqRuleMinMax(
          warning_exclude
          , x
          , "length DQ for Colum"
          , 1020
          , if (x.getDQ_MinLen != null) x.getDQ_MinLen.toString else null
          , if (x.getDQ_MaxLen != null) x.getDQ_MaxLen.toString else null
          , x.getDQ_MinLen_Notification
          , x.getDQ_MaxLen_Notification
          , s"length(${colMyName}) >= ${x.getDQ_MinLen}"
          , s"length(${colMyName}) <= ${x.getDQ_MaxLen}"
          , x.getDQ_MinLen_externalCode
          , x.getDQ_MaxLen_externalCode)

        dqRules.foreach(dq => ArrayDQ.append(dq))
      }

    //Validate DQ rule max/mín number value
    getColumns()
      .filter { x => (
        (x.getDQ_MinDecimalValue != null && validateDQRun(warning_exclude, x.getDQ_MinDecimalValue_Notification ) )
          || (x.getDQ_MaxDecimalValue != null && validateDQRun(warning_exclude, x.getDQ_MaxDecimalValue_Notification ))
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[huemul_DataQuality] = dqRuleMinMax(
          warning_exclude
          , x
          , "Number DQ for Column"
          , 1021
          , if (x.getDQ_MinDecimalValue != null) x.getDQ_MinDecimalValue.toString else null
          , if (x.getDQ_MaxDecimalValue != null) x.getDQ_MaxDecimalValue.toString else null
          , x.getDQ_MinDecimalValue_Notification
          , x.getDQ_MaxDecimalValue_Notification
          , s" ${colMyName} >= ${x.getDQ_MinDecimalValue} "
          , s" ${colMyName} <= ${x.getDQ_MaxDecimalValue} "
          , x.getDQ_MinDecimalValue_externalCode
          , x.getDQ_MaxDecimalValue_externalCode)

        dqRules.foreach(dq => ArrayDQ.append(dq))
      }

    //Validación DQ máximo y mínimo de fechas
    getColumns()
      .filter { x => (
        (x.getDQ_MinDateTimeValue != null && validateDQRun(warning_exclude, x.getDQ_MinDateTimeValue_Notification ) )
          || (x.getDQ_MaxDateTimeValue != null && validateDQRun(warning_exclude, x.getDQ_MaxDateTimeValue_Notification ))
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[huemul_DataQuality] = dqRuleMinMax(
          warning_exclude
          , x
          , "DateTime DQ for Column"
          , 1022
          , x.getDQ_MinDateTimeValue
          , x.getDQ_MaxDateTimeValue
          , x.getDQ_MinDateTimeValue_Notification
          , x.getDQ_MaxDateTimeValue_Notification
          , s" ${colMyName} >= '${x.getDQ_MinDateTimeValue}' "
          , s" ${colMyName} <= '${x.getDQ_MaxDateTimeValue}' "
          , x.getDQ_MinDateTimeValue_externalCode
          , x.getDQ_MaxDateTimeValue_externalCode)

        dqRules.foreach(dq => ArrayDQ.append(dq))
      }

    log.debug(s"DF_SAVE DQ: DQ Rule size ${ArrayDQ.size}")
    ArrayDQ.foreach(x => log.debug(s"DF_SAVE DQ: Rule - ${x.getDescription()}"))

    val ResultDQ = this.DataFramehuemul.DF_RunDataQuality(
      this.getDataQuality(warning_exclude)
      , ArrayDQ
      , this.DataFramehuemul.Alias
      , this
      , this.getSaveDQErrorOnce)

    val stepRunType = if (warning_exclude) "warning_exclude" else "error"

    // 2.[6] add condition if its has warning and warning_exclude=true
    if (ResultDQ.isError || (ResultDQ.isWarning && warning_exclude)){

      Result.isWarning = ResultDQ.isWarning
      Result.isError = ResultDQ.isError
      Result.Description += s"\n${ResultDQ.Description}"
      Result.Error_Code = ResultDQ.Error_Code

      //version 2.6: add PK error detail
      executeRuleTypeAggSaveData(warning_exclude, ResultDQ, LocalControl)
    }
    
    //Save errors to disk
    if (huemulBigDataGov.GlobalSettings.DQ_SaveErrorDetails && ResultDQ.DetailErrorsDF != null && this.getSaveDQResult) {
      LocalControl.NewStep(s"Start Save DQ $stepRunType Details ")
      if (!savePersist_DQ(Control, ResultDQ.DetailErrorsDF)){
        huemulBigDataGov.logMessageWarn(s"Warning: DQ $stepRunType can't save to disk")
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
        ColumnName = x.get(this).asInstanceOf[huemul_Columns].get_MyName(this.getStorageType)
    }
    
    return ColumnName
  }
  
 
  /**
   Create final DataFrame with full join New DF with old DF
   */
  private def DF_MDM_Dohuemul(LocalControl: huemul_Control, AliasNewData: String, isInsert: Boolean, isUpdate: Boolean, isDelete: Boolean, isSelectiveUpdate: Boolean, PartitionValuesForSelectiveUpdate: ArrayBuffer[String] , storageLevelOfDF: org.apache.spark.storage.StorageLevel = null) {
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
      //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      //from 2.6 --> validate numpartitions = numvalues
      //if (!huemulBigDataGov.HasName(PartitionValueForSelectiveUpdate) && _TableType == huemulType_Tables.Transaction)
      if (getPartitionList.length > 0 && (PartitionValuesForSelectiveUpdate.length != getPartitionList.length ||
                                          PartitionValuesForSelectiveUpdate.filter { x => x == null }.length > 0)) 
        raiseError(s"huemul_Table Error: Partition Value not defined (partitions: ${getPartitionList.mkString(",") }, values: ${PartitionValuesForSelectiveUpdate.mkString(",")}) ", 1044)
        
      
      var FullPathString = this.getFullNameWithPath()
        
      if (_TableType == huemulType_Tables.Transaction) {
        val partitionList = getPartitionList;
        var i: Int = 0
        while (i < getPartitionList.length) {
          FullPathString += s"/${partitionList(i).get_MyName(getStorageType)}=${PartitionValuesForSelectiveUpdate(i)}"
          i+=1
        }
        
        //FullPathString = s"${getFullNameWithPath()}/${getPartitionField.toLowerCase()}=${PartitionValueForSelectiveUpdate}"
      }
      
      val FullPath = new org.apache.hadoop.fs.Path(FullPathString)
      //Google FS compatibility
      val fs = FullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      if (fs.exists(FullPath)){
        //Exist, copy for use
        val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
        //Open actual file
        val DFTempCopy = huemulBigDataGov.spark.read.format( _getSaveFormat(this.getStorageType)).load(FullPathString)
        val tempPath = huemulBigDataGov.GlobalSettings.GetDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, TempAlias)
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to temp dir: $tempPath ")
        if (this.getNumPartitions == null || this.getNumPartitions <= 0)
          DFTempCopy.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
        else
          DFTempCopy.repartition(this.getNumPartitions).write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
          
        DFTempCopy.unpersist()
       
        //Open temp file
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"open temp old df: $tempPath ")
        
        var lineageWhere: ArrayBuffer[String] = new ArrayBuffer[String] 
        var DFTempOpen = huemulBigDataGov.spark.read.parquet(tempPath)
        //from 2.6 --> add partition columns and values
        var i: Int = 0
        val partitionList = getPartitionList;
        while (i < getPartitionList.length) {
          DFTempOpen = DFTempOpen.withColumn(partitionList(i).get_MyName(getStorageType) , lit(PartitionValuesForSelectiveUpdate(i)))
          lineageWhere.append(s"${partitionList(i).get_MyName(getStorageType)} = '${PartitionValuesForSelectiveUpdate(i)}'")
          i += 1
        }
        /*
        val DFTempOpen = if (_TableType == huemulType_Tables.Transaction) 
                            huemulBigDataGov.spark.read.parquet(tempPath).withColumn(getPartitionField.toLowerCase(), lit(PartitionValueForSelectiveUpdate))
                         else huemulBigDataGov.spark.read.parquet(tempPath)
                         * 
                         */
        NumRowsOldDataFrame = DFTempOpen.count()
        DFTempOpen.createOrReplaceTempView(TempAlias)  
        
        val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
        huemulBigDataGov.DF_SaveLineage(TempAlias
                                    , s"""SELECT * FROM ${this.internalGetTable(huemulType_InternalTableType.Normal)} ${ if (lineageWhere.length > 0)  /*if (_TableType == huemulType_Tables.Transaction)*/ 
                                                                                                                      s" WHERE ${lineageWhere.mkString(" and ")}"} """ //sql
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
      
      //from 2.2 --> persist to improve performance
      DataFramehuemul.DataFrame.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
      
      //LocalControl.NewStep("Transaction: Get Statistics info")
      this.UpdateStatistics(LocalControl, "Transaction", null)
      
      
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
    } 
    else if (_TableType == huemulType_Tables.Reference || _TableType == huemulType_Tables.Master) {
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
      //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      var oldDataFrameExists = false
      
      if (this.getStorageType == huemulType_StorageType.HBASE) {
        val hBaseConnector = new huemul_TableConnector(huemulBigDataGov, LocalControl)
        if (hBaseConnector.tableExistsHBase(getHBaseNamespace(huemulType_InternalTableType.Normal),getHBaseTableName(huemulType_InternalTableType.Normal))) {
          oldDataFrameExists = true
          //println(getHBaseCatalog(huemulType_InternalTableType.Normal))
          //val DFHBase = hBaseConnector.getDFFromHBase(TempAlias, getHBaseCatalog(huemulType_InternalTableType.Normal))
          
          //create external table if doesn't exists
          CreateTableScript = DF_CreateTableScript()
          
          //new from 2.3
          runSQLexternalTable(CreateTableScript, true)
            
          LocalControl.NewStep("Ref & Master: Reading HBase data...")     
          val DFHBase = huemulBigDataGov.DF_ExecuteQuery(TempAlias, s"SELECT * FROM ${this.internalGetTable(huemulType_InternalTableType.Normal)}")
          val numRows = DFHBase.count()
          if (huemulBigDataGov.GlobalSettings.MDM_SaveBackup && this._SaveBackup){
            var tempPath = this.getFullNameWithPath_Backup(Control.Control_Id )
            _BackupPath = tempPath
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to backup dir: $tempPath ")
         
            LocalControl.NewStep("Ref & Master: Backup...")
            DFHBase.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)     //2.2 -> this._StorageType.toString() instead of "parquet"
          }
          
          
          //DFHBase.show()
        }
      } else {
        val lpath = new org.apache.hadoop.fs.Path(this.getFullNameWithPath())
        val fs = lpath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration) 
        if (fs.exists(lpath)){
          //Exist, copy for use
          oldDataFrameExists = true
          
          //Open actual file
          val DFTempCopy = huemulBigDataGov.spark.read.format(_getSaveFormat(this.getStorageType)).load(this.getFullNameWithPath())
          if (huemulBigDataGov.DebugMode) DFTempCopy.printSchema()

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
          
          val fsPath = new org.apache.hadoop.fs.Path(tempPath)
          val fs = fsPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration) 
          if (fs.exists(fsPath)){  
            fs.delete(fsPath, true)
          }
          
          if (this.getNumPartitions == null || this.getNumPartitions <= 0)
            DFTempCopy.write.mode(SaveMode.Overwrite).format(_getSaveFormat(this.getStorageType)).save(tempPath)     //2.2 -> this._StorageType.toString() instead of "parquet"
          else
            DFTempCopy.repartition(this.getNumPartitions).write.mode(SaveMode.Overwrite).format(_getSaveFormat(this.getStorageType)).save(tempPath)   //2.2 -> this._StorageType.toString() instead of "parquet"
          DFTempCopy.unpersist()
         
          //Open temp file
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"open temp old df: $tempPath ")
          val DFTempOpen = huemulBigDataGov.spark.read.format(_getSaveFormat(this.getStorageType)).load(tempPath)  //2.2 --> read.format(this._StorageType.toString()).load(tempPath)    instead of  read.parquet(tempPath)
          if (huemulBigDataGov.DebugMode) DFTempOpen.printSchema()
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
      
      //from 2.2 --> add to compatibility with HBase (without this, OldValueTrace doesn't work becacuse it recalculate with new HBase values                                       
      SQLFullJoin_DF.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
                                             
                                             
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
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.printSchema()
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
    var Invoker: StackTraceElement = null
    var InvokerName: String = null
    var ClassNameInvoker: String = null
    var PackageNameInvoker: String = null
    
    var numClassBack: Int = 1
    do {
      //repeat until find className different to myself
      numClassBack += 1
      Invoker = new Exception().getStackTrace()(numClassBack)
      InvokerName = Invoker.getClassName().replace("$", "")
      
      val ArrayResult = InvokerName.split('.')
      
      ClassNameInvoker = ArrayResult(ArrayResult.length-1)
      PackageNameInvoker = InvokerName.replace(".".concat(ClassNameInvoker), "")
      
      //println(s"${numClassBack}, ${ClassNameInvoker.toLowerCase()} == ${this.getClass.getSimpleName.toLowerCase()}")
      if (ClassNameInvoker.toLowerCase() == this.getClass.getSimpleName.toLowerCase() || ClassNameInvoker.toLowerCase() == "huemul_table") {
        Invoker = null
      }
        
    } while (Invoker == null) 
    
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
    val newValue: ArrayBuffer[String] = new ArrayBuffer[String]()
    newValue.append(PartitionValueForSelectiveUpdate)

    val whoExecute = getClassAndPackage()
    if (this._WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = executeSelectiveUpdate(NewAlias, newValue, storageLevelOfDF)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeSelectiveUpdate in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}",1012)
    }

    return Result
  }
  
  def executeSelectiveUpdate(NewAlias: String, PartitionValueForSelectiveUpdate: ArrayBuffer[String], storageLevelOfDF: org.apache.spark.storage.StorageLevel ): Boolean = {
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
        val ColumnNames = x.get_MyName(getStorageType)
        val _dataType  = x.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider(), this.getStorageType)
        
        val ColumnInSchema = Schema.filter { y => y.name.toLowerCase() == ColumnNames.toLowerCase() }
        if (ColumnInSchema == null || ColumnInSchema.length == 0)
          raiseError(s"huemul_Table Error: column missing in Schema ${ColumnNames}", 1038)
        if (ColumnInSchema.length != 1)
          raiseError(s"huemul_Table Error: multiples columns found in Schema with name ${ColumnNames}", 1039)
        
          val dataType = ColumnInSchema(0).dataType
        if (dataType != _dataType) {
          Errores = Errores.concat(s"Error Column ${ColumnNames}, Requiered: ${_dataType}, actual: ${dataType}  \n")
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
  private def executeSave(AliasNewData: String, IsInsert: Boolean, IsUpdate: Boolean, IsDelete: Boolean, IsSelectiveUpdate: Boolean, PartitionValueForSelectiveUpdate: ArrayBuffer[String], storageLevelOfDF: org.apache.spark.storage.StorageLevel, RegisterOnlyInsertInDQ: Boolean): Boolean = {
    if (!this.DefinitionIsClose)
      this.raiseError(s"huemul_Table Error: MUST call ApplyTableDefinition ${this.TableName}", 1048)
    
    _NumRows_Excluded = 0  
      
    var partitionValueString = ""
    if (PartitionValueForSelectiveUpdate != null)
      partitionValueString = PartitionValueForSelectiveUpdate.mkString(", ")
    
    var LocalControl = new huemul_Control(huemulBigDataGov, Control ,huemulType_Frequency.ANY_MOMENT, false )
    LocalControl.AddParamInformation("AliasNewData", AliasNewData)
    LocalControl.AddParamInformation("IsInsert", IsInsert.toString())
    LocalControl.AddParamInformation("IsUpdate", IsUpdate.toString())
    LocalControl.AddParamInformation("IsDelete", IsDelete.toString())
    LocalControl.AddParamInformation("IsSelectiveUpdate", IsSelectiveUpdate.toString())
    LocalControl.AddParamInformation("PartitionValueForSelectiveUpdate", partitionValueString)
    
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
      val FKResult_EXCLUDE = DF_ForeingKeyMasterAuto(true, LocalControl)
      
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
      val FKResult = DF_ForeingKeyMasterAuto(false, LocalControl)
      
      
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
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.printSchema()
      val ResultCompareSchemaFinal = CompareSchema(this.getColumns(), this.DataFramehuemul.DataFrame.schema) 
      if (ResultCompareSchemaFinal != "") {
        ErrorCode = 1014
        raiseError(s"huemul_Table Error: User Error: incorrect DataType: \n${ResultCompareSchemaFinal}",ErrorCode)
      }
      
      //Create table persistent
      if (huemulBigDataGov.DebugMode){
        huemulBigDataGov.logMessageDebug(s"Saving ${internalGetTable(huemulType_InternalTableType.Normal)} Table with params: ") 
        huemulBigDataGov.logMessageDebug(s"${getPartitionField} field for partitioning table")
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
  
  /*
  def copyToDest(PartitionValue: String, DestEnvironment: String) {
    if (huemulBigDataGov.HasName(getPartitionField)) {
       val ProdFullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath()}/${getPartitionField.toLowerCase()}=${PartitionValue}")
       val ManualFullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath2(DestEnvironment)}/${getPartitionField.toLowerCase()}=${PartitionValue}")
       
       val fsProd = ProdFullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration) //FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       val fsMan = ManualFullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration) 
       org.apache.hadoop.fs.FileUtil.copy(fsProd, ProdFullPath, fsMan, ManualFullPath, false, true, huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       
       val DestTableName: String = InternalGetTable(DestEnvironment)
       if (this.getStorageType != huemulType_StorageType.DELTA) {
         huemulBigDataGov.logMessageInfo(s"MSCK REPAIR TABLE ${DestTableName}")
         huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${DestTableName}")
       }     
    } else {
       val ProdFullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath()}/${getPartitionField.toLowerCase()}")
       val ManualFullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath2(DestEnvironment)}/${getPartitionField.toLowerCase()}")
       
       val fsProd = ProdFullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration) //FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       val fsMan = ManualFullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration) 
       
       org.apache.hadoop.fs.FileUtil.copy(fsProd, ProdFullPath, fsMan, ManualFullPath, false, true, huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
    }
    
  }
  * 
  */
  
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
        var Field = x.get(this).asInstanceOf[huemul_Columns]
        val _colMyName = Field.get_MyName(this.getStorageType)
        dq_StringSQL_PK += s" ${coma}${_colMyName}"
        dq_StringSQL_PK_join += s" ${_and} PK.${_colMyName} = EXCLUDE.${_colMyName}" 
        if (dq_firstPK == null)
          dq_firstPK = _colMyName
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
      OldValueTrace_save("__FullJoin", huemulBigDataGov.ProcessNameCall, LocalControl)
      
      /*
      var tempSQL_OldValueFullTrace_DF : DataFrame = null 
      if (SQL_FullTrace != null){ //if null, doesn't have the mdm old "value full trace" to get          
        tempSQL_OldValueFullTrace_DF = huemulBigDataGov.DF_ExecuteQuery("__SQL_OldValueFullTrace_DF",SQL_FullTrace) 
      
        if (tempSQL_OldValueFullTrace_DF != null) {
          Result = savePersist_OldValueTrace(LocalControl,tempSQL_OldValueFullTrace_DF)
          
          if (!Result)
            return Result 
        }
      }
      * 
      */
    }

    
    if (this._TableType == huemulType_Tables.Reference || this._TableType == huemulType_Tables.Master || IsSelectiveUpdate) {
      LocalControl.NewStep("Save: Drop ActionType column")
   
      if (OnlyInsert && !IsSelectiveUpdate) {
        DF_Final = DF_Final.where("___ActionType__ = 'NEW'") 
      } else if (this.getStorageType == huemulType_StorageType.HBASE){
        //exclude "EQUAL" to improve write performance on HBase
        DF_Final = DF_Final.where("___ActionType__ != 'EQUAL'")
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
                          ,LocalControl
                          )
        }
      }
      
    }
      
    
    try {
      if (getPartitionList.length == 0 || this._TableType == huemulType_Tables.Reference || this._TableType == huemulType_Tables.Master ){
        
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
        else {
          if (huemulBigDataGov.DebugMode) DF_Final.printSchema()
          if (getPartitionList.length == 0) //save without partitions
            DF_Final.write.mode(localSaveMode).format(_getSaveFormat(this.getStorageType)).save(getFullNameWithPath())
          else  //save with partitions
            DF_Final.write.mode(localSaveMode).format(_getSaveFormat(this.getStorageType)).partitionBy(getPartitionListForSave:_*).save(getFullNameWithPath())
        }
        
        
        //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)       
        //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath()), new FsPermission("770"))
      }
      else{
        //get partition to start drop and all partitions before it (path)
        val dropPartitions : ArrayBuffer[huemul_Columns] = new ArrayBuffer[huemul_Columns]
        var continueSearch: Boolean = true
        getPartitionList.foreach { x =>  
          if (continueSearch) {
            dropPartitions.append(x)
            
            if (x.getPartitionDropBeforeSave)
              continueSearch = false
          }
        }
        
         
        //drop old partitions only if one of them was marked as dropBeforeInsert
        if (!continueSearch && dropPartitions.nonEmpty) {
          this.PartitionValue = new ArrayBuffer[String]
          //get partitions value to drop
          LocalControl.NewStep("Save: Get partitions values to delete (distinct)")
          //val partitionFieldsName = dropPartitions.map { x => x.get_MyName(getStorageType) }.mkString(",")
          var DFDistinct_step = DF_Final.select(dropPartitions.map(name => col(name.get_MyName(getStorageType))): _*).distinct()
          //var DFDistinct_step = DF_Final.select(getPartitionListForSave.map(name => col(name)): _*).distinct()
          
          //add columns cast
          dropPartitions.foreach { x => 
            DFDistinct_step = DFDistinct_step.withColumn( x.get_MyName(getStorageType), DF_Final.col(x.get_MyName(getStorageType)).cast(StringType))  
          }

          if (huemulBigDataGov.DebugMode) {
            huemulBigDataGov.logMessageDebug("show distinct values to delete")
            DFDistinct_step.show()
          }

          //get data
          val DFDistinct = DFDistinct_step.collect()

          //validate num distinct values per column according to definition
          //add columns cast
          dropPartitions.foreach { x =>
            val numDistinctValues = DFDistinct_step.select(x.get_MyName(getStorageType)).distinct().count()
            if (x.getPartitionOneValuePerProcess && numDistinctValues > 1) {
              raiseError(s"huemul_Table Error: N° values in partition wrong for column ${x.get_MyName(getStorageType)}!, expected: 1, real: ${numDistinctValues}",1015)
            }
          }

            //get data values
          DFDistinct.foreach { xData => 
            var pathToDelete: String = ""
            var whereToDelete: ArrayBuffer[String] = new ArrayBuffer[String]
            
            //get columns name in order to create path to delete
            dropPartitions.foreach { xPartitions =>
                val colPartitionName = xPartitions.get_MyName(getStorageType)
                val colData = xData.getAs[String](colPartitionName)
                pathToDelete += s"/${colPartitionName.toLowerCase()}=${colData}"
                whereToDelete.append(s"${colPartitionName} = '${colData}'")
            }
            this.PartitionValue.append(pathToDelete)
            
            LocalControl.NewStep(s"Save: deleting partition ${pathToDelete} ")
            pathToDelete = getFullNameWithPath().concat(pathToDelete)

            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"deleting $pathToDelete")
            //create fileSystema link
            val FullPath = new org.apache.hadoop.fs.Path(pathToDelete)
            val fs = FullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)   
            
            if (this.getStorageType == huemulType_StorageType.DELTA) {
              //FROM 2.4 --> NEW DELETE FOR DATABRICKS             
              val FullPath = new org.apache.hadoop.fs.Path(getFullNameWithPath())
              if (fs.exists(FullPath)) {
                val strSQL_delete: String = s"DELETE FROM delta.`${getFullNameWithPath()}` WHERE ${whereToDelete.mkString(" and ")} "
                if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(strSQL_delete)
                val dfDelete = huemulBigDataGov.spark.sql(strSQL_delete)
              }
            } else {      
              fs.delete(FullPath, true)
            }
          }
        }
        
        
        if (this.getNumPartitions > 0) {
          LocalControl.NewStep("Save: Set num FileParts")
          DF_Final = DF_Final.repartition(this.getNumPartitions)
        }
          
        LocalControl.NewStep("Save: OverWrite partition with new data")
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: ${getFullNameWithPath()} ")     
          
        DF_Final.write.mode(SaveMode.Append).format(_getSaveFormat(this.getStorageType)).partitionBy(getPartitionListForSave:_*).save(getFullNameWithPath())
        
        
        /*
        //Get Partition_Id Values
        LocalControl.NewStep("Save: Validating N° partitions")
        val DFDistinct = DF_Final.select(getPartitionField).distinct().withColumn(getPartitionField, DF_Final.col(getPartitionField).cast(StringType)).collect()
        if (DFDistinct.length != 1){
          raiseError(s"huemul_Table Error: N° values in partition wrong!, expected: 1, real: ${DFDistinct.length}",1015)
        } else {
          
          this.PartitionValue = DFDistinct(0).getAs[String](getPartitionField)
          val FullPath = new org.apache.hadoop.fs.Path(s"${getFullNameWithPath()}/${getPartitionField.toLowerCase()}=${this.PartitionValue}")
          val fs = FullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)   
          
          //if (huemulBigDataGov.GlobalSettings.getBigDataProvider() == huemulType_bigDataProvider.databricks) {
          if (this.getStorageType == huemulType_StorageType.DELTA) {
            //FROM 2.4 --> NEW DELETE FOR DATABRICKS
            LocalControl.NewStep("Save: Drop old partition")
            
            val FullPath = new org.apache.hadoop.fs.Path(getFullNameWithPath())
            if (fs.exists(FullPath)) {
              val strSQL_delete: String = s"DELETE FROM delta.`${getFullNameWithPath}` WHERE ${getPartitionField} = '${this.PartitionValue}' "
              if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(strSQL_delete)
              val dfDelete = huemulBigDataGov.spark.sql(strSQL_delete)
            }
          } else {
            LocalControl.NewStep("Save: Drop old partition")      
            fs.delete(FullPath, true)
          }
          
          if (this.getNumPartitions > 0) {
            LocalControl.NewStep("Save: Set num FileParts")
            DF_Final = DF_Final.repartition(this.getNumPartitions)
          }
          
          LocalControl.NewStep("Save: OverWrite partition with new data")
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: ${FullPath} ")     
          
          DF_Final.write.mode(SaveMode.Append).format(_getSaveFormat(this.getStorageType)).partitionBy(getPartitionField).save(getFullNameWithPath())
                
          //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath()), new FsPermission("770"))
  
        }   
        */
      }
    } catch {
      case e: Exception => 

        if (this.Error_Code == null || this.Error_Code == 0) {
          this.Error_Text = s"huemul_Table Error: write in disk failed. ${e.getMessage}"
          this.Error_Code = 1026
        } else
          this.Error_Text =  this.Error_Text.concat(s"huemul_Table Error: write in disk failed. ${e.getMessage}")

        this.Error_isError = true

        Result = false
        LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
    }
    
    if (Result) {
  
      
      LocalControl.NewStep("Save: Drop Hive table Def")
      try {
        //new from 2.3
        runSQL_dropExternalTable(huemulType_InternalTableType.Normal, getCurrentDataBase(), TableName, false)        
      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }
     
      
        
      try {
        //create table
        LocalControl.NewStep("Save: Create Table in Hive Metadata")
        CreateTableScript = DF_CreateTableScript() 
              
        if (getStorageType == huemulType_StorageType.HBASE) {          
          //new from 2.3
          runSQLexternalTable(CreateTableScript, true)
          
        } else{
          //new from 2.3
          runSQLexternalTable(CreateTableScript, false)
        }
        
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableName : String = internalGetTable(huemulType_InternalTableType.Normal)
        if (getPartitionList.length > 0) {
          if (this.getStorageType != huemulType_StorageType.DELTA) {
            LocalControl.NewStep("Save: Repair Hive Metadata")
            val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableName}"
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(_refreshTable)
            //new from 2.3
            if (getStorageType == huemulType_StorageType.HBASE) {   
              runSQLexternalTable(_refreshTable, true) 
            } else {
              runSQLexternalTable(_refreshTable, false)
            }
          }
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
    Control.RegisterMASTER_UPDATE_isused(this)
    
      
      
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
      if (getStorageType_OldValueTrace == huemulType_StorageType.PARQUET || getStorageType_OldValueTrace == huemulType_StorageType.ORC || 
          getStorageType_OldValueTrace == huemulType_StorageType.DELTA   || getStorageType_OldValueTrace == huemulType_StorageType.AVRO){
        DF_Final.write.mode(SaveMode.Append).partitionBy("mdm_columnname").format(_getSaveFormat(getStorageType_OldValueTrace)).save(getFullNameWithPath_OldValueTrace())
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).partitionBy("mdm_columnname").format(getStorageType_OldValueTrace.toString()).save(getFullNameWithPath_OldValueTrace())
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).format(_StorageType_OldValueTrace).save(GetFullNameWithPath_OldValueTrace())
      }
      else
        DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).option("delimiter", "\t").option("emptyValue", "").option("treatEmptyValuesAsNulls", "false").option("nullValue", "null").format(_getSaveFormat(getStorageType_OldValueTrace)).save(getFullNameWithPath_OldValueTrace())
      
    } catch {
      case e: Exception => 
        this.Error_isError = true
        this.Error_Text = s"huemul_Table OldVT Error: write in disk failed for Old Value Trace result. ${e.getMessage}"
        this.Error_Code = 1052
        Result = false
        LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
    }
    
    if (Result) {
      val sqlDrop01 = s"drop table if exists ${internalGetTable(huemulType_InternalTableType.OldValueTrace)}"
      LocalControl.NewStep("Save: OldVT Result: Drop Hive table Def")
      if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
      try {
        //new from 2.3
        runSQL_dropExternalTable(huemulType_InternalTableType.OldValueTrace,getDataBase(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_DataBase), internalGetTable(huemulType_InternalTableType.OldValueTrace,false), false)         
      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }
     
      
        
      try {
        //create table
        LocalControl.NewStep("Save: OldVT Result: Create Table in Hive Metadata")
        val lscript = DF_CreateTable_OldValueTrace_Script()       
        //new from 2.3
        runSQLexternalTable(lscript, false)        
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableNameOldValueTrace: String = internalGetTable(huemulType_InternalTableType.OldValueTrace)
        
        if (this.getStorageType_OldValueTrace != huemulType_StorageType.DELTA) {
          LocalControl.NewStep("Save: OldVT Result: Repair Hive Metadata")
          val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableNameOldValueTrace}"
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"RMSCK REPAIR TABLE ${_tableNameOldValueTrace}")
          //new from 2.3
          runSQLexternalTable(_refreshTable, false)        
        }
        
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
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).format(this.getStorageType_DQResult.toString()).partitionBy("dq_control_id").save(getFullNameWithPath_DQ())
        DF_Final.write.mode(SaveMode.Append).format(_getSaveFormat(this.getStorageType_DQResult)).partitionBy("dq_control_id").save(getFullNameWithPath_DQ())
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
      val sqlDrop01 = s"drop table if exists ${internalGetTable(huemulType_InternalTableType.DQ)}"
      LocalControl.NewStep("Save: Drop Hive table Def")
      if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
      try {
        //new from 2.3
        runSQL_dropExternalTable(huemulType_InternalTableType.DQ,getDataBase(huemulBigDataGov.GlobalSettings.DQError_DataBase), internalGetTable(huemulType_InternalTableType.DQ,false), false)
          
      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }
     
      
        
      try {
        //create table
        LocalControl.NewStep("Save: Create Table in Hive Metadata")
        val lscript = DF_CreateTable_DQ_Script() 
       
        //new from 2.3
        runSQLexternalTable(lscript, false)       
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableNameDQ: String = internalGetTable(huemulType_InternalTableType.DQ)
        
        if (this.getStorageType_DQResult != huemulType_StorageType.DELTA) {
          val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableNameDQ}"
          LocalControl.NewStep("Save: Repair Hive Metadata")
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(_refreshTable)
          //new from 2.3
          runSQLexternalTable(_refreshTable, false)
        }
        
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
  
  private def runSQL_dropExternalTable(tableType: huemulType_InternalTableType, databaseName:String, tableName:String, onlyHBASE: Boolean) {
    //new from 2.3
    val sqlDrop01 = s"drop table if exists ${internalGetTable(tableType)}"
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
      
    if (onlyHBASE) {
      /**** D R O P   F O R   H B A S E ******/
      
      //FOR SPARK--> NOT SUPPORTED FOR SPARK
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_SPARK.getActiveForHBASE()){
        //comment two next lines: get error when doesn't have serDe
        //val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(databaseName).collect()
        //  if (TablesListFromHive.filter { x => x.name.toLowerCase() == tableName.toLowerCase()  }.length > 0) 
            huemulBigDataGov.spark.sql(sqlDrop01)
      }
      
      //FOR HIVE
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getActiveForHBASE())
        huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov).ExecuteJDBC_NoResulSet(sqlDrop01)
        
      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HWC.getActiveForHBASE())
        huemulBigDataGov.getHive_HWC.execute_NoResulSet(sqlDrop01)
    } else {
      /**** D R O P   F O R    O T H E R S ******/
      
      //FOR SPARK
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_SPARK.getActive()) {
        //comment two next lines: get error when doesn't have serDe
        //val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(databaseName).collect()
        //  if (TablesListFromHive.filter { x => x.name.toLowerCase() == tableName.toLowerCase()  }.length > 0) 
            huemulBigDataGov.spark.sql(sqlDrop01)
      }
      
      //FOR HIVE
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getActive())
        huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov).ExecuteJDBC_NoResulSet(sqlDrop01)
        
      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HWC.getActive())
        huemulBigDataGov.getHive_HWC.execute_NoResulSet(sqlDrop01)
    }
    
  }
  
  
  private def runSQLexternalTable(sqlSentence:String, onlyHBASE: Boolean) {
    //new from 2.3
    if (onlyHBASE) {
      /**** D R O P   F O R   H B A S E ******/
      
      //FOR SPARK --> NOT SUPPORTED FOR SPARK
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_SPARK.getActiveForHBASE())
        huemulBigDataGov.spark.sql(sqlSentence)
      
      //FOR HIVE
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getActiveForHBASE())
          huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov).ExecuteJDBC_NoResulSet(sqlSentence)
          
      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HWC.getActiveForHBASE())
          huemulBigDataGov.getHive_HWC.execute_NoResulSet(sqlSentence)
    } else {
      /**** D R O P   F O R   O T H E R ******/
      
      //FOR SPARK
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_SPARK.getActive())
        huemulBigDataGov.spark.sql(sqlSentence)
      
      //FOR HIVE
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getActive())
        huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov).ExecuteJDBC_NoResulSet(sqlSentence)
    
      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HWC.getActive())
        huemulBigDataGov.getHive_HWC.execute_NoResulSet(sqlSentence)
    
    }
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

