package com.huemulsolutions.bigdata.tables

import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
//import java.lang.Long


import scala.collection.mutable._
import HuemulTypeTables._
import HuemulTypeStorageType._
//import com.huemulsolutions.bigdata._


import com.huemulsolutions.bigdata.dataquality.HuemulDataQuality
import com.huemulsolutions.bigdata.dataquality.HuemulDataQualityResult
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control.HuemulControl
import com.huemulsolutions.bigdata.control.HuemulTypeFrequency
import com.huemulsolutions.bigdata.control.HuemulTypeFrequency._

import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqQueryLevel
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDqNotification
import com.huemulsolutions.bigdata.dataquality.HuemulDqRecord
import com.huemulsolutions.bigdata.datalake.HuemulDataLake
import com.huemulsolutions.bigdata.tables.HuemulTypeInternalTableType.HuemulTypeInternalTableType

//import com.huemulsolutions.bigdata.control.huemulType_Frequency.huemulType_Frequency



class HuemulTable(huemulBigDataGov: HuemulBigDataGovernance, control: HuemulControl) extends HuemulTableDq with Serializable  {


  if (control == null) sys.error("Control is null in huemul_DataFrame")

  /*  ********************************************************************************
   *****   T A B L E   P R O P E R T I E S    ****************************************
   ******************************************************************************** */
  /**
   Table Name
   */
  val tableName : String= this.getClass.getSimpleName.replace("$", "") // ""
  huemulBigDataGov.logMessageInfo(s"HuemulControl:    table instance: $tableName")

  def setDataBase(value: ArrayBuffer[HuemulKeyValuePath]) {
    if (definitionIsClose)
      this.raiseError("You can't change value of DataBase, definition is close", 1033)
    else
      _dataBase = value
  }
  private var _dataBase: ArrayBuffer[HuemulKeyValuePath] = _

  /**
   Type of Table (Reference; Master; Transaction
   */
  def setTableType(value: HuemulTypeTables) {
    if (definitionIsClose)
      this.raiseError("You can't change value of TableType, definition is close", 1033)
    else
      _tableType = value
  }
  def getTableType: HuemulTypeTables = { _tableType}
  private var _tableType : HuemulTypeTables = HuemulTypeTables.Transaction

  private var _pkExternalCode: String = "HUEMUL_DQ_002"
  def getPkExternalCode: String = { if (_pkExternalCode==null) "HUEMUL_DQ_002" else _pkExternalCode }
  def setPkExternalCode(value: String) {
    _pkExternalCode = value
  }

  private var _pkNotification:HuemulTypeDqNotification = HuemulTypeDqNotification.ERROR

  /** Set Primary Key notification level: ERROR(default),WARNING, WARNING_EXCLUDE
   * @author  christian.sattler@gmail.com; update: sebas_rod@hotmail.com
   * @since   2.[6]
   * @group   column_attribute
   *
   * @param value   HuemulTypeDQNotification ERROR,WARNING, WARNING_EXCLUDE
   * @return        huemul_Columns
   * @note 2020-06-17 by sebas_rod@hotmail.com: add huemul_Table as return
   */
  def setPkNotification(value: HuemulTypeDqNotification ): HuemulTable = {
    _pkNotification = value
    this
  }

  private def _storageTypeDefault = HuemulTypeStorageType.PARQUET
  /**
   Save DQ Result to disk, only if DQ_SaveErrorDetails in GlobalPath is true
   */
  def setSaveDQResult(value: Boolean) {
    if (definitionIsClose)
      this.raiseError("You can't change value of SaveDQResult, definition is close", 1033)
    else
      _saveDQResult = value
  }
  def getSaveDQResult: Boolean = { _saveDQResult}
  private var _saveDQResult : Boolean = true

  /**
   Save Backup to disk, only if MDM_SaveBackup in GlobalPath is false
   */
  def setSaveBackup(value: Boolean) {
    if (definitionIsClose)
      this.raiseError("You can't change value of SaveBackup, definition is close", 1033)
    else
      _saveBackup = value
  }
  def getSaveBackup: Boolean = { _saveBackup}
  private var _saveBackup : Boolean = false
  def _getBackupPath: String = { _BackupPath }
  private var _BackupPath: String = ""

  /**
   Type of Persistent storage (parquet, csv, json)
   */
  def setStorageType(value: HuemulTypeStorageType) {
    if (definitionIsClose)
      this.raiseError("You can't change value of StorageType, definition is close", 1033)
    else
      _storageType = value
  }
  def getStorageType: HuemulTypeStorageType = { _storageType}
  private var _storageType : HuemulTypeStorageType = _


  /**
   Type of Persistent storage (parquet, csv, json) for DQ
   */
  def setStorageTypeDqResult(value: HuemulTypeStorageType) {
    if (_storageTypeDqResult == HuemulTypeStorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)

    if (definitionIsClose)
      this.raiseError("You can't change value of StorageType_DQResult, definition is close", 1033)
    else
      _storageTypeDqResult = value
  }
  def getStorageTypeDqResult: HuemulTypeStorageType = {
    if (_storageTypeDqResult == HuemulTypeStorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)

    //return
    if (_storageTypeDqResult != null)
      _storageTypeDqResult
    else if (_storageTypeDqResult == null && getStorageType == HuemulTypeStorageType.HBASE )
      _storageTypeDefault
    else
      getStorageType
  }
  private var _storageTypeDqResult : HuemulTypeStorageType = _

  /**
   Type of Persistent storage (parquet, csv, json) for DQ
   */
  def setStorageTypeOldValueTrace(value: HuemulTypeStorageType) {
    if (_StorageTypeOldValueTrace == HuemulTypeStorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for OldValueTraceTable", 1063)

    if (definitionIsClose)
      this.raiseError("You can't change value of StorageType_OldValueTrace, definition is close", 1033)
    else
      _StorageTypeOldValueTrace = value
  }
  def getStorageTypeOldValueTrace: HuemulTypeStorageType = {
    if (_StorageTypeOldValueTrace == HuemulTypeStorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for OldValueTraceTable", 1063)

    //return
    if (_StorageTypeOldValueTrace != null) _StorageTypeOldValueTrace
        else if (_StorageTypeOldValueTrace == null && getStorageType == HuemulTypeStorageType.HBASE ) _storageTypeDefault
        else getStorageType
  }
  private var _StorageTypeOldValueTrace : HuemulTypeStorageType = _

  //val _StorageType_OldValueTrace: String = "parquet"  //csv, json, parquet, orc
  /**
    Table description
   */
  def setDescription(value: String) {
    if (definitionIsClose)
      this.raiseError("You can't change value of Description, definition is close", 1033)
    else
      _description = value
  }
  def getDescription: String = { _description}
  private var _description   : String= ""

  /**
    Responsible contact in IT
   */
  def setItResponsibleName(value: String) {
    if (definitionIsClose)
      this.raiseError("You can't change value of IT_ResponsibleName, definition is close", 1033)
    else
      _itResponsibleName = value
  }
  def getItResponsibleName: String = { _itResponsibleName}
  private var _itResponsibleName   : String= ""

  /**
    Responsible contact in Business (Name & Area)
   */
  def setBusinessResponsibleName(value: String) {
    if (definitionIsClose)
      this.raiseError("You can't change value of Business_ResponsibleName, definition is close", 1033)
    else
      _BusinessResponsibleName = value
  }
  def getBusinessResponsibleName: String = { _BusinessResponsibleName}
  private var _BusinessResponsibleName   : String= ""


  private var _partitionFieldValueTemp: String = ""
  /**
    Fields used to partition table
   */
  @deprecated("this method will be removed, instead use huemul_Columns.setPartitionColumn(position: Integer, dropBeforeInsert: Boolean)", "3.0")
  def setPartitionField(value: String) {
    if (definitionIsClose)
      this.raiseError("You can't change value of PartitionField, definition is close", 1033)
    else {
      _partitionFieldValueTemp = value
    }
  }

  //from 2.6 --> return array with partitionColumns defined (ordered)
  def getPartitionList: Array[HuemulColumns] = {
     val partListOrder = getAllDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                                     x.get(this).isInstanceOf[HuemulColumns] &&
                                                     x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition >= 1
                                             }.sortBy { x => x.setAccessible(true)
                                                             x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition}

     //val b2 = partListOrder.ma
     //                                        }.map { x => x.setAccessible(true)
     //                                                        x.get(this).asInstanceOf[huemul_Columns]}
     val result = partListOrder.map { x => x.setAccessible(true)
       x.get(this).asInstanceOf[HuemulColumns]
     }


     result

     //return getColumns().filter { x => x.getPartitionColumnPosition >= 1 }
     //                                     .sortBy { x => x.getPartitionColumnPosition }

  }

  //from 2.6 --> change code, get results from huemul_Columns.getPartitionColumnPosition
  def getPartitionField: String = {
      getPartitionList.map { x => x.getMyName(this.getStorageType) }.mkString(",")

  }

  //from 2.6 --> method to save data
  private def getPartitionListForSave: Array[String] = {
     getPartitionList.map { x => x.getMyName(this.getStorageType) }

  }

  //From 2.6 --> drop this attribute
  //private var _PartitionField   : String= null

  /**
    Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  def setLocalPath(value: String) {
    if (definitionIsClose)
      this.raiseError("You can't change value of LocalPath, definition is close", 1033)
    else
      _localPath = value
  }
  private var _localPath   : String= ""
  def getLocalPath: String = { if (_localPath == null) "null/"
                                     else if (_localPath.takeRight(1) != "/") _localPath.concat("/")
                                     else _localPath }


  def setGlobalPaths(value: ArrayBuffer[HuemulKeyValuePath]) {
    if (definitionIsClose)
      this.raiseError("You can't change value of GlobalPaths, definition is close", 1033)
    else
      _globalPaths = value
  }
  def getGlobalPaths: ArrayBuffer[HuemulKeyValuePath] = { _globalPaths}
  private var _globalPaths: ArrayBuffer[HuemulKeyValuePath] = _

  def whoCanRunExecuteFullAddAccess(className: String, packageName: String) {
    if (definitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeFull, definition is close", 1033)
    else
      _whoCanRunExecuteFull.addAccess(className, packageName)
  }
  def getWhoCanRunExecuteFull: HuemulAuthorization = { _whoCanRunExecuteFull}
  private val _whoCanRunExecuteFull: HuemulAuthorization = new HuemulAuthorization()

  def whoCanRunExecuteOnlyInsertAddAccess(className: String, packageName: String) {
    if (definitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeOnlyInsert, definition is close", 1033)
    else
      _whoCanRunExecuteOnlyInsert.addAccess(className, packageName)
  }
  def getWhoCanRunExecuteOnlyInsert: HuemulAuthorization = { _whoCanRunExecuteOnlyInsert}
  private val _whoCanRunExecuteOnlyInsert: HuemulAuthorization = new HuemulAuthorization()

  def whoCanRunExecuteOnlyUpdateAddAccess(className: String, packageName: String) {
    if (definitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeOnlyUpdate, definition is close", 1033)
    else
      _whoCanRunExecuteOnlyUpdate.addAccess(className, packageName)
  }
  def getWhoCanRunExecuteOnlyUpdate: HuemulAuthorization = { _whoCanRunExecuteOnlyUpdate}
  private val _whoCanRunExecuteOnlyUpdate: HuemulAuthorization = new HuemulAuthorization()

  /**
    Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  def setFrequency(value: HuemulTypeFrequency) {
    if (definitionIsClose)
      this.raiseError("You can't change value of Frequency, definition is close", 1033)
    else
      _frequency = value
  }
  private var _frequency: HuemulTypeFrequency = _
  def getFrequency: HuemulTypeFrequency = { _frequency}

  /**
   * SaveDQErrorOnce: false (default value) to save DQ result to disk only once (all together)
   * false to save DQ result to disk independently (each DQ error or warning)
  */
  def setSaveDqErrorOnce(value: Boolean) {
    if (definitionIsClose)
      this.raiseError("You can't change value of SaveDQErrorOnce, definition is close", 1033)
    else
      _saveDqErrorOnce = value
  }
  private var _saveDqErrorOnce: Boolean = false
  def getSaveDQErrorOnce: Boolean = { _saveDqErrorOnce}

  //from 2.3 --> #85 add new options to determine whether create External Table
  //var createExternalTable_Normal: Boolean = true
  //var createExternalTable_DQ: Boolean = true
  //var createExternalTable_OVT: Boolean = true

  private var _statusDeleteAsDeleted: Boolean = true

  /**
   * set row missing behavior in reference and master tables (true: mdm_status = -1, false: mdm_status = old_mdm_status
   * @return
   */
  def setRowStatusDeleteAsDeleted(value: Boolean): HuemulTable = {
    _statusDeleteAsDeleted = value

    this
  }

  /**
   * get row missing behavior in reference and master tables
   * @return
   */
  def getRowStatusDeleteAsDeleted: Boolean = {
    _statusDeleteAsDeleted
  }

  /**
   * Automatically map query names
   */
  def setMappingAuto() {
    getAllDeclaredFields(OnlyUserDefined = true).foreach { x =>
      x.setAccessible(true)

      //Nombre de campos
      x.get(this) match {
        case dataField: HuemulColumns =>
          dataField.setMapping (dataField.getMyName (this.getStorageType) )
        case _ =>
      }

      /*
      if (x.get(this).isInstanceOf[huemul_Columns]) {
        val DataField = x.get(this).asInstanceOf[huemul_Columns]
        DataField.setMapping(DataField.getMyName(this.getStorageType))
      }
       */
    }
  }


  /**
   DataQuality: max N° records, null does'nt apply  DQ , 0 value doesn't accept new records (raiseError if new record found)
   If table is empty, DQ doesn't apply
   */
  def setDQMaxNewRecordsNum(value: java.lang.Long) {
    if (definitionIsClose)
      this.raiseError("You can't change value of DQ_MaxNewRecords_Num, definition is close", 1033)
    else
      _dQMaxNewRecordsNum = value
  }
  def getDqMaxNewRecordsNum: java.lang.Long = { _dQMaxNewRecordsNum}
  private var _dQMaxNewRecordsNum: java.lang.Long = _
  /**
   DataQuality: Max % new records vs old records, null does'n apply DQ, 0% doesn't accept new records (raiseError if new record found), 100%-> 0.1 accept double old records)
   */
  def setDqMaxNewRecordsPerc(value: Decimal) {
    if (definitionIsClose)
      this.raiseError("You can't change value of DQ_MaxNewRecords_Perc, definition is close", 1033)
    else
      _dQMaxNewRecordsPerc = value
  }
  def getDqMaxNewRecordsPerc: Decimal = { _dQMaxNewRecordsPerc}
  private var _dQMaxNewRecordsPerc: Decimal = _

  //V1.3 --> set num partitions with repartition
  /**
   Set num of partitions (repartition function used). Value null for default behavior
   */
  def setNumPartitions(value: Integer) {
    if (definitionIsClose)
      this.raiseError("You can't change value of NumPartitions, definition is close", 1033)
    else
      _numPartitions = value
  }
  def getNumPartitions: Integer = { _numPartitions}
  private var _numPartitions : Integer = 0

  private val numPartitionsForDQFiles: Integer = 2

  //FROM 2.5
  //ADD AVRO SUPPORT
  private var _avroFormat: String = huemulBigDataGov.globalSettings.getAvroFormat
  def getAvroFormat: String = {  _avroFormat}
  def setAvroFormat(value: String) {_avroFormat = value}

  /*
  private var _Tablecodec_compression: String = null
  def getTableCodec_compression(): String = {return  _Tablecodec_compression}
  def setTableCodec_compression(value: String) {_Tablecodec_compression = value}
  */

  private def _getSaveFormat(storageType: HuemulTypeStorageType): String = {
    //return
    if (storageType == HuemulTypeStorageType.AVRO)
      this.getAvroFormat
    else
      storageType.toString

  }

  /****** METODOS DEL LADO DEL "USUARIO" **************************/

  private var autoCast: Boolean = true
  def setAutoCast(value: Boolean) {autoCast = value}

  private var applyDistinct: Boolean = true
  def setApplyDistinct(value: Boolean) {applyDistinct = value}


  //private var Table_id: String = ""



  //private var CreateInHive: Boolean = true
  private var createTableScript: String = ""
  private var partitionValue: ArrayBuffer[String] = new ArrayBuffer[String]  //column,value
  def getPartitionValue: String = { if (partitionValue.nonEmpty) partitionValue(0) else ""}
  var _tablesUseId: String = _
  //private var DF_DQErrorDetails: DataFrame = _

  /*  ********************************************************************************
   *****   F I E L D   P R O P E R T I E S    ****************************************
   ******************************************************************************** */
  private val mdmNewValueInternalName2: String = "MDM_newValue"
  private val mdmOldValueInternalName2: String = "MDM_oldValue"
  private val mdmAutoIncInternalName2: String = "MDM_AutoInc"
  private val processExecIdInternalName2: String= "processExec_id"
  private val mdmFhNewInternalName2: String = "MDM_fhNew"
  private val mdmProcessNewInternalName2: String = "MDM_ProcessNew"
  private val mdmFhChangeInternalName2: String = "MDM_fhChange"
  private val mdmProcessChangeInternalName2: String = "MDM_ProcessChange"
  private val mdmStatusRegInternalName2: String = "MDM_StatusReg"
  private val mdmHashInternalName2: String = "MDM_hash"
  private val mdmColumnNameInternalName2: String = "mdm_columnname"
  private val hsRowKeyInternalName2: String = "hs_rowKey"

  private val nameForMdmNewValue: String = mdmNewValueInternalName2
  /*
  private def setNameForMDM_newValue(value: String): huemul_Table = {
    nameForMDM_newValue = value
    this
  }

   */
  private def getNameForMdmNewValue: String = nameForMdmNewValue

  private val nameForMdmOldValue: String = mdmOldValueInternalName2
  /*
  private def setNameForMDM_oldValue(value: String): huemul_Table = {
    nameForMDM_oldValue = value
    this
  }

   */
  private def getNameForMdmOldValue: String = nameForMdmOldValue

  private val nameForMdmAutoInc: String = mdmAutoIncInternalName2
  /*
  private def setNameForMDM_AutoInc(value: String): huemul_Table = {
    nameForMDM_AutoInc = value
    this
  }

   */
  private def getNameForMdmAutoInc: String = nameForMdmAutoInc

  private val nameForProcessExecId: String = processExecIdInternalName2
  /*
  private def setNameForprocessExec_id(value: String): huemul_Table = {
    nameForprocessExec_id = value
    this
  }

   */
  private def getNameForProcessExecId: String = nameForProcessExecId


  private var nameForMDMFhNew: String = mdmFhNewInternalName2
  def setNameForMdmFhNew(value: String): HuemulTable = {
    nameForMDMFhNew = value
    this
  }
  def getNameForMdmFhNew: String = nameForMDMFhNew

  private var nameForMdmProcessNew: String = mdmProcessNewInternalName2
  def setNameForMDM_ProcessNew(value: String): HuemulTable = {
    nameForMdmProcessNew = value
    this
  }
  def getNameForMdmProcessNew: String = nameForMdmProcessNew

  private var nameForMdmFhChange: String = mdmFhChangeInternalName2
  def setNameForMdmFhChange(value: String): HuemulTable = {
    nameForMdmFhChange = value
    this
  }
  def getNameForMdmFhChange: String = nameForMdmFhChange

  private var nameForMdmProcessChange: String = mdmProcessChangeInternalName2
  def setNameForMdmProcessChange(value: String): HuemulTable = {
    nameForMdmProcessChange = value
    this
  }
  def getNameForMdmProcessChange: String = nameForMdmProcessChange

  private var nameForMdmStatusReg: String = mdmStatusRegInternalName2
  def setNameForMDM_StatusReg(value: String): HuemulTable = {
    nameForMdmStatusReg = value
    this
  }
  def getNameForMdmStatusReg: String = nameForMdmStatusReg

  private var nameForMdmHash: String = mdmHashInternalName2
  def setNameForMDM_hash(value: String): HuemulTable = {
    nameForMdmHash = value
    this
  }
  def getNameForMdmHash: String = nameForMdmHash

  private val nameForMdmColumnName: String = mdmColumnNameInternalName2
  /*
  private def setNameFormdm_columnname(value: String): huemul_Table = {
    nameForMdm_columnname = value
    this
  }

   */
  private def getNameForMdmColumnName: String = nameForMdmColumnName




  val mdmNewValue:HuemulColumns = new HuemulColumns (StringType, true, "New value updated in table", false).setHBaseCatalogMapping("loginfo")
  val mdmOldValue:HuemulColumns = new HuemulColumns (StringType, true, "Old value", false).setHBaseCatalogMapping("loginfo")
  val mdmAutoInc:HuemulColumns = new HuemulColumns (LongType, true, "auto incremental for version control", false).setHBaseCatalogMapping("loginfo")
  val processExecId:HuemulColumns = new HuemulColumns (StringType, true, "Process Execution Id (control model)", false).setHBaseCatalogMapping("loginfo")

  val mdmFhNew:HuemulColumns = new HuemulColumns (TimestampType, true, "Fecha/hora cuando se insertaron los datos nuevos", false).setHBaseCatalogMapping("loginfo")
  val mdmProcessNew:HuemulColumns = new HuemulColumns (StringType, false, "Nombre del proceso que insertó los datos", false).setHBaseCatalogMapping("loginfo")
  val mdmFhChange:HuemulColumns = new HuemulColumns (TimestampType, false, "fecha / hora de último cambio de valor en los campos de negocio", false).setHBaseCatalogMapping("loginfo")
  val mdmProcessChange:HuemulColumns = new HuemulColumns (StringType, false, "Nombre del proceso que cambió los datos", false).setHBaseCatalogMapping("loginfo")
  val mdmStatusReg:HuemulColumns = new HuemulColumns (IntegerType, true, "indica si el registro fue insertado en forma automática por otro proceso (1), o fue insertado por el proceso formal (2), si está eliminado (-1)", false).setHBaseCatalogMapping("loginfo")
  val mdmHash:HuemulColumns = new HuemulColumns (StringType, true, "Valor hash de los datos de la tabla", false).setHBaseCatalogMapping("loginfo")

  val mdmColumnName:HuemulColumns = new HuemulColumns (StringType, true, "Column Name", false).setHBaseCatalogMapping("loginfo")

  //from 2.2 --> add rowKey compatibility with HBase
  val hsRowKey:HuemulColumns = new HuemulColumns (StringType, true, "Concatenated PK", false).setHBaseCatalogMapping("loginfo")

  var additionalRowsForDistinct: String = ""
  private var definitionIsClose: Boolean = false

  /***
   * from Global path (small files, large files, DataBase, etc)
   */
  def globalPath()  : String= {
     getPath(_globalPaths)
  }

  def globalPath(ManualEnvironment: String)  : String= {
     getPath(_globalPaths, ManualEnvironment)
  }


  /**
   * Get Fullpath hdfs = GlobalPaths + LocalPaths + TableName
   */
  def getFullNameWithPath: String = {
     globalPath + getLocalPath + tableName
  }

  /**
   * Get Fullpath hdfs for DQ results = GlobalPaths + DQError_Path + TableName + "_dq"
   */
  def getFullNameWithPathDq: String = {
     this.getPath(huemulBigDataGov.globalSettings.dqErrorPath) + this.getDataBase(this._dataBase) + '/' + getLocalPath + tableName + "_dq"
  }

  /**
   * Get Fullpath hdfs for _oldvalue results = GlobalPaths + DQError_Path + TableName + "_oldvalue"
   */
  def getFullNameWithPathOldValueTrace: String = {
     this.getPath(huemulBigDataGov.globalSettings.mdmOldValueTracePath) + this.getDataBase(this._dataBase) + '/' + getLocalPath + tableName + "_oldvalue"
  }

   /**
   * Get Fullpath hdfs for backpu  = Backup_Path + database + TableName + "_backup"
   */
  def getFullNameWithPathBackup(control_id: String) : String = {
     this.getPath(huemulBigDataGov.globalSettings.mdmBackupPath) + this.getDataBase(this._dataBase) + '/' + getLocalPath + 'c' + control_id + '/' + tableName + "_backup"
  }

  def getFullNameWithPath2(ManualEnvironment: String) : String = {
     globalPath(ManualEnvironment) + getLocalPath + tableName
  }

  def getFullPath: String = {
     globalPath + getLocalPath
  }


  /**
   * Return DataBaseName.TableName
   */
  private var _TableWasRegistered: Boolean = false
  def getTable: String = {
    if (!_TableWasRegistered) {
      control.RegisterMASTER_USE(this)
      _TableWasRegistered = true
    }

     internalGetTable(HuemulTypeInternalTableType.Normal)
  }

  //new from 2.1
  /**
   * Return DataBaseName.TableName for DQ
   */
  def getTableDq: String = {
     internalGetTable(HuemulTypeInternalTableType.DQ)
  }

  //new from 2.1
  /**
   * Return DataBaseName.TableName for OldValueTrace
   */
  def getTableOldValueTrace: String = {
     internalGetTable(HuemulTypeInternalTableType.OldValueTrace)
  }

  //new from 2.2
  /**
   * Define HBase Table and namespace
   */
  private var _hbaseNamespace: String = _
  private var _hbaseTableName: String = _
  def setHBaseTableName(namespace: String, tableName: String = null) {
    _hbaseNamespace = namespace
    _hbaseTableName = tableName
  }

  def getHBaseNamespace(internalTableType: HuemulTypeInternalTableType): String = {
    var valueName: String = null
    if (internalTableType == HuemulTypeInternalTableType.DQ) {
      valueName = getDataBase(huemulBigDataGov.globalSettings.dqErrorDataBase)
    } else if (internalTableType == HuemulTypeInternalTableType.Normal) {
      valueName = if ( _hbaseNamespace == null) this.getDataBase(this._dataBase) else _hbaseNamespace
    } else if (internalTableType == HuemulTypeInternalTableType.OldValueTrace) {
      valueName = getDataBase(huemulBigDataGov.globalSettings.mdmOldValueTraceDataBase)
    } else {
      raiseError(s"huemul_Table Error: Type '$internalTableType' doesn't exist in InternalGetTable method (getHBaseNamespace)", 1059)
    }

      valueName

  }

  def getHBaseTableName(internalTableType: HuemulTypeInternalTableType): String = {
    var valueName: String = if (_hbaseTableName == null) this.tableName else _hbaseTableName
    if (internalTableType == HuemulTypeInternalTableType.DQ) {
      valueName =  valueName + "_dq"
    } else if (internalTableType == HuemulTypeInternalTableType.Normal) {
      valueName = valueName
    } else if (internalTableType == HuemulTypeInternalTableType.OldValueTrace) {
      valueName = valueName + "_oldvalue"
    } else {
      raiseError(s"huemul_Table Error: Type '$internalTableType' doesn't exist in InternalGetTable method (getHBaseTableName)", 1060)
    }

     valueName
  }

  /**
   * get Catalog for HBase mapping
   */

  def getHBaseCatalog(tableType: HuemulTypeInternalTableType): String = {

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    //create fields
    var fields: String = s""""${if (_numPkColumns == 1) _hBasePkColumn else hsRowKeyInternalName2}":{"cf":"rowkey","col":"key","type":"string"} """
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns]
    } foreach { x =>
      x.setAccessible(true)
      val dataField = x.get(this).asInstanceOf[HuemulColumns]
      val colMyName = dataField.getMyName(this.getStorageType)

      if (!(dataField.getIsPk && _numPkColumns == 1)) {
        //fields = fields + s""", \n "${x.getName}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}","type":"${dataField.getHBaseDataType()}"} """
        fields = fields + s""", \n "$colMyName":{"cf":"${dataField.getHBaseCatalogFamily}","col":"${dataField.getHBaseCatalogColumn}","type":"${dataField.getHBaseDataType}"} """

        if (tableType != HuemulTypeInternalTableType.OldValueTrace) {
          if (dataField.getMdmEnableOldValue)
            fields = fields + s""", \n "$colMyName${__old}":{"cf":"${dataField.getHBaseCatalogFamily}","col":"${dataField.getHBaseCatalogColumn}${__old}","type":"${dataField.getHBaseDataType}"} """
          if (dataField.getMdmEnableDTLog)
            fields = fields + s""", \n "$colMyName${__fhChange}":{"cf":"${dataField.getHBaseCatalogFamily}","col":"${dataField.getHBaseCatalogColumn}${__fhChange}","type":"string"} """
          if (dataField.getMdmEnableProcessLog)
            fields = fields + s""", \n "$colMyName${__ProcessLog}":{"cf":"${dataField.getHBaseCatalogFamily}","col":"${dataField.getHBaseCatalogColumn}${__ProcessLog}","type":"string"} """
        }

      }
    }

    //create struct
    val result = s"""{
        "table":{"namespace":"${getHBaseNamespace(tableType)}", "name":"${getHBaseNamespace(tableType)}:${getHBaseTableName(tableType)}"},
        "rowkey":"key",
        "columns":{$fields
         }
      }""".stripMargin

     result
  }


  /**
   * get Catalog for HBase mapping
   */
  def getHBaseCatalogForHive(tableType: HuemulTypeInternalTableType): String = {
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

   //WARNING!!! Any changes you make to this code, repeat it in getColumns_CreateTable
    //create fields
    var fields: String = s":key"
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns]
    } foreach { x =>
      x.setAccessible(true)
      val dataField = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = dataField.getMyName(this.getStorageType).toLowerCase()
      val _dataType  = dataField.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)

      if (!(dataField.getIsPk && _numPkColumns == 1) && (_colMyName != hsRowKeyInternalName2.toLowerCase())) {

        if (tableType == HuemulTypeInternalTableType.DQ) {
          //create StructType
          if ("dq_control_id".toLowerCase() != _colMyName) {
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}#b"""
          }
        }
        else if (tableType == HuemulTypeInternalTableType.OldValueTrace) {
          //create StructType mdm_columnname
          if (getNameForMdmColumnName.toLowerCase() != _colMyName) {
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}#b"""
          }
        }
        else {
          //create StructType
          fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}${if (_dataType == TimestampType || _dataType == DateType ||  _dataType.typeName.toLowerCase().contains("decimal")) "" else "#b"}"""

        }

        if (tableType != HuemulTypeInternalTableType.OldValueTrace) {
          if (dataField.getMdmEnableOldValue)
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}${__old}${if (_dataType == TimestampType || _dataType == DateType ||  _dataType.typeName.toLowerCase().contains("decimal") ) "" else "#b"}"""
          if (dataField.getMdmEnableDTLog)
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}${__fhChange}"""
          if (dataField.getMdmEnableProcessLog)
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}${__ProcessLog}#b"""
        }


      }
    }


     fields
  }


  //get PK for HBase Tables rowKey
  private var _hBaseRowKeyCalc: String = ""
  private var _hBasePkColumn: String = ""

  private var _numPkColumns: Int = 0





  private def internalGetTable(internalTableType: HuemulTypeInternalTableType, withDataBase: Boolean = true): String = {
    var _getTable: String = ""
    var _database: String = ""
    var _tableName: String = ""
    if (internalTableType == HuemulTypeInternalTableType.DQ) {
      _database = getDataBase(huemulBigDataGov.globalSettings.dqErrorDataBase)
      _tableName = tableName + "_dq"
    } else if (internalTableType == HuemulTypeInternalTableType.Normal) {
      _database = getDataBase(_dataBase)
      _tableName = tableName
    } else if (internalTableType == HuemulTypeInternalTableType.OldValueTrace) {
      _database = getDataBase(huemulBigDataGov.globalSettings.mdmOldValueTraceDataBase)
      _tableName = tableName + "_oldvalue"
    } else {
      raiseError("huemul_Table DQ Error: Type '${Type}' doesn't exist in InternalGetTable method", 1051)
    }

    if (withDataBase)
      _getTable = s"${_database}.${_tableName}"
    else
      _getTable = s"${_tableName}"

     _getTable
  }

  def getCurrentDataBase: String = {
     s"${getDataBase(_dataBase)}"
  }

  /*
  /**
   * Return DataBaseName.TableName
   */
  private def internalGetTable(ManualEnvironment: String): String = {
     s"${getDataBase(this._DataBase, ManualEnvironment)}.$TableName"
  }
  */

  def getDataBase(Division: ArrayBuffer[HuemulKeyValuePath]): String = {
     huemulBigDataGov.globalSettings.getDataBase(huemulBigDataGov, Division)
  }

  def getDataBase(Division: ArrayBuffer[HuemulKeyValuePath], ManualEnvironment: String): String = {
     huemulBigDataGov.globalSettings.getDataBase(Division, ManualEnvironment)
  }

  def getPath(Division: ArrayBuffer[HuemulKeyValuePath]): String = {
     huemulBigDataGov.globalSettings.getPath(huemulBigDataGov, Division)
  }

  def getPath(Division: ArrayBuffer[HuemulKeyValuePath], ManualEnvironment: String): String = {
     huemulBigDataGov.globalSettings.getPath(Division, ManualEnvironment)
  }



  /**
   get execution's info about rows and DF
   */
  var dataFrameHuemul: HuemulDataFrame = new HuemulDataFrame(huemulBigDataGov, control)


  var errorIsError: Boolean = false
  var errorText: String = ""
  var errorCode: Integer = _
  //var HasColumns: Boolean = false
  var hasPk: Boolean = false

  def applyTableDefinition(): Boolean = {
    if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: starting ApplyTableDefinition")

    //if (this.getPartitionField == null)
    //  _PartitionField = ""

    if (this._globalPaths == null)
      raiseError(s"huemul_Table Error: GlobalPaths must be defined",1000)

    if (this._localPath == null)
      raiseError(s"huemul_Table Error: LocalPath must be defined",1001)

    if (this.getStorageType == null)
      raiseError(s"huemul_Table Error: StorageType must be defined",1002)

    if (this.getStorageTypeDqResult == null)
      raiseError(s"huemul_Table Error: HBase is not available for DQ Table",1061)

    if (this.getStorageTypeOldValueTrace == null)
      raiseError(s"huemul_Table Error: HBase is not available for OldValueTraceTable",1063)

    if (_partitionFieldValueTemp != null && _partitionFieldValueTemp != "") {
      val partitionCol2 =  getAllDeclaredFields().filter { x => x.setAccessible(true)
                                                               x.get(this).isInstanceOf[HuemulColumns] && x.getName.toUpperCase() == _partitionFieldValueTemp.toUpperCase() }


      if (partitionCol2.length == 1) {
        partitionCol2(0).setAccessible(true)
        val DataField = partitionCol2(0).get(this).asInstanceOf[HuemulColumns]
        DataField.setPartitionColumn(1)
      } else
        this.raiseError(s"Partition Column name '${_partitionFieldValueTemp}' not found", 1064)
    }

      /*
    val partitionCol =  getALLDeclaredFields().filter { x => x.setAccessible(true)
                                                             x.get(this).isInstanceOf[huemul_Columns] }
    &&
                                                             x.getName.toUpperCase() == _partitionFieldValueTemp.toUpperCase()
      //getColumns().filter { x => x.getMyName(this.getStorageType).toUpperCase == value.toUpperCase  }
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


    if (this._tableType == null)
      raiseError(s"huemul_Table Error: TableType must be defined",1034)

    //from 2.6 --> allow partitionColumns in all tables, this check was disabled
    //else if (this._TableType != huemulType_Tables.Transaction && getPartitionField != "")
    //  raiseError(s"huemul_Table Error: PartitionField shouldn't be defined if TableType is ${this._TableType}",1036)

    //from 2.2 --> validate tableType with Format
    if (this._tableType == HuemulTypeTables.Transaction && !(this.getStorageType == HuemulTypeStorageType.PARQUET ||
                                                              this.getStorageType == HuemulTypeStorageType.ORC ||
                                                              this.getStorageType == HuemulTypeStorageType.DELTA ||
                                                              this.getStorageType == HuemulTypeStorageType.AVRO
                                                              ))
      raiseError(s"huemul_Table Error: Transaction Tables only available with PARQUET, DELTA, AVRO or ORC StorageType ",1057)


    //Fron 2.2 --> validate tableType HBASE and turn on globalSettings
    if (this.getStorageType == HuemulTypeStorageType.HBASE && !huemulBigDataGov.globalSettings.getHBaseAvailable)
      raiseError(s"huemul_Table Error: StorageType is set to HBASE, requires invoke HBase_available in globalSettings  ",1058)

    if (this._dataBase == null)
      raiseError(s"huemul_Table Error: DataBase must be defined",1037)
    if (this._frequency == null)
      raiseError(s"huemul_Table Error: Frequency must be defined",1047)

    if (this.getSaveBackup  && this.getTableType == HuemulTypeTables.Transaction)
      raiseError(s"huemul_Table Error: SaveBackup can't be true for transactional tables",1054)


    //var PartitionFieldValid: Boolean = false
    var comaPKConcat = ""

    _numPkColumns = 0
    _hBaseRowKeyCalc = ""
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns] || x.get(this).isInstanceOf[HuemulDataQuality] || x.get(this).isInstanceOf[HuemulTableRelationship]
    } foreach { x =>
      x.setAccessible(true)
      //Nombre de campos
      x.get(this) match {
        case dataField: HuemulColumns =>
          if (x.getName.toLowerCase().equals(mdmHashInternalName2.toLowerCase())) {
            dataField.setMyName(getNameForMdmHash)
          } else if (x.getName.toLowerCase().equals(mdmStatusRegInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMdmStatusReg)
          else if (x.getName.toLowerCase().equals(mdmProcessChangeInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMdmProcessChange)
          else if (x.getName.toLowerCase().equals(mdmFhChangeInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMdmFhChange)
          else if (x.getName.toLowerCase().equals(mdmProcessNewInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMdmProcessNew)
          else if (x.getName.toLowerCase().equals(mdmFhNewInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMdmFhNew)
          else if (x.getName.toLowerCase().equals(mdmAutoIncInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMdmAutoInc)
          else if (x.getName.toLowerCase().equals(mdmOldValueInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMdmOldValue)
          else if (x.getName.toLowerCase().equals(mdmNewValueInternalName2.toLowerCase())) {
            dataField.setMyName(getNameForMdmNewValue)
          } else
            dataField.setMyName(x.getName)

          if (dataField.getDqMaxLen != null && dataField.getDqMaxLen < 0)
            raiseError(s"Error column ${
              x.getName
            }: DQ_MaxLen must be positive", 1003)
          else if (dataField.getDqMaxLen != null && dataField.getDqMinLen != null && dataField.getDqMaxLen < dataField.getDqMinLen)
            raiseError(s"Error column ${
              x.getName
            }: DQ_MinLen(${
              dataField.getDqMinLen
            }) must be less than DQ_MaxLen(${
              dataField.getDqMaxLen
            })", 1028)
          else if (dataField.getDqMaxDecimalValue != null && dataField.getDqMinDecimalValue != null && dataField.getDqMaxDecimalValue < dataField.getDqMinDecimalValue)
            raiseError(s"Error column ${
              x.getName
            }: DQ_MinDecimalValue(${
              dataField.getDqMinDecimalValue
            }) must be less than DQ_MaxDecimalValue(${
              dataField.getDqMaxDecimalValue
            })", 1029)
          else if (dataField.getDqMaxDateTimeValue != null && dataField.getDqMinDateTimeValue != null && dataField.getDqMaxDateTimeValue < dataField.getDqMinDateTimeValue)
            raiseError(s"Error column ${
              x.getName
            }: DQ_MinDateTimeValue(${
              dataField.getDqMinDateTimeValue
            }) must be less than DQ_MaxDateTimeValue(${
              dataField.getDqMaxDateTimeValue
            })", 1030)
          else if (dataField.getDefaultValue != null && dataField.getDataType == StringType && dataField.getDefaultValue.toUpperCase() != "NULL" && !dataField.getDefaultValue.contains("'"))
            raiseError(s"Error column ${
              x.getName
            }: DefaultValue  must be like this: 'something', not something wihtout ')", 1031)

          if (dataField.getIsPk) {
            dataField.setIsNull(value = false) // setNullable (false)
            hasPk = true

            if (dataField.getMdmEnableDTLog || dataField.getMdmEnableOldValue || dataField.getMdmEnableProcessLog || dataField.getMdmEnableOldValueFullTrace) {
              raiseError(s"Error column ${
                x.getName
              }:, is PK, can't enabled MDM_EnableDTLog, MDM_EnableOldValue, MDM_EnableOldValue_FullTrace or MDM_EnableProcessLog", 1040)
            }
          }

          dataField.setDefinitionIsClose()
          val _colMyName = dataField.getMyName(this.getStorageType)
          //from 2.6 --> disabled this code, #98
          //if (this.getTableType == huemulType_Tables.Transaction && _colMyName.toLowerCase() == this.getPartitionField.toLowerCase())
          //  PartitionFieldValid = true

          //from 2.2 --> get concatenaded key for HBase
          if (dataField.getIsPk && getStorageType == HuemulTypeStorageType.HBASE) {
            _hBaseRowKeyCalc += s"$comaPKConcat'[', ${
              if (dataField.getDataType == StringType) _colMyName else s"CAST(${
                _colMyName
              } AS STRING)"
            },']'"
            comaPKConcat = ","
            _hBasePkColumn = _colMyName
            _numPkColumns += 1
          }

        case _ =>
      }

      //Nombre de DQ
      x.get(this) match {
        case dQField: HuemulDataQuality =>
          dQField.setMyName(x.getName)

          if (dQField.getNotification == HuemulTypeDqNotification.WARNING_EXCLUDE && !dQField.getSaveErrorDetails)
            raiseError(s"huemul_Table Error: DQ ${
              x.getName
            }:, Notification is set to WARNING_EXCLUDE, but SaveErrorDetails is set to false. Use setSaveErrorDetails to set true ", 1055)
          else if (dQField.getNotification == HuemulTypeDqNotification.WARNING_EXCLUDE && dQField.getQueryLevel != HuemulTypeDqQueryLevel.Row)
            raiseError(s"huemul_Table Error: DQ ${
              x.getName
            }:, Notification is set to WARNING_EXCLUDE, QueryLevel MUST set to huemulType_DQQueryLevel.Row", 1056)

        case _ =>
      }

      //Nombre de FK
      x.get(this) match {
        case fKField: HuemulTableRelationship =>
          fKField.myName = x.getName

        //TODO: Validate FK Setting

        case _ =>
      }
    }

    //add OVT
    getAllDeclaredFields(OnlyUserDefined = false,partitionColumnToEnd = false,HuemulTypeInternalTableType.OldValueTrace).filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns] } foreach { x =>
      x.setAccessible(true)
      val DataField = x.get(this).asInstanceOf[HuemulColumns]

      if (!DataField.getDefinitionIsClose) {
        DataField.setMyName(x.getName)
        DataField.setDefinitionIsClose()
      }
    }

    //add DQ and OVT
    getAllDeclaredFields(OnlyUserDefined = false,partitionColumnToEnd = false,HuemulTypeInternalTableType.DQ).filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns] } foreach { x =>
      x.setAccessible(true)
      val DataField = x.get(this).asInstanceOf[HuemulColumns]
      if (!DataField.getDefinitionIsClose) {
        DataField.setMyName(x.getName)
        DataField.setDefinitionIsClose()
      }
    }


    //from 2.2 --> set _HBase_rowKey for hBase Tables
    if (getStorageType == HuemulTypeStorageType.HBASE) {
      if (_numPkColumns == 1)
        _hBaseRowKeyCalc = _hBasePkColumn
      else
        _hBaseRowKeyCalc = s"concat(${_hBaseRowKeyCalc})"
    }

    if (this._tableType == HuemulTypeTables.Transaction && getPartitionField == "")
      raiseError(s"huemul_Table Error: Partitions should be defined if TableType is Transactional, use column.setPartitionColumn",1035)


    if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: register metadata")
    //Register TableName and fields
    control.RegisterMasterCreateBasic(this)
    if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: end ApplyTableDefinition")
    if (!hasPk) this.raiseError("huemul_Table Error: PK not defined", 1017)
    //from 2.6 --> disabled this code, is controlled before
    //if (this.getTableType == huemulType_Tables.Transaction && !PartitionFieldValid)
    //  raiseError(s"huemul_Table Error: PartitionField should be defined if TableType is Transactional (invalid name ${this.getPartitionField} )",1035)
    //getColumns.foreach(x=>println(x.getMyName(this.getStorageType)))
    definitionIsClose = true
     true
  }

  def _setAutoIncUpate(value: java.lang.Long): Unit = {
    _mdmAutoInc = value
  }

  def _setControlTableId(value: String) {
    _ControlTableId = value
  }

  def _getControlTableId(): String = { _ControlTableId}

  /**
   * Get all declared fields from class
   */
  private def getAllDeclaredFields(OnlyUserDefined: Boolean = false, partitionColumnToEnd: Boolean = false, tableType: HuemulTypeInternalTableType = HuemulTypeInternalTableType.Normal) : Array[java.lang.reflect.Field] = {
    val pClass = getClass

    val a = pClass.getDeclaredFields  //huemul_table


    var c = a

    //only PK for save result to old value trace
    if (tableType == HuemulTypeInternalTableType.OldValueTrace) {
        c = c.filter { x => x.setAccessible(true)
                            x.get(this).isInstanceOf[HuemulColumns] &&
                            x.get(this).asInstanceOf[HuemulColumns].getIsPk
                            }
    }

    if (!OnlyUserDefined){ //all columns, including MDM
      var b = pClass.getSuperclass.getDeclaredFields


      if (tableType == HuemulTypeInternalTableType.OldValueTrace) {
        b = b.filter { x => x.getName == mdmColumnNameInternalName2 || x.getName == mdmNewValueInternalName2 || x.getName == mdmOldValueInternalName2 || x.getName == mdmAutoIncInternalName2 ||
                            x.getName == mdmFhChangeInternalName2 || x.getName == mdmProcessChangeInternalName2 || x.getName == processExecIdInternalName2  }
      } else {
        //exclude OldValuestrace columns
        b = b.filter { x => x.getName != mdmColumnNameInternalName2 && x.getName != mdmNewValueInternalName2 && x.getName != mdmOldValueInternalName2 && x.getName != mdmAutoIncInternalName2 && x.getName != processExecIdInternalName2  }

        if (this._tableType == HuemulTypeTables.Transaction)
          b = b.filter { x => x.getName != mdmProcessChangeInternalName2 && x.getName != mdmFhChangeInternalName2 && x.getName != mdmStatusRegInternalName2   }

        //       EXCLUDE FOR PARQUET, ORC, ETC             OR            EXCLUDE FOR HBASE AND NUM PKs == 1
        if (getStorageType != HuemulTypeStorageType.HBASE || (getStorageType == HuemulTypeStorageType.HBASE && _numPkColumns == 1) )
          b = b.filter { x => x.getName != hsRowKeyInternalName2  }

      }


      c = c.union(b)
    }

    if (tableType == HuemulTypeInternalTableType.DQ) {
        val DQClass = pClass.getSuperclass.getSuperclass //huemul_TableDQ
        val d = DQClass.getDeclaredFields.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
        //from 2.5 --> add setMyName
        d.foreach { x =>
          x.setAccessible(true)
          val DataField = x.get(this).asInstanceOf[HuemulColumns]
          DataField.setMyName(x.getName)
        }

        c = d.union(c)
    }

    if (partitionColumnToEnd) {
        //from 2.6 --> old logic
        //val partitionlast = c.filter { x => x.getName.toLowerCase() == DataField.getm this.getPartitionField.toLowerCase() }
        //val rest_array = c.filter { x => x.getName.toLowerCase() != this.getPartitionField.toLowerCase() }
        //c = rest_array.union(partitionlast)



      //from 2.6 --> change logic
      //exclude partitioned columns
      val rest_array = c.filter { x => x.setAccessible(true)
                                     x.get(this).isInstanceOf[HuemulColumns] &&
                                     x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition == 0}

      //from 2.6
      //get partitioned columns ordered by getPartitionColumnPosition
      val partitionlast = pClass.getDeclaredFields.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] &&
                                      x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition >= 1 }
                                .sortBy { x => x.setAccessible(true)
                                          x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition }

      c = rest_array.union(partitionlast)
    }


     c
  }

  /**
   * Get all declared fields from class, transform to huemul_Columns
   */
  private def getALLDeclaredFields_forHBase(allDeclaredFields: Array[java.lang.reflect.Field]) : Array[(java.lang.reflect.Field, String, String, DataType)] = {

    val result = allDeclaredFields.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }.map { x =>
      //Get field
      x.setAccessible(true)
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)
      (x,Field.getHBaseCatalogFamily, Field.getHBaseCatalogColumn, _dataType)
    }

     result
  }

  //private var _SQL_OldValueFullTrace_DF: DataFrame = null
  private var _mdmAutoInc: java.lang.Long = 0L
  private var _ControlTableId: String = _
  def _getMDM_AutoInc(): java.lang.Long = { _mdmAutoInc}
  private var _tableDqIsUsed: Int = 0
  def _getTable_dq_isused(): Int = { _tableDqIsUsed}
  private var _tableOvtIsUsed: Int = 0
  def _getTable_ovt_isused(): Int = { _tableOvtIsUsed}
  private var _numRowsNew: java.lang.Long = _
  private var _numRowsUpdate: java.lang.Long = _
  private var _numRowsUpdatable: java.lang.Long = _
  private var _numRowsDelete: java.lang.Long = _
  private var _numRowsTotal: java.lang.Long = _
  private var _numRowsNoChange: java.lang.Long = _
  private var _numRowsExcluded: java.lang.Long = 0L

  def numRowsNew(): java.lang.Long = {
     _numRowsNew
  }

  def numRowsUpdate(): java.lang.Long = {
     _numRowsUpdate
  }

  def numRowsUpdatable(): java.lang.Long = {
     _numRowsUpdatable
  }

  def numRowsDelete(): java.lang.Long = {
     _numRowsDelete
  }

  def numRowsTotal(): java.lang.Long = {
     _numRowsTotal
  }

  def numRowsNoChange(): java.lang.Long = {
     _numRowsNoChange
  }

  def numRowsExcluded(): java.lang.Long = {
     _numRowsExcluded
  }
   /*  ********************************************************************************
   *****   F I E L D   M E T H O D S    ****************************************
   ******************************************************************************** */
  def getSchema: StructType = {
    val fieldsStruct: ArrayBuffer[StructField] = getColumns.map(x=> StructField(x.getMyName(this.getStorageType), x.getDataType , nullable = x.getNullable , null))
    StructType.apply(fieldsStruct)
  }

  def getDataQuality(warning_exclude: Boolean): ArrayBuffer[HuemulDataQuality] = {
    val getDeclaredFields = getAllDeclaredFields()
    val result = new ArrayBuffer[HuemulDataQuality]()

    if (getDeclaredFields != null ) {
        getDeclaredFields.filter { x => x.setAccessible(true)
                             x.get(this).isInstanceOf[HuemulDataQuality] }
        .foreach { x =>
          //Get DQ
          val DQRule = x.get(this).asInstanceOf[HuemulDataQuality]

          if (warning_exclude && DQRule.getNotification == HuemulTypeDqNotification.WARNING_EXCLUDE)
            result.append(DQRule)
          else if (!warning_exclude && DQRule.getNotification != HuemulTypeDqNotification.WARNING_EXCLUDE)
            result.append(DQRule)
        }
      }

     result
  }

  def getForeignKey: ArrayBuffer[HuemulTableRelationship] = {
    val getDeclaredFields = getAllDeclaredFields()
    val Result = new ArrayBuffer[HuemulTableRelationship]()

    if (getDeclaredFields != null ) {
        getDeclaredFields.filter { x => x.setAccessible(true)
                             x.get(this).isInstanceOf[HuemulTableRelationship] }
        .foreach { x =>
          //Get DQ
          val FKRule = x.get(this).asInstanceOf[HuemulTableRelationship]
          Result.append(FKRule)
        }
      }

     Result
  }

  /**
   Create schema from DataDef Definition
   */
  def getColumns: ArrayBuffer[HuemulColumns] = {
    val result: ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]()
    val fieldList = getAllDeclaredFields()
    //val NumFields = fieldList.filter { x => x.setAccessible(true)
    //                                  x.get(this).isInstanceOf[huemul_Columns] }.length

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    //var ColumnsCreateTable : String = ""
    //var coma: String = ""
    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)

      //Field.setMyName(x.getName)
      result.append(Field)


      if (Field.getMdmEnableOldValue) {
        val MDM_EnableOldValue = new HuemulColumns(_dataType, false, s"Old value for ${_colMyName}")
        MDM_EnableOldValue.setMyName(s"${_colMyName}${__old}")
        result.append(MDM_EnableOldValue)
      }
      if (Field.getMdmEnableDTLog){
        val MDM_EnableDTLog = new HuemulColumns(TimestampType, false, s"Last change DT for ${_colMyName}")
        MDM_EnableDTLog.setMyName(s"${_colMyName}${__fhChange}")
        result.append(MDM_EnableDTLog)
      }
      if (Field.getMdmEnableProcessLog) {
        val MDM_EnableProcessLog = new HuemulColumns(StringType, false, s"System Name change for ${_colMyName}")
        MDM_EnableProcessLog.setMyName(s"${_colMyName}${__ProcessLog}")
        result.append(MDM_EnableProcessLog)
      }

    }

     result
  }


  /**
   Create schema from DataDef Definition
   */
  private def getColumnsCreateTable(forHive: Boolean = false, tableType: HuemulTypeInternalTableType = HuemulTypeInternalTableType.Normal ): String = {
    //WARNING!!! Any changes you make to this code, repeat it in getHBaseCatalogForHIVE
    val partitionColumnToEnd = if (getStorageType == HuemulTypeStorageType.HBASE) false else true
    val fieldList = getAllDeclaredFields(OnlyUserDefined = false,partitionColumnToEnd = partitionColumnToEnd,tableType)
   // val NumFields = fieldList.filter { x => x.setAccessible(true)
   //   x.get(this).isInstanceOf[huemul_Columns] }.length

    //set name according to getStorageType (AVRO IS LowerCase)
    val __storageType =  if (tableType == HuemulTypeInternalTableType.Normal) this.getStorageType
                    else if (tableType == HuemulTypeInternalTableType.DQ) this.getStorageTypeDqResult
                    else if (tableType == HuemulTypeInternalTableType.OldValueTrace) this.getStorageTypeOldValueTrace
                    else this.getStorageType

    val __old = huemulBigDataGov.getCaseType( __storageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( __storageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( __storageType, "_ProcessLog")

    var columnsCreateTable : String = ""
    var coma: String = ""

    //for HBase, add hs_rowKey as Key column
    if (getStorageType == HuemulTypeStorageType.HBASE && _numPkColumns > 1) {
      fieldList.filter { x => x.setAccessible(true)
        x.getName == hsRowKeyInternalName2 }.foreach { x =>
        val Field = x.get(this).asInstanceOf[HuemulColumns]
        val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, __storageType)
        val DataTypeLocal = _dataType.sql

        columnsCreateTable += s"$coma${x.getName} $DataTypeLocal \n"
        coma = ","
      }
    }

    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] && x.getName != hsRowKeyInternalName2 }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, __storageType)
      var DataTypeLocal = _dataType.sql
      //from 2.5 --> replace DECIMAL to STRING when storage type = AVRO. this happen because AVRO outside DATABRICKS doesn't support DECIMAL, DATE AND TIMESTAMP TYPE.
      if (    huemulBigDataGov.globalSettings.getBigDataProvider != HuemulTypeBigDataProvider.databricks
          && __storageType == HuemulTypeStorageType.AVRO
          && (  DataTypeLocal.toUpperCase().contains("DECIMAL") ||
                DataTypeLocal.toUpperCase().contains("DATE") ||
                DataTypeLocal.toUpperCase().contains("TIMESTAMP"))
          ) {
        DataTypeLocal = StringType.sql
      }
      val _colMyName = Field.getMyName(__storageType)

      if (tableType == HuemulTypeInternalTableType.DQ) {
        //create StructType
        //FROM 2.4 --> INCLUDE PARTITIONED COLUMN IN CREATE TABLE ONLY FOR databricks COMPATIBILITY
        if (huemulBigDataGov.globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
          columnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        } else if ("dq_control_id".toLowerCase() != _colMyName.toLowerCase()) {
          columnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        }
      }
      else if (tableType == HuemulTypeInternalTableType.OldValueTrace) {
        //create StructType mdm_columnname
        //FROM 2.4 --> INCLUDE PARTITIONED COLUMN IN CREATE TABLE ONLY FOR databricks COMPATIBILITY
        if (huemulBigDataGov.globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
          columnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        } else if (getNameForMdmColumnName.toLowerCase() != _colMyName.toLowerCase()) {
          columnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        }
      }
      else {
        //create StructType
        //if (getPartitionField != null && getPartitionField.toLowerCase() != _colMyName.toLowerCase()) {
        if (Field.getPartitionColumnPosition == 0 || getStorageType == HuemulTypeStorageType.HBASE) {
          columnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        } else if (huemulBigDataGov.globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
          //from 2.4 --> add partitioned field
          columnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        }
      }

      if (tableType != HuemulTypeInternalTableType.OldValueTrace) {
        if (Field.getMdmEnableOldValue)
          columnsCreateTable += s"$coma${_colMyName}${__old} $DataTypeLocal \n"
        if (Field.getMdmEnableDTLog)
          columnsCreateTable += s"$coma${_colMyName}${__fhChange} ${TimestampType.sql} \n"
        if (Field.getMdmEnableProcessLog)
          columnsCreateTable += s"$coma${_colMyName}${__ProcessLog} ${StringType.sql} \n"
      }
    }

     columnsCreateTable
  }




  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE PRIMARY KEY
   */
  private def sqlPrimaryKeyFinalTable(): String = {


    var stringSQL: String = ""
    var coma: String = ""
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] &&
                                      x.get(this).asInstanceOf[HuemulColumns].getIsPk }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = field.getMyName(this.getStorageType)

      //All PK columns shouldn't be null
      field.setIsNullInternal(value = false)

      if (!huemulBigDataGov.hasName(field.getMappedName))
        sys.error(s"field ${_colMyName} doesn't have an assigned name in 'name' attribute")
      stringSQL += coma + _colMyName
      coma = ","
    }


     stringSQL
  }

  /**
  CREATE SQL SCRIPT FOR TRANSACTIONAL DATA
   */
  private def sqlStep0TxHash(officialAlias: String, processName: String): String = {

    var stringSql: String = ""
    //var StringSQl_PK: String = ""

    //var StringSQL_partition: String = ""
    var stringSqlHash: String = "sha2(concat( "
    var comaHash: String = ""
    var coma: String = ""

    val partitionList : ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]

    getAllDeclaredFields(partitionColumnToEnd = true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)

      val NewColumnCast = applyAutoCast(if (huemulBigDataGov.hasName(Field.getSQLForInsert)) s"${Field.getSQLForInsert} " else s"New.${Field.getMappedName}"
                                        ,_dataType.sql)


      //New value (from field or compute column )
      if (huemulBigDataGov.hasName(Field.getMappedName )){
        if (Field.getPartitionColumnPosition == 0){
          stringSql += s"$coma$NewColumnCast as ${_colMyName} \n"
          coma = ","
        } else {
          partitionList.append(Field)
        }

      } else {
        if (_colMyName.toLowerCase() == getNameForMdmFhNew.toLowerCase()) {
          val colUserName = Field.getCaseType(this.getStorageType,getNameForMdmFhNew) //from 2.6 #112
          stringSql += s",CAST(now() AS ${_dataType.sql} ) as $colUserName \n"
          coma = ","
        } else if (_colMyName.toLowerCase() == getNameForMdmProcessNew.toLowerCase()) {
          val colUserName = Field.getCaseType(this.getStorageType,getNameForMdmProcessNew) //from 2.6 #112
          stringSql += s"${coma}CAST('$processName' AS ${_dataType.sql} ) as $colUserName \n"
          coma = ","
        }
      }


      if (Field.getUsedForCheckSum) {
        stringSqlHash += s"""$comaHash${s"coalesce($NewColumnCast,'null')" }"""
        comaHash = ","
      }


    }

    stringSqlHash += "),256) "

    //set name according to getStorageType (AVRO IS LowerCase)
    val __mdmHash = huemulBigDataGov.getCaseType( this.getStorageType, getNameForMdmHash)

    //from 2.6 --> create partitions sql (replace commented text)
    val stringSqlPartition = partitionList.sortBy { x => x.getPartitionColumnPosition}.map{x =>
      val _colMyName = x.getMyName(this.getStorageType)
      val _dataType  = x.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)

      val NewColumnCast = applyAutoCast(if (huemulBigDataGov.hasName(x.getSQLForInsert)) s"${x.getSQLForInsert} " else s"New.${x.getMappedName}"
                                        ,_dataType.sql)

      val result = s"$NewColumnCast as ${_colMyName} \n"

      result
    }.mkString(",")

    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    stringSql = s"""SELECT $stringSql
                    ,$stringSqlHash as ${__mdmHash}
                    ,$stringSqlPartition
                    FROM $officialAlias new
                          """

     stringSql
  }

  /**
  CREATE SQL SCRIPT DISTINCT WITH NEW DATAFRAME
   */
  private def sqlStep0Distinct(newAlias: String): String = {

    var stringSql: String = ""
    val distintos: ArrayBuffer[String] = new ArrayBuffer[String]()
    var coma: String = ""
    getAllDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns]
                                        }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      val mappedField: String = field.getMappedName
      if (huemulBigDataGov.hasName(mappedField)) {
        if (!distintos.contains(mappedField)){
          distintos.append(mappedField)
          stringSql += s"$coma$mappedField  \n"
          coma = ","
        }
      }
    }

    //Aditional field from new table
    if (huemulBigDataGov.hasName(additionalRowsForDistinct))
      stringSql += s",$additionalRowsForDistinct"

    //set field New or Update action
    stringSql = s"""SELECT DISTINCT $stringSql
                     FROM $newAlias"""

     stringSql
  }


  private def applyAutoCast(columnName: String, dataType: String): String = {
     if (autoCast) s"CAST($columnName AS $dataType)" else columnName
  }


  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def sqlStep2LeftJoin(officialAlias: String, newAlias: String): String = {
    //Get fields in old table
    val officialColumns = huemulBigDataGov.spark.catalog.listColumns(officialAlias).collect()

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    var stringSqlLeftJoin: String = ""
    var stringSqlPk: String = ""
    var stringSqlActionType: String = ""

    var coma: String = ""
    var sand: String = ""
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = field.getMyName(this.getStorageType)
      val _dataType  = field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)

      val newColumnCast = applyAutoCast(s"New.${field.getMappedName}",_dataType.sql)
      //string for key on
      if (field.getIsPk){
        val NewPKSentence = if (huemulBigDataGov.hasName(field.getSQLForInsert)) s"CAST(${field.getSQLForInsert} as ${_dataType.sql} )" else newColumnCast
        stringSqlLeftJoin += s"${coma}Old.${_colMyName} AS ${_colMyName} \n"
        stringSqlPk += s" $sand Old.${_colMyName} = $NewPKSentence  "
        sand = " and "

        if (stringSqlActionType == "")
          stringSqlActionType = s" case when Old.${_colMyName} is null then 'NEW' when $newColumnCast is null then 'EQUAL' else 'UPDATE' END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = officialColumns.exists { y => y.name.toLowerCase() == _colMyName.toLowerCase() }

        //String for new DataFrame fulljoin
        if (columnExist){
          stringSqlLeftJoin += s"${coma}cast(old.${_colMyName} as ${_dataType.sql} ) as old_${_colMyName} \n"
          //StringSQL_LeftJoin += s"${coma}old.${_colMyName} as old_${_colMyName} \n"
        }
        else
          stringSqlLeftJoin += s"${coma}cast(null as ${_dataType.sql} ) as old_${_colMyName} \n"

        //New value (from field or compute column )
        if (huemulBigDataGov.hasName(field.getMappedName))
          stringSqlLeftJoin += s",$newColumnCast as new_${_colMyName} \n"
        if (huemulBigDataGov.hasName(field.getSQLForInsert))
          stringSqlLeftJoin += s",CAST(${field.getSQLForInsert} as ${_dataType.sql} ) as new_insert_${_colMyName} \n"
        if (huemulBigDataGov.hasName(field.getSQLForUpdate))
          stringSqlLeftJoin += s",CAST(${field.getSQLForUpdate} as ${_dataType.sql} ) as new_update_${_colMyName} \n"

        if (field.getMdmEnableOldValue || field.getMdmEnableDTLog || field.getMdmEnableProcessLog || field.getMdmEnableOldValueFullTrace) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.hasName(field.getMappedName)) {
            val NewFieldTXT = applyAutoCast(if (this.huemulBigDataGov.hasName(field.getSQLForUpdate)) field.getSQLForUpdate else "new.".concat(field.getMappedName),_dataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(_colMyName) else "null"
            //from 2.5 --> add cast to avro timestamp dataType
            //if (this.getStorageType == huemulType_StorageType.AVRO && (Field._DataType.sql.toUpperCase().contains("TIMESTAMP") || Field._DataType.sql.toUpperCase().contains("DATE")))
            //  OldFieldTXT =   ApplyAutoCast(OldFieldTXT,Field._DataType.sql)
            stringSqlLeftJoin += s",CAST(CASE WHEN $NewFieldTXT = $OldFieldTXT or ($NewFieldTXT is null and $OldFieldTXT is null) THEN 0 ELSE 1 END as Integer ) as __Change_${_colMyName}  \n"
          }
          else {
            stringSqlLeftJoin += s",CAST(0 as Integer ) as __Change_${_colMyName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae,
            //la solución es poner el campo "tiene cambio" en falso
          }

        }

        if (field.getMdmEnableOldValue){
          if (!officialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__old}".toLowerCase() }) //no existe columna en dataframe
            stringSqlLeftJoin += s",CAST(null AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
          else //existe columna en dataframe
            stringSqlLeftJoin += s",CAST(old.${_colMyName}${__old} AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
        }
        if (field.getMdmEnableDTLog){
          if (!officialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__fhChange}".toLowerCase() }) //no existe columna en dataframe
            stringSqlLeftJoin += s",CAST(null AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
          else //existe columna en dataframe
            stringSqlLeftJoin += s",CAST(old.${_colMyName}${__fhChange} AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
        }
        if (field.getMdmEnableProcessLog){
          if (!officialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__ProcessLog}".toLowerCase() }) //no existe columna en dataframe
            stringSqlLeftJoin += s",CAST(null AS STRING) as old_${_colMyName}${__ProcessLog} \n"
          else //existe columna en dataframe
            stringSqlLeftJoin += s",CAST(old.${_colMyName}${__ProcessLog} AS STRING) as old_${_colMyName}${__ProcessLog} \n"
        }

      }

      coma = ","
    }


    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    stringSqlLeftJoin = s"""SELECT $stringSqlLeftJoin
                                   ,$stringSqlActionType
                             FROM $officialAlias Old
                                LEFT JOIN $newAlias New \n
                                   on $stringSqlPk
                          """

     stringSqlLeftJoin
  }


  /**
  SCRIPT FOR OLD VALUE TRACE INSERT
   */
  private def oldValueTraceSave(alias: String, processName: String, localControl: HuemulControl)  {

    //set name according to getStorageType (AVRO IS LowerCase)
    val __mdmNewValue = huemulBigDataGov.getCaseType( this.getStorageTypeOldValueTrace, getNameForMdmNewValue)
    val __mdmOldValue = huemulBigDataGov.getCaseType( this.getStorageTypeOldValueTrace, getNameForMdmOldValue)
    val __mdmAutoInc = huemulBigDataGov.getCaseType( this.getStorageTypeOldValueTrace, getNameForMdmAutoInc)
    val __mdmProcessChange = huemulBigDataGov.getCaseType( this.getStorageTypeOldValueTrace, getNameForMdmProcessChange)
    val __mdmColumnName = huemulBigDataGov.getCaseType( this.getStorageTypeOldValueTrace, getNameForMdmColumnName).toLowerCase() //because it's partitioned column
    val __mdmFhChange = huemulBigDataGov.getCaseType( this.getStorageTypeOldValueTrace, getNameForMdmFhChange)
    val __processExecId = huemulBigDataGov.getCaseType( this.getStorageTypeOldValueTrace, getNameForProcessExecId)

    //Get PK
    var stringSqlPkBase: String = "SELECT "
    var coma: String = ""
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[HuemulColumns] &&
                                         x.get(this).asInstanceOf[HuemulColumns].getIsPk }
    .foreach { x =>
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      stringSqlPkBase += s" $coma${Field.getMyName(this.getStorageTypeOldValueTrace)}"
      coma = ","
    }

    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] &&
                                      x.get(this).asInstanceOf[HuemulColumns].getMdmEnableOldValueFullTrace }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = field.getMyName(this.getStorageTypeOldValueTrace)
      val _dataTypeMdmFhChange = mdmFhChange.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageTypeOldValueTrace)
      val __MdmFhChangeCast: String = if (_dataTypeMdmFhChange == mdmFhChange.getDataType) s"now() as ${__mdmFhChange}" else s"CAST(now() as STRING) as ${__mdmFhChange}"
      val stringSql =  s"$stringSqlPkBase, CAST(new_${_colMyName} as string) AS ${__mdmNewValue}, CAST(old_${_colMyName} as string) AS ${__mdmOldValue}, CAST(${_mdmAutoInc} AS BIGINT) as ${__mdmAutoInc}, cast('${control.Control_Id}' as string) as ${__processExecId}, ${__MdmFhChangeCast}, cast('$processName' as string) as ${__mdmProcessChange}, cast('${_colMyName.toLowerCase()}' as string) as ${__mdmColumnName} FROM $alias WHERE ___ActionType__ = 'UPDATE' and __Change_${_colMyName} = 1 "
      val aliasFullTrace: String = s"__SQL_ovt_full_${_colMyName}"

      val tempSqlOldValueFullTraceDf = huemulBigDataGov.dfExecuteQuery(aliasFullTrace,stringSql)

      val numRowsAffected = tempSqlOldValueFullTraceDf.count()
      if (numRowsAffected > 0) {
        val Result = savePersistOldValueTrace(localControl,tempSqlOldValueFullTraceDf)
        if (!Result)
            huemulBigDataGov.logMessageWarn(s"Old value trace full trace can't save to disk, column ${_colMyName}")
      }

      localControl.newStep(s"Ref & Master: ovt full trace finished, $numRowsAffected rows changed for ${_colMyName} column ")
    }
  }


  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def sqlStep1FullJoin(officialAlias: String, newAlias: String, isUpdate: Boolean, isDelete: Boolean): String = {
    //Get fields in old table
    val officialColumns = huemulBigDataGov.spark.catalog.listColumns(officialAlias).collect()

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    var stringSqlFullJoin: String = ""
    var stringSqlPk: String = ""
    var stringSqlActionType: String = ""

    var coma: String = ""
    var sand: String = ""
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = field.getMyName(this.getStorageType)
      val _dataType  = field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)

      val newColumnCast = applyAutoCast(s"New.${field.getMappedName}",_dataType.sql)
      //string for key on
      if (field.getIsPk){
        val newPkSentence = if (huemulBigDataGov.hasName(field.getSQLForInsert)) s"CAST(${field.getSQLForInsert} as ${_dataType.sql} )" else newColumnCast
        stringSqlFullJoin += s"${coma}CAST(coalesce(Old.${_colMyName}, $newPkSentence) as ${_dataType.sql}) AS ${_colMyName} \n"
        stringSqlPk += s" $sand Old.${_colMyName} = $newPkSentence  "
        sand = " and "

        if (stringSqlActionType == "")
          stringSqlActionType = s" case when Old.${_colMyName} is null then 'NEW' when $newColumnCast is null then ${if (isDelete) "'DELETE'" else "'EQUAL'"} else ${if (isUpdate) "'UPDATE'" else "'EQUAL'"} END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = officialColumns.exists { y => y.name.toLowerCase() == _colMyName.toLowerCase() }

        //String for new DataFrame fulljoin
        if (columnExist)
          stringSqlFullJoin += s"${coma}cast(old.${_colMyName} as ${_dataType.sql} ) as old_${_colMyName} \n"
        else
          stringSqlFullJoin += s"${coma}cast(null as ${_dataType.sql} ) as old_${_colMyName} \n"

        //New value (from field or compute column )
        if (huemulBigDataGov.hasName(field.getMappedName))
          stringSqlFullJoin += s",$newColumnCast as new_${_colMyName} \n"
        if (huemulBigDataGov.hasName(field.getSQLForInsert))
          stringSqlFullJoin += s",CAST(${field.getSQLForInsert} as ${_dataType.sql} ) as new_insert_${_colMyName} \n"
        if (huemulBigDataGov.hasName(field.getSQLForUpdate))
          stringSqlFullJoin += s",CAST(${field.getSQLForUpdate} as ${_dataType.sql} ) as new_update_${_colMyName} \n"

        if (field.getMdmEnableOldValue || field.getMdmEnableDTLog || field.getMdmEnableProcessLog || field.getMdmEnableOldValueFullTrace) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.hasName(field.getMappedName)) {
            val NewFieldTXT = applyAutoCast(if (this.huemulBigDataGov.hasName(field.getSQLForUpdate)) field.getSQLForUpdate else "new.".concat(field.getMappedName),_dataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(_colMyName) else "null"
            //from 2.5 --> add cast to avro timestamp dataType
            //if (this.getStorageType == huemulType_StorageType.AVRO && (Field._DataType.sql.toUpperCase().contains("TIMESTAMP") || Field._DataType.sql.toUpperCase().contains("DATE")) )
            //  OldFieldTXT =   ApplyAutoCast(OldFieldTXT,Field._DataType.sql)
            stringSqlFullJoin += s",CAST(CASE WHEN $NewFieldTXT = $OldFieldTXT or ($NewFieldTXT is null and $OldFieldTXT is null) THEN 0 ELSE 1 END as Integer ) as __Change_${_colMyName}  \n"
          }
          else {
            stringSqlFullJoin += s",CAST(0 as Integer ) as __Change_${_colMyName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae,
            //la solución es poner el campo "tiene cambio" en falso
          }

        }

        if (field.getMdmEnableOldValue){
          if (!officialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__old}".toLowerCase() }) //no existe columna en dataframe
            stringSqlFullJoin += s",CAST(null AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
          else //existe columna en dataframe
            stringSqlFullJoin += s",CAST(old.${_colMyName}${__old} AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
        }
        if (field.getMdmEnableDTLog){
          if (!officialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__fhChange}".toLowerCase() }) //no existe columna en dataframe
            stringSqlFullJoin += s",CAST(null AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
          else //existe columna en dataframe
            stringSqlFullJoin += s",CAST(old.${_colMyName}${__fhChange} AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
        }
        if (field.getMdmEnableProcessLog){
          if (!officialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__ProcessLog}".toLowerCase() }) //no existe columna en dataframe
            stringSqlFullJoin += s",CAST(null AS STRING) as old_${_colMyName}${__ProcessLog} \n"
          else //existe columna en dataframe
            stringSqlFullJoin += s",CAST(old.${_colMyName}${__ProcessLog} AS STRING) as old_${_colMyName}${__ProcessLog} \n"
        }



      }

      coma = ","
    }


    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    stringSqlFullJoin = s"""SELECT $stringSqlFullJoin
                                   ,$stringSqlActionType
                             FROM $officialAlias Old
                                FULL JOIN $newAlias New \n
                                   on $stringSqlPk
                          """

     stringSqlFullJoin
  }

  /**
  CREATE SQL SCRIPT LEFT JOIN
   */
  private def sqlStep4Update(newAlias: String, ProcessName: String): String = {

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    var stringSql: String = "SELECT "

    var coma: String = ""
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = field.getMyName(this.getStorageType)

      //string for key on
      if (field.getIsPk){
        stringSql += s" $coma${_colMyName} as ${_colMyName} \n"

      } else {
        if (   _colMyName.toLowerCase() == getNameForMdmFhNew.toLowerCase() || _colMyName.toLowerCase() == getNameForMdmProcessNew.toLowerCase() || _colMyName.toLowerCase() == getNameForMdmFhChange.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMdmProcessChange.toLowerCase() || _colMyName.toLowerCase() == getNameForMdmStatusReg.toLowerCase() || _colMyName.toLowerCase() == hsRowKeyInternalName2.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMdmHash.toLowerCase())
          stringSql += s"${coma}old_${_colMyName}  \n"
        else {
          stringSql += s""" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.hasName(field.getSQLForUpdate)) s"new_update_${_colMyName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (field.getReplaceValueOnUpdate && this.huemulBigDataGov.hasName(field.getMappedName)) s"new_${_colMyName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${_colMyName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${_colMyName} END  as ${_colMyName} \n"""

           if (field.getMdmEnableOldValue)
             stringSql += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN old_${_colMyName} ELSE old_${_colMyName}${__old} END as ${_colMyName}${__old} \n"""
           if (field.getMdmEnableDTLog)
             stringSql += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN now() ELSE old_${_colMyName}${__fhChange} END AS TimeStamp) as ${_colMyName}${__fhChange} \n"""
           if (field.getMdmEnableProcessLog)
             stringSql += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN '$ProcessName' WHEN ___ActionType__ = 'NEW' THEN '$ProcessName' ELSE old_${_colMyName}${__ProcessLog} END as ${_colMyName}${__ProcessLog} \n"""
        }
      }

      coma = ","
    }


    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    stringSql += s""" ,___ActionType__
                      FROM $newAlias New
                  """
     stringSql
  }


  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def sqlStep2UpdateAndInsert(newAlias: String, processName: String, isInsert: Boolean): String = {

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    var stringSql: String = "SELECT "

    var coma: String = ""
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = field.getMyName(this.getStorageType)
      val _dataType  = field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)

      //string for key on
      if (field.getIsPk){
        stringSql += s" $coma${_colMyName} as ${_colMyName} \n"

      } else {
        if (   _colMyName.toLowerCase() == getNameForMdmFhNew.toLowerCase()         || _colMyName.toLowerCase() == getNameForMdmProcessNew.toLowerCase() || _colMyName.toLowerCase() == getNameForMdmFhChange.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMdmProcessChange.toLowerCase() || _colMyName.toLowerCase() == getNameForMdmStatusReg.toLowerCase()  || _colMyName.toLowerCase() == hsRowKeyInternalName2.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMdmHash.toLowerCase())
          stringSql += s"${coma}old_${_colMyName}  \n"
        else {
          stringSql += s""" ${coma}CASE WHEN ___ActionType__ = 'NEW'    THEN ${if (this.huemulBigDataGov.hasName(field.getSQLForInsert)) s"new_insert_${_colMyName}" //si tiene valor en SQL insert, lo usa
                                                                               else if (this.huemulBigDataGov.hasName(field.getMappedName)) s"new_${_colMyName}"     //si no, si tiene nombre de campo en DataFrame nuevo, lo usa
                                                                               else applyAutoCast(field.getDefaultValue,_dataType.sql)                        //si no tiene campo asignado, pone valor por default
                                                                           }
                                        WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.hasName(field.getSQLForUpdate)) s"new_update_${_colMyName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (field.getReplaceValueOnUpdate && this.huemulBigDataGov.hasName(field.getMappedName)) s"new_${_colMyName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${_colMyName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${_colMyName} END  as ${_colMyName} \n"""

           if (field.getMdmEnableOldValue)
             stringSql += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN old_${_colMyName} ELSE old_${_colMyName}${__old} END as ${_colMyName}${__old} \n"""
           if (field.getMdmEnableDTLog)
             stringSql += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN now() ELSE old_${_colMyName}${__fhChange} END AS TimeStamp) as ${_colMyName}${__fhChange} \n"""
           if (field.getMdmEnableProcessLog)
             stringSql += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN '$processName' WHEN ___ActionType__ = 'NEW' THEN '$processName' ELSE old_${_colMyName}${__ProcessLog} END as ${_colMyName}${__ProcessLog} \n"""
        }
      }

      coma = ","
    }


    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    stringSql += s""" ,___ActionType__
                      FROM $newAlias New
                      ${if (isInsert) "" else "WHERE ___ActionType__ <> 'NEW' "}
                  """


     stringSql
  }

  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA and for Selective update
   */
  private def sqlStep3HashP1(newAlias: String, isSelectiveUpdate: Boolean): String = {

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    var stringSql: String = "SELECT "
    var stringSqlHash: String = "sha2(concat( "


    var coma: String = ""
    var comaHash: String = ""

    //Obtiene todos los campos
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns]
                                    }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = field.getMyName(this.getStorageType)

      if (     _colMyName.toLowerCase() == getNameForMdmFhNew.toLowerCase()         || _colMyName.toLowerCase() == getNameForMdmProcessNew.toLowerCase() || _colMyName.toLowerCase() == getNameForMdmFhChange.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMdmProcessChange.toLowerCase() || _colMyName.toLowerCase() == getNameForMdmStatusReg.toLowerCase()  || _colMyName.toLowerCase() == hsRowKeyInternalName2.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMdmHash.toLowerCase())
        stringSql += s"${coma}old_${_colMyName}  \n"
      else
        stringSql += s" $coma${_colMyName}  \n"

      if (field.getMdmEnableOldValue)
        stringSql += s""",${_colMyName}${__old} \n"""
      if (field.getMdmEnableDTLog)
        stringSql += s""",${_colMyName}${__fhChange} \n"""
      if (field.getMdmEnableProcessLog)
        stringSql += s""",${_colMyName}${__ProcessLog} \n"""

      if (field.getUsedForCheckSum) {
        stringSqlHash += s"""$comaHash${s"coalesce(${_colMyName},'null')"}"""
        comaHash = ","
      }

      coma = ","
    }

    //Hash fields
    if (comaHash == "")
      sys.error(s"must define one field as UsedForCheckSum")

    stringSqlHash += "),256) "

    //set name according to getStorageType (AVRO IS LowerCase)
    val __mdmHash = huemulBigDataGov.getCaseType( this.getStorageType, getNameForMdmHash)

    //Include HashFields to SQL
    stringSql += s""",$stringSqlHash as ${__mdmHash} \n"""

    if (this._tableType == HuemulTypeTables.Reference || this._tableType == HuemulTypeTables.Master || isSelectiveUpdate) {
      stringSql += s""",case when old_${__mdmHash} = $stringSqlHash THEN 1 ELSE 0 END AS SameHashKey  \n ,___ActionType__ \n"""
    }

    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    stringSql += s""" FROM $newAlias New
                  """

     stringSql
  }


  private def sqlStep4Final(newAlias: String, processName: String): String = {
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    val __MDM_hash = huemulBigDataGov.getCaseType( this.getStorageType, getNameForMdmHash)

    var stringSql: String = "SELECT "

    var coma: String = ""

    //Obtiene todos los campos
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns]
                                    }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)

      if (_colMyName.toLowerCase() == getNameForMdmFhNew.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMdmFhNew)//from 2.6 #112
        stringSql += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN now() ELSE old_${_colMyName} END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMdmProcessNew.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMdmProcessNew) //from 2.6 #112
        stringSql += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN '$processName' ELSE old_${_colMyName} END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMdmFhChange.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMdmFhChange)//from 2.6 #112
        stringSql += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN now() ELSE old_${_colMyName} END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMdmProcessChange.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMdmProcessChange)//from 2.6 #112
        stringSql += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN '$processName' ELSE old_${_colMyName} END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMdmStatusReg.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMdmStatusReg)//from 2.6 #112
        stringSql += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN CAST(2 as Int) WHEN ___ActionType__ = 'NEW' THEN CAST(2 as Int) WHEN ___ActionType__ = 'DELETE' AND ${this.getRowStatusDeleteAsDeleted} = true THEN CAST(-1 AS Int)  ELSE CAST(old_${_colMyName} AS Int) END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMdmHash.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMdmHash)//from 2.6 #112
        stringSql += s"$coma${__MDM_hash} as $colUserName \n"
      } else if (_colMyName.toLowerCase() == hsRowKeyInternalName2.toLowerCase()){
        if (getStorageType == HuemulTypeStorageType.HBASE && _numPkColumns > 1)
          stringSql += s"$coma${_hBaseRowKeyCalc} as ${_colMyName} \n"
      }
      else
        stringSql += s" $coma${_colMyName}  \n"

      if (Field.getMdmEnableOldValue)
        stringSql += s""",${_colMyName}${__old} \n"""
      if (Field.getMdmEnableDTLog)
        stringSql += s""",${_colMyName}${__fhChange} \n"""
      if (Field.getMdmEnableProcessLog)
        stringSql += s""",${_colMyName}${__ProcessLog} \n"""


      coma = ","
    }

    //if (IncludeActionType)
      stringSql += s", ___ActionType__, SameHashKey \n"

    stringSql += s"FROM $newAlias New\n"

     stringSql
  }



  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE PRIMARY KEY
   */
  private def sqlUniqueFinalTable(): ArrayBuffer[HuemulColumns] = {

    val stringSql: ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]()
    getAllDeclaredFields().filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[HuemulColumns] &&
                                         x.get(this).asInstanceOf[HuemulColumns].getIsUnique && huemulBigDataGov.hasName(x.get(this).asInstanceOf[HuemulColumns].getMappedName)}
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]

      stringSql.append(field)
    }

     stringSql
  }

  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE NOT NULL ATTRIBUTES //warning_exclude: Boolean
   */
  private def sqlNotNullFinalTable(): ArrayBuffer[HuemulColumns] = {
    val stringSql: ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]()
    getAllDeclaredFields().filter { x => x.setAccessible(true)
         x.get(this).isInstanceOf[HuemulColumns] &&
        !x.get(this).asInstanceOf[HuemulColumns].getNullable && //warning_exclude == false &&
        huemulBigDataGov.hasName(x.get(this).asInstanceOf[HuemulColumns].getMappedName)}
      .foreach { x =>
        //Get field
        val field = x.get(this).asInstanceOf[HuemulColumns]

        stringSql.append(field)
      }

     stringSql
  }

  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required
   */
  private def missingRequiredFields(isSelectiveUpdate: Boolean): ArrayBuffer[String] = {
    val stringSql: ArrayBuffer[String] = new ArrayBuffer[String]()
    if (isSelectiveUpdate) return stringSql


    getAllDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[HuemulColumns] &&
                                         x.get(this).asInstanceOf[HuemulColumns].required
                                          }
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]
      //huemulBigDataGov.logMessageInfo(s"${_colMyName} Field.getMappedName: ${Field.getMappedName}, Field.getSQLForUpdate: ${Field.getSQLForUpdate}, Field.getSQLForInsert: ${Field.getSQLForInsert}")

      var isOK: Boolean = false
      if (huemulBigDataGov.hasName(field.getMappedName))
        isOK = true
      else if (!huemulBigDataGov.hasName(field.getMappedName) &&
              (huemulBigDataGov.hasName(field.getSQLForUpdate) && huemulBigDataGov.hasName(field.getSQLForInsert)))
        isOK = true
      else
        isOK = false

      if (!isOK)
        stringSql.append(field.getMyName(this.getStorageType))
    }

     stringSql
  }

  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required
   */
  private def missingRequiredFieldsSelectiveUpdate(): String = {
    var PkNotMapped: String = ""
    var oneColumnMapped: Boolean = false
    //STEP 1: validate name setting
    getAllDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                    x.get(this).isInstanceOf[HuemulColumns]}
    .foreach { x =>
      //Get field
      val field = x.get(this).asInstanceOf[HuemulColumns]

      if (field.getIsPk) {

        var isOK: Boolean = false
        if (huemulBigDataGov.hasName(field.getMappedName))
          isOK = true
        else if (!huemulBigDataGov.hasName(field.getMappedName) &&
                (huemulBigDataGov.hasName(field.getSQLForUpdate) && huemulBigDataGov.hasName(field.getSQLForInsert)))
          isOK = true
        else
          isOK = false

        if (!isOK) {
          PkNotMapped = s"${if (PkNotMapped == "") "" else ", "}${field.getMyName(this.getStorageType)} "
        }

      } else
        oneColumnMapped = true

    }

    if (!oneColumnMapped)
        raiseError(s"huemul_Table Error: Only PK mapped, no columns mapped for update",1042)

     PkNotMapped
  }


  /*  ********************************************************************************
   *****   T A B L E   M E T H O D S    ****************************************
   ******************************************************************************** */
  def getSQLCreateTableScript: String = dfCreateTableScript()
  private def dfCreateTableScript(): String = {

    //var coma_partition = ""

    //Get SQL DataType for Partition Columns
    val partitionForCreateTable = getAllDeclaredFields().filter { x => x.setAccessible(true)
                                                                  x.get(this).isInstanceOf[HuemulColumns] &&
                                                                  x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition >= 1 }
                          .sortBy { x => x.setAccessible(true)
                                         x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition}
                          .map { x =>
                            val Field = x.get(this).asInstanceOf[HuemulColumns]
              val _colMyName = Field.getMyName(this.getStorageType)
              if (huemulBigDataGov.globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
                 s"${_colMyName}" //without datatype
              } else {
                val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)
                s"${_colMyName} ${_dataType.sql}"
              }
          }.mkString(",")

    /*
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val _colMyName = Field.getMyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)

      if (getPartitionField.toLowerCase() == _colMyName.toLowerCase() ) {
        if (huemulBigDataGov.GlobalSettings.getBigDataProvider == huemulType_bigDataProvider.databricks) {
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
    if (huemulBigDataGov.globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
      if (getStorageType == HuemulTypeStorageType.PARQUET || getStorageType == HuemulTypeStorageType.ORC ||
          getStorageType == HuemulTypeStorageType.DELTA   || getStorageType == HuemulTypeStorageType.AVRO) {
        //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                     CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.Normal)} (${getColumnsCreateTable(forHive = true) })
                                     USING ${getStorageType.toString}
                                     ${if (partitionForCreateTable.nonEmpty) s"PARTITIONED BY ($partitionForCreateTable)" else "" }
                                     LOCATION '$getFullNameWithPath'"""
      } else if (getStorageType == HuemulTypeStorageType.HBASE)  {
        lCreateTableScript = s"""
                                     CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.Normal)} (${getColumnsCreateTable(forHive = true) })
                                     USING 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                                     WITH SERDEPROPERTIES ("hbase.columns.mapping"="${getHBaseCatalogForHive(HuemulTypeInternalTableType.Normal)}")
                                     TBLPROPERTIES ("hbase.table.name"="${getHBaseNamespace(HuemulTypeInternalTableType.Normal)}:${getHBaseTableName(HuemulTypeInternalTableType.Normal)}")"""
      }
    } else {
      if (getStorageType == HuemulTypeStorageType.PARQUET || getStorageType == HuemulTypeStorageType.ORC ||
          getStorageType == HuemulTypeStorageType.DELTA   || getStorageType == HuemulTypeStorageType.AVRO) {
        //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                     CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.Normal)} (${getColumnsCreateTable(forHive = true) })
                                     ${if (partitionForCreateTable.nonEmpty) s"PARTITIONED BY ($partitionForCreateTable)" else "" }
                                     STORED AS ${getStorageType.toString}
                                     LOCATION '$getFullNameWithPath'"""
      } else if (getStorageType == HuemulTypeStorageType.HBASE)  {
        lCreateTableScript = s"""
                                     CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.Normal)} (${getColumnsCreateTable(forHive = true) })
                                     ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
                                     STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                                     WITH SERDEPROPERTIES ("hbase.columns.mapping"="${getHBaseCatalogForHive(HuemulTypeInternalTableType.Normal)}")
                                     TBLPROPERTIES ("hbase.table.name"="${getHBaseNamespace(HuemulTypeInternalTableType.Normal)}:${getHBaseTableName(HuemulTypeInternalTableType.Normal)}")"""
      }
    }

    if (huemulBigDataGov.debugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: $lCreateTableScript ")

     lCreateTableScript
  }

  /**
   * Create table script to save DQ Results
   */
  private def dfCreateTableDqScript(): String = {

    //var coma_partition = ""
    val partitionForCreateTable = if (huemulBigDataGov.globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) s"dq_control_id" else s"dq_control_id STRING"

    var lCreateTableScript: String = ""
    //FROM 2.4 --> INCLUDE SPECIAL OPTIONS FOR DATABRICKS
    if (huemulBigDataGov.globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
      if (getStorageTypeDqResult == HuemulTypeStorageType.PARQUET || getStorageTypeDqResult == HuemulTypeStorageType.ORC || getStorageTypeDqResult == HuemulTypeStorageType.AVRO ) {
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.DQ)} (${getColumnsCreateTable(forHive = true, HuemulTypeInternalTableType.DQ) })
                                   USING ${getStorageTypeDqResult.toString}
                                   PARTITIONED BY ($partitionForCreateTable)
                                   LOCATION '$getFullNameWithPathDq'"""
      } else if (getStorageTypeDqResult == HuemulTypeStorageType.HBASE)  {
        raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
      } else if (getStorageTypeDqResult == HuemulTypeStorageType.DELTA) {
        //for delta, databricks get all columns and partition columns
        //see https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html
        lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.DQ)}
                                   USING ${getStorageTypeDqResult.toString}
                                   LOCATION '$getFullNameWithPathDq'"""
      }
    }  else {
      if (getStorageTypeDqResult == HuemulTypeStorageType.PARQUET || getStorageTypeDqResult == HuemulTypeStorageType.ORC || getStorageTypeDqResult == HuemulTypeStorageType.AVRO) {
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.DQ)} (${getColumnsCreateTable(forHive = true, HuemulTypeInternalTableType.DQ) })
                                   PARTITIONED BY ($partitionForCreateTable)
                                   STORED AS ${getStorageTypeDqResult.toString}
                                   LOCATION '$getFullNameWithPathDq'"""
      } else if (getStorageTypeDqResult == HuemulTypeStorageType.HBASE)  {
        raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
      } else if (getStorageTypeDqResult == HuemulTypeStorageType.DELTA) {
        lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.DQ)}
                                   STORED AS ${getStorageTypeDqResult.toString}
                                   LOCATION '$getFullNameWithPathDq'"""
      }
    }

    if (huemulBigDataGov.debugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: $lCreateTableScript ")

     lCreateTableScript
  }



  /**
  CREATE TABLE SCRIPT TO SAVE OLD VALUE TRACE
   */
  private def dfCreateTableOldValueTraceScript(): String = {
    //var coma_partition = ""

    var lCreateTableScript: String = ""
    //FROM 2.4 --> INCLUDE SPECIAL OPTIONS FOR DATABRICKS
    if (huemulBigDataGov.globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
      lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.OldValueTrace)} (${getColumnsCreateTable(forHive = true, HuemulTypeInternalTableType.OldValueTrace) })
                                    ${if (getStorageTypeOldValueTrace == HuemulTypeStorageType.PARQUET) {
                                     """USING PARQUET
                                        PARTITIONED BY (mdm_columnname)"""
                                    }
                                    else if (getStorageTypeOldValueTrace == HuemulTypeStorageType.ORC) {
                                     """USING ORC
                                        PARTITIONED BY (mdm_columnname)"""
                                    }
                                    else if (getStorageTypeOldValueTrace == HuemulTypeStorageType.DELTA) {
                                     """USING DELTA
                                        PARTITIONED BY (mdm_columnname)"""
                                    }
                                    else if (getStorageTypeOldValueTrace == HuemulTypeStorageType.AVRO) {
                                     s"""USING AVRO
                                        PARTITIONED BY (mdm_columnname)"""
                                    }
                                   }
                                   LOCATION '$getFullNameWithPathOldValueTrace'"""
                                    //${if (_StorageType_OldValueTrace == "csv") {s"""
                                    //TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ")"""}}"""
    } else {
      //STORED AS ${_StorageType_OldValueTrace}
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
      lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.OldValueTrace)} (${getColumnsCreateTable(forHive = true, HuemulTypeInternalTableType.OldValueTrace) })
                                    ${if (getStorageTypeOldValueTrace.toString == "csv") {s"""
                                    ROW FORMAT DELIMITED
                                    FIELDS TERMINATED BY '\t'
                                    STORED AS TEXTFILE """}
                                    else if (getStorageTypeOldValueTrace.toString == "json") {
                                      s"""
                                       ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
                                      """
                                    }
                                    else if (getStorageTypeOldValueTrace == HuemulTypeStorageType.PARQUET) {
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS PARQUET"""
                                    }
                                    else if (getStorageTypeOldValueTrace == HuemulTypeStorageType.ORC) {
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS ORC"""
                                    }
                                    else if (getStorageTypeOldValueTrace == HuemulTypeStorageType.DELTA) {
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS DELTA"""
                                    }
                                    else if (getStorageTypeOldValueTrace == HuemulTypeStorageType.AVRO) {
                                     s"""PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS AVRO"""
                                    }
                                   }
                                   LOCATION '$getFullNameWithPathOldValueTrace'"""
                                    //${if (_StorageType_OldValueTrace == "csv") {s"""
                                    //TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ")"""}}"""
    }

    if (huemulBigDataGov.debugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: $lCreateTableScript ")

     lCreateTableScript
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

  private def dfForeingKeyMasterAuto(warning_exclude: Boolean, LocalControl: HuemulControl): HuemulDataQualityResult = {
    val result: HuemulDataQualityResult = new HuemulDataQualityResult()
    val arrayFK = this.getForeignKey
    val dataBaseName = this.getDataBase(this._dataBase)
    //For Each Foreing Key Declared
    arrayFK.filter { x => (warning_exclude && x.getNotification == HuemulTypeDqNotification.WARNING_EXCLUDE) ||
                          (!warning_exclude  && x.getNotification != HuemulTypeDqNotification.WARNING_EXCLUDE)
                   } foreach { x =>
      val pkInstanceTable = x._Class_TableName.asInstanceOf[HuemulTable]

      //Step1: Create distinct FROM NEW DF
      var sqlFields: String = ""
      var sqlLeftJoin: String = ""
      var sqlAnd: String = ""
      var sqlComa: String = ""
      var firstRowPK: String = ""
      var firstRowFK: String = ""
      x.relationship.foreach { y =>
        //Get fields name
        firstRowPK = s"PK.${y.PK.getMyName(pkInstanceTable.getStorageType)}"
        firstRowFK = y.FK.getMyName(this.getStorageType)
        sqlFields += s"$sqlComa${y.FK.getMyName(this.getStorageType)}"
        sqlLeftJoin += s"${sqlAnd}PK.${y.PK.getMyName(pkInstanceTable.getStorageType)} = FK.${y.FK.getMyName(this.getStorageType)}"
        sqlAnd = " and "
        sqlComa = ","
      }

      //val FKRuleName: String = GetFieldName[DAPI_MASTER_FK](this, x)
      val aliasDistinct_B: String = s"___${x.myName}_FKRuleDistB__"
      val dfDistinct = huemulBigDataGov.dfExecuteQuery(aliasDistinct_B, s"SELECT DISTINCT $sqlFields FROM ${this.dataFrameHuemul.alias} ${if (x.allowNull) s" WHERE $firstRowFK is not null " else "" }")

      var broadcast_sql: String = ""
      if (x.getBroadcastJoin)
        broadcast_sql = "/*+ BROADCAST(PK) */"

      //Step2: left join with TABLE MASTER DATA
      //val AliasLeft: String = s"___${x.MyName}_FKRuleLeft__"

      val pkTableName = pkInstanceTable.internalGetTable(HuemulTypeInternalTableType.Normal)
      val sqlLeft: String = s"""SELECT $broadcast_sql FK.*
                                 FROM $aliasDistinct_B FK
                                   LEFT JOIN $pkTableName PK
                                     ON $sqlLeftJoin
                                 WHERE $firstRowPK IS NULL
                              """

      val aliasDistinct: String = s"___${x.myName}_FKRuleDist__"
      val dtStart = huemulBigDataGov.getCurrentDateTimeJava
      val dfLeft = huemulBigDataGov.dfExecuteQuery(aliasDistinct, sqlLeft)
      var totalLeft = dfLeft.count()

      if (totalLeft > 0) {
        result.isError = true
        result.description = s"huemul_Table Error: Foreing Key DQ Error, $totalLeft records not found"
        result.errorCode = 1024
        result.dqDF = dfLeft
        result.profilingResult.count_all_Col = totalLeft
        dfLeft.show()

        val dfLeftDetail = huemulBigDataGov.dfExecuteQuery("__DF_leftDetail", s"""SELECT FK.*
                                                                                    FROM ${this.dataFrameHuemul.alias} FK
                                                                                      LEFT JOIN $pkTableName PK
                                                                                         ON $sqlLeftJoin
                                                                                    WHERE $firstRowPK IS NULL""")


        totalLeft = dfLeftDetail.count()

      }

      //val NumTotalDistinct = DF_Distinct.count()
      val numTotal = this.dataFrameHuemul.getNumRows
      val dtEnd = huemulBigDataGov.getCurrentDateTimeJava
      val duration = huemulBigDataGov.getDateTimeDiff(dtStart, dtEnd)

      val values = new HuemulDqRecord(huemulBigDataGov)
      values.tableName =tableName
      values.bbddName =dataBaseName
      values.dfAlias = dataFrameHuemul.alias
      values.columnName =firstRowFK
      values.dqName =s"FK - $sqlFields"
      values.dqDescription =s"FK Validation: PK Table: ${pkInstanceTable.internalGetTable(HuemulTypeInternalTableType.Normal)} "
      values.dqQueryLevel = HuemulTypeDqQueryLevel.Row // IsAggregate =false
      values.dqNotification = x.getNotification //from 2.1, before --> HuemulTypeDQNotification.ERROR// RaiseError =true
      values.dqSqlFormula =sqlLeft
      values.dqErrorCode = result.errorCode
      values.dqToleranceError_Rows =0L
      values.dqToleranceError_Percent =null
      values.dqResultDq =result.description
      values.dqNumRowsOk =numTotal - totalLeft
      values.dqNumRowsError =totalLeft
      values.dqNumRowsTotal =numTotal
      values.dqIsError = if (x.getNotification == HuemulTypeDqNotification.ERROR) result.isError else false
      values.dqIsWarning = if (x.getNotification != HuemulTypeDqNotification.ERROR) result.isError else false
      values.dqExternalCode = x.getExternalCode // "HUEMUL_DQ_001"
      values.dqDurationHour = duration.hour.toInt
      values.dqDurationMinute = duration.minute.toInt
      values.dqDurationSecond = duration.second.toInt

      this.dataFrameHuemul.DqRegister(values)

      //Step3: Return DQ Validation
      if (totalLeft > 0) {


        dfProcessToDq( "__DF_leftDetail"   //sqlfrom
                        , null           //sqlwhere
                        , haveField = true            //haveField
                        , firstRowFK      //fieldname
                        , values.dqId
                        , x.getNotification //from 2.1, before -->HuemulTypeDQNotification.ERROR //dq_error_notification
                        , result.errorCode //error_code
                        , s"(${result.errorCode}) FK ERROR ON $pkTableName"// dq_error_description
                        , LocalControl
                        )

      }


      dfDistinct.unpersist()
    }

     result
  }

  private def dfProcessToDq(fromSQL: String
                  ,whereSQL: String
                  ,haveField: Boolean
                  ,fieldName: String
                  ,dq_id: String
                  ,dq_error_notification: HuemulTypeDqNotification.HuemulTypeDqNotification
                  ,error_code: Integer
                  ,dq_error_description: String
                  ,LocalControl: HuemulControl
                  ) {
    //get SQL to save error details to DQ_Error_Table
        val sqlProcessToDqDetail = this.dataFrameHuemul.DqGenQuery( fromSQL
                                                              , whereSQL
                                                              , haveField
                                                              , fieldName
                                                              , dq_id
                                                              , dq_error_notification
                                                              , error_code
                                                              , dq_error_description
                                                              )
        val detailDf = huemulBigDataGov.dfExecuteQuery("__DetailDF", sqlProcessToDqDetail)

        //Save errors to disk
        if (huemulBigDataGov.globalSettings.dqSaveErrorDetails && detailDf != null && this.getSaveDQResult) {
          LocalControl.newStep("Start Save DQ Error Details for FK ")
          if (!savePersistDq(control, detailDf)){
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
  private def validateDqRun(warningExclude:Boolean
                            , dqNotification:HuemulTypeDqNotification): Boolean = (
    (warningExclude && dqNotification == HuemulTypeDqNotification.WARNING_EXCLUDE)
      || (!warningExclude && dqNotification != HuemulTypeDqNotification.WARNING_EXCLUDE)
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
                           , columnDefinition:HuemulColumns
                           , dqRuleDescription:String
                           , dqErrorCode:Integer
                           , minValue:String
                           , maxValue:String
                           , minValueNotification: HuemulTypeDqNotification
                           , maxValueNotification: HuemulTypeDqNotification
                           , sqlMinSentence:String
                           , sqlMaxSentence:String
                           , minExternalCode:String
                           , maxExternalCode:String):List[HuemulDataQuality] = {
    var listDqRule:List[HuemulDataQuality] = List[HuemulDataQuality]()
    val colMyName = columnDefinition.getMyName(this.getStorageType)
    val SQLFormula: HashMap[String, String] = new HashMap[String, String]
    var notificationMin:HuemulTypeDqNotification = null
    var notificationMax:HuemulTypeDqNotification = null
    var externalCode:String = null

    if (minValue != null && validateDqRun(dqWarningExclude,minValueNotification) ){
      SQLFormula += ( "Min" -> s" $sqlMinSentence ")
      notificationMin = minValueNotification
      externalCode = minExternalCode
    }

    if (maxValue != null && validateDqRun(dqWarningExclude,maxValueNotification)) {
      SQLFormula += ("Max" -> s" $sqlMaxSentence ")
      notificationMax = maxValueNotification
      externalCode = if (externalCode == null) maxExternalCode else externalCode
    }

    if (notificationMin == notificationMax || notificationMin == null || notificationMax == null ) {
      val ruleMinMax = new HuemulDataQuality(
        columnDefinition
        , s"${SQLFormula.keys.mkString("/")} $dqRuleDescription $colMyName"
        , s" (${SQLFormula.values.mkString(" and ")}) or ($colMyName is null) "
        , dqErrorCode
        , HuemulTypeDqQueryLevel.Row
        , if (notificationMin == null ) notificationMax else notificationMin
        , true
        , externalCode)
      ruleMinMax.setTolerance(0L, null)
      listDqRule  = listDqRule :+ ruleMinMax
    } else {
      val ruleMin: HuemulDataQuality = new HuemulDataQuality(
        columnDefinition
        , s"Min $dqRuleDescription $colMyName"
        , s" (${SQLFormula("Min")}) or ($colMyName is null) "
        , dqErrorCode
        , HuemulTypeDqQueryLevel.Row
        , notificationMin
        , true
        , if (minExternalCode != null && minExternalCode.nonEmpty)
          minExternalCode else maxExternalCode)
      ruleMin.setTolerance(0L, null)
      listDqRule = listDqRule :+ ruleMin

      val ruleMax: HuemulDataQuality = new HuemulDataQuality(
        columnDefinition
        , s"Max $dqRuleDescription $colMyName"
        , s" (${SQLFormula("Max")}) or ($colMyName is null) "
        , dqErrorCode
        , HuemulTypeDqQueryLevel.Row
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
 *
   * @author  christian.sattler@gmail.com
   * @since   2.[6]
   * @param warningExclude  It separates WARNING_EXCLUDE (true) from ERROR/WARNING (false)
   * @param resultDQ        DQ Rules execution result
   * @param localControl    huemul_control refernce for logging
   * @return [[com.huemulsolutions.bigdata.dataquality.HuemulDataQualityResult]] with new records
   */
  private def executeRuleTypeAggSaveData(warningExclude:Boolean
                                         , resultDQ:HuemulDataQualityResult
                                         , localControl: HuemulControl): Unit = {

    //.filter( f => (typeOpr == "pk" && f.DQ_ErrorCode == 1018) || (typeOpr == "uk" && f.DQ_ErrorCode == 2006) )
    //get on sentence, until now it only process pk and unique rules
    val dqResultData:List[HuemulDqRecord] = resultDQ
      .getDqResult
      .filter( f => (
        ((warningExclude && f.dqNotification == HuemulTypeDqNotification.WARNING_EXCLUDE)
          || !warningExclude && f.dqNotification != HuemulTypeDqNotification.WARNING_EXCLUDE)
          && (f.dqErrorCode == 1018 || f.dqErrorCode == 2006)) )
      .toList

    //Start loop each pk, uk data rule with error, warning or warning_exclude
    for(dqKeyRule <- dqResultData ) {
      val typeOpr:String = if (dqKeyRule.dqErrorCode==1018) "pk" else "uk"
      val dqId = dqKeyRule.dqId
      val notification:HuemulTypeDqNotification = dqKeyRule.dqNotification
      val stepRunType = notification match {
        case HuemulTypeDqNotification.WARNING_EXCLUDE => "WARNING_EXCLUDE"
        case HuemulTypeDqNotification.WARNING => "WARNING"
        case _ => "ERROR"
      }
      val colData:List[HuemulColumns] = getColumns
        .filter(f => (typeOpr == "pk" && f.getIsPk)
          || (typeOpr == "uk" && f.getIsUnique && f.getMyName(this.getStorageType).toLowerCase() == dqKeyRule.columnName.toLowerCase()))
        .toList
      val keyColumns:String = colData.head.getMyName(this.getStorageType)
      val joinColList:String = colData.map(m => m.getMyName(this.getStorageType)).mkString(",")
      val sourceAlias:String = this.dataFrameHuemul.alias
      val keyJoinSql = colData
        .map(m => s" $typeOpr.${m.getMyName(this.getStorageType)} = dup.${m.getMyName(this.getStorageType)}").mkString(" and ")

      localControl.newStep(s"Step: DQ Result: Get detail for ${typeOpr.toUpperCase} $stepRunType (step1)")
      val step1SQL = s"""SELECT $joinColList, COUNT(1) as Cantidad
                        | FROM $sourceAlias
                        | GROUP BY $joinColList
                        | HAVING COUNT(1) > 1 """.stripMargin
      if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"Step: DQ Result: Step1 Sql : $step1SQL")
      val dfDupRecords:DataFrame = huemulBigDataGov.dfExecuteQuery(s"___temp_${typeOpr}_det_01", step1SQL)
      dfDupRecords.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

      //Broadcast Hint
      val broadcastHint : String = if (dfDupRecords.count() < 50000) "/*+ BROADCAST(dup) */" else ""

      //get rows duplicated
      localControl.newStep(s"Step: DQ Result: Get detail for ${typeOpr.toUpperCase} $stepRunType (step2)")
      val step2SQL =s"""SELECT $broadcastHint $typeOpr.*
                       | FROM $sourceAlias $typeOpr
                       | INNER JOIN ___temp_${typeOpr}_det_01 dup
                       |  ON $keyJoinSql""".stripMargin
      if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"Step: DQ Result: Step2 Sql : $step2SQL")
      val dfDupRecordsDetail:DataFrame = huemulBigDataGov.dfExecuteQuery(s"___temp_${typeOpr}_det_02", step2SQL)
      dfDupRecordsDetail.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
      val numReg = dfDupRecordsDetail.count()

      val dqSqlTableSentence = this.dataFrameHuemul.DqGenQuery(
        s"___temp_${typeOpr}_det_02"
        , null
        , haveField = true
        , keyColumns
        , dqId
        , notification
        , dqKeyRule.dqErrorCode
        , s"(${dqKeyRule.dqErrorCode}) ${typeOpr.toUpperCase} $stepRunType ON $keyJoinSql ($numReg reg)"// dq_error_description
      )
      if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"Step: DQ Result: Step3 Sql : $dqSqlTableSentence")

      //Execute query
      localControl.newStep(s"Step: DQ Result: Get detail for ${typeOpr.toUpperCase} $stepRunType (step3, $numReg rows)")
      val dfErrorRecords = huemulBigDataGov.dfExecuteQuery(s"temp_DQ_KEY", dqSqlTableSentence)
      dfErrorRecords.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
      //val numCount = dfErrorRecords.count()
      if (resultDQ.detailErrorsDF == null)
        resultDQ.detailErrorsDF = dfErrorRecords
      else
        resultDQ.detailErrorsDF = resultDQ.detailErrorsDF.union(dfErrorRecords)

    }
  }

  /**create dataQuality for run
   *
   * @param isSelectiveUpdate: true if selective update
   * @param localControl: instance of local control
   * @param warningExclude: true for run with warning exclude mode
   * @return
   */
  private def dfDataQualityMasterAuto(isSelectiveUpdate: Boolean
                                      , localControl: HuemulControl
                                      , warningExclude: Boolean): HuemulDataQualityResult = {

    val result: HuemulDataQualityResult = new HuemulDataQualityResult()
    val arrayDQ: ArrayBuffer[HuemulDataQuality] = new ArrayBuffer[HuemulDataQuality]()

    //All required fields have been set
    val sqlMissing = missingRequiredFields(isSelectiveUpdate)
    if (sqlMissing.nonEmpty) {
      result.isError = true
      result.description += "\nhuemul_Table Error: requiered columns missing "
      result.errorCode = 1016
      sqlMissing.foreach { x => result.description +=  s",$x " }
    }

    //*********************************
    //Primary Key Validation
    //*********************************
    val sqlPk: String = sqlPrimaryKeyFinalTable()
    if (sqlPk == "" || sqlPk == null) {
      result.isError = true
      result.description += "\nhuemul_Table Error: PK not defined"
      result.errorCode = 1017
    }

    // Validate Primary Key Rule
    if (sqlPk != null && validateDqRun(warningExclude, _pkNotification )) {
      if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageInfo(s"DQ Rule: Validate PK for columns $sqlPk")
      val dqPk: HuemulDataQuality = new HuemulDataQuality(
        null
        , s"PK Validation"
        , s"count(1) = count(distinct $sqlPk )"
        , 1018
        , HuemulTypeDqQueryLevel.Aggregate
        , _pkNotification
        , true
        , getPkExternalCode)
      dqPk.setTolerance(0L, null)
      arrayDQ.append(dqPk)
    }

    //Apply Data Quality according to field definition in DataDefDQ: Unique Values
    //Note: It doesn't apply getDQ_Notification, it must be explicit se the notification
    sqlUniqueFinalTable()
      .filter(f => validateDqRun(warningExclude, f.getIsUniqueNotification ))
      .foreach { x =>
        if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: VALIDATE UNIQUE FOR FIELD ${x.getMyName(this.getStorageType)}")

        val DqUnique : HuemulDataQuality = new HuemulDataQuality(
          x
          , s"UNIQUE Validation ${x.getMyName(this.getStorageType)}"
          ,s"count(1) = count(distinct ${x.getMyName(this.getStorageType)} )"
          , 2006
          , HuemulTypeDqQueryLevel.Aggregate
          , x.getIsUniqueNotification
          , true
          , x.getIsUniqueExternalCode )
        DqUnique.setTolerance(0L, null)
        arrayDQ.append(DqUnique)
      }

    //Apply Data Quality according to field definition in DataDefDQ: Accept nulls (nullable)
    //Note: If it has default value and it is NOT NULL it will not be evaluated
    //ToDO: Checks te notes for default values
    sqlNotNullFinalTable()
      .filter( f => (!f.getNullable  && (f.getDefaultValue == null || f.getDefaultValue.equals("null")))
        && validateDqRun(warningExclude, f.getDqNullableNotification ) )
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: VALIDATE NOT NULL FOR FIELD $colMyName")

        val NotNullDQ : HuemulDataQuality = new HuemulDataQuality(
          x
          , s"Not Null for field $colMyName "
          , s"$colMyName IS NOT NULL"
          , 1023
          , HuemulTypeDqQueryLevel.Row
          , x.getDqNullableNotification
          , true
          , x.getNullableExternalCode)
        NotNullDQ.setTolerance(0L, null)
        arrayDQ.append(NotNullDQ)
      }

    //Validación DQ_RegExp
    getColumns.filter {
      x => x.getDqRegExpression != null && validateDqRun(warningExclude, x.getDqRegExpressionNotification)
    }.foreach { x =>
      val _colMyName = x.getMyName(this.getStorageType)
      val sqlFormula : String = s"""${_colMyName} rlike "${x.getDqRegExpression}" """

      val regExp : HuemulDataQuality = new HuemulDataQuality(
        x
        , s"RegExp Column ${_colMyName}"
        , s" ($sqlFormula) or (${_colMyName} is null) "
        , 1041
        , HuemulTypeDqQueryLevel.Row
        , x.getDqRegExpressionNotification
        , true
        , x.getDqRegExpressionExternalCode  )

      regExp.setTolerance(0L, null)
      arrayDQ.append(regExp)
    }


    //Validate DQ rule max/mín length the field
    getColumns
      .filter { x =>(
        (x.getDqMinLen != null && validateDqRun(warningExclude, x.getDqMinLenNotification) )
          || (x.getDqMaxLen != null && validateDqRun(warningExclude,x.getDqMaxLenNotification) )
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[HuemulDataQuality] = dqRuleMinMax(
          warningExclude
          , x
          , "length DQ for Colum"
          , 1020
          , if (x.getDqMinLen != null) x.getDqMinLen.toString else null
          , if (x.getDqMaxLen != null) x.getDqMaxLen.toString else null
          , x.getDqMinLenNotification
          , x.getDqMaxLenNotification
          , s"length($colMyName) >= ${x.getDqMinLen}"
          , s"length($colMyName) <= ${x.getDqMaxLen}"
          , x.getDqMinLenExternalCode
          , x.getDqMaxLenExternalCode)

        dqRules.foreach(dq => arrayDQ.append(dq))
      }

    //Validate DQ rule max/mín number value
    getColumns
      .filter { x => (
        (x.getDqMinDecimalValue != null && validateDqRun(warningExclude, x.getDqMinDecimalValueNotification ) )
          || (x.getDqMaxDecimalValue != null && validateDqRun(warningExclude, x.getDqMaxDecimalValueNotification ))
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[HuemulDataQuality] = dqRuleMinMax(
          warningExclude
          , x
          , "Number DQ for Column"
          , 1021
          , if (x.getDqMinDecimalValue != null) x.getDqMinDecimalValue.toString else null
          , if (x.getDqMaxDecimalValue != null) x.getDqMaxDecimalValue.toString else null
          , x.getDqMinDecimalValueNotification
          , x.getDqMaxDecimalValueNotification
          , s" $colMyName >= ${x.getDqMinDecimalValue} "
          , s" $colMyName <= ${x.getDqMaxDecimalValue} "
          , x.getDqMinDecimalValueExternalCode
          , x.getDqMaxDecimalValueExternalCode)

        dqRules.foreach(dq => arrayDQ.append(dq))
      }

    //Validación DQ máximo y mínimo de fechas
    getColumns
      .filter { x => (
        (x.getDqMinDateTimeValue != null && validateDqRun(warningExclude, x.getDqMinDateTimeValueNotification ) )
          || (x.getDqMaxDateTimeValue != null && validateDqRun(warningExclude, x.getDqMaxDateTimeValueNotification ))
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[HuemulDataQuality] = dqRuleMinMax(
          warningExclude
          , x
          , "DateTime DQ for Column"
          , 1022
          , x.getDqMinDateTimeValue
          , x.getDqMaxDateTimeValue
          , x.getDqMinDateTimeValueNotification
          , x.getDqMaxDateTimeValueNotification
          , s" $colMyName >= '${x.getDqMinDateTimeValue}' "
          , s" $colMyName <= '${x.getDqMaxDateTimeValue}' "
          , x.getDqMinDateTimeValueExternalCode
          , x.getDqMaxDateTimeValueExternalCode)

        dqRules.foreach(dq => arrayDQ.append(dq))
      }

    if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: DQ Rule size ${arrayDQ.size}")
    arrayDQ.foreach(x => if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: Rule - ${x.getDescription}"))

    val ResultDQ = this.dataFrameHuemul.DF_RunDataQuality(
      this.getDataQuality(warningExclude)
      , arrayDQ
      , this.dataFrameHuemul.alias
      , this
      , this.getSaveDQErrorOnce)

    val stepRunType = if (warningExclude) "warning_exclude" else "error"

    // 2.[6] add condition if its has warning and warning_exclude=true
    if (ResultDQ.isError || (ResultDQ.isWarning && warningExclude)){

      result.isWarning = ResultDQ.isWarning
      result.isError = ResultDQ.isError
      result.description += s"\n${ResultDQ.description}"
      result.errorCode = ResultDQ.errorCode

      //version 2.6: add PK error detail
      executeRuleTypeAggSaveData(warningExclude, ResultDQ, localControl)
    }

    //Save errors to disk
    if (huemulBigDataGov.globalSettings.dqSaveErrorDetails && ResultDQ.detailErrorsDF != null && this.getSaveDQResult) {
      localControl.newStep(s"Start Save DQ $stepRunType Details ")
      if (!savePersistDq(control, ResultDQ.detailErrorsDF)){
        huemulBigDataGov.logMessageWarn(s"Warning: DQ $stepRunType can't save to disk")
      }
    }

     result
  }


  def getOrderByColumn: String = {
    var columnName: String = null

    this.getAllDeclaredFields(OnlyUserDefined = true, partitionColumnToEnd = true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] &&
                                      x.get(this).asInstanceOf[HuemulColumns].getIsPk }.foreach { x =>
      x.setAccessible(true)
      if (columnName == null)
        columnName = x.get(this).asInstanceOf[HuemulColumns].getMyName(this.getStorageType)
    }

     columnName
  }


  /**
   Create final DataFrame with full join New DF with old DF
   */
  private def dfMdmDoHuemul(localControl: HuemulControl, aliasNewData: String, isInsert: Boolean, isUpdate: Boolean, isDelete: Boolean, isSelectiveUpdate: Boolean, partitionValuesForSelectiveUpdate: ArrayBuffer[String], storageLevelOfDF: org.apache.spark.storage.StorageLevel = null) {
    if (isSelectiveUpdate) {
      //Update some rows with some columns
      //Cant update PK fields

      //***********************************************************************/
      //STEP 1:    V A L I D A T E   M A P P E D   C O L U M N  S   *********/
      //***********************************************************************/

      localControl.newStep("Selective Update: Validating fields ")

      val PkNotMapped = this.missingRequiredFieldsSelectiveUpdate()

      if (PkNotMapped != "")
        raiseError(s"huemul_Table Error: PK not defined: $PkNotMapped",1017)

      //Get N° rows from user dataframe
      val numRowsUserData: java.lang.Long = this.dataFrameHuemul.dataFrame.count()
      var numRowsOldDataFrame: java.lang.Long = 0L
      //**************************************************//
      //STEP 2:   CREATE BACKUP
      //**************************************************//
      localControl.newStep("Selective Update: Select Old Table")
      val tempAlias: String = s"__${this.tableName}_old"
      //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)

      //from 2.6 --> validate numpartitions = numvalues
      //if (!huemulBigDataGov.HasName(PartitionValueForSelectiveUpdate) && _TableType == huemulType_Tables.Transaction)
      if (getPartitionList.length > 0 && (partitionValuesForSelectiveUpdate.length != getPartitionList.length ||
                                          partitionValuesForSelectiveUpdate.contains(null)))
        raiseError(s"huemul_Table Error: Partition Value not defined (partitions: ${getPartitionList.mkString(",") }, values: ${partitionValuesForSelectiveUpdate.mkString(",")}) ", 1044)


      var fullPathString = this.getFullNameWithPath

      if (_tableType == HuemulTypeTables.Transaction) {
        val partitionList = getPartitionList
        var i: Int = 0
        while (i < getPartitionList.length) {
          fullPathString += s"/${partitionList(i).getMyName(getStorageType)}=${partitionValuesForSelectiveUpdate(i)}"
          i+=1
        }

        //FullPathString = s"${getFullNameWithPath}/${getPartitionField.toLowerCase()}=${PartitionValueForSelectiveUpdate}"
      }

      val fullPath = new org.apache.hadoop.fs.Path(fullPathString)
      //Google FS compatibility
      val fs = fullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)

      if (fs.exists(fullPath)){
        //Exist, copy for use
        val dt_start = huemulBigDataGov.getCurrentDateTimeJava
        //Open actual file
        val dfTempCopy = huemulBigDataGov.spark.read.format( _getSaveFormat(this.getStorageType)).load(fullPathString)
        val tempPath = huemulBigDataGov.globalSettings.getDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, tempAlias)
        if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"copy to temp dir: $tempPath ")
        if (this.getNumPartitions == null || this.getNumPartitions <= 0)
          dfTempCopy.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
        else
          dfTempCopy.repartition(this.getNumPartitions).write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)

        dfTempCopy.unpersist()

        //Open temp file
        if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"open temp old df: $tempPath ")

        val lineageWhere: ArrayBuffer[String] = new ArrayBuffer[String]
        var dfTempOpen = huemulBigDataGov.spark.read.parquet(tempPath)
        //from 2.6 --> add partition columns and values
        var i: Int = 0
        val partitionList = getPartitionList
        while (i < getPartitionList.length) {
          dfTempOpen = dfTempOpen.withColumn(partitionList(i).getMyName(getStorageType) , lit(partitionValuesForSelectiveUpdate(i)))
          lineageWhere.append(s"${partitionList(i).getMyName(getStorageType)} = '${partitionValuesForSelectiveUpdate(i)}'")
          i += 1
        }
        /*
        val DFTempOpen = if (_TableType == huemulType_Tables.Transaction)
                            huemulBigDataGov.spark.read.parquet(tempPath).withColumn(getPartitionField.toLowerCase(), lit(PartitionValueForSelectiveUpdate))
                         else huemulBigDataGov.spark.read.parquet(tempPath)
                         *
                         */
        numRowsOldDataFrame = dfTempOpen.count()
        dfTempOpen.createOrReplaceTempView(tempAlias)

        val dt_end = huemulBigDataGov.getCurrentDateTimeJava
        huemulBigDataGov.dfSaveLineage(tempAlias
                                    , s"""SELECT * FROM ${this.internalGetTable(HuemulTypeInternalTableType.Normal)} ${ if (lineageWhere.nonEmpty)  /*if (_TableType == huemulType_Tables.Transaction)*/
                                                                                                                      s" WHERE ${lineageWhere.mkString(" and ")}"} """ //sql
                                    , dt_start
                                    , dt_end
                                    , control
                                    , null  //FinalTable
                                    , isQuery = false //isQuery
                                    , isReferenced = true //isReferenced
                                    )
      } else
        raiseError(s"huemul_Table Error: Table $fullPathString doesn't exists",1043)


      //**************************************************//
      //STEP 3: Create left Join updating columns
      //**************************************************//
      localControl.newStep("Selective Update: Left Join")
      val sqlLeftJoinDf = huemulBigDataGov.dfExecuteQuery("__LeftJoin"
                                              , sqlStep2LeftJoin(tempAlias, this.dataFrameHuemul.alias)
                                              , control //parent control
                                             )


      //**************************************************//
      //STEP 4: Create Table with Update and Insert result
      //**************************************************//

      localControl.newStep("Selective Update: Update Logic")
      val sqlHashP1Df = huemulBigDataGov.dfExecuteQuery("__update_p1"
                                          , sqlStep4Update("__LeftJoin", huemulBigDataGov.ProcessNameCall)
                                          , control //parent control
                                         )

      //**************************************************//
      //STEP 5: Create Hash
      //**************************************************//
      localControl.newStep("Selective Update: Hash Code")
      val sqlHashP2Df = huemulBigDataGov.dfExecuteQuery("__Hash_p1"
                                          , sqlStep3HashP1("__update_p1", isSelectiveUpdate)
                                          , control //parent control
                                         )

      this.updateStatistics(localControl, "Selective Update", "__Hash_p1")

      //N° Rows Updated == NumRowsUserData
      if (numRowsUserData != this._numRowsUpdatable) {
        raiseError(s"huemul_Table Error: it was expected to update $numRowsUserData rows, only ${_numRowsUpdatable} was found", 1045)
      } else if (this._numRowsTotal != numRowsOldDataFrame ) {
        raiseError(s"huemul_Table Error: Different number of rows in dataframes, original: $numRowsOldDataFrame, new: ${this._numRowsTotal}, check your dataframe, maybe have duplicate keys", 1046)
      }

      localControl.newStep("Selective Update: Final Table")
      val sqlFinalTable = sqlStep4Final("__Hash_p1", huemulBigDataGov.ProcessNameCall)


      //STEP 2: Execute final table  //Add this.getNumPartitions param in v1.3
      dataFrameHuemul._createFinalQuery(aliasNewData , sqlFinalTable, huemulBigDataGov.debugMode , this.getNumPartitions, this, storageLevelOfDF)
      if (huemulBigDataGov.debugMode) this.dataFrameHuemul.dataFrame.show()

      //Unpersist first DF
      sqlHashP2Df.unpersist()
      sqlHashP1Df.unpersist()
      sqlLeftJoinDf.unpersist()
    }
    else if (_tableType == HuemulTypeTables.Transaction) {
      localControl.newStep("Transaction: Validating fields ")
      //STEP 1: validate name setting
      getAllDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
      .foreach { x =>
        //Get field
        val field = x.get(this).asInstanceOf[HuemulColumns]

        //New value (from field or compute column )
        if (!huemulBigDataGov.hasName(field.getMappedName) ){
          raiseError(s"${x.getName} MUST have an assigned local name",1004)
        }
      }

      //STEP 2: Create Hash
      localControl.newStep("Transaction: Create Hash Field")
      val sqlFinalTable = sqlStep0TxHash(this.dataFrameHuemul.alias, huemulBigDataGov.ProcessNameCall)
      if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery)
        huemulBigDataGov.logMessageDebug(sqlFinalTable)
      //STEP 2: Execute final table //Add debugmode and getnumpartitions in v1.3
      dataFrameHuemul._createFinalQuery(aliasNewData , sqlFinalTable, huemulBigDataGov.debugMode , this.getNumPartitions, this, storageLevelOfDF)

      //from 2.2 --> persist to improve performance
      dataFrameHuemul.dataFrame.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

      //LocalControl.NewStep("Transaction: Get Statistics info")
      this.updateStatistics(localControl, "Transaction", null)


      if (huemulBigDataGov.debugMode) this.dataFrameHuemul.dataFrame.show()
    }
    else if (_tableType == HuemulTypeTables.Reference || _tableType == HuemulTypeTables.Master) {
      /*
       * isInsert: se aplica en SQL_Step2_UpdateAndInsert, si no permite insertar, filtra esos registros y no los inserta
       * isUpdate: se aplica en SQL_Step1_FullJoin: si no permite update, cambia el tipo ___ActionType__ de UPDATE a EQUAL
       */

      //val OnlyInsert: Boolean = isInsert && !isUpdate

      //All required fields have been set
      val sqlMissing = missingRequiredFields(isSelectiveUpdate)
      if (sqlMissing.nonEmpty) {
        var columnsMissing: String = ""
        sqlMissing.foreach { x => columnsMissing +=  s",$x " }
        this.raiseError(s"huemul_Table Error: requiered fields missing $columnsMissing", 1016)

      }

      //**************************************************//
      //STEP 0: Apply distinct to New DataFrame
      //**************************************************//
      var nextAlias = this.dataFrameHuemul.alias

      if (applyDistinct) {
        localControl.newStep("Ref & Master: Select distinct")
        huemulBigDataGov.dfExecuteQuery("__Distinct"
                                                , sqlStep0Distinct(this.dataFrameHuemul.alias)
                                                , control //parent control
                                               )
        nextAlias = "__Distinct"
      }


      //**************************************************//
      //STEP 0.1: CREATE TEMP TABLE IF MASTER TABLE DOES NOT EXIST
      //**************************************************//
      localControl.newStep("Ref & Master: Select Old Table")
      val dtStart = huemulBigDataGov.getCurrentDateTimeJava
      val tempAlias: String = s"__${this.tableName}_old"
      //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)

      var oldDataFrameExists = false

      if (this.getStorageType == HuemulTypeStorageType.HBASE) {
        val hBaseConnector = new HuemulTableConnector(huemulBigDataGov, localControl)
        if (hBaseConnector.tableExistsHBase(getHBaseNamespace(HuemulTypeInternalTableType.Normal),getHBaseTableName(HuemulTypeInternalTableType.Normal))) {
          oldDataFrameExists = true
          //println(getHBaseCatalog(huemulType_InternalTableType.Normal))
          //val DFHBase = hBaseConnector.getDFFromHBase(TempAlias, getHBaseCatalog(huemulType_InternalTableType.Normal))

          //create external table if doesn't exists
          createTableScript = dfCreateTableScript()

          //new from 2.3
          runSqlExternalTable(createTableScript, onlyHBase = true)

          localControl.newStep("Ref & Master: Reading HBase data...")
          val dfHBase = huemulBigDataGov.dfExecuteQuery(tempAlias, s"SELECT * FROM ${this.internalGetTable(HuemulTypeInternalTableType.Normal)}")
          dfHBase.count()
          if (huemulBigDataGov.globalSettings.mdmSaveBackup && this._saveBackup){
            val tempPath = this.getFullNameWithPathBackup(control.Control_Id )
            _BackupPath = tempPath
            if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"copy to backup dir: $tempPath ")

            localControl.newStep("Ref & Master: Backup...")
            dfHBase.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)     //2.2 -> this._StorageType.toString() instead of "parquet"
          }


          //DFHBase.show()
        }
      } else {
        val lPath = new org.apache.hadoop.fs.Path(this.getFullNameWithPath)
        val fs = lPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
        if (fs.exists(lPath)){
          //Exist, copy for use
          oldDataFrameExists = true

          //Open actual file
          val dfTempCopy = huemulBigDataGov.spark.read.format(_getSaveFormat(this.getStorageType)).load(this.getFullNameWithPath)
          if (huemulBigDataGov.debugMode) dfTempCopy.printSchema()

          //2.0: save previous to backup
          var tempPath: String = null
          if (huemulBigDataGov.globalSettings.mdmSaveBackup && this._saveBackup){
            tempPath = this.getFullNameWithPathBackup(control.Control_Id )
            _BackupPath = tempPath
            if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"copy to backup dir: $tempPath ")
          } else {
            tempPath = huemulBigDataGov.globalSettings.getDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, tempAlias)
            if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"copy to temp dir: $tempPath ")
          }

          val fsPath = new org.apache.hadoop.fs.Path(tempPath)
          val fs = fsPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
          if (fs.exists(fsPath)){
            fs.delete(fsPath, true)
          }

          if (this.getNumPartitions == null || this.getNumPartitions <= 0)
            dfTempCopy.write.mode(SaveMode.Overwrite).format(_getSaveFormat(this.getStorageType)).save(tempPath)     //2.2 -> this._StorageType.toString() instead of "parquet"
          else
            dfTempCopy.repartition(this.getNumPartitions).write.mode(SaveMode.Overwrite).format(_getSaveFormat(this.getStorageType)).save(tempPath)   //2.2 -> this._StorageType.toString() instead of "parquet"
          dfTempCopy.unpersist()

          //Open temp file
          if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"open temp old df: $tempPath ")
          val dfTempOpen = huemulBigDataGov.spark.read.format(_getSaveFormat(this.getStorageType)).load(tempPath)  //2.2 --> read.format(this._StorageType.toString()).load(tempPath)    instead of  read.parquet(tempPath)
          if (huemulBigDataGov.debugMode) dfTempOpen.printSchema()
          dfTempOpen.createOrReplaceTempView(tempAlias)
        }
      }

      if (!oldDataFrameExists) {
          //huemulBigDataGov.logMessageInfo(s"create empty dataframe because ${this.internalGetTable(huemulType_InternalTableType.Normal)} does not exist")
          huemulBigDataGov.logMessageInfo(s"create empty dataframe because oldDF does not exist")
          val schema = getSchema
          val schemaForEmpty = StructType(schema.map { x => StructField(x.name, x.dataType, x.nullable) })
          val emptyRdd = huemulBigDataGov.spark.sparkContext.emptyRDD[Row]
          val emptyDf = huemulBigDataGov.spark.createDataFrame(emptyRdd, schemaForEmpty)
          emptyDf.createOrReplaceTempView(tempAlias)
          if (huemulBigDataGov.debugMode) emptyDf.show()
      }
      val dtEnd = huemulBigDataGov.getCurrentDateTimeJava

      huemulBigDataGov.dfSaveLineage(tempAlias
                                    , s"SELECT * FROM ${this.internalGetTable(HuemulTypeInternalTableType.Normal)} " //sql
                                    , dtStart
                                    , dtEnd
                                    , control
                                    , null  //FinalTable
                                    , isQuery = false //isQuery
                                    , isReferenced = true //isReferenced
                                    )



      //**************************************************//
      //STEP 1: Create Full Join with all fields
      //**************************************************//
      localControl.newStep("Ref & Master: Full Join")
      val SqlFullJoinDf = huemulBigDataGov.dfExecuteQuery("__FullJoin"
                                              , sqlStep1FullJoin(tempAlias, nextAlias, isUpdate, isDelete)
                                              , control //parent control
                                             )

      //from 2.2 --> add to compatibility with HBase (without this, OldValueTrace doesn't work becacuse it recalculate with new HBase values
      SqlFullJoinDf.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)


      //STEP 2: Create Tabla with Update and Insert result
      localControl.newStep("Ref & Master: Update & Insert Logic")
      huemulBigDataGov.dfExecuteQuery("__logic_p1"
                                          , sqlStep2UpdateAndInsert("__FullJoin", huemulBigDataGov.ProcessNameCall, isInsert)
                                          , control //parent control
                                         )

      //STEP 3: Create Hash
      localControl.newStep("Ref & Master: Hash Code")
      huemulBigDataGov.dfExecuteQuery("__Hash_p1"
                                          , sqlStep3HashP1("__logic_p1", isSelectiveUpdate = false)
                                          , control //parent control
                                         )


      this.updateStatistics(localControl, "Ref & Master", "__Hash_p1")


      localControl.newStep("Ref & Master: Final Table")
      //val SQLFinalTable = SQL_Step4_Final("__Hash_p1", huemulBigDataGov.ProcessNameCall, if (OnlyInsert) true else false)
      //from 2.1: incluye ActionType fro all master and transaction table types, to use on statistics
      val sqlFinalTable = sqlStep4Final("__Hash_p1", huemulBigDataGov.ProcessNameCall )


      //STEP 2: Execute final table // Add debugmode and getnumpartitions in v1.3
      dataFrameHuemul._createFinalQuery(aliasNewData , sqlFinalTable, huemulBigDataGov.debugMode , this.getNumPartitions, this, storageLevelOfDF)
      if (huemulBigDataGov.debugMode) this.dataFrameHuemul.dataFrame.printSchema()
      if (huemulBigDataGov.debugMode) this.dataFrameHuemul.dataFrame.show()

    } else
      raiseError(s"huemul_Table Error: ${_tableType} found, Master o Reference required ", 1007)
  }

  private def updateStatistics(localControl: HuemulControl, typeOfCall: String, alias: String) {
    localControl.newStep(s"$typeOfCall: Statistics")

    if (alias == null) {
      this._numRowsTotal = this.dataFrameHuemul.getNumRows
      this._numRowsNew = this._numRowsTotal
      this._numRowsUpdate  = 0L
      this._numRowsUpdatable = 0L
      this._numRowsDelete = 0L
      this._numRowsNoChange = 0L
      //this._NumRows_Excluded = 0
    } else {
      //DQ for Reference and Master Data
      val dqReferenceData: DataFrame = huemulBigDataGov.spark.sql(
                        s"""SELECT CAST(SUM(CASE WHEN ___ActionType__ = 'NEW' then 1 else 0 end) as Long) as __New
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'UPDATE' and SameHashKey = 0 then 1 else 0 end) as Long) as __Update
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'UPDATE' then 1 else 0 end) as Long) as __Updatable
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'DELETE' then 1 else 0 end) as Long) as __Delete
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'EQUAL' OR
                                                (___ActionType__ = 'UPDATE' and SameHashKey <> 0) then 1 else 0 end) as Long) as __NoChange
                                  ,CAST(count(1) AS Long) as __Total
                            FROM $alias temp
                         """)

      if (huemulBigDataGov.debugMode) dqReferenceData.show()

      val firstRow = dqReferenceData.collect()(0) // .first()
      this._numRowsTotal = firstRow.getAs("__Total")
      this._numRowsNew = firstRow.getAs("__New")
      this._numRowsUpdate  = firstRow.getAs("__Update")
      this._numRowsUpdatable  = firstRow.getAs("__Updatable")
      this._numRowsDelete = firstRow.getAs("__Delete")
      this._numRowsNoChange= firstRow.getAs("__NoChange")


      localControl.newStep(s"$typeOfCall: Validating Insert & Update")
      var dqError: String = ""
      var errorNumber: Integer = null
      if (this._numRowsTotal == this._numRowsNew)
        dqError = "" //Doesn't have error, first run
      else if (this._dQMaxNewRecordsNum != null && this._dQMaxNewRecordsNum > 0 && this._numRowsNew > this._dQMaxNewRecordsNum){
        dqError = s"huemul_Table Error: DQ MDM Error: N° New Rows (${this._numRowsNew}) exceeds max defined (${this._dQMaxNewRecordsNum}) "
        errorNumber = 1005
      }
      else if (this._dQMaxNewRecordsPerc != null && this._dQMaxNewRecordsPerc > Decimal.apply(0) && (Decimal.apply(this._numRowsNew) / Decimal.apply(this._numRowsTotal)) > this._dQMaxNewRecordsPerc) {
        dqError = s"huemul_Table Error: DQ MDM Error: % New Rows (${Decimal.apply(this._numRowsNew) / Decimal.apply(this._numRowsTotal)}) exceeds % max defined (${this._dQMaxNewRecordsPerc}) "
        errorNumber = 1006
      }

      if (dqError != "")
        raiseError(dqError, errorNumber)

      dqReferenceData.unpersist()
    }
  }

  private def getClassAndPackage: HuemulAuthorizationPair = {
    var invoker: StackTraceElement = null
    var invokerName: String = null
    var classNameInvoker: String = null
    var packageNameInvoker: String = null

    var numClassBack: Int = 1
    do {
      //repeat until find className different to myself
      numClassBack += 1
      invoker = new Exception().getStackTrace()(numClassBack)
      invokerName = invoker.getClassName.replace("$", "")

      val arrayResult = invokerName.split('.')

      classNameInvoker = arrayResult(arrayResult.length-1)
      packageNameInvoker = invokerName.replace(".".concat(classNameInvoker), "")

      //println(s"${numClassBack}, ${ClassNameInvoker.toLowerCase()} == ${this.getClass.getSimpleName.toLowerCase()}")
      if (classNameInvoker.toLowerCase() == this.getClass.getSimpleName.toLowerCase() || classNameInvoker.toLowerCase() == "huemul_table") {
        invoker = null
      }

    } while (invoker == null)

     new HuemulAuthorizationPair(classNameInvoker,packageNameInvoker)
  }

  def executeFull(newAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
    var result: Boolean = false
    val whoExecute = getClassAndPackage
    if (this._whoCanRunExecuteFull.hasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      result = this.executeSave(newAlias, IsInsert = true, IsUpdate = true, IsDelete = true, IsSelectiveUpdate = false, null, storageLevelOfDF, registerOnlyInsertInDq = false)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeFull in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}", 1008)
    }

     result
  }

  //2.1: compatibility with previous version
  def executeOnlyInsert(newAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
     executeOnlyInsert(newAlias, storageLevelOfDF, registerOnlyInsertInDq = false)
  }

  //2.1: add RegisterOnlyInsertInDQ params
  def executeOnlyInsert(newAlias: String, registerOnlyInsertInDQ: Boolean): Boolean = {
     executeOnlyInsert(newAlias, null, registerOnlyInsertInDQ)
  }

  //2.1: introduce RegisterOnlyInsertInDQ
  def executeOnlyInsert(newAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel, registerOnlyInsertInDq: Boolean): Boolean = {
    var result: Boolean = false
    if (this._tableType == HuemulTypeTables.Transaction)
      raiseError("huemul_Table Error: DoOnlyInserthuemul is not available for Transaction Tables",1009)

    val whoExecute = getClassAndPackage
    if (this._whoCanRunExecuteOnlyInsert.hasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      result = this.executeSave(newAlias, IsInsert = true, IsUpdate = false, IsDelete = false, IsSelectiveUpdate = false, null, storageLevelOfDF, registerOnlyInsertInDq = registerOnlyInsertInDq)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeOnlyInsert in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}", 1010)
    }

     result
  }

  def executeOnlyUpdate(newAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
    var result: Boolean = false
    if (this._tableType == HuemulTypeTables.Transaction)
      raiseError("huemul_Table Error: DoOnlyUpdatehuemul is not available for Transaction Tables", 1011)

    val whoExecute = getClassAndPackage
    if (this._whoCanRunExecuteOnlyUpdate.hasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      result = this.executeSave(newAlias, IsInsert = false, IsUpdate = true, IsDelete = false, IsSelectiveUpdate = false, null, storageLevelOfDF, registerOnlyInsertInDq = false)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeOnlyUpdate in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}",1012)
    }

     result

  }

  def executeSelectiveUpdate(newAlias: String, PartitionValueForSelectiveUpdate: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
    var result: Boolean = false
    val newValue: ArrayBuffer[String] = new ArrayBuffer[String]()
    newValue.append(PartitionValueForSelectiveUpdate)

    val whoExecute = getClassAndPackage
    if (this._whoCanRunExecuteOnlyUpdate.hasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      result = executeSelectiveUpdate(newAlias, newValue, storageLevelOfDF)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeSelectiveUpdate in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}",1012)
    }

     result
  }

  def executeSelectiveUpdate(newAlias: String, partitionValueForSelectiveUpdate: ArrayBuffer[String], storageLevelOfDF: org.apache.spark.storage.StorageLevel ): Boolean = {
    var result: Boolean = false

    val whoExecute = getClassAndPackage
    if (this._whoCanRunExecuteOnlyUpdate.hasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      result = this.executeSave(newAlias, IsInsert = false, IsUpdate = false, IsDelete = false, IsSelectiveUpdate = true, partitionValueForSelectiveUpdate, storageLevelOfDF, registerOnlyInsertInDq = false)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeSelectiveUpdate in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}",1012)
    }

     result

  }

  private def compareSchema(columns: ArrayBuffer[HuemulColumns], schema: StructType): String = {
    var errores: String = ""
    columns.foreach { x =>
      if (huemulBigDataGov.hasName(x.getMappedName)) {
        //val ColumnNames = if (UseAliasColumnName) x.getMappedName else x.getMyName()
        val columnNames = x.getMyName(getStorageType)
        val _dataType  = x.getDataTypeDeploy(huemulBigDataGov.globalSettings.getBigDataProvider, this.getStorageType)

        val columnInSchema = schema.filter { y => y.name.toLowerCase() == columnNames.toLowerCase() }
        if (columnInSchema == null || columnInSchema.isEmpty)
          raiseError(s"huemul_Table Error: column missing in Schema $columnNames", 1038)
        if (columnInSchema.length != 1)
          raiseError(s"huemul_Table Error: multiples columns found in Schema with name $columnNames", 1039)

          val dataType = columnInSchema.head.dataType
        if (dataType != _dataType) {
          errores = errores.concat(s"Error Column $columnNames, Requiered: ${_dataType}, actual: $dataType  \n")
        }
      }
    }

     errores
  }

  /**
   Save Full data: <br>
   Master & Reference: update and Insert
   Transactional: Delete and Insert new data
   */
  private def executeSave(AliasNewData: String, IsInsert: Boolean, IsUpdate: Boolean, IsDelete: Boolean, IsSelectiveUpdate: Boolean, partitionValueForSelectiveUpdate: ArrayBuffer[String], storageLevelOfDF: org.apache.spark.storage.StorageLevel, registerOnlyInsertInDq: Boolean): Boolean = {
    if (!this.definitionIsClose)
      this.raiseError(s"huemul_Table Error: MUST call ApplyTableDefinition ${this.tableName}", 1048)

    _numRowsExcluded = 0L

    var partitionValueString = ""
    if (partitionValueForSelectiveUpdate != null)
      partitionValueString = partitionValueForSelectiveUpdate.mkString(", ")

    val localControl = new HuemulControl(huemulBigDataGov, control ,HuemulTypeFrequency.ANY_MOMENT, false )
    localControl.AddParamInformation("AliasNewData", AliasNewData)
    localControl.AddParamInformation("IsInsert", IsInsert.toString)
    localControl.AddParamInformation("IsUpdate", IsUpdate.toString)
    localControl.AddParamInformation("IsDelete", IsDelete.toString)
    localControl.AddParamInformation("IsSelectiveUpdate", IsSelectiveUpdate.toString)
    localControl.AddParamInformation("PartitionValueForSelectiveUpdate", partitionValueString)

    var result : Boolean = true
    var errorCode: Integer = null

    try {
      val OnlyInsert: Boolean = IsInsert && !IsUpdate

      //Compare schemas
      if (!autoCast) {
        localControl.newStep("Compare Schema")
        val resultCompareSchema = compareSchema(this.getColumns, this.dataFrameHuemul.dataFrame.schema)
        if (resultCompareSchema != "") {
          result = false
          errorCode = 1013
          raiseError(s"huemul_Table Error: User Error: incorrect DataType: \n$resultCompareSchema", errorCode)
        }
      }

      //do work
      dfMdmDoHuemul(localControl, AliasNewData,IsInsert, IsUpdate, IsDelete, IsSelectiveUpdate, partitionValueForSelectiveUpdate, storageLevelOfDF)


   //WARNING_EXCLUDE (starting 2.1)
      //DataQuality by Columns
      localControl.newStep("Start DataQuality WARNING_EXCLUDE")
      dfDataQualityMasterAuto(IsSelectiveUpdate, localControl, warningExclude = true)
      //Foreing Keys by Columns
      localControl.newStep("Start ForeingKey WARNING_EXCLUDE ")
      dfForeingKeyMasterAuto(warning_exclude = true, localControl)

      //from 2.1: exclude_warnings, update DataFramehuemul
      excludeRows(localControl)

      //from 2.2 --> raiserror if DF is empty
      if (this.dataFrameHuemul.getNumRows == 0) {
        this.raiseError(s"huemul_Table Error: Dataframe is empty, nothing to save", 1062)
      }

    //REST OF RULES (DIFFERENT FROM WARNING_EXCLUDE)
      //DataQuality by Columns
      localControl.newStep("Start DataQuality ERROR AND WARNING")
      val dqResult = dfDataQualityMasterAuto(IsSelectiveUpdate, localControl, warningExclude = false)
      //Foreing Keys by Columns
      localControl.newStep("Start ForeingKey ERROR AND WARNING ")
      val fkResult = dfForeingKeyMasterAuto(warning_exclude = false, localControl)


      localControl.newStep("Validating errors ")
      var localErrorCode: Integer = null
      if (dqResult.isError || fkResult.isError ) {
        result = false
        var errorDetail: String = ""

        if (dqResult.isError) {
          errorDetail = s"DataQuality Error: \n${dqResult.description}"
          localErrorCode = dqResult.errorCode
        }

        if (fkResult.isError) {
          errorDetail += s"\nForeing Key Validation Error: \n${fkResult.description}"
          localErrorCode = dqResult.errorCode
        }


        raiseError(errorDetail, localErrorCode)
      }

      //Compare schemas final table
      localControl.newStep("Compare Schema Final DF")
      if (huemulBigDataGov.debugMode) this.dataFrameHuemul.dataFrame.printSchema()
      val ResultCompareSchemaFinal = compareSchema(this.getColumns, this.dataFrameHuemul.dataFrame.schema)
      if (ResultCompareSchemaFinal != "") {
        errorCode = 1014
        raiseError(s"huemul_Table Error: User Error: incorrect DataType: \n$ResultCompareSchemaFinal",errorCode)
      }

      //Create table persistent
      if (huemulBigDataGov.debugMode){
        huemulBigDataGov.logMessageDebug(s"Saving ${internalGetTable(HuemulTypeInternalTableType.Normal)} Table with params: ")
        huemulBigDataGov.logMessageDebug(s"$getPartitionField field for partitioning table")
        huemulBigDataGov.logMessageDebug(s"$getFullNameWithPath path")
      }

      localControl.newStep("Start Save ")
      if (savePersist(localControl, dataFrameHuemul, OnlyInsert, IsSelectiveUpdate, registerOnlyInsertInDq )){
        localControl.finishProcessOk
      }
      else {
        result = false
        localControl.finishProcessError()
      }

    } catch {
      case e: Exception =>
        result = false
        if (errorCode == null) {
          if (this.errorCode != null && this.errorCode != 0)
            errorCode  = this.errorCode
          else if (localControl.controlError.controlErrorErrorCode == null)
            errorCode = 1027
          else
            errorCode = localControl.controlError.controlErrorErrorCode
        }

        localControl.controlError.setError(e, getClass.getSimpleName, errorCode)
        localControl.finishProcessError()
    }

     result
  }


  private def excludeRows(localControl: HuemulControl): Unit = {
    //Add from 2.1: exclude DQ from WARNING_EXCLUDE
    val warningExcludeDetail = control.controlGetDqResultForWarningExclude()
    if (warningExcludeDetail.nonEmpty) {

      //get DQ with WARNING_EXCLUDE > 0 rows
      val dqIdList: String = warningExcludeDetail.mkString(",")

      val _tableNameDQ: String = internalGetTable(HuemulTypeInternalTableType.DQ)

      //get PK Details
      localControl.newStep("WARNING_EXCLUDE: Get details")
      val dqDet = huemulBigDataGov.dfExecuteQuery("__DQ_Det", s"""SELECT DISTINCT mdm_hash FROM ${_tableNameDQ} WHERE dq_control_id="${control.Control_Id}" AND dq_dq_id in ($dqIdList)""")
      //Broadcast
      _numRowsExcluded = dqDet.count()
      var apply_broadcast: String = ""
      if (_numRowsExcluded < 50000)
        apply_broadcast = "/*+ BROADCAST(EXCLUDE) */"

      //exclude
      localControl.newStep(s"WARNING_EXCLUDE: EXCLUDE rows")
      huemulBigDataGov.logMessageInfo(s"WARNING_EXCLUDE: ${_numRowsExcluded} rows excluded")
      dataFrameHuemul.DF_from_SQL(dataFrameHuemul.alias, s"SELECT $apply_broadcast PK.* FROM ${dataFrameHuemul.alias} PK LEFT JOIN __DQ_Det EXCLUDE ON PK.mdm_hash = EXCLUDE.mdm_hash WHERE EXCLUDE.mdm_hash IS NULL ")

      //from 2.6
      dataFrameHuemul.dataFrame.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

      if (_tableType == HuemulTypeTables.Transaction)
        this.updateStatistics(localControl, "WARNING_EXCLUDE", null)
      else
        this.updateStatistics(localControl, "WARNING_EXCLUDE", dataFrameHuemul.alias)
    }


  }

  /**
   * Save data to disk
   */
  private def savePersist(localControl: HuemulControl, dfHuemul: HuemulDataFrame, onlyInsert: Boolean, IsSelectiveUpdate: Boolean, registerOnlyInsertInDq:Boolean): Boolean = {
    var dfFinal = dfHuemul.dataFrame
    var result: Boolean = true

    //from 2.1 --> change position of this code, to get after WARNING_EXCLUDE
    //Add from 2.0: save Old Value Trace
    //CREATE NEW DATAFRAME WITH MDM OLD VALUE FULL TRACE
    if (huemulBigDataGov.globalSettings.mdmSaveOldValueTrace) {
      localControl.newStep("Ref & Master: MDM Old Value Full Trace")
      oldValueTraceSave("__FullJoin", huemulBigDataGov.ProcessNameCall, localControl)
    }


    if (this._tableType == HuemulTypeTables.Reference || this._tableType == HuemulTypeTables.Master || IsSelectiveUpdate) {
      localControl.newStep("Save: Drop ActionType column")

      if (onlyInsert && !IsSelectiveUpdate) {
        dfFinal = dfFinal.where("___ActionType__ = 'NEW'")
      } else if (this.getStorageType == HuemulTypeStorageType.HBASE){
        //exclude "EQUAL" to improve write performance on HBase
        dfFinal = dfFinal.where("___ActionType__ != 'EQUAL'")
      }

      dfFinal = dfFinal.drop("___ActionType__").drop("SameHashKey") //from 2.1: always this columns exist on reference and master tables
      if (onlyInsert) {

        if (registerOnlyInsertInDq) {
          val dt_start = huemulBigDataGov.getCurrentDateTimeJava
          dfFinal.createOrReplaceTempView("__tempnewtodq")
          val numRowsDQ = dfFinal.count()
          val dt_end = huemulBigDataGov.getCurrentDateTimeJava
          val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)

          //from 2.1: insertOnlyNew as DQ issue
          val Values = new HuemulDqRecord(huemulBigDataGov)
          Values.tableName =tableName
          Values.bbddName =this.getDataBase(this._dataBase)
          Values.dfAlias = ""
          Values.columnName = null
          Values.dqName =s"OnlyInsert"
          Values.dqDescription =s"new values inserted to master or reference table"
          Values.dqQueryLevel = HuemulTypeDqQueryLevel.Row // IsAggregate =false
          Values.dqNotification = HuemulTypeDqNotification.WARNING// RaiseError =true
          Values.dqSqlFormula =""
          Values.dqErrorCode = 1055
          Values.dqToleranceError_Rows = 0L
          Values.dqToleranceError_Percent = null
          Values.dqResultDq ="new values inserted to master or reference table"
          Values.dqNumRowsOk = 0
          Values.dqNumRowsError = numRowsDQ
          Values.dqNumRowsTotal =numRowsDQ
          Values.dqIsError = false
          Values.dqIsWarning = true
          Values.dqExternalCode = "HUEMUL_DQ_009"
          Values.dqDurationHour = duration.hour.toInt
          Values.dqDurationMinute = duration.minute.toInt
          Values.dqDurationSecond = duration.second.toInt

          this.dataFrameHuemul.DqRegister(Values)

          //from 2.1: add only insert to DQ record
          dfProcessToDq(   "__tempnewtodq"   //sqlfrom
                          , null              //sqlwhere
                          , haveField = false             //haveField
                          , null              //fieldname
                          , Values.dqId
                          , HuemulTypeDqNotification.WARNING //dq_error_notification
                          , Values.dqErrorCode //error_code
                          , s"(1055) huemul_Table Warning: new values inserted to master or reference table"// dq_error_description
                          ,localControl
                          )
        }
      }

    }


    try {
      if (getPartitionList.length == 0 || this._tableType == HuemulTypeTables.Reference || this._tableType == HuemulTypeTables.Master ){

        if (this.getNumPartitions > 0) {
          localControl.newStep("Save: Set num FileParts")
          dfFinal = dfFinal.repartition(this.getNumPartitions)
        }

        var localSaveMode: SaveMode = null
        if (onlyInsert) {
          localControl.newStep("Save: Append Master & Ref Data")
          localSaveMode = SaveMode.Append
        }
        else {
          localControl.newStep("Save: Overwrite Master & Ref Data")
          localSaveMode = SaveMode.Overwrite
        }


        if (this.getStorageType == HuemulTypeStorageType.HBASE){
          if (dfFinal.count() > 0) {
            val huemulDriver = new HuemulTableConnector(huemulBigDataGov, localControl)
            huemulDriver.saveToHBase(dfFinal
                                    ,getHBaseNamespace(HuemulTypeInternalTableType.Normal)
                                    ,getHBaseTableName(HuemulTypeInternalTableType.Normal)
                                    ,this.getNumPartitions //numPartitions
                                    ,onlyInsert
                                    ,if (_numPkColumns == 1) _hBasePkColumn else hsRowKeyInternalName2 //PKName
                                    ,getALLDeclaredFields_forHBase(getAllDeclaredFields())
                                    )
          }
        }
        else {
          if (huemulBigDataGov.debugMode) dfFinal.printSchema()
          if (getPartitionList.length == 0) //save without partitions
            dfFinal.write.mode(localSaveMode).format(_getSaveFormat(this.getStorageType)).save(getFullNameWithPath)
          else  //save with partitions
            dfFinal.write.mode(localSaveMode).format(_getSaveFormat(this.getStorageType)).partitionBy(getPartitionListForSave:_*).save(getFullNameWithPath)
        }


        //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
        //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath), new FsPermission("770"))
        localControl.newStep("Register Master Information ")
        control.RegisterMASTER_CREATE_Use(this)
      }
      else{
        //get partition to start drop and all partitions before it (path)
        val dropPartitions : ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]
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
          this.partitionValue = new ArrayBuffer[String]
          //get partitions value to drop
          localControl.newStep("Save: Get partitions values to delete (distinct)")
          var dfDistinctStep = dfFinal.select(dropPartitions.map(name => col(name.getMyName(getStorageType))): _*).distinct()

          //add columns cast
          dropPartitions.foreach { x =>
            dfDistinctStep = dfDistinctStep.withColumn( x.getMyName(getStorageType), dfFinal.col(x.getMyName(getStorageType)).cast(StringType))
          }

          if (huemulBigDataGov.debugMode) {
            huemulBigDataGov.logMessageDebug("show distinct values to delete")
            dfDistinctStep.show()
          }

          //get data
          val dfDistinct = dfDistinctStep.collect()

          //validate num distinct values per column according to definition
          //add columns cast
          dropPartitions.foreach { x =>
            val numDistinctValues = dfDistinctStep.select(x.getMyName(getStorageType)).distinct().count()
            if (x.getPartitionOneValuePerProcess && numDistinctValues > 1) {
              raiseError(s"huemul_Table Error: N° values in partition wrong for column ${x.getMyName(getStorageType)}!, expected: 1, real: $numDistinctValues",1015)
            }
          }

            //get data values
          dfDistinct.foreach { xData =>
            var pathToDelete: String = ""
            val whereToDelete: ArrayBuffer[String] = new ArrayBuffer[String]

            //get columns name in order to create path to delete
            dropPartitions.foreach { xPartitions =>
                val colPartitionName = huemulBigDataGov.getCaseType(this.getStorageType,xPartitions.getMyName(getStorageType))
                val colData = xData.getAs[String](colPartitionName)
                pathToDelete += s"/$colPartitionName=$colData"
                whereToDelete.append(s"$colPartitionName = '$colData'")
            }
            this.partitionValue.append(pathToDelete)

            localControl.newStep(s"Save: deleting partition $pathToDelete ")
            pathToDelete = getFullNameWithPath.concat(pathToDelete)

            if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"deleting $pathToDelete")
            //create fileSystema link
            val FullPath = new org.apache.hadoop.fs.Path(pathToDelete)
            val fs = FullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)

            if (this.getStorageType == HuemulTypeStorageType.DELTA) {
              //FROM 2.4 --> NEW DELETE FOR DATABRICKS
              val FullPath = new org.apache.hadoop.fs.Path(getFullNameWithPath)
              if (fs.exists(FullPath)) {
                val strSQL_delete: String = s"DELETE FROM delta.`$getFullNameWithPath` WHERE ${whereToDelete.mkString(" and ")} "
                if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(strSQL_delete)
                huemulBigDataGov.spark.sql(strSQL_delete)
              }
            } else {
              fs.delete(FullPath, true)
            }
          }
        }

        localControl.newStep("Save: get Partition Detailes counts")
        //get statistics 2.6.1
        val groupNames: String = getPartitionList.map(name => col(name.getMyName(getStorageType))).mkString(",")
        var dfDistinctControlP1 = huemulBigDataGov.spark.sql(s"SELECT $groupNames,cast(count(1) as Long) as numRows FROM ${dfHuemul.alias} group by $groupNames")

        //cast to string
        getPartitionList.foreach { x =>
          dfDistinctControlP1 = dfDistinctControlP1.withColumn( x.getMyName(getStorageType), dfFinal.col(x.getMyName(getStorageType)).cast(StringType))
        }
        val dfDistinctControl = dfDistinctControlP1.collect()

        localControl.newStep("Save: get Partition Details: save numRows to control model")
        //for each row, save to tableUSes control model
        dfDistinctControl.foreach { xData =>
          var pathToSave: String = ""

          //get columns name in order to create path to delete
          getPartitionList.foreach { xPartitions =>
            val colPartitionName = xPartitions.getMyName(getStorageType)
            val colData = xData.getAs[String](colPartitionName)
            pathToSave += s"/${colPartitionName.toLowerCase()}=$colData"
          }
          val numRows:java.lang.Long = xData.getAs[java.lang.Long]("numRows")

          control.RegisterMASTER_CREATE_UsePartition(this,pathToSave ,numRows )
        }

        if (this.getNumPartitions > 0) {
          localControl.newStep("Save: Set num FileParts")
          dfFinal = dfFinal.repartition(this.getNumPartitions)
        }

        localControl.newStep("Save: OverWrite partition with new data")
        if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"saving path: $getFullNameWithPath ")

        dfFinal.write.mode(SaveMode.Append).format(_getSaveFormat(this.getStorageType)).partitionBy(getPartitionListForSave:_*).save(getFullNameWithPath)

        //get staticstics from 2.6.1
        dfFinal.select(dropPartitions.map(name => col(name.getMyName(getStorageType))): _*).distinct()

      }
    } catch {
      case e: Exception =>

        if (this.errorCode == null || this.errorCode == 0) {
          this.errorText = s"huemul_Table Error: write in disk failed. ${e.getMessage}"
          this.errorCode = 1026
        } else
          this.errorText =  this.errorText.concat(s"huemul_Table Error: write in disk failed. ${e.getMessage}")

        this.errorIsError = true

        result = false
        localControl.controlError.setError(e, getClass.getSimpleName, this.errorCode)
    }

    if (result) {


      localControl.newStep("Save: Drop Hive table Def")
      try {
        //new from 2.3
        runSqlDropExternalTable(HuemulTypeInternalTableType.Normal, onlyHbase = false)
      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }



      try {
        //create table
        localControl.newStep("Save: Create Table in Hive Metadata")
        createTableScript = dfCreateTableScript()

        if (getStorageType == HuemulTypeStorageType.HBASE) {
          //new from 2.3
          runSqlExternalTable(createTableScript, onlyHBase = true)

        } else{
          //new from 2.3
          runSqlExternalTable(createTableScript, onlyHBase = false)
        }


        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableName : String = internalGetTable(HuemulTypeInternalTableType.Normal)
        if (getPartitionList.length > 0) {
          if (this.getStorageType != HuemulTypeStorageType.DELTA) {
            localControl.newStep("Save: Repair Hive Metadata")
            val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableName}"
            if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(_refreshTable)
            //new from 2.3
            if (getStorageType == HuemulTypeStorageType.HBASE) {
              runSqlExternalTable(_refreshTable, onlyHBase = true)
            } else {
              runSqlExternalTable(_refreshTable, onlyHBase = false)
            }
          }
        }

        if (huemulBigDataGov.impalaEnabled) {
          localControl.newStep("Save: refresh Impala Metadata")
          huemulBigDataGov.impalaConnection.executeJdbcNoResultSet(s"invalidate metadata ${_tableName}")
          huemulBigDataGov.impalaConnection.executeJdbcNoResultSet(s"refresh ${_tableName}")
        }
      } catch {
        case e: Exception =>

          this.errorIsError = true
          this.errorText = s"huemul_Table Error: create external table failed. ${e.getMessage}"
          this.errorCode = 1025
          result = false
          localControl.controlError.setError(e, getClass.getSimpleName, this.errorCode)
      }
    }

    //from 2.1: assing final DF to huemulDataFrame
    dfHuemul.setDataFrame(dfFinal, dfHuemul.alias, SaveInTemp = false)

    //from 2.0: update dq and ovt used
    control.registerMasterUpdateIsUsed(this)
    result
  }



  /**
   * Save DQ Result data to disk
   */
  private def savePersistOldValueTrace(localControl: HuemulControl, DF: DataFrame): Boolean = {
    val dfFinal = DF
    var result: Boolean = true
    this._tableOvtIsUsed = 1

    try {
      localControl.newStep("Save: OldVT Result: Saving Old Value Trace result")
      if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"saving path: $getFullNameWithPathOldValueTrace ")
      if (getStorageTypeOldValueTrace == HuemulTypeStorageType.PARQUET || getStorageTypeOldValueTrace == HuemulTypeStorageType.ORC ||
          getStorageTypeOldValueTrace == HuemulTypeStorageType.DELTA   || getStorageTypeOldValueTrace == HuemulTypeStorageType.AVRO){
        dfFinal.write.mode(SaveMode.Append).partitionBy(getNameForMdmColumnName).format(_getSaveFormat(getStorageTypeOldValueTrace)).save(getFullNameWithPathOldValueTrace)
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).partitionBy(getNameForMDM_columnName).format(getStorageType_OldValueTrace.toString()).save(getFullNameWithPath_OldValueTrace)
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).format(_StorageType_OldValueTrace).save(getFullNameWithPath_OldValueTrace)
      }
      else
        dfFinal.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).option("delimiter", "\t").option("emptyValue", "").option("treatEmptyValuesAsNulls", "false").option("nullValue", "null").format(_getSaveFormat(getStorageTypeOldValueTrace)).save(getFullNameWithPathOldValueTrace)

    } catch {
      case e: Exception =>
        this.errorIsError = true
        this.errorText = s"huemul_Table OldVT Error: write in disk failed for Old Value Trace result. ${e.getMessage}"
        this.errorCode = 1052
        result = false
        localControl.controlError.setError(e, getClass.getSimpleName, this.errorCode)
    }

    if (result) {
      val sqlDrop01 = s"drop table if exists ${internalGetTable(HuemulTypeInternalTableType.OldValueTrace)}"
      localControl.newStep("Save: OldVT Result: Drop Hive table Def")
      if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
      try {
        //new from 2.3
        runSqlDropExternalTable(HuemulTypeInternalTableType.OldValueTrace, onlyHbase = false)
      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }



      try {
        //create table
        localControl.newStep("Save: OldVT Result: Create Table in Hive Metadata")
        val lScript = dfCreateTableOldValueTraceScript()
        //new from 2.3
        runSqlExternalTable(lScript, onlyHBase = false)

        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableNameOldValueTrace: String = internalGetTable(HuemulTypeInternalTableType.OldValueTrace)

        if (this.getStorageTypeOldValueTrace != HuemulTypeStorageType.DELTA) {
          localControl.newStep("Save: OldVT Result: Repair Hive Metadata")
          val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableNameOldValueTrace}"
          if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"RMSCK REPAIR TABLE ${_tableNameOldValueTrace}")
          //new from 2.3
          runSqlExternalTable(_refreshTable, onlyHBase = false)
        }

        if (huemulBigDataGov.impalaEnabled) {
          localControl.newStep("Save: OldVT Result: refresh Impala Metadata")
          huemulBigDataGov.impalaConnection.executeJdbcNoResultSet(s"invalidate metadata ${_tableNameOldValueTrace}")
          huemulBigDataGov.impalaConnection.executeJdbcNoResultSet(s"refresh ${_tableNameOldValueTrace}")
        }
      } catch {
        case e: Exception =>

          this.errorIsError = true
          this.errorText = s"huemul_Table OldVT Error: create external table OldValueTrace output failed. ${e.getMessage}"
          this.errorCode = 1053
          result = false
          localControl.controlError.setError(e, getClass.getSimpleName, this.errorCode)
      }
    }

     result

  }

  /**
   * Save DQ Result data to disk
   */
  def savePersistDq(localControl: HuemulControl, df: DataFrame): Boolean = {
    val dfFinal = df
    var result: Boolean = true
    this._tableDqIsUsed = 1

    try {
      localControl.newStep("Save DQ Result: Saving new DQ result")
      //from 2.2 --> add HBase storage (from table definition) --> NOT SUPPORTED
      if (this.getStorageTypeDqResult == HuemulTypeStorageType.HBASE) {
        val huemulDriver = new HuemulTableConnector(huemulBigDataGov, localControl)
        huemulDriver.saveToHBase(dfFinal
                                  ,getHBaseNamespace(HuemulTypeInternalTableType.DQ)
                                  ,getHBaseTableName(HuemulTypeInternalTableType.DQ)
                                  ,this.getNumPartitions //numPartitions
                                  ,isOnlyInsert = true
                                  ,if (_numPkColumns == 1) _hBasePkColumn else hsRowKeyInternalName2 //PKName
                                  ,getALLDeclaredFields_forHBase(getAllDeclaredFields())
                                  )
      } else {
        if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"saving path: $getFullNameWithPathDq ")
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).format(this.getStorageType_DQResult.toString()).partitionBy("dq_control_id").save(getFullNameWithPath_DQ)
        dfFinal.write.mode(SaveMode.Append).format(_getSaveFormat(this.getStorageTypeDqResult)).partitionBy("dq_control_id").save(getFullNameWithPathDq)
      }

    } catch {
      case e: Exception =>
        this.errorIsError = true
        this.errorText = s"huemul_Table DQ Error: write in disk failed for DQ result. ${e.getMessage}"
        this.errorCode = 1049
        result = false
        localControl.controlError.setError(e, getClass.getSimpleName, this.errorCode)
    }

    if (result) {
      val sqlDrop01 = s"drop table if exists ${internalGetTable(HuemulTypeInternalTableType.DQ)}"
      localControl.newStep("Save: Drop Hive table Def")
      if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
      try {
        //new from 2.3
        runSqlDropExternalTable(HuemulTypeInternalTableType.DQ, onlyHbase = false)

      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }



      try {
        //create table
        localControl.newStep("Save: Create Table in Hive Metadata")
        val lScript = dfCreateTableDqScript()

        //new from 2.3
        runSqlExternalTable(lScript, onlyHBase = false)

        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableNameDQ: String = internalGetTable(HuemulTypeInternalTableType.DQ)

        if (this.getStorageTypeDqResult != HuemulTypeStorageType.DELTA) {
          val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableNameDQ}"
          localControl.newStep("Save: Repair Hive Metadata")
          if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(_refreshTable)
          //new from 2.3
          runSqlExternalTable(_refreshTable, onlyHBase = false)
        }

        if (huemulBigDataGov.impalaEnabled) {
          localControl.newStep("Save: refresh Impala Metadata")
          huemulBigDataGov.impalaConnection.executeJdbcNoResultSet(s"invalidate metadata ${_tableNameDQ}")
          huemulBigDataGov.impalaConnection.executeJdbcNoResultSet(s"refresh ${_tableNameDQ}")
        }
      } catch {
        case e: Exception =>

          this.errorIsError = true
          this.errorText = s"huemul_Table DQ Error: create external table DQ output failed. ${e.getMessage}"
          this.errorCode = 1050
          result = false
          localControl.controlError.setError(e, getClass.getSimpleName, this.errorCode)
      }
    }

     result

  }

  /**
   * Return an Empty table with all columns
   */
  def getEmptyTable(Alias: String): DataFrame = {
    val schema = getSchema
    val schemaForEmpty = StructType(schema.map { x => StructField(x.name, x.dataType, x.nullable) })
    val emptyRdd = huemulBigDataGov.spark.sparkContext.emptyRDD[Row]
    val emptyDf = huemulBigDataGov.spark.createDataFrame(emptyRdd, schemaForEmpty)
    emptyDf.createOrReplaceTempView(Alias)

     emptyDf
  }

  def dfFromSql(alias: String, sql: String, saveInTemp: Boolean = true, numPartitions: Integer = null) {
    this.dataFrameHuemul.DF_from_SQL(alias, sql, saveInTemp, numPartitions)
  }


  def dfFromDf(dfFrom: DataFrame, aliasFrom: String, aliasTo: String, saveInTemp: Boolean = true) {
    val dtStart = huemulBigDataGov.getCurrentDateTimeJava
    this.dataFrameHuemul.setDataFrame(dfFrom, aliasTo, saveInTemp)
    val dtEnd = huemulBigDataGov.getCurrentDateTimeJava

    huemulBigDataGov.dfSaveLineage(aliasTo
                                 , s"SELECT * FROM $aliasFrom" //sql
                                 , dtStart
                                 , dtEnd
                                 , control
                                 , null //FinalTable
                                 , isQuery = false //isQuery
                                 , isReferenced = true //isReferenced
                                 )
  }

  def dfFromRaw(dataLakeRaw: HuemulDataLake, aliasTo: String) {
    val dtStart = huemulBigDataGov.getCurrentDateTimeJava
    this.dataFrameHuemul.setDataFrame(dataLakeRaw.dataFrameHuemul.dataFrame , aliasTo, huemulBigDataGov.debugMode)
    val dtEnd = huemulBigDataGov.getCurrentDateTimeJava

    huemulBigDataGov.dfSaveLineage(aliasTo
                                 , s"SELECT * FROM ${dataLakeRaw.dataFrameHuemul.alias}" //sql
                                 , dtStart
                                 , dtEnd
                                 , control
                                 , null //FinalTable
                                 , isQuery = false //isQuery
                                 , isReferenced = true //isReferenced
                                 )
  }

  private def runSqlDropExternalTable(tableType: HuemulTypeInternalTableType, onlyHbase: Boolean) {
    //new from 2.3
    val sqlDrop01 = s"drop table if exists ${internalGetTable(tableType)}"
    if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)

    if (onlyHbase) {
      /**** D R O P   F O R   H B A S E ******/

      //FOR SPARK--> NOT SUPPORTED FOR SPARK
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingSpark.getActiveForHBase){
        //comment two next lines: get error when doesn't have serDe
        //val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(databaseName).collect()
        //  if (TablesListFromHive.filter { x => x.name.toLowerCase() == tableName.toLowerCase()  }.length > 0)
            huemulBigDataGov.spark.sql(sqlDrop01)
      }

      //FOR HIVE
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingHive.getActiveForHBase)
        huemulBigDataGov.globalSettings.externalBbddConf.usingHive.getJdbcConnection(huemulBigDataGov).executeJdbcNoResultSet(sqlDrop01)

      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingHwc.getActiveForHBase)
        huemulBigDataGov.getHiveHwc.executeNoResultSet(sqlDrop01)
    } else {
      /**** D R O P   F O R    O T H E R S ******/

      //FOR SPARK
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingSpark.getActive) {
        //comment two next lines: get error when doesn't have serDe
        //val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(databaseName).collect()
        //  if (TablesListFromHive.filter { x => x.name.toLowerCase() == tableName.toLowerCase()  }.length > 0)
            huemulBigDataGov.spark.sql(sqlDrop01)
      }

      //FOR HIVE
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingHive.getActive)
        huemulBigDataGov.globalSettings.externalBbddConf.usingHive.getJdbcConnection(huemulBigDataGov).executeJdbcNoResultSet(sqlDrop01)

      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingHwc.getActive)
        huemulBigDataGov.getHiveHwc.executeNoResultSet(sqlDrop01)
    }

  }


  private def runSqlExternalTable(sqlSentence:String, onlyHBase: Boolean) {
    //new from 2.3
    if (onlyHBase) {
      /**** D R O P   F O R   H B A S E ******/

      //FOR SPARK --> NOT SUPPORTED FOR SPARK
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingSpark.getActiveForHBase)
        huemulBigDataGov.spark.sql(sqlSentence)

      //FOR HIVE
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingHive.getActiveForHBase)
          huemulBigDataGov.globalSettings.externalBbddConf.usingHive.getJdbcConnection(huemulBigDataGov).executeJdbcNoResultSet(sqlSentence)

      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingHwc.getActiveForHBase)
          huemulBigDataGov.getHiveHwc.executeNoResultSet(sqlSentence)
    } else {
      /**** D R O P   F O R   O T H E R ******/

      //FOR SPARK
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingSpark.getActive)
        huemulBigDataGov.spark.sql(sqlSentence)

      //FOR HIVE
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingHive.getActive)
        huemulBigDataGov.globalSettings.externalBbddConf.usingHive.getJdbcConnection(huemulBigDataGov).executeJdbcNoResultSet(sqlSentence)

      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.globalSettings.externalBbddConf.usingHwc.getActive)
        huemulBigDataGov.getHiveHwc.executeNoResultSet(sqlSentence)

    }
  }

  /**
   * Raise Error
   */
  def raiseError(txt: String, code: Integer) {
    errorText = txt
    errorIsError = true
    errorCode = code
    control.controlError.controlErrorErrorCode = code
    if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageError(txt)
    control.raiseError(txt)
    //sys.error(txt)
  }
}

