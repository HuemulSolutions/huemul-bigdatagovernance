package com.huemulsolutions.bigdata.tables

import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQNotification._
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

import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQQueryLevel
import com.huemulsolutions.bigdata.dataquality.HuemulTypeDQNotification
import com.huemulsolutions.bigdata.dataquality.HuemulDQRecord
import com.huemulsolutions.bigdata.datalake.HuemulDataLake
import com.huemulsolutions.bigdata.tables.HuemulTypeInternalTableType.HuemulTypeInternalTableType

//import com.huemulsolutions.bigdata.control.huemulType_Frequency.huemulType_Frequency



class HuemulTable(huemulBigDataGov: HuemulBigDataGovernance, Control: HuemulControl) extends HuemulTableDQ with Serializable  {


  if (Control == null) sys.error("Control is null in huemul_DataFrame")

  /*  ********************************************************************************
   *****   T A B L E   P R O P E R T I E S    ****************************************
   ******************************************************************************** */
  /**
   Table Name
   */
  val TableName : String= this.getClass.getSimpleName.replace("$", "") // ""
  huemulBigDataGov.logMessageInfo(s"HuemulControl:    table instance: $TableName")

  def setDataBase(value: ArrayBuffer[HuemulKeyValuePath]) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of DataBase, definition is close", 1033)
    else
      _DataBase = value
  }
  private var _DataBase: ArrayBuffer[HuemulKeyValuePath] = _

  /**
   Type of Table (Reference; Master; Transaction
   */
  def setTableType(value: HuemulTypeTables) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of TableType, definition is close", 1033)
    else
      _TableType = value
  }
  def getTableType: HuemulTypeTables = { _TableType}
  private var _TableType : HuemulTypeTables = HuemulTypeTables.Transaction

  private var _PK_externalCode: String = "HUEMUL_DQ_002"
  def getPK_externalCode: String = { if (_PK_externalCode==null) "HUEMUL_DQ_002" else _PK_externalCode }
  def setPK_externalCode(value: String) {
    _PK_externalCode = value
  }

  private var _PkNotification:HuemulTypeDQNotification = HuemulTypeDQNotification.ERROR

  /** Set Primary Key notification level: ERROR(default),WARNING, WARNING_EXCLUDE
   * @author  christian.sattler@gmail.com; update: sebas_rod@hotmail.com
   * @since   2.[6]
   * @group   column_attribute
   *
   * @param value   HuemulTypeDQNotification ERROR,WARNING, WARNING_EXCLUDE
   * @return        huemul_Columns
   * @note 2020-06-17 by sebas_rod@hotmail.com: add huemul_Table as return
   */
  def setPkNotification(value: HuemulTypeDQNotification ): HuemulTable = {
    _PkNotification = value
    this
  }

  private def _storageType_default = HuemulTypeStorageType.PARQUET
  /**
   Save DQ Result to disk, only if DQ_SaveErrorDetails in GlobalPath is true
   */
  def setSaveDQResult(value: Boolean) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of SaveDQResult, definition is close", 1033)
    else
      _SaveDQResult = value
  }
  def getSaveDQResult: Boolean = { _SaveDQResult}
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
  def getSaveBackup: Boolean = { _SaveBackup}
  private var _SaveBackup : Boolean = false
  def _getBackupPath: String = { _BackupPath }
  private var _BackupPath: String = ""

  /**
   Type of Persistent storage (parquet, csv, json)
   */
  def setStorageType(value: HuemulTypeStorageType) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of StorageType, definition is close", 1033)
    else
      _StorageType = value
  }
  def getStorageType: HuemulTypeStorageType = { _StorageType}
  private var _StorageType : HuemulTypeStorageType = _


  /**
   Type of Persistent storage (parquet, csv, json) for DQ
   */
  def setStorageType_DQResult(value: HuemulTypeStorageType) {
    if (_StorageType_DQResult == HuemulTypeStorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)

    if (DefinitionIsClose)
      this.raiseError("You can't change value of StorageType_DQResult, definition is close", 1033)
    else
      _StorageType_DQResult = value
  }
  def getStorageType_DQResult: HuemulTypeStorageType = {
    if (_StorageType_DQResult == HuemulTypeStorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)

    //return
    if (_StorageType_DQResult != null)
      _StorageType_DQResult
    else if (_StorageType_DQResult == null && getStorageType == HuemulTypeStorageType.HBASE )
      _storageType_default
    else
      getStorageType
  }
  private var _StorageType_DQResult : HuemulTypeStorageType = _

  /**
   Type of Persistent storage (parquet, csv, json) for DQ
   */
  def setStorageType_OldValueTrace(value: HuemulTypeStorageType) {
    if (_StorageType_OldValueTrace == HuemulTypeStorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for OldValueTraceTable", 1063)

    if (DefinitionIsClose)
      this.raiseError("You can't change value of StorageType_OldValueTrace, definition is close", 1033)
    else
      _StorageType_OldValueTrace = value
  }
  def getStorageType_OldValueTrace: HuemulTypeStorageType = {
    if (_StorageType_OldValueTrace == HuemulTypeStorageType.HBASE)
      raiseError("huemul_Table Error: HBase is not available for OldValueTraceTable", 1063)

    //return
    if (_StorageType_OldValueTrace != null) _StorageType_OldValueTrace
        else if (_StorageType_OldValueTrace == null && getStorageType == HuemulTypeStorageType.HBASE ) _storageType_default
        else getStorageType
  }
  private var _StorageType_OldValueTrace : HuemulTypeStorageType = _

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
  def getDescription: String = { _Description}
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
  def getIT_ResponsibleName: String = { _IT_ResponsibleName}
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
  def getBusiness_ResponsibleName: String = { _Business_ResponsibleName}
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
  def getPartitionList: Array[HuemulColumns] = {
     val partListOrder = getALLDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
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
    if (DefinitionIsClose)
      this.raiseError("You can't change value of LocalPath, definition is close", 1033)
    else
      _LocalPath = value
  }
  private var _LocalPath   : String= ""
  def getLocalPath: String = { if (_LocalPath == null) "null/"
                                     else if (_LocalPath.takeRight(1) != "/") _LocalPath.concat("/")
                                     else _LocalPath }


  def setGlobalPaths(value: ArrayBuffer[HuemulKeyValuePath]) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of GlobalPaths, definition is close", 1033)
    else
      _GlobalPaths = value
  }
  def getGlobalPaths: ArrayBuffer[HuemulKeyValuePath] = { _GlobalPaths}
  private var _GlobalPaths: ArrayBuffer[HuemulKeyValuePath] = _

  def WhoCanRun_executeFull_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeFull, definition is close", 1033)
    else
      _WhoCanRun_executeFull.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeFull: HuemulAuthorization = { _WhoCanRun_executeFull}
  private val _WhoCanRun_executeFull: HuemulAuthorization = new HuemulAuthorization()

  def WhoCanRun_executeOnlyInsert_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeOnlyInsert, definition is close", 1033)
    else
      _WhoCanRun_executeOnlyInsert.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeOnlyInsert: HuemulAuthorization = { _WhoCanRun_executeOnlyInsert}
  private val _WhoCanRun_executeOnlyInsert: HuemulAuthorization = new HuemulAuthorization()

  def WhoCanRun_executeOnlyUpdate_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of WhoCanRun_executeOnlyUpdate, definition is close", 1033)
    else
      _WhoCanRun_executeOnlyUpdate.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeOnlyUpdate: HuemulAuthorization = { _WhoCanRun_executeOnlyUpdate}
  private val _WhoCanRun_executeOnlyUpdate: HuemulAuthorization = new HuemulAuthorization()

  /**
    Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  def setFrequency(value: HuemulTypeFrequency) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of Frequency, definition is close", 1033)
    else
      _Frequency = value
  }
  private var _Frequency: HuemulTypeFrequency = _
  def getFrequency: HuemulTypeFrequency = { _Frequency}

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
  def getSaveDQErrorOnce: Boolean = { _SaveDQErrorOnce}

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
    getALLDeclaredFields(OnlyUserDefined = true).foreach { x =>
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
  def setDQ_MaxNewRecords_Num(value: java.lang.Long) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of DQ_MaxNewRecords_Num, definition is close", 1033)
    else
      _DQ_MaxNewRecords_Num = value
  }
  def getDQ_MaxNewRecords_Num: java.lang.Long = { _DQ_MaxNewRecords_Num}
  private var _DQ_MaxNewRecords_Num: java.lang.Long = _
  /**
   DataQuality: Max % new records vs old records, null does'n apply DQ, 0% doesn't accept new records (raiseError if new record found), 100%-> 0.1 accept double old records)
   */
  def setDQ_MaxNewRecords_Perc(value: Decimal) {
    if (DefinitionIsClose)
      this.raiseError("You can't change value of DQ_MaxNewRecords_Perc, definition is close", 1033)
    else
      _DQ_MaxNewRecords_Perc = value
  }
  def getDQ_MaxNewRecords_Perc: Decimal = { _DQ_MaxNewRecords_Perc}
  private var _DQ_MaxNewRecords_Perc: Decimal = _

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
  def getNumPartitions: Integer = { _NumPartitions}
  private var _NumPartitions : Integer = 0

  private val numPartitionsForDQFiles: Integer = 2

  //FROM 2.5
  //ADD AVRO SUPPORT
  private var _avro_format: String = huemulBigDataGov.GlobalSettings.getAVRO_format
  def getAVRO_format: String = {  _avro_format}
  def setAVRO_format(value: String) {_avro_format = value}

  /*
  private var _Tablecodec_compression: String = null
  def getTableCodec_compression(): String = {return  _Tablecodec_compression}
  def setTableCodec_compression(value: String) {_Tablecodec_compression = value}
  */

  private def _getSaveFormat(storageType: HuemulTypeStorageType): String = {
    //return
    if (storageType == HuemulTypeStorageType.AVRO)
      this.getAVRO_format
    else
      storageType.toString

  }

  /****** METODOS DEL LADO DEL "USUARIO" **************************/

  private var autoCast: Boolean = true
  def setAutoCast(value: Boolean) {autoCast = value}

  private var ApplyDistinct: Boolean = true
  def setApplyDistinct(value: Boolean) {ApplyDistinct = value}


  //private var Table_id: String = ""



  //private var CreateInHive: Boolean = true
  private var CreateTableScript: String = ""
  private var PartitionValue: ArrayBuffer[String] = new ArrayBuffer[String]  //column,value
  def getPartitionValue: String = { if (PartitionValue.nonEmpty) PartitionValue(0) else ""}
  var _tablesUseId: String = _
  //private var DF_DQErrorDetails: DataFrame = _

  /*  ********************************************************************************
   *****   F I E L D   P R O P E R T I E S    ****************************************
   ******************************************************************************** */
  private val MDM_newValueInternalName2: String = "MDM_newValue"
  private val MDM_oldValueInternalName2: String = "MDM_oldValue"
  private val MDM_AutoIncInternalName2: String = "MDM_AutoInc"
  private val processExec_idInternalName2: String= "processExec_id"
  private val MDM_fhNewInternalName2: String = "MDM_fhNew"
  private val MDM_ProcessNewInternalName2: String = "MDM_ProcessNew"
  private val MDM_fhChangeInternalName2: String = "MDM_fhChange"
  private val MDM_ProcessChangeInternalName2: String = "MDM_ProcessChange"
  private val MDM_StatusRegInternalName2: String = "MDM_StatusReg"
  private val MDM_hashInternalName2: String = "MDM_hash"
  private val MDM_columnNameInternalName2: String = "mdm_columnname"
  private val hs_rowKeyInternalName2: String = "hs_rowKey"

  private val nameForMDM_newValue: String = MDM_newValueInternalName2
  /*
  private def setNameForMDM_newValue(value: String): huemul_Table = {
    nameForMDM_newValue = value
    this
  }

   */
  private def getNameForMDM_newValue: String = nameForMDM_newValue

  private val nameForMDM_oldValue: String = MDM_oldValueInternalName2
  /*
  private def setNameForMDM_oldValue(value: String): huemul_Table = {
    nameForMDM_oldValue = value
    this
  }

   */
  private def getNameForMDM_oldValue: String = nameForMDM_oldValue

  private val nameForMDM_AutoInc: String = MDM_AutoIncInternalName2
  /*
  private def setNameForMDM_AutoInc(value: String): huemul_Table = {
    nameForMDM_AutoInc = value
    this
  }

   */
  private def getNameForMDM_AutoInc: String = nameForMDM_AutoInc

  private val nameForProcessExec_id: String = processExec_idInternalName2
  /*
  private def setNameForprocessExec_id(value: String): huemul_Table = {
    nameForprocessExec_id = value
    this
  }

   */
  private def getNameForprocessExec_id: String = nameForProcessExec_id


  private var nameForMDM_fhNew: String = MDM_fhNewInternalName2
  def setNameForMDM_fhNew(value: String): HuemulTable = {
    nameForMDM_fhNew = value
    this
  }
  def getNameForMDM_fhNew: String = nameForMDM_fhNew

  private var nameForMDM_ProcessNew: String = MDM_ProcessNewInternalName2
  def setNameForMDM_ProcessNew(value: String): HuemulTable = {
    nameForMDM_ProcessNew = value
    this
  }
  def getNameForMDM_ProcessNew: String = nameForMDM_ProcessNew

  private var nameForMDM_fhChange: String = MDM_fhChangeInternalName2
  def setNameForMDM_fhChange(value: String): HuemulTable = {
    nameForMDM_fhChange = value
    this
  }
  def getNameForMDM_fhChange: String = nameForMDM_fhChange

  private var nameForMDM_ProcessChange: String = MDM_ProcessChangeInternalName2
  def setNameForMDM_ProcessChange(value: String): HuemulTable = {
    nameForMDM_ProcessChange = value
    this
  }
  def getNameForMDM_ProcessChange: String = nameForMDM_ProcessChange

  private var nameForMDM_StatusReg: String = MDM_StatusRegInternalName2
  def setNameForMDM_StatusReg(value: String): HuemulTable = {
    nameForMDM_StatusReg = value
    this
  }
  def getNameForMDM_StatusReg: String = nameForMDM_StatusReg

  private var nameForMDM_hash: String = MDM_hashInternalName2
  def setNameForMDM_hash(value: String): HuemulTable = {
    nameForMDM_hash = value
    this
  }
  def getNameForMDM_hash: String = nameForMDM_hash

  private val nameForMDM_columnName: String = MDM_columnNameInternalName2
  /*
  private def setNameFormdm_columnname(value: String): huemul_Table = {
    nameForMdm_columnname = value
    this
  }

   */
  private def getNameForMDM_columnName: String = nameForMDM_columnName




  val MDM_newValue:HuemulColumns = new HuemulColumns (StringType, true, "New value updated in table", false).setHBaseCatalogMapping("loginfo")
  val MDM_oldValue:HuemulColumns = new HuemulColumns (StringType, true, "Old value", false).setHBaseCatalogMapping("loginfo")
  val MDM_AutoInc:HuemulColumns = new HuemulColumns (LongType, true, "auto incremental for version control", false).setHBaseCatalogMapping("loginfo")
  val processExec_id:HuemulColumns = new HuemulColumns (StringType, true, "Process Execution Id (control model)", false).setHBaseCatalogMapping("loginfo")

  val MDM_fhNew:HuemulColumns = new HuemulColumns (TimestampType, true, "Fecha/hora cuando se insertaron los datos nuevos", false).setHBaseCatalogMapping("loginfo")
  val MDM_ProcessNew:HuemulColumns = new HuemulColumns (StringType, false, "Nombre del proceso que insertó los datos", false).setHBaseCatalogMapping("loginfo")
  val MDM_fhChange:HuemulColumns = new HuemulColumns (TimestampType, false, "fecha / hora de último cambio de valor en los campos de negocio", false).setHBaseCatalogMapping("loginfo")
  val MDM_ProcessChange:HuemulColumns = new HuemulColumns (StringType, false, "Nombre del proceso que cambió los datos", false).setHBaseCatalogMapping("loginfo")
  val MDM_StatusReg:HuemulColumns = new HuemulColumns (IntegerType, true, "indica si el registro fue insertado en forma automática por otro proceso (1), o fue insertado por el proceso formal (2), si está eliminado (-1)", false).setHBaseCatalogMapping("loginfo")
  val MDM_hash:HuemulColumns = new HuemulColumns (StringType, true, "Valor hash de los datos de la tabla", false).setHBaseCatalogMapping("loginfo")

  val mdm_columnname:HuemulColumns = new HuemulColumns (StringType, true, "Column Name", false).setHBaseCatalogMapping("loginfo")

  //from 2.2 --> add rowKey compatibility with HBase
  val hs_rowKey:HuemulColumns = new HuemulColumns (StringType, true, "Concatenated PK", false).setHBaseCatalogMapping("loginfo")

  var AdditionalRowsForDistint: String = ""
  private var DefinitionIsClose: Boolean = false

  /***
   * from Global path (small files, large files, DataBase, etc)
   */
  def globalPath()  : String= {
     getPath(_GlobalPaths)
  }

  def globalPath(ManualEnvironment: String)  : String= {
     getPath(_GlobalPaths, ManualEnvironment)
  }


  /**
   * Get Fullpath hdfs = GlobalPaths + LocalPaths + TableName
   */
  def getFullNameWithPath: String = {
     globalPath + getLocalPath + TableName
  }

  /**
   * Get Fullpath hdfs for DQ results = GlobalPaths + DQError_Path + TableName + "_dq"
   */
  def getFullNameWithPath_DQ: String = {
     this.getPath(huemulBigDataGov.GlobalSettings.DQError_Path) + this.getDataBase(this._DataBase) + '/' + getLocalPath + TableName + "_dq"
  }

  /**
   * Get Fullpath hdfs for _oldvalue results = GlobalPaths + DQError_Path + TableName + "_oldvalue"
   */
  def getFullNameWithPath_OldValueTrace: String = {
     this.getPath(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_Path) + this.getDataBase(this._DataBase) + '/' + getLocalPath + TableName + "_oldvalue"
  }

   /**
   * Get Fullpath hdfs for backpu  = Backup_Path + database + TableName + "_backup"
   */
  def getFullNameWithPath_Backup(control_id: String) : String = {
     this.getPath(huemulBigDataGov.GlobalSettings.MDM_Backup_Path) + this.getDataBase(this._DataBase) + '/' + getLocalPath + 'c' + control_id + '/' + TableName + "_backup"
  }

  def getFullNameWithPath2(ManualEnvironment: String) : String = {
     globalPath(ManualEnvironment) + getLocalPath + TableName
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
      Control.RegisterMASTER_USE(this)
      _TableWasRegistered = true
    }

     internalGetTable(HuemulTypeInternalTableType.Normal)
  }

  //new from 2.1
  /**
   * Return DataBaseName.TableName for DQ
   */
  def getTable_DQ: String = {
     internalGetTable(HuemulTypeInternalTableType.DQ)
  }

  //new from 2.1
  /**
   * Return DataBaseName.TableName for OldValueTrace
   */
  def getTable_OldValueTrace: String = {
     internalGetTable(HuemulTypeInternalTableType.OldValueTrace)
  }

  //new from 2.2
  /**
   * Define HBase Table and namespace
   */
  private var _hbase_namespace: String = _
  private var _hbase_tableName: String = _
  def setHBaseTableName(namespace: String, tableName: String = null) {
    _hbase_namespace = namespace
    _hbase_tableName = tableName
  }

  def getHBaseNamespace(internalTableType: HuemulTypeInternalTableType): String = {
    var valueName: String = null
    if (internalTableType == HuemulTypeInternalTableType.DQ) {
      valueName = getDataBase(huemulBigDataGov.GlobalSettings.DQError_DataBase)
    } else if (internalTableType == HuemulTypeInternalTableType.Normal) {
      valueName = if ( _hbase_namespace == null) this.getDataBase(this._DataBase) else _hbase_namespace
    } else if (internalTableType == HuemulTypeInternalTableType.OldValueTrace) {
      valueName = getDataBase(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_DataBase)
    } else {
      raiseError(s"huemul_Table Error: Type '$internalTableType' doesn't exist in InternalGetTable method (getHBaseNamespace)", 1059)
    }

      valueName

  }

  def getHBaseTableName(internalTableType: HuemulTypeInternalTableType): String = {
    var valueName: String = if (_hbase_tableName == null) this.TableName else _hbase_tableName
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
    var fields: String = s""""${if (_numPKColumns == 1) _HBase_PKColumn else hs_rowKeyInternalName2}":{"cf":"rowkey","col":"key","type":"string"} """
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns]
    } foreach { x =>
      x.setAccessible(true)
      val dataField = x.get(this).asInstanceOf[HuemulColumns]
      val colMyName = dataField.getMyName(this.getStorageType)

      if (!(dataField.getIsPK && _numPKColumns == 1)) {
        //fields = fields + s""", \n "${x.getName}":{"cf":"${dataField.getHBaseCatalogFamily()}","col":"${dataField.getHBaseCatalogColumn()}","type":"${dataField.getHBaseDataType()}"} """
        fields = fields + s""", \n "$colMyName":{"cf":"${dataField.getHBaseCatalogFamily}","col":"${dataField.getHBaseCatalogColumn}","type":"${dataField.getHBaseDataType}"} """

        if (tableType != HuemulTypeInternalTableType.OldValueTrace) {
          if (dataField.getMDM_EnableOldValue)
            fields = fields + s""", \n "$colMyName${__old}":{"cf":"${dataField.getHBaseCatalogFamily}","col":"${dataField.getHBaseCatalogColumn}${__old}","type":"${dataField.getHBaseDataType}"} """
          if (dataField.getMDM_EnableDTLog)
            fields = fields + s""", \n "$colMyName${__fhChange}":{"cf":"${dataField.getHBaseCatalogFamily}","col":"${dataField.getHBaseCatalogColumn}${__fhChange}","type":"string"} """
          if (dataField.getMDM_EnableProcessLog)
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
  def getHBaseCatalogForHIVE(tableType: HuemulTypeInternalTableType): String = {
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

   //WARNING!!! Any changes you make to this code, repeat it in getColumns_CreateTable
    //create fields
    var fields: String = s":key"
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns]
    } foreach { x =>
      x.setAccessible(true)
      val dataField = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = dataField.getMyName(this.getStorageType).toLowerCase()
      val _dataType  = dataField.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)

      if (!(dataField.getIsPK && _numPKColumns == 1) && (_colMyName != hs_rowKeyInternalName2.toLowerCase())) {

        if (tableType == HuemulTypeInternalTableType.DQ) {
          //create StructType
          if ("dq_control_id".toLowerCase() != _colMyName) {
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}#b"""
          }
        }
        else if (tableType == HuemulTypeInternalTableType.OldValueTrace) {
          //create StructType mdm_columnname
          if (getNameForMDM_columnName.toLowerCase() != _colMyName) {
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}#b"""
          }
        }
        else {
          //create StructType
          fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}${if (_dataType == TimestampType || _dataType == DateType ||  _dataType.typeName.toLowerCase().contains("decimal")) "" else "#b"}"""

        }

        if (tableType != HuemulTypeInternalTableType.OldValueTrace) {
          if (dataField.getMDM_EnableOldValue)
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}${__old}${if (_dataType == TimestampType || _dataType == DateType ||  _dataType.typeName.toLowerCase().contains("decimal") ) "" else "#b"}"""
          if (dataField.getMDM_EnableDTLog)
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}${__fhChange}"""
          if (dataField.getMDM_EnableProcessLog)
            fields = fields + s""",${dataField.getHBaseCatalogFamily}:${dataField.getHBaseCatalogColumn}${__ProcessLog}#b"""
        }


      }
    }


     fields
  }


  //get PK for HBase Tables rowKey
  private var _HBase_rowKeyCalc: String = ""
  private var _HBase_PKColumn: String = ""

  private var _numPKColumns: Int = 0





  private def internalGetTable(internalTableType: HuemulTypeInternalTableType, withDataBase: Boolean = true): String = {
    var _getTable: String = ""
    var _database: String = ""
    var _tableName: String = ""
    if (internalTableType == HuemulTypeInternalTableType.DQ) {
      _database = getDataBase(huemulBigDataGov.GlobalSettings.DQError_DataBase)
      _tableName = TableName + "_dq"
    } else if (internalTableType == HuemulTypeInternalTableType.Normal) {
      _database = getDataBase(_DataBase)
      _tableName = TableName
    } else if (internalTableType == HuemulTypeInternalTableType.OldValueTrace) {
      _database = getDataBase(huemulBigDataGov.GlobalSettings.MDM_OldValueTrace_DataBase)
      _tableName = TableName + "_oldvalue"
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
     s"${getDataBase(_DataBase)}"
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
     huemulBigDataGov.GlobalSettings.getDataBase(huemulBigDataGov, Division)
  }

  def getDataBase(Division: ArrayBuffer[HuemulKeyValuePath], ManualEnvironment: String): String = {
     huemulBigDataGov.GlobalSettings.getDataBase(Division, ManualEnvironment)
  }

  def getPath(Division: ArrayBuffer[HuemulKeyValuePath]): String = {
     huemulBigDataGov.GlobalSettings.getPath(huemulBigDataGov, Division)
  }

  def getPath(Division: ArrayBuffer[HuemulKeyValuePath], ManualEnvironment: String): String = {
     huemulBigDataGov.GlobalSettings.getPath(Division, ManualEnvironment)
  }



  /**
   get execution's info about rows and DF
   */
  var DataFramehuemul: HuemulDataFrame = new HuemulDataFrame(huemulBigDataGov, Control)


  var Error_isError: Boolean = false
  var Error_Text: String = ""
  var Error_Code: Integer = _
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


    if (this._TableType == null)
      raiseError(s"huemul_Table Error: TableType must be defined",1034)

    //from 2.6 --> allow partitionColumns in all tables, this check was disabled
    //else if (this._TableType != huemulType_Tables.Transaction && getPartitionField != "")
    //  raiseError(s"huemul_Table Error: PartitionField shouldn't be defined if TableType is ${this._TableType}",1036)

    //from 2.2 --> validate tableType with Format
    if (this._TableType == HuemulTypeTables.Transaction && !(this.getStorageType == HuemulTypeStorageType.PARQUET ||
                                                              this.getStorageType == HuemulTypeStorageType.ORC ||
                                                              this.getStorageType == HuemulTypeStorageType.DELTA ||
                                                              this.getStorageType == HuemulTypeStorageType.AVRO
                                                              ))
      raiseError(s"huemul_Table Error: Transaction Tables only available with PARQUET, DELTA, AVRO or ORC StorageType ",1057)


    //Fron 2.2 --> validate tableType HBASE and turn on globalSettings
    if (this.getStorageType == HuemulTypeStorageType.HBASE && !huemulBigDataGov.GlobalSettings.getHBase_available)
      raiseError(s"huemul_Table Error: StorageType is set to HBASE, requires invoke HBase_available in globalSettings  ",1058)

    if (this._DataBase == null)
      raiseError(s"huemul_Table Error: DataBase must be defined",1037)
    if (this._Frequency == null)
      raiseError(s"huemul_Table Error: Frequency must be defined",1047)

    if (this.getSaveBackup  && this.getTableType == HuemulTypeTables.Transaction)
      raiseError(s"huemul_Table Error: SaveBackup can't be true for transactional tables",1054)


    //var PartitionFieldValid: Boolean = false
    var comaPKConcat = ""

    _numPKColumns = 0
    _HBase_rowKeyCalc = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns] || x.get(this).isInstanceOf[HuemulDataQuality] || x.get(this).isInstanceOf[HuemulTableRelationship]
    } foreach { x =>
      x.setAccessible(true)
      //Nombre de campos
      x.get(this) match {
        case dataField: HuemulColumns =>
          if (x.getName.toLowerCase().equals(MDM_hashInternalName2.toLowerCase())) {
            dataField.setMyName(getNameForMDM_hash)
          } else if (x.getName.toLowerCase().equals(MDM_StatusRegInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMDM_StatusReg)
          else if (x.getName.toLowerCase().equals(MDM_ProcessChangeInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMDM_ProcessChange)
          else if (x.getName.toLowerCase().equals(MDM_fhChangeInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMDM_fhChange)
          else if (x.getName.toLowerCase().equals(MDM_ProcessNewInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMDM_ProcessNew)
          else if (x.getName.toLowerCase().equals(MDM_fhNewInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMDM_fhNew)
          else if (x.getName.toLowerCase().equals(MDM_AutoIncInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMDM_AutoInc)
          else if (x.getName.toLowerCase().equals(MDM_oldValueInternalName2.toLowerCase()))
            dataField.setMyName(getNameForMDM_oldValue)
          else if (x.getName.toLowerCase().equals(MDM_newValueInternalName2.toLowerCase())) {
            dataField.setMyName(getNameForMDM_newValue)
          } else
            dataField.setMyName(x.getName)

          if (dataField.getDQ_MaxLen != null && dataField.getDQ_MaxLen < 0)
            raiseError(s"Error column ${
              x.getName
            }: DQ_MaxLen must be positive", 1003)
          else if (dataField.getDQ_MaxLen != null && dataField.getDQ_MinLen != null && dataField.getDQ_MaxLen < dataField.getDQ_MinLen)
            raiseError(s"Error column ${
              x.getName
            }: DQ_MinLen(${
              dataField.getDQ_MinLen
            }) must be less than DQ_MaxLen(${
              dataField.getDQ_MaxLen
            })", 1028)
          else if (dataField.getDQ_MaxDecimalValue != null && dataField.getDQ_MinDecimalValue != null && dataField.getDQ_MaxDecimalValue < dataField.getDQ_MinDecimalValue)
            raiseError(s"Error column ${
              x.getName
            }: DQ_MinDecimalValue(${
              dataField.getDQ_MinDecimalValue
            }) must be less than DQ_MaxDecimalValue(${
              dataField.getDQ_MaxDecimalValue
            })", 1029)
          else if (dataField.getDQ_MaxDateTimeValue != null && dataField.getDQ_MinDateTimeValue != null && dataField.getDQ_MaxDateTimeValue < dataField.getDQ_MinDateTimeValue)
            raiseError(s"Error column ${
              x.getName
            }: DQ_MinDateTimeValue(${
              dataField.getDQ_MinDateTimeValue
            }) must be less than DQ_MaxDateTimeValue(${
              dataField.getDQ_MaxDateTimeValue
            })", 1030)
          else if (dataField.getDefaultValue != null && dataField.DataType == StringType && dataField.getDefaultValue.toUpperCase() != "NULL" && !dataField.getDefaultValue.contains("'"))
            raiseError(s"Error column ${
              x.getName
            }: DefaultValue  must be like this: 'something', not something wihtout ')", 1031)

          if (dataField.getIsPK) {
            dataField.setIsNull(value = false) // setNullable (false)
            HasPK = true

            if (dataField.getMDM_EnableDTLog || dataField.getMDM_EnableOldValue || dataField.getMDM_EnableProcessLog || dataField.getMDM_EnableOldValue_FullTrace) {
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
          if (dataField.getIsPK && getStorageType == HuemulTypeStorageType.HBASE) {
            _HBase_rowKeyCalc += s"$comaPKConcat'[', ${
              if (dataField.DataType == StringType) _colMyName else s"CAST(${
                _colMyName
              } AS STRING)"
            },']'"
            comaPKConcat = ","
            _HBase_PKColumn = _colMyName
            _numPKColumns += 1
          }

        case _ =>
      }

      //Nombre de DQ
      x.get(this) match {
        case dQField: HuemulDataQuality =>
          dQField.setMyName(x.getName)

          if (dQField.getNotification == HuemulTypeDQNotification.WARNING_EXCLUDE && !dQField.getSaveErrorDetails)
            raiseError(s"huemul_Table Error: DQ ${
              x.getName
            }:, Notification is set to WARNING_EXCLUDE, but SaveErrorDetails is set to false. Use setSaveErrorDetails to set true ", 1055)
          else if (dQField.getNotification == HuemulTypeDQNotification.WARNING_EXCLUDE && dQField.getQueryLevel != HuemulTypeDQQueryLevel.Row)
            raiseError(s"huemul_Table Error: DQ ${
              x.getName
            }:, Notification is set to WARNING_EXCLUDE, QueryLevel MUST set to huemulType_DQQueryLevel.Row", 1056)

        case _ =>
      }

      //Nombre de FK
      x.get(this) match {
        case fKField: HuemulTableRelationship =>
          fKField.MyName = x.getName

        //TODO: Validate FK Setting

        case _ =>
      }
    }

    //add OVT
    getALLDeclaredFields(OnlyUserDefined = false,PartitionColumnToEnd = false,HuemulTypeInternalTableType.OldValueTrace).filter { x => x.setAccessible(true)
                x.get(this).isInstanceOf[HuemulColumns] } foreach { x =>
      x.setAccessible(true)
      val DataField = x.get(this).asInstanceOf[HuemulColumns]

      if (!DataField.getDefinitionIsClose) {
        DataField.setMyName(x.getName)
        DataField.setDefinitionIsClose()
      }
    }

    //add DQ and OVT
    getALLDeclaredFields(OnlyUserDefined = false,PartitionColumnToEnd = false,HuemulTypeInternalTableType.DQ).filter { x => x.setAccessible(true)
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
      if (_numPKColumns == 1)
        _HBase_rowKeyCalc = _HBase_PKColumn
      else
        _HBase_rowKeyCalc = s"concat(${_HBase_rowKeyCalc})"
    }

    if (this._TableType == HuemulTypeTables.Transaction && getPartitionField == "")
      raiseError(s"huemul_Table Error: Partitions should be defined if TableType is Transactional, use column.setPartitionColumn",1035)


    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: register metadata")
    //Register TableName and fields
    Control.RegisterMASTER_CREATE_Basic(this)
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulControl: end ApplyTableDefinition")
    if (!HasPK) this.raiseError("huemul_Table Error: PK not defined", 1017)
    //from 2.6 --> disabled this code, is controlled before
    //if (this.getTableType == huemulType_Tables.Transaction && !PartitionFieldValid)
    //  raiseError(s"huemul_Table Error: PartitionField should be defined if TableType is Transactional (invalid name ${this.getPartitionField} )",1035)
    //getColumns.foreach(x=>println(x.getMyName(this.getStorageType)))
    DefinitionIsClose = true
     true
  }

  def _setAutoIncUpate(value: java.lang.Long): Unit = {
    _MDM_AutoInc = value
  }

  def _setControlTableId(value: String) {
    _ControlTableId = value
  }

  def _getControlTableId(): String = { _ControlTableId}

  /**
   * Get all declared fields from class
   */
  private def getALLDeclaredFields(OnlyUserDefined: Boolean = false, PartitionColumnToEnd: Boolean = false, tableType: HuemulTypeInternalTableType = HuemulTypeInternalTableType.Normal) : Array[java.lang.reflect.Field] = {
    val pClass = getClass

    val a = pClass.getDeclaredFields  //huemul_table


    var c = a

    //only PK for save result to old value trace
    if (tableType == HuemulTypeInternalTableType.OldValueTrace) {
        c = c.filter { x => x.setAccessible(true)
                            x.get(this).isInstanceOf[HuemulColumns] &&
                            x.get(this).asInstanceOf[HuemulColumns].getIsPK
                            }
    }

    if (!OnlyUserDefined){ //all columns, including MDM
      var b = pClass.getSuperclass.getDeclaredFields


      if (tableType == HuemulTypeInternalTableType.OldValueTrace) {
        b = b.filter { x => x.getName == MDM_columnNameInternalName2 || x.getName == MDM_newValueInternalName2 || x.getName == MDM_oldValueInternalName2 || x.getName == MDM_AutoIncInternalName2 ||
                            x.getName == MDM_fhChangeInternalName2 || x.getName == MDM_ProcessChangeInternalName2 || x.getName == processExec_idInternalName2  }
      } else {
        //exclude OldValuestrace columns
        b = b.filter { x => x.getName != MDM_columnNameInternalName2 && x.getName != MDM_newValueInternalName2 && x.getName != MDM_oldValueInternalName2 && x.getName != MDM_AutoIncInternalName2 && x.getName != processExec_idInternalName2  }

        if (this._TableType == HuemulTypeTables.Transaction)
          b = b.filter { x => x.getName != MDM_ProcessChangeInternalName2 && x.getName != MDM_fhChangeInternalName2 && x.getName != MDM_StatusRegInternalName2   }

        //       EXCLUDE FOR PARQUET, ORC, ETC             OR            EXCLUDE FOR HBASE AND NUM PKs == 1
        if (getStorageType != HuemulTypeStorageType.HBASE || (getStorageType == HuemulTypeStorageType.HBASE && _numPKColumns == 1) )
          b = b.filter { x => x.getName != hs_rowKeyInternalName2  }

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

    if (PartitionColumnToEnd) {
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
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)
      (x,Field.getHBaseCatalogFamily, Field.getHBaseCatalogColumn, _dataType)
    }

     result
  }

  //private var _SQL_OldValueFullTrace_DF: DataFrame = null
  private var _MDM_AutoInc: java.lang.Long = 0L
  private var _ControlTableId: String = _
  def _getMDM_AutoInc(): java.lang.Long = { _MDM_AutoInc}
  private var _table_dq_isused: Int = 0
  def _getTable_dq_isused(): Int = { _table_dq_isused}
  private var _table_ovt_isused: Int = 0
  def _getTable_ovt_isused(): Int = { _table_ovt_isused}
  private var _NumRows_New: java.lang.Long = _
  private var _NumRows_Update: java.lang.Long = _
  private var _NumRows_Updatable: java.lang.Long = _
  private var _NumRows_Delete: java.lang.Long = _
  private var _NumRows_Total: java.lang.Long = _
  private var _NumRows_NoChange: java.lang.Long = _
  private var _NumRows_Excluded: java.lang.Long = 0L

  def NumRows_New(): java.lang.Long = {
     _NumRows_New
  }

  def NumRows_Update(): java.lang.Long = {
     _NumRows_Update
  }

  def NumRows_Updatable(): java.lang.Long = {
     _NumRows_Updatable
  }

  def NumRows_Delete(): java.lang.Long = {
     _NumRows_Delete
  }

  def NumRows_Total(): java.lang.Long = {
     _NumRows_Total
  }

  def NumRows_NoChange(): java.lang.Long = {
     _NumRows_NoChange
  }

  def NumRows_Excluded(): java.lang.Long = {
     _NumRows_Excluded
  }
   /*  ********************************************************************************
   *****   F I E L D   M E T H O D S    ****************************************
   ******************************************************************************** */
  def getSchema: StructType = {
    val fieldsStruct: ArrayBuffer[StructField] = getColumns.map(x=> StructField(x.getMyName(this.getStorageType), x.DataType , nullable = x.getNullable , null))
    StructType.apply(fieldsStruct)
  }

  def getDataQuality(warning_exclude: Boolean): ArrayBuffer[HuemulDataQuality] = {
    val GetDeclaredfields = getALLDeclaredFields()
    val Result = new ArrayBuffer[HuemulDataQuality]()

    if (GetDeclaredfields != null ) {
        GetDeclaredfields.filter { x => x.setAccessible(true)
                             x.get(this).isInstanceOf[HuemulDataQuality] }
        .foreach { x =>
          //Get DQ
          val DQRule = x.get(this).asInstanceOf[HuemulDataQuality]

          if (warning_exclude && DQRule.getNotification == HuemulTypeDQNotification.WARNING_EXCLUDE)
            Result.append(DQRule)
          else if (!warning_exclude && DQRule.getNotification != HuemulTypeDQNotification.WARNING_EXCLUDE)
            Result.append(DQRule)
        }
      }

     Result
  }

  def getForeingKey: ArrayBuffer[HuemulTableRelationship] = {
    val GetDeclaredfields = getALLDeclaredFields()
    val Result = new ArrayBuffer[HuemulTableRelationship]()

    if (GetDeclaredfields != null ) {
        GetDeclaredfields.filter { x => x.setAccessible(true)
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
    val Result: ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]()
    val fieldList = getALLDeclaredFields()
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
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)

      //Field.setMyName(x.getName)
      Result.append(Field)


      if (Field.getMDM_EnableOldValue) {
        val MDM_EnableOldValue = new HuemulColumns(_dataType, false, s"Old value for ${_colMyName}")
        MDM_EnableOldValue.setMyName(s"${_colMyName}${__old}")
        Result.append(MDM_EnableOldValue)
      }
      if (Field.getMDM_EnableDTLog){
        val MDM_EnableDTLog = new HuemulColumns(TimestampType, false, s"Last change DT for ${_colMyName}")
        MDM_EnableDTLog.setMyName(s"${_colMyName}${__fhChange}")
        Result.append(MDM_EnableDTLog)
      }
      if (Field.getMDM_EnableProcessLog) {
        val MDM_EnableProcessLog = new HuemulColumns(StringType, false, s"System Name change for ${_colMyName}")
        MDM_EnableProcessLog.setMyName(s"${_colMyName}${__ProcessLog}")
        Result.append(MDM_EnableProcessLog)
      }

    }

     Result
  }


  /**
   Create schema from DataDef Definition
   */
  private def getColumns_CreateTable(ForHive: Boolean = false, tableType: HuemulTypeInternalTableType = HuemulTypeInternalTableType.Normal ): String = {
    //WARNING!!! Any changes you make to this code, repeat it in getHBaseCatalogForHIVE
    val partitionColumnToEnd = if (getStorageType == HuemulTypeStorageType.HBASE) false else true
    val fieldList = getALLDeclaredFields(OnlyUserDefined = false,PartitionColumnToEnd = partitionColumnToEnd,tableType)
   // val NumFields = fieldList.filter { x => x.setAccessible(true)
   //   x.get(this).isInstanceOf[huemul_Columns] }.length

    //set name according to getStorageType (AVRO IS LowerCase)
    val __storageType =  if (tableType == HuemulTypeInternalTableType.Normal) this.getStorageType
                    else if (tableType == HuemulTypeInternalTableType.DQ) this.getStorageType_DQResult
                    else if (tableType == HuemulTypeInternalTableType.OldValueTrace) this.getStorageType_OldValueTrace
                    else this.getStorageType

    val __old = huemulBigDataGov.getCaseType( __storageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( __storageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( __storageType, "_ProcessLog")

    var ColumnsCreateTable : String = ""
    var coma: String = ""

    //for HBase, add hs_rowKey as Key column
    if (getStorageType == HuemulTypeStorageType.HBASE && _numPKColumns > 1) {
      fieldList.filter { x => x.setAccessible(true)
        x.getName == hs_rowKeyInternalName2 }.foreach { x =>
        val Field = x.get(this).asInstanceOf[HuemulColumns]
        val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, __storageType)
        val DataTypeLocal = _dataType.sql

        ColumnsCreateTable += s"$coma${x.getName} $DataTypeLocal \n"
        coma = ","
      }
    }

    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] && x.getName != hs_rowKeyInternalName2 }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, __storageType)
      var DataTypeLocal = _dataType.sql
      //from 2.5 --> replace DECIMAL to STRING when storage type = AVRO. this happen because AVRO outside DATABRICKS doesn't support DECIMAL, DATE AND TIMESTAMP TYPE.
      if (    huemulBigDataGov.GlobalSettings.getBigDataProvider != HuemulTypeBigDataProvider.databricks
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
        if (huemulBigDataGov.GlobalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
          ColumnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        } else if ("dq_control_id".toLowerCase() != _colMyName.toLowerCase()) {
          ColumnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        }
      }
      else if (tableType == HuemulTypeInternalTableType.OldValueTrace) {
        //create StructType mdm_columnname
        //FROM 2.4 --> INCLUDE PARTITIONED COLUMN IN CREATE TABLE ONLY FOR databricks COMPATIBILITY
        if (huemulBigDataGov.GlobalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
          ColumnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        } else if (getNameForMDM_columnName.toLowerCase() != _colMyName.toLowerCase()) {
          ColumnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        }
      }
      else {
        //create StructType
        //if (getPartitionField != null && getPartitionField.toLowerCase() != _colMyName.toLowerCase()) {
        if (Field.getPartitionColumnPosition == 0 || getStorageType == HuemulTypeStorageType.HBASE) {
          ColumnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        } else if (huemulBigDataGov.GlobalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
          //from 2.4 --> add partitioned field
          ColumnsCreateTable += s"$coma${_colMyName} $DataTypeLocal \n"
          coma = ","
        }
      }

      if (tableType != HuemulTypeInternalTableType.OldValueTrace) {
        if (Field.getMDM_EnableOldValue)
          ColumnsCreateTable += s"$coma${_colMyName}${__old} $DataTypeLocal \n"
        if (Field.getMDM_EnableDTLog)
          ColumnsCreateTable += s"$coma${_colMyName}${__fhChange} ${TimestampType.sql} \n"
        if (Field.getMDM_EnableProcessLog)
          ColumnsCreateTable += s"$coma${_colMyName}${__ProcessLog} ${StringType.sql} \n"
      }
    }

     ColumnsCreateTable
  }




  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE PRIMARY KEY
   */
  private def SQL_PrimaryKey_FinalTable(): String = {


    var StringSQL: String = ""
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] &&
                                      x.get(this).asInstanceOf[HuemulColumns].getIsPK }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)

      //All PK columns shouldn't be null
      Field.setIsNullInternal(value = false)

      if (!huemulBigDataGov.HasName(Field.getMappedName))
        sys.error(s"field ${_colMyName} doesn't have an assigned name in 'name' attribute")
      StringSQL += coma + _colMyName
      coma = ","
    }


     StringSQL
  }

  /**
  CREATE SQL SCRIPT FOR TRANSACTIONAL DATA
   */
  private def SQL_Step0_TXHash(OfficialAlias: String, processName: String): String = {

    var StringSQL: String = ""
    //var StringSQl_PK: String = ""

    //var StringSQL_partition: String = ""
    var StringSQL_hash: String = "sha2(concat( "
    var coma_hash: String = ""
    var coma: String = ""

    val partitionList : ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]

    getALLDeclaredFields(PartitionColumnToEnd = true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)

      val NewColumnCast = ApplyAutoCast(if (huemulBigDataGov.HasName(Field.getSQLForInsert)) s"${Field.getSQLForInsert} " else s"New.${Field.getMappedName}"
                                        ,_dataType.sql)


      //New value (from field or compute column )
      if (huemulBigDataGov.HasName(Field.getMappedName )){
        if (Field.getPartitionColumnPosition == 0){
          StringSQL += s"$coma$NewColumnCast as ${_colMyName} \n"
          coma = ","
        } else {
          partitionList.append(Field)
        }

      } else {
        if (_colMyName.toLowerCase() == getNameForMDM_fhNew.toLowerCase()) {
          val colUserName = Field.getCaseType(this.getStorageType,getNameForMDM_fhNew) //from 2.6 #112
          StringSQL += s",CAST(now() AS ${_dataType.sql} ) as $colUserName \n"
          coma = ","
        } else if (_colMyName.toLowerCase() == getNameForMDM_ProcessNew.toLowerCase()) {
          val colUserName = Field.getCaseType(this.getStorageType,getNameForMDM_ProcessNew) //from 2.6 #112
          StringSQL += s"${coma}CAST('$processName' AS ${_dataType.sql} ) as $colUserName \n"
          coma = ","
        }
      }


      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""$coma_hash${s"coalesce($NewColumnCast,'null')" }"""
        coma_hash = ","
      }


    }

    StringSQL_hash += "),256) "

    //set name according to getStorageType (AVRO IS LowerCase)
    val __MDM_hash = huemulBigDataGov.getCaseType( this.getStorageType, getNameForMDM_hash)

    //from 2.6 --> create partitions sql (replace commented text)
    val StringSQL_partition = partitionList.sortBy { x => x.getPartitionColumnPosition}.map{x =>
      val _colMyName = x.getMyName(this.getStorageType)
      val _dataType  = x.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)

      val NewColumnCast = ApplyAutoCast(if (huemulBigDataGov.HasName(x.getSQLForInsert)) s"${x.getSQLForInsert} " else s"New.${x.getMappedName}"
                                        ,_dataType.sql)

      val result = s"$NewColumnCast as ${_colMyName} \n"

      result
    }.mkString(",")

    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL = s"""SELECT $StringSQL
                    ,$StringSQL_hash as ${__MDM_hash}
                    ,$StringSQL_partition
                    FROM $OfficialAlias new
                          """

     StringSQL
  }

  /**
  CREATE SQL SCRIPT DISTINCT WITH NEW DATAFRAME
   */
  private def SQL_Step0_Distinct(newAlias: String): String = {

    var StringSQL: String = ""
    val Distintos: ArrayBuffer[String] = new ArrayBuffer[String]()
    var coma: String = ""
    getALLDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns]
                                        }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val MappedField: String = Field.getMappedName
      if (huemulBigDataGov.HasName(MappedField)) {
        if (!Distintos.contains(MappedField)){
          Distintos.append(MappedField)
          StringSQL += s"$coma$MappedField  \n"
          coma = ","
        }
      }
    }

    //Aditional field from new table
    if (huemulBigDataGov.HasName(AdditionalRowsForDistint))
      StringSQL += s",$AdditionalRowsForDistint"

    //set field New or Update action
    StringSQL = s"""SELECT DISTINCT $StringSQL
                     FROM $newAlias"""

     StringSQL
  }


  private def ApplyAutoCast(ColumnName: String, DataType: String): String = {
     if (autoCast) s"CAST($ColumnName AS $DataType)" else ColumnName
  }


  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step2_LeftJoin(OfficialAlias: String, newAlias: String): String = {
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
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)

      val NewColumnCast = ApplyAutoCast(s"New.${Field.getMappedName}",_dataType.sql)
      //string for key on
      if (Field.getIsPK){
        val NewPKSentence = if (huemulBigDataGov.HasName(Field.getSQLForInsert)) s"CAST(${Field.getSQLForInsert} as ${_dataType.sql} )" else NewColumnCast
        StringSQL_LeftJoin += s"${coma}Old.${_colMyName} AS ${_colMyName} \n"
        StringSQl_PK += s" $sand Old.${_colMyName} = $NewPKSentence  "
        sand = " and "

        if (StringSQL_ActionType == "")
          StringSQL_ActionType = s" case when Old.${_colMyName} is null then 'NEW' when $NewColumnCast is null then 'EQUAL' else 'UPDATE' END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = OfficialColumns.exists { y => y.name.toLowerCase() == _colMyName.toLowerCase() }

        //String for new DataFrame fulljoin
        if (columnExist){
          StringSQL_LeftJoin += s"${coma}cast(old.${_colMyName} as ${_dataType.sql} ) as old_${_colMyName} \n"
          //StringSQL_LeftJoin += s"${coma}old.${_colMyName} as old_${_colMyName} \n"
        }
        else
          StringSQL_LeftJoin += s"${coma}cast(null as ${_dataType.sql} ) as old_${_colMyName} \n"

        //New value (from field or compute column )
        if (huemulBigDataGov.HasName(Field.getMappedName))
          StringSQL_LeftJoin += s",$NewColumnCast as new_${_colMyName} \n"
        if (huemulBigDataGov.HasName(Field.getSQLForInsert))
          StringSQL_LeftJoin += s",CAST(${Field.getSQLForInsert} as ${_dataType.sql} ) as new_insert_${_colMyName} \n"
        if (huemulBigDataGov.HasName(Field.getSQLForUpdate))
          StringSQL_LeftJoin += s",CAST(${Field.getSQLForUpdate} as ${_dataType.sql} ) as new_update_${_colMyName} \n"

        if (Field.getMDM_EnableOldValue || Field.getMDM_EnableDTLog || Field.getMDM_EnableProcessLog || Field.getMDM_EnableOldValue_FullTrace) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.HasName(Field.getMappedName)) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulBigDataGov.HasName(Field.getSQLForUpdate)) Field.getSQLForUpdate else "new.".concat(Field.getMappedName),_dataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(_colMyName) else "null"
            //from 2.5 --> add cast to avro timestamp dataType
            //if (this.getStorageType == huemulType_StorageType.AVRO && (Field._DataType.sql.toUpperCase().contains("TIMESTAMP") || Field._DataType.sql.toUpperCase().contains("DATE")))
            //  OldFieldTXT =   ApplyAutoCast(OldFieldTXT,Field._DataType.sql)
            StringSQL_LeftJoin += s",CAST(CASE WHEN $NewFieldTXT = $OldFieldTXT or ($NewFieldTXT is null and $OldFieldTXT is null) THEN 0 ELSE 1 END as Integer ) as __Change_${_colMyName}  \n"
          }
          else {
            StringSQL_LeftJoin += s",CAST(0 as Integer ) as __Change_${_colMyName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae,
            //la solución es poner el campo "tiene cambio" en falso
          }

        }

        if (Field.getMDM_EnableOldValue){
          if (!OfficialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__old}".toLowerCase() }) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${_colMyName}${__old} AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
        }
        if (Field.getMDM_EnableDTLog){
          if (!OfficialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__fhChange}".toLowerCase() }) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${_colMyName}${__fhChange} AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
        }
        if (Field.getMDM_EnableProcessLog){
          if (!OfficialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__ProcessLog}".toLowerCase() }) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS STRING) as old_${_colMyName}${__ProcessLog} \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${_colMyName}${__ProcessLog} AS STRING) as old_${_colMyName}${__ProcessLog} \n"
        }

      }

      coma = ","
    }


    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL_LeftJoin = s"""SELECT $StringSQL_LeftJoin
                                   ,$StringSQL_ActionType
                             FROM $OfficialAlias Old
                                LEFT JOIN $newAlias New \n
                                   on $StringSQl_PK
                          """

     StringSQL_LeftJoin
  }


  /**
  SCRIPT FOR OLD VALUE TRACE INSERT
   */
  private def OldValueTrace_save(Alias: String, ProcessName: String, LocalControl: HuemulControl)  {

    //set name according to getStorageType (AVRO IS LowerCase)
    val __MDM_newValue = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, getNameForMDM_newValue)
    val __MDM_oldValue = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, getNameForMDM_oldValue)
    val __MDM_AutoInc = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, getNameForMDM_AutoInc)
    val __MDM_ProcessChange = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, getNameForMDM_ProcessChange)
    val __mdm_columnName = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, getNameForMDM_columnName).toLowerCase() //because it's partitioned column
    val __MDM_fhChange = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, getNameForMDM_fhChange)
    val __processExec_id = huemulBigDataGov.getCaseType( this.getStorageType_OldValueTrace, getNameForprocessExec_id)

    //Get PK
    var StringSQl_PK_base: String = "SELECT "
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[HuemulColumns] &&
                                         x.get(this).asInstanceOf[HuemulColumns].getIsPK }
    .foreach { x =>
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      StringSQl_PK_base += s" $coma${Field.getMyName(this.getStorageType_OldValueTrace)}"
      coma = ","
    }

    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] &&
                                      x.get(this).asInstanceOf[HuemulColumns].getMDM_EnableOldValue_FullTrace }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType_OldValueTrace)
      val _dataType_MDM_fhChange = MDM_fhChange.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType_OldValueTrace)
      val __MDM_fhChange_cast: String = if (_dataType_MDM_fhChange == MDM_fhChange.DataType) s"now() as ${__MDM_fhChange}" else s"CAST(now() as STRING) as ${__MDM_fhChange}"
      val StringSQL =  s"$StringSQl_PK_base, CAST(new_${_colMyName} as string) AS ${__MDM_newValue}, CAST(old_${_colMyName} as string) AS ${__MDM_oldValue}, CAST(${_MDM_AutoInc} AS BIGINT) as ${__MDM_AutoInc}, cast('${Control.Control_Id}' as string) as ${__processExec_id}, ${__MDM_fhChange_cast}, cast('$ProcessName' as string) as ${__MDM_ProcessChange}, cast('${_colMyName.toLowerCase()}' as string) as ${__mdm_columnName} FROM $Alias WHERE ___ActionType__ = 'UPDATE' and __Change_${_colMyName} = 1 "
      val aliasFullTrace: String = s"__SQL_ovt_full_${_colMyName}"

      val tempSQL_OldValueFullTrace_DF = huemulBigDataGov.DF_ExecuteQuery(aliasFullTrace,StringSQL)

      val numRowsAffected = tempSQL_OldValueFullTrace_DF.count()
      if (numRowsAffected > 0) {
        val Result = savePersist_OldValueTrace(LocalControl,tempSQL_OldValueFullTrace_DF)
        if (!Result)
            huemulBigDataGov.logMessageWarn(s"Old value trace full trace can't save to disk, column ${_colMyName}")
      }

      LocalControl.NewStep(s"Ref & Master: ovt full trace finished, $numRowsAffected rows changed for ${_colMyName} column ")
    }
  }


  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step1_FullJoin(OfficialAlias: String, newAlias: String, isUpdate: Boolean, isDelete: Boolean): String = {
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
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)

      val NewColumnCast = ApplyAutoCast(s"New.${Field.getMappedName}",_dataType.sql)
      //string for key on
      if (Field.getIsPK){
        val NewPKSentence = if (huemulBigDataGov.HasName(Field.getSQLForInsert)) s"CAST(${Field.getSQLForInsert} as ${_dataType.sql} )" else NewColumnCast
        StringSQL_FullJoin += s"${coma}CAST(coalesce(Old.${_colMyName}, $NewPKSentence) as ${_dataType.sql}) AS ${_colMyName} \n"
        StringSQl_PK += s" $sand Old.${_colMyName} = $NewPKSentence  "
        sand = " and "

        if (StringSQL_ActionType == "")
          StringSQL_ActionType = s" case when Old.${_colMyName} is null then 'NEW' when $NewColumnCast is null then ${if (isDelete) "'DELETE'" else "'EQUAL'"} else ${if (isUpdate) "'UPDATE'" else "'EQUAL'"} END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = OfficialColumns.exists { y => y.name.toLowerCase() == _colMyName.toLowerCase() }

        //String for new DataFrame fulljoin
        if (columnExist)
          StringSQL_FullJoin += s"${coma}cast(old.${_colMyName} as ${_dataType.sql} ) as old_${_colMyName} \n"
        else
          StringSQL_FullJoin += s"${coma}cast(null as ${_dataType.sql} ) as old_${_colMyName} \n"

        //New value (from field or compute column )
        if (huemulBigDataGov.HasName(Field.getMappedName))
          StringSQL_FullJoin += s",$NewColumnCast as new_${_colMyName} \n"
        if (huemulBigDataGov.HasName(Field.getSQLForInsert))
          StringSQL_FullJoin += s",CAST(${Field.getSQLForInsert} as ${_dataType.sql} ) as new_insert_${_colMyName} \n"
        if (huemulBigDataGov.HasName(Field.getSQLForUpdate))
          StringSQL_FullJoin += s",CAST(${Field.getSQLForUpdate} as ${_dataType.sql} ) as new_update_${_colMyName} \n"

        if (Field.getMDM_EnableOldValue || Field.getMDM_EnableDTLog || Field.getMDM_EnableProcessLog || Field.getMDM_EnableOldValue_FullTrace) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.HasName(Field.getMappedName)) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulBigDataGov.HasName(Field.getSQLForUpdate)) Field.getSQLForUpdate else "new.".concat(Field.getMappedName),_dataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(_colMyName) else "null"
            //from 2.5 --> add cast to avro timestamp dataType
            //if (this.getStorageType == huemulType_StorageType.AVRO && (Field._DataType.sql.toUpperCase().contains("TIMESTAMP") || Field._DataType.sql.toUpperCase().contains("DATE")) )
            //  OldFieldTXT =   ApplyAutoCast(OldFieldTXT,Field._DataType.sql)
            StringSQL_FullJoin += s",CAST(CASE WHEN $NewFieldTXT = $OldFieldTXT or ($NewFieldTXT is null and $OldFieldTXT is null) THEN 0 ELSE 1 END as Integer ) as __Change_${_colMyName}  \n"
          }
          else {
            StringSQL_FullJoin += s",CAST(0 as Integer ) as __Change_${_colMyName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae,
            //la solución es poner el campo "tiene cambio" en falso
          }

        }

        if (Field.getMDM_EnableOldValue){
          if (!OfficialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__old}".toLowerCase() }) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${_colMyName}${__old} AS ${_dataType.sql}) as old_${_colMyName}${__old} \n"
        }
        if (Field.getMDM_EnableDTLog){
          if (!OfficialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__fhChange}".toLowerCase() }) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${_colMyName}${__fhChange} AS TimeStamp) as old_${_colMyName}${__fhChange} \n"
        }
        if (Field.getMDM_EnableProcessLog){
          if (!OfficialColumns.exists { y => y.name.toLowerCase() == s"${_colMyName}${__ProcessLog}".toLowerCase() }) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS STRING) as old_${_colMyName}${__ProcessLog} \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${_colMyName}${__ProcessLog} AS STRING) as old_${_colMyName}${__ProcessLog} \n"
        }



      }

      coma = ","
    }


    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL_FullJoin = s"""SELECT $StringSQL_FullJoin
                                   ,$StringSQL_ActionType
                             FROM $OfficialAlias Old
                                FULL JOIN $newAlias New \n
                                   on $StringSQl_PK
                          """

     StringSQL_FullJoin
  }

  /**
  CREATE SQL SCRIPT LEFT JOIN
   */
  private def SQL_Step4_Update(newAlias: String, ProcessName: String): String = {

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    var StringSQL: String = "SELECT "

    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)

      //string for key on
      if (Field.getIsPK){
        StringSQL += s" $coma${_colMyName} as ${_colMyName} \n"

      } else {
        if (   _colMyName.toLowerCase() == getNameForMDM_fhNew.toLowerCase() || _colMyName.toLowerCase() == getNameForMDM_ProcessNew.toLowerCase() || _colMyName.toLowerCase() == getNameForMDM_fhChange.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMDM_ProcessChange.toLowerCase() || _colMyName.toLowerCase() == getNameForMDM_StatusReg.toLowerCase() || _colMyName.toLowerCase() == hs_rowKeyInternalName2.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMDM_hash.toLowerCase())
          StringSQL += s"${coma}old_${_colMyName}  \n"
        else {
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.HasName(Field.getSQLForUpdate)) s"new_update_${_colMyName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.getReplaceValueOnUpdate && this.huemulBigDataGov.HasName(Field.getMappedName)) s"new_${_colMyName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${_colMyName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${_colMyName} END  as ${_colMyName} \n"""

           if (Field.getMDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN old_${_colMyName} ELSE old_${_colMyName}${__old} END as ${_colMyName}${__old} \n"""
           if (Field.getMDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN now() ELSE old_${_colMyName}${__fhChange} END AS TimeStamp) as ${_colMyName}${__fhChange} \n"""
           if (Field.getMDM_EnableProcessLog)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN '$ProcessName' WHEN ___ActionType__ = 'NEW' THEN '$ProcessName' ELSE old_${_colMyName}${__ProcessLog} END as ${_colMyName}${__ProcessLog} \n"""
        }
      }

      coma = ","
    }


    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" ,___ActionType__
                      FROM $newAlias New
                  """
     StringSQL
  }


  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step2_UpdateAndInsert(newAlias: String, ProcessName: String, isInsert: Boolean): String = {

    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")

    var StringSQL: String = "SELECT "

    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)
      val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)

      //string for key on
      if (Field.getIsPK){
        StringSQL += s" $coma${_colMyName} as ${_colMyName} \n"

      } else {
        if (   _colMyName.toLowerCase() == getNameForMDM_fhNew.toLowerCase()         || _colMyName.toLowerCase() == getNameForMDM_ProcessNew.toLowerCase() || _colMyName.toLowerCase() == getNameForMDM_fhChange.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMDM_ProcessChange.toLowerCase() || _colMyName.toLowerCase() == getNameForMDM_StatusReg.toLowerCase()  || _colMyName.toLowerCase() == hs_rowKeyInternalName2.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMDM_hash.toLowerCase())
          StringSQL += s"${coma}old_${_colMyName}  \n"
        else {
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'NEW'    THEN ${if (this.huemulBigDataGov.HasName(Field.getSQLForInsert)) s"new_insert_${_colMyName}" //si tiene valor en SQL insert, lo usa
                                                                               else if (this.huemulBigDataGov.HasName(Field.getMappedName)) s"new_${_colMyName}"     //si no, si tiene nombre de campo en DataFrame nuevo, lo usa
                                                                               else ApplyAutoCast(Field.getDefaultValue,_dataType.sql)                        //si no tiene campo asignado, pone valor por default
                                                                           }
                                        WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.HasName(Field.getSQLForUpdate)) s"new_update_${_colMyName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.getReplaceValueOnUpdate && this.huemulBigDataGov.HasName(Field.getMappedName)) s"new_${_colMyName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${_colMyName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${_colMyName} END  as ${_colMyName} \n"""

           if (Field.getMDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN old_${_colMyName} ELSE old_${_colMyName}${__old} END as ${_colMyName}${__old} \n"""
           if (Field.getMDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN now() ELSE old_${_colMyName}${__fhChange} END AS TimeStamp) as ${_colMyName}${__fhChange} \n"""
           if (Field.getMDM_EnableProcessLog)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${_colMyName} = 1 THEN '$ProcessName' WHEN ___ActionType__ = 'NEW' THEN '$ProcessName' ELSE old_${_colMyName}${__ProcessLog} END as ${_colMyName}${__ProcessLog} \n"""
        }
      }

      coma = ","
    }


    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" ,___ActionType__
                      FROM $newAlias New
                      ${if (isInsert) "" else "WHERE ___ActionType__ <> 'NEW' "}
                  """


     StringSQL
  }

  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA and for Selective update
   */
  private def SQL_Step3_Hash_p1(newAlias: String, isSelectiveUpdate: Boolean): String = {

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
                                      x.get(this).isInstanceOf[HuemulColumns]
                                    }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)

      if (     _colMyName.toLowerCase() == getNameForMDM_fhNew.toLowerCase()         || _colMyName.toLowerCase() == getNameForMDM_ProcessNew.toLowerCase() || _colMyName.toLowerCase() == getNameForMDM_fhChange.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMDM_ProcessChange.toLowerCase() || _colMyName.toLowerCase() == getNameForMDM_StatusReg.toLowerCase()  || _colMyName.toLowerCase() == hs_rowKeyInternalName2.toLowerCase()
            || _colMyName.toLowerCase() == getNameForMDM_hash.toLowerCase())
        StringSQL += s"${coma}old_${_colMyName}  \n"
      else
        StringSQL += s" $coma${_colMyName}  \n"

      if (Field.getMDM_EnableOldValue)
        StringSQL += s""",${_colMyName}${__old} \n"""
      if (Field.getMDM_EnableDTLog)
        StringSQL += s""",${_colMyName}${__fhChange} \n"""
      if (Field.getMDM_EnableProcessLog)
        StringSQL += s""",${_colMyName}${__ProcessLog} \n"""

      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""$coma_hash${s"coalesce(${_colMyName},'null')"}"""
        coma_hash = ","
      }

      coma = ","
    }

    //Hash fields
    if (coma_hash == "")
      sys.error(s"must define one field as UsedForCheckSum")

    StringSQL_hash += "),256) "

    //set name according to getStorageType (AVRO IS LowerCase)
    val __MDM_hash = huemulBigDataGov.getCaseType( this.getStorageType, getNameForMDM_hash)

    //Include HashFields to SQL
    StringSQL += s""",$StringSQL_hash as ${__MDM_hash} \n"""

    if (this._TableType == HuemulTypeTables.Reference || this._TableType == HuemulTypeTables.Master || isSelectiveUpdate) {
      StringSQL += s""",case when old_${__MDM_hash} = $StringSQL_hash THEN 1 ELSE 0 END AS SameHashKey  \n ,___ActionType__ \n"""
    }

    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" FROM $newAlias New
                  """

     StringSQL
  }


  private def SQL_Step4_Final(newAlias: String, ProcessName: String): String = {
    //set name according to getStorageType (AVRO IS LowerCase)
    val __old = huemulBigDataGov.getCaseType( this.getStorageType, "_old")
    val __fhChange = huemulBigDataGov.getCaseType( this.getStorageType, "_fhChange")
    val __ProcessLog = huemulBigDataGov.getCaseType( this.getStorageType, "_ProcessLog")
    val __MDM_hash = huemulBigDataGov.getCaseType( this.getStorageType, getNameForMDM_hash)

    var StringSQL: String = "SELECT "

    var coma: String = ""

    //Obtiene todos los campos
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns]
                                    }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      val _colMyName = Field.getMyName(this.getStorageType)

      if (_colMyName.toLowerCase() == getNameForMDM_fhNew.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMDM_fhNew)//from 2.6 #112
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN now() ELSE old_${_colMyName} END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMDM_ProcessNew.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMDM_ProcessNew) //from 2.6 #112
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN '$ProcessName' ELSE old_${_colMyName} END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMDM_fhChange.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMDM_fhChange)//from 2.6 #112
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN now() ELSE old_${_colMyName} END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMDM_ProcessChange.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMDM_ProcessChange)//from 2.6 #112
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN '$ProcessName' ELSE old_${_colMyName} END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMDM_StatusReg.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMDM_StatusReg)//from 2.6 #112
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN CAST(2 as Int) WHEN ___ActionType__ = 'NEW' THEN CAST(2 as Int) WHEN ___ActionType__ = 'DELETE' AND ${this.getRowStatusDeleteAsDeleted} = true THEN CAST(-1 AS Int)  ELSE CAST(old_${_colMyName} AS Int) END as $colUserName \n"
      } else if (_colMyName.toLowerCase() == getNameForMDM_hash.toLowerCase()) {
        val colUserName = Field.getCaseType(this.getStorageType,getNameForMDM_hash)//from 2.6 #112
        StringSQL += s"$coma${__MDM_hash} as $colUserName \n"
      } else if (_colMyName.toLowerCase() == hs_rowKeyInternalName2.toLowerCase()){
        if (getStorageType == HuemulTypeStorageType.HBASE && _numPKColumns > 1)
          StringSQL += s"$coma${_HBase_rowKeyCalc} as ${_colMyName} \n"
      }
      else
        StringSQL += s" $coma${_colMyName}  \n"

      if (Field.getMDM_EnableOldValue)
        StringSQL += s""",${_colMyName}${__old} \n"""
      if (Field.getMDM_EnableDTLog)
        StringSQL += s""",${_colMyName}${__fhChange} \n"""
      if (Field.getMDM_EnableProcessLog)
        StringSQL += s""",${_colMyName}${__ProcessLog} \n"""


      coma = ","
    }

    //if (IncludeActionType)
      StringSQL += s", ___ActionType__, SameHashKey \n"

    StringSQL += s"FROM $newAlias New\n"

     StringSQL
  }



  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE PRIMARY KEY
   */
  private def SQL_Unique_FinalTable(): ArrayBuffer[HuemulColumns] = {

    val StringSQL: ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]()
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[HuemulColumns] &&
                                         x.get(this).asInstanceOf[HuemulColumns].getIsUnique && huemulBigDataGov.HasName(x.get(this).asInstanceOf[HuemulColumns].getMappedName)}
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]

      StringSQL.append(Field)
    }

     StringSQL
  }

  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE NOT NULL ATTRIBUTES //warning_exclude: Boolean
   */
  private def SQL_NotNull_FinalTable(): ArrayBuffer[HuemulColumns] = {
    val StringSQL: ArrayBuffer[HuemulColumns] = new ArrayBuffer[HuemulColumns]()
    getALLDeclaredFields().filter { x => x.setAccessible(true)
         x.get(this).isInstanceOf[HuemulColumns] &&
        !x.get(this).asInstanceOf[HuemulColumns].getNullable && //warning_exclude == false &&
        huemulBigDataGov.HasName(x.get(this).asInstanceOf[HuemulColumns].getMappedName)}
      .foreach { x =>
        //Get field
        val Field = x.get(this).asInstanceOf[HuemulColumns]

        StringSQL.append(Field)
      }

     StringSQL
  }

  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required
   */
  private def missingRequiredFields(IsSelectiveUpdate: Boolean): ArrayBuffer[String] = {
    val StringSQL: ArrayBuffer[String] = new ArrayBuffer[String]()
    if (IsSelectiveUpdate) return StringSQL


    getALLDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[HuemulColumns] &&
                                         x.get(this).asInstanceOf[HuemulColumns].Required
                                          }
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]
      //huemulBigDataGov.logMessageInfo(s"${_colMyName} Field.getMappedName: ${Field.getMappedName}, Field.getSQLForUpdate: ${Field.getSQLForUpdate}, Field.getSQLForInsert: ${Field.getSQLForInsert}")

      var isOK: Boolean = false
      if (huemulBigDataGov.HasName(Field.getMappedName))
        isOK = true
      else if (!huemulBigDataGov.HasName(Field.getMappedName) &&
              (huemulBigDataGov.HasName(Field.getSQLForUpdate) && huemulBigDataGov.HasName(Field.getSQLForInsert)))
        isOK = true
      else
        isOK = false

      if (!isOK)
        StringSQL.append(Field.getMyName(this.getStorageType))
    }

     StringSQL
  }

  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required
   */
  private def missingRequiredFields_SelectiveUpdate(): String = {
    var PKNotMapped: String = ""
    var OneColumnMapped: Boolean = false
    //STEP 1: validate name setting
    getALLDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                    x.get(this).isInstanceOf[HuemulColumns]}
    .foreach { x =>
      //Get field
      val Field = x.get(this).asInstanceOf[HuemulColumns]

      if (Field.getIsPK) {

        var isOK: Boolean = false
        if (huemulBigDataGov.HasName(Field.getMappedName))
          isOK = true
        else if (!huemulBigDataGov.HasName(Field.getMappedName) &&
                (huemulBigDataGov.HasName(Field.getSQLForUpdate) && huemulBigDataGov.HasName(Field.getSQLForInsert)))
          isOK = true
        else
          isOK = false

        if (!isOK) {
          PKNotMapped = s"${if (PKNotMapped == "") "" else ", "}${Field.getMyName(this.getStorageType)} "
        }

      } else
        OneColumnMapped = true

    }

    if (!OneColumnMapped)
        raiseError(s"huemul_Table Error: Only PK mapped, no columns mapped for update",1042)

     PKNotMapped
  }


  /*  ********************************************************************************
   *****   T A B L E   M E T H O D S    ****************************************
   ******************************************************************************** */
  def getSQLCreateTableScript: String = DF_CreateTableScript()
  private def DF_CreateTableScript(): String = {

    //var coma_partition = ""

    //Get SQL DataType for Partition Columns
    val PartitionForCreateTable = getALLDeclaredFields().filter { x => x.setAccessible(true)
                                                                  x.get(this).isInstanceOf[HuemulColumns] &&
                                                                  x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition >= 1 }
                          .sortBy { x => x.setAccessible(true)
                                         x.get(this).asInstanceOf[HuemulColumns].getPartitionColumnPosition}
                          .map { x =>
                            val Field = x.get(this).asInstanceOf[HuemulColumns]
              val _colMyName = Field.getMyName(this.getStorageType)
              if (huemulBigDataGov.GlobalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
                 s"${_colMyName}" //without datatype
              } else {
                val _dataType  = Field.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)
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
    if (huemulBigDataGov.GlobalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
      if (getStorageType == HuemulTypeStorageType.PARQUET || getStorageType == HuemulTypeStorageType.ORC ||
          getStorageType == HuemulTypeStorageType.DELTA   || getStorageType == HuemulTypeStorageType.AVRO) {
        //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                     CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.Normal)} (${getColumns_CreateTable(ForHive = true) })
                                     USING ${getStorageType.toString}
                                     ${if (PartitionForCreateTable.nonEmpty) s"PARTITIONED BY ($PartitionForCreateTable)" else "" }
                                     LOCATION '$getFullNameWithPath'"""
      } else if (getStorageType == HuemulTypeStorageType.HBASE)  {
        lCreateTableScript = s"""
                                     CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.Normal)} (${getColumns_CreateTable(ForHive = true) })
                                     USING 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                                     WITH SERDEPROPERTIES ("hbase.columns.mapping"="${getHBaseCatalogForHIVE(HuemulTypeInternalTableType.Normal)}")
                                     TBLPROPERTIES ("hbase.table.name"="${getHBaseNamespace(HuemulTypeInternalTableType.Normal)}:${getHBaseTableName(HuemulTypeInternalTableType.Normal)}")"""
      }
    } else {
      if (getStorageType == HuemulTypeStorageType.PARQUET || getStorageType == HuemulTypeStorageType.ORC ||
          getStorageType == HuemulTypeStorageType.DELTA   || getStorageType == HuemulTypeStorageType.AVRO) {
        //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                     CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.Normal)} (${getColumns_CreateTable(ForHive = true) })
                                     ${if (PartitionForCreateTable.nonEmpty) s"PARTITIONED BY ($PartitionForCreateTable)" else "" }
                                     STORED AS ${getStorageType.toString}
                                     LOCATION '$getFullNameWithPath'"""
      } else if (getStorageType == HuemulTypeStorageType.HBASE)  {
        lCreateTableScript = s"""
                                     CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.Normal)} (${getColumns_CreateTable(ForHive = true) })
                                     ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
                                     STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
                                     WITH SERDEPROPERTIES ("hbase.columns.mapping"="${getHBaseCatalogForHIVE(HuemulTypeInternalTableType.Normal)}")
                                     TBLPROPERTIES ("hbase.table.name"="${getHBaseNamespace(HuemulTypeInternalTableType.Normal)}:${getHBaseTableName(HuemulTypeInternalTableType.Normal)}")"""
      }
    }

    if (huemulBigDataGov.DebugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: $lCreateTableScript ")

     lCreateTableScript
  }

  /**
   * Create table script to save DQ Results
   */
  private def DF_CreateTable_DQ_Script(): String = {

    //var coma_partition = ""
    val PartitionForCreateTable = if (huemulBigDataGov.GlobalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) s"dq_control_id" else s"dq_control_id STRING"

    var lCreateTableScript: String = ""
    //FROM 2.4 --> INCLUDE SPECIAL OPTIONS FOR DATABRICKS
    if (huemulBigDataGov.GlobalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
      if (getStorageType_DQResult == HuemulTypeStorageType.PARQUET || getStorageType_DQResult == HuemulTypeStorageType.ORC || getStorageType_DQResult == HuemulTypeStorageType.AVRO ) {
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.DQ)} (${getColumns_CreateTable(ForHive = true, HuemulTypeInternalTableType.DQ) })
                                   USING ${getStorageType_DQResult.toString}
                                   PARTITIONED BY ($PartitionForCreateTable)
                                   LOCATION '$getFullNameWithPath_DQ'"""
      } else if (getStorageType_DQResult == HuemulTypeStorageType.HBASE)  {
        raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
      } else if (getStorageType_DQResult == HuemulTypeStorageType.DELTA) {
        //for delta, databricks get all columns and partition columns
        //see https://docs.databricks.com/spark/latest/spark-sql/language-manual/create-table.html
        lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.DQ)}
                                   USING ${getStorageType_DQResult.toString}
                                   LOCATION '$getFullNameWithPath_DQ'"""
      }
    }  else {
      if (getStorageType_DQResult == HuemulTypeStorageType.PARQUET || getStorageType_DQResult == HuemulTypeStorageType.ORC || getStorageType_DQResult == HuemulTypeStorageType.AVRO) {
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
        lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.DQ)} (${getColumns_CreateTable(ForHive = true, HuemulTypeInternalTableType.DQ) })
                                   PARTITIONED BY ($PartitionForCreateTable)
                                   STORED AS ${getStorageType_DQResult.toString}
                                   LOCATION '$getFullNameWithPath_DQ'"""
      } else if (getStorageType_DQResult == HuemulTypeStorageType.HBASE)  {
        raiseError("huemul_Table Error: HBase is not available for DQ Table", 1061)
      } else if (getStorageType_DQResult == HuemulTypeStorageType.DELTA) {
        lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.DQ)}
                                   STORED AS ${getStorageType_DQResult.toString}
                                   LOCATION '$getFullNameWithPath_DQ'"""
      }
    }

    if (huemulBigDataGov.DebugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: $lCreateTableScript ")

     lCreateTableScript
  }



  /**
  CREATE TABLE SCRIPT TO SAVE OLD VALUE TRACE
   */
  private def DF_CreateTable_OldValueTrace_Script(): String = {
    //var coma_partition = ""

    var lCreateTableScript: String = ""
    //FROM 2.4 --> INCLUDE SPECIAL OPTIONS FOR DATABRICKS
    if (huemulBigDataGov.GlobalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) {
      lCreateTableScript = s"""
                                   CREATE TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.OldValueTrace)} (${getColumns_CreateTable(ForHive = true, HuemulTypeInternalTableType.OldValueTrace) })
                                    ${if (getStorageType_OldValueTrace == HuemulTypeStorageType.PARQUET) {
                                     """USING PARQUET
                                        PARTITIONED BY (mdm_columnname)"""
                                    }
                                    else if (getStorageType_OldValueTrace == HuemulTypeStorageType.ORC) {
                                     """USING ORC
                                        PARTITIONED BY (mdm_columnname)"""
                                    }
                                    else if (getStorageType_OldValueTrace == HuemulTypeStorageType.DELTA) {
                                     """USING DELTA
                                        PARTITIONED BY (mdm_columnname)"""
                                    }
                                    else if (getStorageType_OldValueTrace == HuemulTypeStorageType.AVRO) {
                                     s"""USING AVRO
                                        PARTITIONED BY (mdm_columnname)"""
                                    }
                                   }
                                   LOCATION '$getFullNameWithPath_OldValueTrace'"""
                                    //${if (_StorageType_OldValueTrace == "csv") {s"""
                                    //TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ")"""}}"""
    } else {
      //STORED AS ${_StorageType_OldValueTrace}
      //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
      lCreateTableScript = s"""
                                   CREATE EXTERNAL TABLE IF NOT EXISTS ${internalGetTable(HuemulTypeInternalTableType.OldValueTrace)} (${getColumns_CreateTable(ForHive = true, HuemulTypeInternalTableType.OldValueTrace) })
                                    ${if (getStorageType_OldValueTrace.toString == "csv") {s"""
                                    ROW FORMAT DELIMITED
                                    FIELDS TERMINATED BY '\t'
                                    STORED AS TEXTFILE """}
                                    else if (getStorageType_OldValueTrace.toString == "json") {
                                      s"""
                                       ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
                                      """
                                    }
                                    else if (getStorageType_OldValueTrace == HuemulTypeStorageType.PARQUET) {
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS PARQUET"""
                                    }
                                    else if (getStorageType_OldValueTrace == HuemulTypeStorageType.ORC) {
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS ORC"""
                                    }
                                    else if (getStorageType_OldValueTrace == HuemulTypeStorageType.DELTA) {
                                     """PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS DELTA"""
                                    }
                                    else if (getStorageType_OldValueTrace == HuemulTypeStorageType.AVRO) {
                                     s"""PARTITIONED BY (mdm_columnname STRING)
                                       STORED AS AVRO"""
                                    }
                                   }
                                   LOCATION '$getFullNameWithPath_OldValueTrace'"""
                                    //${if (_StorageType_OldValueTrace == "csv") {s"""
                                    //TBLPROPERTIES("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ")"""}}"""
    }

    if (huemulBigDataGov.DebugMode)
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

  private def DF_ForeingKeyMasterAuto(warning_exclude: Boolean, LocalControl: HuemulControl): HuemulDataQualityResult = {
    val Result: HuemulDataQualityResult = new HuemulDataQualityResult()
    val ArrayFK = this.getForeingKey
    val DataBaseName = this.getDataBase(this._DataBase)
    //For Each Foreing Key Declared
    ArrayFK.filter { x => (warning_exclude && x.getNotification == HuemulTypeDQNotification.WARNING_EXCLUDE) ||
                          (!warning_exclude  && x.getNotification != HuemulTypeDQNotification.WARNING_EXCLUDE)
                   } foreach { x =>
      val pk_InstanceTable = x._Class_TableName.asInstanceOf[HuemulTable]
      
      //Step1: Create distinct FROM NEW DF
      var SQLFields: String = ""
      var SQLLeftJoin: String = ""
      var sqlAnd: String = ""
      var sqlComa: String = ""
      var FirstRowPK: String = ""
      var FirstRowFK: String = ""
      x.Relationship.foreach { y =>
        //Get fields name
        FirstRowPK = s"PK.${y.PK.getMyName(pk_InstanceTable.getStorageType)}"
        FirstRowFK = y.FK.getMyName(this.getStorageType)
        SQLFields += s"$sqlComa${y.FK.getMyName(this.getStorageType)}"
        SQLLeftJoin += s"${sqlAnd}PK.${y.PK.getMyName(pk_InstanceTable.getStorageType)} = FK.${y.FK.getMyName(this.getStorageType)}"
        sqlAnd = " and "
        sqlComa = ","
      }
      
      //val FKRuleName: String = GetFieldName[DAPI_MASTER_FK](this, x)
      val AliasDistinct_B: String = s"___${x.MyName}_FKRuleDistB__"
      val DF_Distinct = huemulBigDataGov.DF_ExecuteQuery(AliasDistinct_B, s"SELECT DISTINCT $SQLFields FROM ${this.DataFramehuemul.Alias} ${if (x.AllowNull) s" WHERE $FirstRowFK is not null " else "" }")
        
      var broadcast_sql: String = ""
      if (x.getBroadcastJoin)
        broadcast_sql = "/*+ BROADCAST(PK) */"
        
      //Step2: left join with TABLE MASTER DATA
      //val AliasLeft: String = s"___${x.MyName}_FKRuleLeft__"
      
      val pk_table_name = pk_InstanceTable.internalGetTable(HuemulTypeInternalTableType.Normal)
      val SQLLeft: String = s"""SELECT $broadcast_sql FK.* 
                                 FROM $AliasDistinct_B FK
                                   LEFT JOIN $pk_table_name PK
                                     ON $SQLLeftJoin
                                 WHERE $FirstRowPK IS NULL
                              """
                                 
      val AliasDistinct: String = s"___${x.MyName}_FKRuleDist__"
      val dt_start = huemulBigDataGov.getCurrentDateTimeJava
      val DF_Left = huemulBigDataGov.DF_ExecuteQuery(AliasDistinct, SQLLeft)
      var TotalLeft = DF_Left.count()
      
      if (TotalLeft > 0) {
        Result.isError = true
        Result.Description = s"huemul_Table Error: Foreing Key DQ Error, $TotalLeft records not found"
        Result.Error_Code = 1024
        Result.dqDF = DF_Left
        Result.profilingResult.count_all_Col = TotalLeft
        DF_Left.show()
        
        val DF_leftDetail = huemulBigDataGov.DF_ExecuteQuery("__DF_leftDetail", s"""SELECT FK.* 
                                                                                    FROM ${this.DataFramehuemul.Alias} FK  
                                                                                      LEFT JOIN $pk_table_name PK
                                                                                         ON $SQLLeftJoin
                                                                                    WHERE $FirstRowPK IS NULL""")
                                                                                    
        
        TotalLeft = DF_leftDetail.count()
     
      }
         
      //val NumTotalDistinct = DF_Distinct.count()
      val numTotal = this.DataFramehuemul.getNumRows
      val dt_end = huemulBigDataGov.getCurrentDateTimeJava
      val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
      
      val Values = new HuemulDQRecord(huemulBigDataGov)
      Values.Table_Name =TableName
      Values.BBDD_Name =DataBaseName
      Values.DF_Alias = DataFramehuemul.Alias
      Values.ColumnName =FirstRowFK
      Values.DQ_Name =s"FK - $SQLFields"
      Values.DQ_Description =s"FK Validation: PK Table: ${pk_InstanceTable.internalGetTable(HuemulTypeInternalTableType.Normal)} "
      Values.DQ_QueryLevel = HuemulTypeDQQueryLevel.Row // IsAggregate =false
      Values.DQ_Notification = x.getNotification //from 2.1, before --> HuemulTypeDQNotification.ERROR// RaiseError =true
      Values.DQ_SQLFormula =SQLLeft
      Values.DQ_ErrorCode = Result.Error_Code
      Values.DQ_toleranceError_Rows =0L
      Values.DQ_toleranceError_Percent =null  
      Values.DQ_ResultDQ =Result.Description
      Values.DQ_NumRowsOK =numTotal - TotalLeft
      Values.DQ_NumRowsError =TotalLeft
      Values.DQ_NumRowsTotal =numTotal
      Values.DQ_IsError = if (x.getNotification == HuemulTypeDQNotification.ERROR) Result.isError else false
      Values.DQ_IsWarning = if (x.getNotification != HuemulTypeDQNotification.ERROR) Result.isError else false
      Values.DQ_ExternalCode = x.getExternalCode // "HUEMUL_DQ_001"
      Values.DQ_duration_hour = duration.hour.toInt
      Values.DQ_duration_minute = duration.minute.toInt
      Values.DQ_duration_second = duration.second.toInt

      this.DataFramehuemul.DQ_Register(Values) 
      
      //Step3: Return DQ Validation
      if (TotalLeft > 0) {
       
        
        DF_ProcessToDQ( "__DF_leftDetail"   //sqlfrom
                        , null           //sqlwhere
                        , haveField = true            //haveField
                        , FirstRowFK      //fieldname
                        , Values.DQ_Id
                        , x.getNotification //from 2.1, before -->HuemulTypeDQNotification.ERROR //dq_error_notification
                        , Result.Error_Code //error_code
                        , s"(${Result.Error_Code}) FK ERROR ON $pk_table_name"// dq_error_description
                        , LocalControl
                        )
        
      }
      
      
      DF_Distinct.unpersist()
    }
    
     Result
  }
  
  private def DF_ProcessToDQ(fromSQL: String
                  ,whereSQL: String
                  ,haveField: Boolean
                  ,fieldName: String
                  ,dq_id: String
                  ,dq_error_notification: HuemulTypeDQNotification.HuemulTypeDQNotification
                  ,error_code: Integer
                  ,dq_error_description: String
                  ,LocalControl: HuemulControl
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
                            , dqNotification:HuemulTypeDQNotification): Boolean = (
    (warningExclude && dqNotification == HuemulTypeDQNotification.WARNING_EXCLUDE)
      || (!warningExclude && dqNotification != HuemulTypeDQNotification.WARNING_EXCLUDE)
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
                           , minValueNotification: HuemulTypeDQNotification
                           , maxValueNotification: HuemulTypeDQNotification
                           , sqlMinSentence:String
                           , sqlMaxSentence:String
                           , minExternalCode:String
                           , maxExternalCode:String):List[HuemulDataQuality] = {
    var listDqRule:List[HuemulDataQuality] = List[HuemulDataQuality]()
    val colMyName = columnDefinition.getMyName(this.getStorageType)
    val SQLFormula: HashMap[String, String] = new HashMap[String, String]
    var notificationMin:HuemulTypeDQNotification = null
    var notificationMax:HuemulTypeDQNotification = null
    var externalCode:String = null

    if (minValue != null && validateDQRun(dqWarningExclude,minValueNotification) ){
      SQLFormula += ( "Min" -> s" $sqlMinSentence ")
      notificationMin = minValueNotification
      externalCode = minExternalCode
    }

    if (maxValue != null && validateDQRun(dqWarningExclude,maxValueNotification)) {
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
        , HuemulTypeDQQueryLevel.Row
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
        , HuemulTypeDQQueryLevel.Row
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
        , HuemulTypeDQQueryLevel.Row
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
   * @param ResultDQ        DQ Rules execution result
   * @param LocalControl    huemul_control refernce for logging
   * @return [[com.huemulsolutions.bigdata.dataquality.HuemulDataQualityResult]] with new records
   */
  private def executeRuleTypeAggSaveData(warningExclude:Boolean
                                 , ResultDQ:HuemulDataQualityResult
                                         , LocalControl: HuemulControl): Unit = {

    //.filter( f => (typeOpr == "pk" && f.DQ_ErrorCode == 1018) || (typeOpr == "uk" && f.DQ_ErrorCode == 2006) )
    //get on sentence, until now it only process pk and unique rules
    val dqResultData:List[HuemulDQRecord] = ResultDQ
      .getDQResult
      .filter( f => (
        ((warningExclude && f.DQ_Notification == HuemulTypeDQNotification.WARNING_EXCLUDE)
          || !warningExclude && f.DQ_Notification != HuemulTypeDQNotification.WARNING_EXCLUDE)
          && (f.DQ_ErrorCode == 1018 || f.DQ_ErrorCode == 2006)) )
      .toList

    //Start loop each pk, uk data rule with error, warning or warning_exclude
    for(dqKeyRule <- dqResultData ) {
      val typeOpr:String = if (dqKeyRule.DQ_ErrorCode==1018) "pk" else "uk"
      val dqId = dqKeyRule.DQ_Id
      val notification:HuemulTypeDQNotification = dqKeyRule.DQ_Notification
      val stepRunType = notification match {
        case HuemulTypeDQNotification.WARNING_EXCLUDE => "WARNING_EXCLUDE"
        case HuemulTypeDQNotification.WARNING => "WARNING"
        case _ => "ERROR"
      }
      val colData:List[HuemulColumns] = getColumns
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
      if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"Step: DQ Result: Step1 Sql : $step1SQL")
      val dfDupRecords:DataFrame = huemulBigDataGov.DF_ExecuteQuery(s"___temp_${typeOpr}_det_01", step1SQL)
      dfDupRecords.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

      //Broadcast Hint
      val broadcastHint : String = if (dfDupRecords.count() < 50000) "/*+ BROADCAST(dup) */" else ""

      //get rows duplicated
      LocalControl.NewStep(s"Step: DQ Result: Get detail for ${typeOpr.toUpperCase} $stepRunType (step2)")
      val step2SQL =s"""SELECT $broadcastHint $typeOpr.*
                       | FROM $sourceAlias $typeOpr
                       | INNER JOIN ___temp_${typeOpr}_det_01 dup
                       |  ON $keyJoinSql""".stripMargin
      if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"Step: DQ Result: Step2 Sql : $step2SQL")
      val dfDupRecordsDetail:DataFrame = huemulBigDataGov.DF_ExecuteQuery(s"___temp_${typeOpr}_det_02", step2SQL)
      dfDupRecordsDetail.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
      val numReg = dfDupRecordsDetail.count()

      val dqSqlTableSentence = this.DataFramehuemul.DQ_GenQuery(
        s"___temp_${typeOpr}_det_02"
        , null
        , haveField = true
        , keyColumns
        , dqId
        , notification
        , dqKeyRule.DQ_ErrorCode
        , s"(${dqKeyRule.DQ_ErrorCode}) ${typeOpr.toUpperCase} $stepRunType ON $keyJoinSql ($numReg reg)"// dq_error_description
      )
      if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"Step: DQ Result: Step3 Sql : $dqSqlTableSentence")

      //Execute query
      LocalControl.NewStep(s"Step: DQ Result: Get detail for ${typeOpr.toUpperCase} $stepRunType (step3, $numReg rows)")
      val dfErrorRecords = huemulBigDataGov.DF_ExecuteQuery(s"temp_DQ_KEY", dqSqlTableSentence)
      dfErrorRecords.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
      //val numCount = dfErrorRecords.count()
      if (ResultDQ.DetailErrorsDF == null)
        ResultDQ.DetailErrorsDF = dfErrorRecords
      else
        ResultDQ.DetailErrorsDF = ResultDQ.DetailErrorsDF.union(dfErrorRecords)

    }
  }

  /**create dataQuality for run
   *
   * @param IsSelectiveUpdate: true if selective update
   * @param LocalControl: instance of local control
   * @param warning_exclude: true for run with warning exclude mode
   * @return
   */
  private def DF_DataQualityMasterAuto(IsSelectiveUpdate: Boolean
                                       , LocalControl: HuemulControl
                                       , warning_exclude: Boolean): HuemulDataQualityResult = {

    val Result: HuemulDataQualityResult = new HuemulDataQualityResult()
    val ArrayDQ: ArrayBuffer[HuemulDataQuality] = new ArrayBuffer[HuemulDataQuality]()

    //All required fields have been set
    val SQL_Missing = missingRequiredFields(IsSelectiveUpdate)
    if (SQL_Missing.nonEmpty) {
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
      if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageInfo(s"DQ Rule: Validate PK for columns $SQL_PK")
      val dqPk: HuemulDataQuality = new HuemulDataQuality(
        null
        , s"PK Validation"
        , s"count(1) = count(distinct $SQL_PK )"
        , 1018
        , HuemulTypeDQQueryLevel.Aggregate
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
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: VALIDATE UNIQUE FOR FIELD ${x.getMyName(this.getStorageType)}")

        val DQ_UNIQUE : HuemulDataQuality = new HuemulDataQuality(
          x
          , s"UNIQUE Validation ${x.getMyName(this.getStorageType)}"
          ,s"count(1) = count(distinct ${x.getMyName(this.getStorageType)} )"
          , 2006
          , HuemulTypeDQQueryLevel.Aggregate
          , x.getIsUnique_Notification
          , true
          , x.getIsUnique_externalCode )
        DQ_UNIQUE.setTolerance(0L, null)
        ArrayDQ.append(DQ_UNIQUE)
      }

    //Apply Data Quality according to field definition in DataDefDQ: Accept nulls (nullable)
    //Note: If it has default value and it is NOT NULL it will not be evaluated
    //ToDO: Checks te notes for default values
    SQL_NotNull_FinalTable()
      .filter( f => (!f.getNullable  && (f.getDefaultValue == null || f.getDefaultValue.equals("null")))
        && validateDQRun(warning_exclude, f.getDQ_Nullable_Notification ) )
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: VALIDATE NOT NULL FOR FIELD $colMyName")

        val NotNullDQ : HuemulDataQuality = new HuemulDataQuality(
          x
          , s"Not Null for field $colMyName "
          , s"$colMyName IS NOT NULL"
          , 1023
          , HuemulTypeDQQueryLevel.Row
          , x.getDQ_Nullable_Notification
          , true
          , x.getNullable_externalCode)
        NotNullDQ.setTolerance(0L, null)
        ArrayDQ.append(NotNullDQ)
      }

    //Validación DQ_RegExp
    getColumns.filter {
      x => x.getDQ_RegExpression != null && validateDQRun(warning_exclude, x.getDQ_RegExpression_Notification)
    }.foreach { x =>
      val _colMyName = x.getMyName(this.getStorageType)
      val SQLFormula : String = s"""${_colMyName} rlike "${x.getDQ_RegExpression}" """

      val RegExp : HuemulDataQuality = new HuemulDataQuality(
        x
        , s"RegExp Column ${_colMyName}"
        , s" ($SQLFormula) or (${_colMyName} is null) "
        , 1041
        , HuemulTypeDQQueryLevel.Row
        , x.getDQ_RegExpression_Notification
        , true
        , x.getDQ_RegExpression_externalCode  )

      RegExp.setTolerance(0L, null)
      ArrayDQ.append(RegExp)
    }


    //Validate DQ rule max/mín length the field
    getColumns
      .filter { x =>(
        (x.getDQ_MinLen != null && validateDQRun(warning_exclude, x.getDQ_MinLen_Notification) )
          || (x.getDQ_MaxLen != null && validateDQRun(warning_exclude,x.getDQ_MaxLen_Notification) )
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[HuemulDataQuality] = dqRuleMinMax(
          warning_exclude
          , x
          , "length DQ for Colum"
          , 1020
          , if (x.getDQ_MinLen != null) x.getDQ_MinLen.toString else null
          , if (x.getDQ_MaxLen != null) x.getDQ_MaxLen.toString else null
          , x.getDQ_MinLen_Notification
          , x.getDQ_MaxLen_Notification
          , s"length($colMyName) >= ${x.getDQ_MinLen}"
          , s"length($colMyName) <= ${x.getDQ_MaxLen}"
          , x.getDQ_MinLen_externalCode
          , x.getDQ_MaxLen_externalCode)

        dqRules.foreach(dq => ArrayDQ.append(dq))
      }

    //Validate DQ rule max/mín number value
    getColumns
      .filter { x => (
        (x.getDQ_MinDecimalValue != null && validateDQRun(warning_exclude, x.getDQ_MinDecimalValue_Notification ) )
          || (x.getDQ_MaxDecimalValue != null && validateDQRun(warning_exclude, x.getDQ_MaxDecimalValue_Notification ))
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[HuemulDataQuality] = dqRuleMinMax(
          warning_exclude
          , x
          , "Number DQ for Column"
          , 1021
          , if (x.getDQ_MinDecimalValue != null) x.getDQ_MinDecimalValue.toString else null
          , if (x.getDQ_MaxDecimalValue != null) x.getDQ_MaxDecimalValue.toString else null
          , x.getDQ_MinDecimalValue_Notification
          , x.getDQ_MaxDecimalValue_Notification
          , s" $colMyName >= ${x.getDQ_MinDecimalValue} "
          , s" $colMyName <= ${x.getDQ_MaxDecimalValue} "
          , x.getDQ_MinDecimalValue_externalCode
          , x.getDQ_MaxDecimalValue_externalCode)

        dqRules.foreach(dq => ArrayDQ.append(dq))
      }

    //Validación DQ máximo y mínimo de fechas
    getColumns
      .filter { x => (
        (x.getDQ_MinDateTimeValue != null && validateDQRun(warning_exclude, x.getDQ_MinDateTimeValue_Notification ) )
          || (x.getDQ_MaxDateTimeValue != null && validateDQRun(warning_exclude, x.getDQ_MaxDateTimeValue_Notification ))
        )}
      .foreach { x =>
        val colMyName = x.getMyName(this.getStorageType)
        val dqRules:List[HuemulDataQuality] = dqRuleMinMax(
          warning_exclude
          , x
          , "DateTime DQ for Column"
          , 1022
          , x.getDQ_MinDateTimeValue
          , x.getDQ_MaxDateTimeValue
          , x.getDQ_MinDateTimeValue_Notification
          , x.getDQ_MaxDateTimeValue_Notification
          , s" $colMyName >= '${x.getDQ_MinDateTimeValue}' "
          , s" $colMyName <= '${x.getDQ_MaxDateTimeValue}' "
          , x.getDQ_MinDateTimeValue_externalCode
          , x.getDQ_MaxDateTimeValue_externalCode)

        dqRules.foreach(dq => ArrayDQ.append(dq))
      }

    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: DQ Rule size ${ArrayDQ.size}")
    ArrayDQ.foreach(x => if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"DF_SAVE DQ: Rule - ${x.getDescription}"))

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

     Result
  }
  
  
  def getOrderByColumn: String = {
    var ColumnName: String = null
    
    
    this.getALLDeclaredFields(OnlyUserDefined = true, PartitionColumnToEnd = true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] &&
                                      x.get(this).asInstanceOf[HuemulColumns].getIsPK }.foreach { x =>
      x.setAccessible(true)
      if (ColumnName == null)
        ColumnName = x.get(this).asInstanceOf[HuemulColumns].getMyName(this.getStorageType)
    }
    
     ColumnName
  }
  
 
  /**
   Create final DataFrame with full join New DF with old DF
   */
  private def DF_MDM_Dohuemul(LocalControl: HuemulControl, AliasNewData: String, isInsert: Boolean, isUpdate: Boolean, isDelete: Boolean, isSelectiveUpdate: Boolean, PartitionValuesForSelectiveUpdate: ArrayBuffer[String], storageLevelOfDF: org.apache.spark.storage.StorageLevel = null) {
    if (isSelectiveUpdate) {
      //Update some rows with some columns
      //Cant update PK fields
      
      //***********************************************************************/
      //STEP 1:    V A L I D A T E   M A P P E D   C O L U M N  S   *********/
      //***********************************************************************/
      
      LocalControl.NewStep("Selective Update: Validating fields ")
      
      val PKNotMapped = this.missingRequiredFields_SelectiveUpdate()
      
      if (PKNotMapped != "")
        raiseError(s"huemul_Table Error: PK not defined: $PKNotMapped",1017)
      
      //Get N° rows from user dataframe
      val NumRowsUserData: java.lang.Long = this.DataFramehuemul.DataFrame.count()
      var NumRowsOldDataFrame: java.lang.Long = 0L
      //**************************************************//
      //STEP 2:   CREATE BACKUP
      //**************************************************//
      LocalControl.NewStep("Selective Update: Select Old Table")                                             
      val TempAlias: String = s"__${this.TableName}_old"
      //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      //from 2.6 --> validate numpartitions = numvalues
      //if (!huemulBigDataGov.HasName(PartitionValueForSelectiveUpdate) && _TableType == huemulType_Tables.Transaction)
      if (getPartitionList.length > 0 && (PartitionValuesForSelectiveUpdate.length != getPartitionList.length ||
                                          PartitionValuesForSelectiveUpdate.contains(null)))
        raiseError(s"huemul_Table Error: Partition Value not defined (partitions: ${getPartitionList.mkString(",") }, values: ${PartitionValuesForSelectiveUpdate.mkString(",")}) ", 1044)
        
      
      var FullPathString = this.getFullNameWithPath
        
      if (_TableType == HuemulTypeTables.Transaction) {
        val partitionList = getPartitionList
        var i: Int = 0
        while (i < getPartitionList.length) {
          FullPathString += s"/${partitionList(i).getMyName(getStorageType)}=${PartitionValuesForSelectiveUpdate(i)}"
          i+=1
        }
        
        //FullPathString = s"${getFullNameWithPath}/${getPartitionField.toLowerCase()}=${PartitionValueForSelectiveUpdate}"
      }
      
      val FullPath = new org.apache.hadoop.fs.Path(FullPathString)
      //Google FS compatibility
      val fs = FullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      if (fs.exists(FullPath)){
        //Exist, copy for use
        val dt_start = huemulBigDataGov.getCurrentDateTimeJava
        //Open actual file
        val DFTempCopy = huemulBigDataGov.spark.read.format( _getSaveFormat(this.getStorageType)).load(FullPathString)
        val tempPath = huemulBigDataGov.GlobalSettings.getDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, TempAlias)
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to temp dir: $tempPath ")
        if (this.getNumPartitions == null || this.getNumPartitions <= 0)
          DFTempCopy.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
        else
          DFTempCopy.repartition(this.getNumPartitions).write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
          
        DFTempCopy.unpersist()
       
        //Open temp file
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"open temp old df: $tempPath ")

        val lineageWhere: ArrayBuffer[String] = new ArrayBuffer[String]
        var DFTempOpen = huemulBigDataGov.spark.read.parquet(tempPath)
        //from 2.6 --> add partition columns and values
        var i: Int = 0
        val partitionList = getPartitionList
        while (i < getPartitionList.length) {
          DFTempOpen = DFTempOpen.withColumn(partitionList(i).getMyName(getStorageType) , lit(PartitionValuesForSelectiveUpdate(i)))
          lineageWhere.append(s"${partitionList(i).getMyName(getStorageType)} = '${PartitionValuesForSelectiveUpdate(i)}'")
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
        
        val dt_end = huemulBigDataGov.getCurrentDateTimeJava
        huemulBigDataGov.DF_SaveLineage(TempAlias
                                    , s"""SELECT * FROM ${this.internalGetTable(HuemulTypeInternalTableType.Normal)} ${ if (lineageWhere.nonEmpty)  /*if (_TableType == huemulType_Tables.Transaction)*/
                                                                                                                      s" WHERE ${lineageWhere.mkString(" and ")}"} """ //sql
                                    , dt_start
                                    , dt_end
                                    , Control
                                    , null  //FinalTable
                                    , isQuery = false //isQuery
                                    , isReferenced = true //isReferenced
                                    )
      } else 
        raiseError(s"huemul_Table Error: Table $FullPathString doesn't exists",1043)
        
      
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
        raiseError(s"huemul_Table Error: it was expected to update $NumRowsUserData rows, only ${_NumRows_Updatable} was found", 1045)
      } else if (this._NumRows_Total != NumRowsOldDataFrame ) {
        raiseError(s"huemul_Table Error: Different number of rows in dataframes, original: $NumRowsOldDataFrame, new: ${this._NumRows_Total}, check your dataframe, maybe have duplicate keys", 1046)
      }
      
      LocalControl.NewStep("Selective Update: Final Table")
      val SQLFinalTable = SQL_Step4_Final("__Hash_p1", huemulBigDataGov.ProcessNameCall)

     
      //STEP 2: Execute final table  //Add this.getNumPartitions param in v1.3
      DataFramehuemul._CreateFinalQuery(AliasNewData , SQLFinalTable, huemulBigDataGov.DebugMode , this.getNumPartitions, this, storageLevelOfDF)
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
        
      //Unpersist first DF
      SQLHash_p2_DF.unpersist()
      SQLHash_p1_DF.unpersist()
      SQLLeftJoin_DF.unpersist()
    }
    else if (_TableType == HuemulTypeTables.Transaction) {
      LocalControl.NewStep("Transaction: Validating fields ")
      //STEP 1: validate name setting
      getALLDeclaredFields(OnlyUserDefined = true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[HuemulColumns] }
      .foreach { x =>     
        //Get field
        val Field = x.get(this).asInstanceOf[HuemulColumns]
        
        //New value (from field or compute column )
        if (!huemulBigDataGov.HasName(Field.getMappedName) ){
          raiseError(s"${x.getName} MUST have an assigned local name",1004)
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
    else if (_TableType == HuemulTypeTables.Reference || _TableType == HuemulTypeTables.Master) {
      /*
       * isInsert: se aplica en SQL_Step2_UpdateAndInsert, si no permite insertar, filtra esos registros y no los inserta
       * isUpdate: se aplica en SQL_Step1_FullJoin: si no permite update, cambia el tipo ___ActionType__ de UPDATE a EQUAL
       */
      
      //val OnlyInsert: Boolean = isInsert && !isUpdate
      
      //All required fields have been set
      val SQL_Missing = missingRequiredFields(isSelectiveUpdate)
      if (SQL_Missing.nonEmpty) {
        var ColumnsMissing: String = ""
        SQL_Missing.foreach { x => ColumnsMissing +=  s",$x " }
        this.raiseError(s"huemul_Table Error: requiered fields missing $ColumnsMissing", 1016)
         
      }
    
      //**************************************************//
      //STEP 0: Apply distinct to New DataFrame
      //**************************************************//
      var NextAlias = this.DataFramehuemul.Alias
      
      if (ApplyDistinct) {
        LocalControl.NewStep("Ref & Master: Select distinct")
        huemulBigDataGov.DF_ExecuteQuery("__Distinct"
                                                , SQL_Step0_Distinct(this.DataFramehuemul.Alias)
                                                , Control //parent control 
                                               )
        NextAlias = "__Distinct"
      }
      
      
      //**************************************************//
      //STEP 0.1: CREATE TEMP TABLE IF MASTER TABLE DOES NOT EXIST
      //**************************************************//
      LocalControl.NewStep("Ref & Master: Select Old Table")      
      val dt_start = huemulBigDataGov.getCurrentDateTimeJava
      val TempAlias: String = s"__${this.TableName}_old"
      //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      var oldDataFrameExists = false
      
      if (this.getStorageType == HuemulTypeStorageType.HBASE) {
        val hBaseConnector = new HuemulTableConnector(huemulBigDataGov, LocalControl)
        if (hBaseConnector.tableExistsHBase(getHBaseNamespace(HuemulTypeInternalTableType.Normal),getHBaseTableName(HuemulTypeInternalTableType.Normal))) {
          oldDataFrameExists = true
          //println(getHBaseCatalog(huemulType_InternalTableType.Normal))
          //val DFHBase = hBaseConnector.getDFFromHBase(TempAlias, getHBaseCatalog(huemulType_InternalTableType.Normal))
          
          //create external table if doesn't exists
          CreateTableScript = DF_CreateTableScript()
          
          //new from 2.3
          runSQLexternalTable(CreateTableScript, onlyHBASE = true)
            
          LocalControl.NewStep("Ref & Master: Reading HBase data...")     
          val DFHBase = huemulBigDataGov.DF_ExecuteQuery(TempAlias, s"SELECT * FROM ${this.internalGetTable(HuemulTypeInternalTableType.Normal)}")
          DFHBase.count()
          if (huemulBigDataGov.GlobalSettings.MDM_SaveBackup && this._SaveBackup){
            val tempPath = this.getFullNameWithPath_Backup(Control.Control_Id )
            _BackupPath = tempPath
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to backup dir: $tempPath ")
         
            LocalControl.NewStep("Ref & Master: Backup...")
            DFHBase.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)     //2.2 -> this._StorageType.toString() instead of "parquet"
          }
          
          
          //DFHBase.show()
        }
      } else {
        val lpath = new org.apache.hadoop.fs.Path(this.getFullNameWithPath)
        val fs = lpath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration) 
        if (fs.exists(lpath)){
          //Exist, copy for use
          oldDataFrameExists = true
          
          //Open actual file
          val DFTempCopy = huemulBigDataGov.spark.read.format(_getSaveFormat(this.getStorageType)).load(this.getFullNameWithPath)
          if (huemulBigDataGov.DebugMode) DFTempCopy.printSchema()

          //2.0: save previous to backup
          var tempPath: String = null
          if (huemulBigDataGov.GlobalSettings.MDM_SaveBackup && this._SaveBackup){
            tempPath = this.getFullNameWithPath_Backup(Control.Control_Id )
            _BackupPath = tempPath
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"copy to backup dir: $tempPath ")
          } else {
            tempPath = huemulBigDataGov.GlobalSettings.getDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, TempAlias)
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
          val Schema = getSchema
          val SchemaForEmpty = StructType(Schema.map { x => StructField(x.name, x.dataType, x.nullable) })
          val EmptyRDD = huemulBigDataGov.spark.sparkContext.emptyRDD[Row]
          val EmptyDF = huemulBigDataGov.spark.createDataFrame(EmptyRDD, SchemaForEmpty)
          EmptyDF.createOrReplaceTempView(TempAlias)
          if (huemulBigDataGov.DebugMode) EmptyDF.show()
      }
      val dt_end = huemulBigDataGov.getCurrentDateTimeJava
      
      huemulBigDataGov.DF_SaveLineage(TempAlias
                                    , s"SELECT * FROM ${this.internalGetTable(HuemulTypeInternalTableType.Normal)} " //sql
                                    , dt_start
                                    , dt_end
                                    , Control
                                    , null  //FinalTable
                                    , isQuery = false //isQuery
                                    , isReferenced = true //isReferenced
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
      huemulBigDataGov.DF_ExecuteQuery("__logic_p1"
                                          , SQL_Step2_UpdateAndInsert("__FullJoin", huemulBigDataGov.ProcessNameCall, isInsert)
                                          , Control //parent control 
                                         )
     
      //STEP 3: Create Hash
      LocalControl.NewStep("Ref & Master: Hash Code")                                         
      huemulBigDataGov.DF_ExecuteQuery("__Hash_p1"
                                          , SQL_Step3_Hash_p1("__logic_p1", isSelectiveUpdate = false)
                                          , Control //parent control 
                                         )
                                         
                                        
      this.UpdateStatistics(LocalControl, "Ref & Master", "__Hash_p1")
      
                                         
      LocalControl.NewStep("Ref & Master: Final Table")
      //val SQLFinalTable = SQL_Step4_Final("__Hash_p1", huemulBigDataGov.ProcessNameCall, if (OnlyInsert) true else false)
      //from 2.1: incluye ActionType fro all master and transaction table types, to use on statistics 
      val SQLFinalTable = SQL_Step4_Final("__Hash_p1", huemulBigDataGov.ProcessNameCall )

     
      //STEP 2: Execute final table // Add debugmode and getnumpartitions in v1.3 
      DataFramehuemul._CreateFinalQuery(AliasNewData , SQLFinalTable, huemulBigDataGov.DebugMode , this.getNumPartitions, this, storageLevelOfDF)
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.printSchema()
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
      
      
      
      
    } else
      raiseError(s"huemul_Table Error: ${_TableType} found, Master o Reference required ", 1007)
  }
  
  private def UpdateStatistics(LocalControl: HuemulControl, TypeOfCall: String, Alias: String) {
    LocalControl.NewStep(s"$TypeOfCall: Statistics")
    
    if (Alias == null) {
      this._NumRows_Total = this.DataFramehuemul.getNumRows
      this._NumRows_New = this._NumRows_Total
      this._NumRows_Update  = 0L
      this._NumRows_Updatable = 0L
      this._NumRows_Delete = 0L
      this._NumRows_NoChange = 0L
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
                            FROM $Alias temp
                         """)
      
      if (huemulBigDataGov.DebugMode) DQ_ReferenceData.show()
      
      val FirstRow = DQ_ReferenceData.collect()(0) // .first()
      this._NumRows_Total = FirstRow.getAs("__Total")
      this._NumRows_New = FirstRow.getAs("__New")
      this._NumRows_Update  = FirstRow.getAs("__Update")
      this._NumRows_Updatable  = FirstRow.getAs("__Updatable")
      this._NumRows_Delete = FirstRow.getAs("__Delete")
      this._NumRows_NoChange= FirstRow.getAs("__NoChange")
      
      
      
      LocalControl.NewStep(s"$TypeOfCall: Validating Insert & Update")
      var DQ_Error: String = ""
      var Error_Number: Integer = null
      if (this._NumRows_Total == this._NumRows_New)
        DQ_Error = "" //Doesn't have error, first run
      else if (this._DQ_MaxNewRecords_Num != null && this._DQ_MaxNewRecords_Num > 0 && this._NumRows_New > this._DQ_MaxNewRecords_Num){
        DQ_Error = s"huemul_Table Error: DQ MDM Error: N° New Rows (${this._NumRows_New}) exceeds max defined (${this._DQ_MaxNewRecords_Num}) "
        Error_Number = 1005
      }
      else if (this._DQ_MaxNewRecords_Perc != null && this._DQ_MaxNewRecords_Perc > Decimal.apply(0) && (Decimal.apply(this._NumRows_New) / Decimal.apply(this._NumRows_Total)) > this._DQ_MaxNewRecords_Perc) {
        DQ_Error = s"huemul_Table Error: DQ MDM Error: % New Rows (${Decimal.apply(this._NumRows_New) / Decimal.apply(this._NumRows_Total)}) exceeds % max defined (${this._DQ_MaxNewRecords_Perc}) "
        Error_Number = 1006
      }
  
      if (DQ_Error != "")
        raiseError(DQ_Error, Error_Number)
        
      DQ_ReferenceData.unpersist()
    }
  }
  
  private def getClassAndPackage: HuemulAuthorizationPair = {
    var Invoker: StackTraceElement = null
    var InvokerName: String = null
    var ClassNameInvoker: String = null
    var PackageNameInvoker: String = null
    
    var numClassBack: Int = 1
    do {
      //repeat until find className different to myself
      numClassBack += 1
      Invoker = new Exception().getStackTrace()(numClassBack)
      InvokerName = Invoker.getClassName.replace("$", "")
      
      val ArrayResult = InvokerName.split('.')
      
      ClassNameInvoker = ArrayResult(ArrayResult.length-1)
      PackageNameInvoker = InvokerName.replace(".".concat(ClassNameInvoker), "")
      
      //println(s"${numClassBack}, ${ClassNameInvoker.toLowerCase()} == ${this.getClass.getSimpleName.toLowerCase()}")
      if (ClassNameInvoker.toLowerCase() == this.getClass.getSimpleName.toLowerCase() || ClassNameInvoker.toLowerCase() == "huemul_table") {
        Invoker = null
      }
        
    } while (Invoker == null) 
    
     new HuemulAuthorizationPair(ClassNameInvoker,PackageNameInvoker)
  }
  
  def executeFull(newAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
    var Result: Boolean = false
    val whoExecute = getClassAndPackage
    if (this._WhoCanRun_executeFull.HasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      Result = this.executeSave(newAlias, IsInsert = true, IsUpdate = true, IsDelete = true, IsSelectiveUpdate = false, null, storageLevelOfDF, RegisterOnlyInsertInDQ = false)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeFull in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}", 1008)
      
    }
    
     Result
  }
  
  //2.1: compatibility with previous version
  def executeOnlyInsert(newAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
     executeOnlyInsert(newAlias, storageLevelOfDF, RegisterOnlyInsertInDQ = false)
  }
  
  //2.1: add RegisterOnlyInsertInDQ params
  def executeOnlyInsert(newAlias: String, RegisterOnlyInsertInDQ: Boolean): Boolean = {
     executeOnlyInsert(newAlias, null, RegisterOnlyInsertInDQ)
  }
  
  //2.1: introduce RegisterOnlyInsertInDQ
  def executeOnlyInsert(newAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel, RegisterOnlyInsertInDQ: Boolean): Boolean = {
    var Result: Boolean = false
    if (this._TableType == HuemulTypeTables.Transaction)
      raiseError("huemul_Table Error: DoOnlyInserthuemul is not available for Transaction Tables",1009)

    val whoExecute = getClassAndPackage
    if (this._WhoCanRun_executeOnlyInsert.HasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      Result = this.executeSave(newAlias, IsInsert = true, IsUpdate = false, IsDelete = false, IsSelectiveUpdate = false, null, storageLevelOfDF, RegisterOnlyInsertInDQ = RegisterOnlyInsertInDQ)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeOnlyInsert in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}", 1010)
    }
         
     Result
  }
  
  def executeOnlyUpdate(newAlias: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
    var Result: Boolean = false
    if (this._TableType == HuemulTypeTables.Transaction)
      raiseError("huemul_Table Error: DoOnlyUpdatehuemul is not available for Transaction Tables", 1011)
      
    val whoExecute = getClassAndPackage
    if (this._WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      Result = this.executeSave(newAlias, IsInsert = false, IsUpdate = true, IsDelete = false, IsSelectiveUpdate = false, null, storageLevelOfDF, RegisterOnlyInsertInDQ = false)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeOnlyUpdate in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}",1012)
    }
    
     Result
        
  }
  
  def executeSelectiveUpdate(newAlias: String, PartitionValueForSelectiveUpdate: String, storageLevelOfDF: org.apache.spark.storage.StorageLevel = null): Boolean = {
    var Result: Boolean = false
    val newValue: ArrayBuffer[String] = new ArrayBuffer[String]()
    newValue.append(PartitionValueForSelectiveUpdate)

    val whoExecute = getClassAndPackage
    if (this._WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      Result = executeSelectiveUpdate(newAlias, newValue, storageLevelOfDF)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeSelectiveUpdate in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}",1012)
    }

     Result
  }
  
  def executeSelectiveUpdate(newAlias: String, PartitionValueForSelectiveUpdate: ArrayBuffer[String], storageLevelOfDF: org.apache.spark.storage.StorageLevel ): Boolean = {
    var Result: Boolean = false
      
    val whoExecute = getClassAndPackage
    if (this._WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName, whoExecute.getLocalPackageName))
      Result = this.executeSave(newAlias, IsInsert = false, IsUpdate = false, IsDelete = false, IsSelectiveUpdate = true, PartitionValueForSelectiveUpdate, storageLevelOfDF, RegisterOnlyInsertInDQ = false)
    else {
      raiseError(s"huemul_Table Error: Don't have access to executeSelectiveUpdate in ${this.getClass.getSimpleName.replace("$", "")}  : Class: ${whoExecute.getLocalClassName}, Package: ${whoExecute.getLocalPackageName}",1012)
    }
    
     Result
        
  }
  
  private def CompareSchema(Columns: ArrayBuffer[HuemulColumns], Schema: StructType): String = {
    var Errores: String = ""
    Columns.foreach { x => 
      if (huemulBigDataGov.HasName(x.getMappedName)) {
        //val ColumnNames = if (UseAliasColumnName) x.getMappedName else x.getMyName()
        val ColumnNames = x.getMyName(getStorageType)
        val _dataType  = x.getDataTypeDeploy(huemulBigDataGov.GlobalSettings.getBigDataProvider, this.getStorageType)
        
        val ColumnInSchema = Schema.filter { y => y.name.toLowerCase() == ColumnNames.toLowerCase() }
        if (ColumnInSchema == null || ColumnInSchema.isEmpty)
          raiseError(s"huemul_Table Error: column missing in Schema $ColumnNames", 1038)
        if (ColumnInSchema.length != 1)
          raiseError(s"huemul_Table Error: multiples columns found in Schema with name $ColumnNames", 1039)
        
          val dataType = ColumnInSchema.head.dataType
        if (dataType != _dataType) {
          Errores = Errores.concat(s"Error Column $ColumnNames, Requiered: ${_dataType}, actual: $dataType  \n")
        }
      }
    }
    
     Errores
  }
  
  /**
   Save Full data: <br>
   Master & Reference: update and Insert
   Transactional: Delete and Insert new data
   */
  private def executeSave(AliasNewData: String, IsInsert: Boolean, IsUpdate: Boolean, IsDelete: Boolean, IsSelectiveUpdate: Boolean, PartitionValueForSelectiveUpdate: ArrayBuffer[String], storageLevelOfDF: org.apache.spark.storage.StorageLevel, RegisterOnlyInsertInDQ: Boolean): Boolean = {
    if (!this.DefinitionIsClose)
      this.raiseError(s"huemul_Table Error: MUST call ApplyTableDefinition ${this.TableName}", 1048)
    
    _NumRows_Excluded = 0L
      
    var partitionValueString = ""
    if (PartitionValueForSelectiveUpdate != null)
      partitionValueString = PartitionValueForSelectiveUpdate.mkString(", ")

    val LocalControl = new HuemulControl(huemulBigDataGov, Control ,HuemulTypeFrequency.ANY_MOMENT, false )
    LocalControl.AddParamInformation("AliasNewData", AliasNewData)
    LocalControl.AddParamInformation("IsInsert", IsInsert.toString)
    LocalControl.AddParamInformation("IsUpdate", IsUpdate.toString)
    LocalControl.AddParamInformation("IsDelete", IsDelete.toString)
    LocalControl.AddParamInformation("IsSelectiveUpdate", IsSelectiveUpdate.toString)
    LocalControl.AddParamInformation("PartitionValueForSelectiveUpdate", partitionValueString)
    
    var result : Boolean = true
    var ErrorCode: Integer = null
    
    try {
      val OnlyInsert: Boolean = IsInsert && !IsUpdate
      
      //Compare schemas
      if (!autoCast) {
        LocalControl.NewStep("Compare Schema")
        val ResultCompareSchema = CompareSchema(this.getColumns, this.DataFramehuemul.DataFrame.schema)
        if (ResultCompareSchema != "") {
          result = false
          ErrorCode = 1013
          raiseError(s"huemul_Table Error: User Error: incorrect DataType: \n$ResultCompareSchema", ErrorCode)
        }
      }
    
      //do work
      DF_MDM_Dohuemul(LocalControl, AliasNewData,IsInsert, IsUpdate, IsDelete, IsSelectiveUpdate, PartitionValueForSelectiveUpdate, storageLevelOfDF)
  
      
   //WARNING_EXCLUDE (starting 2.1)
      //DataQuality by Columns
      LocalControl.NewStep("Start DataQuality WARNING_EXCLUDE")
      DF_DataQualityMasterAuto(IsSelectiveUpdate, LocalControl, warning_exclude = true)
      //Foreing Keys by Columns
      LocalControl.NewStep("Start ForeingKey WARNING_EXCLUDE ")
      DF_ForeingKeyMasterAuto(warning_exclude = true, LocalControl)
      
      //from 2.1: exclude_warnings, update DataFramehuemul 
      excludeRows(LocalControl)
   
      //from 2.2 --> raiserror if DF is empty
      if (this.DataFramehuemul.getNumRows == 0) {
        this.raiseError(s"huemul_Table Error: Dataframe is empty, nothing to save", 1062)
      }
      
    //REST OF RULES (DIFFERENT FROM WARNING_EXCLUDE)
      //DataQuality by Columns
      LocalControl.NewStep("Start DataQuality ERROR AND WARNING")
      val DQResult = DF_DataQualityMasterAuto(IsSelectiveUpdate, LocalControl, warning_exclude = false)
      //Foreing Keys by Columns
      LocalControl.NewStep("Start ForeingKey ERROR AND WARNING ")
      val FKResult = DF_ForeingKeyMasterAuto(warning_exclude = false, LocalControl)
      
      
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
      val ResultCompareSchemaFinal = CompareSchema(this.getColumns, this.DataFramehuemul.DataFrame.schema)
      if (ResultCompareSchemaFinal != "") {
        ErrorCode = 1014
        raiseError(s"huemul_Table Error: User Error: incorrect DataType: \n$ResultCompareSchemaFinal",ErrorCode)
      }
      
      //Create table persistent
      if (huemulBigDataGov.DebugMode){
        huemulBigDataGov.logMessageDebug(s"Saving ${internalGetTable(HuemulTypeInternalTableType.Normal)} Table with params: ")
        huemulBigDataGov.logMessageDebug(s"$getPartitionField field for partitioning table")
        huemulBigDataGov.logMessageDebug(s"$getFullNameWithPath path")
      }
      
      LocalControl.NewStep("Start Save ")                
      if (savePersist(LocalControl, DataFramehuemul, OnlyInsert, IsSelectiveUpdate, RegisterOnlyInsertInDQ )){
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
        
        LocalControl.Control_Error.GetError(e, getClass.getSimpleName, ErrorCode)
        LocalControl.FinishProcessError()
    }
    
     result
  }

  
  private def excludeRows(LocalControl: HuemulControl): Unit = {
    //Add from 2.1: exclude DQ from WARNING_EXCLUDE
    val warning_Exclude_detail = Control.control_getDQResultForWarningExclude()
    if (warning_Exclude_detail.nonEmpty) {

      //get DQ with WARNING_EXCLUDE > 0 rows
      val dq_id_list: String = warning_Exclude_detail.mkString(",")

      val _tableNameDQ: String = internalGetTable(HuemulTypeInternalTableType.DQ)
      
      //get PK Details
      LocalControl.NewStep("WARNING_EXCLUDE: Get details")
      val DQ_Det = huemulBigDataGov.DF_ExecuteQuery("__DQ_Det", s"""SELECT DISTINCT mdm_hash FROM ${_tableNameDQ} WHERE dq_control_id="${Control.Control_Id}" AND dq_dq_id in ($dq_id_list)""")
      //Broadcast
      _NumRows_Excluded = DQ_Det.count()
      var apply_broadcast: String = ""
      if (_NumRows_Excluded < 50000)
        apply_broadcast = "/*+ BROADCAST(EXCLUDE) */"
        
      //exclude
      LocalControl.NewStep(s"WARNING_EXCLUDE: EXCLUDE rows")
      huemulBigDataGov.logMessageInfo(s"WARNING_EXCLUDE: ${_NumRows_Excluded} rows excluded")
      DataFramehuemul.DF_from_SQL(DataFramehuemul.Alias, s"SELECT $apply_broadcast PK.* FROM ${DataFramehuemul.Alias} PK LEFT JOIN __DQ_Det EXCLUDE ON PK.mdm_hash = EXCLUDE.mdm_hash WHERE EXCLUDE.mdm_hash IS NULL ")

      //from 2.6
      DataFramehuemul.DataFrame.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

      if (_TableType == HuemulTypeTables.Transaction)
        this.UpdateStatistics(LocalControl, "WARNING_EXCLUDE", null)
      else 
        this.UpdateStatistics(LocalControl, "WARNING_EXCLUDE", DataFramehuemul.Alias)
    }
    
    
  }
  
  /**
   * Save data to disk
   */
  private def savePersist(LocalControl: HuemulControl, DF_huemul: HuemulDataFrame, OnlyInsert: Boolean, IsSelectiveUpdate: Boolean, RegisterOnlyInsertInDQ:Boolean): Boolean = {
    var DF_Final = DF_huemul.DataFrame
    var Result: Boolean = true
    
    //from 2.1 --> change position of this code, to get after WARNING_EXCLUDE
    //Add from 2.0: save Old Value Trace
    //CREATE NEW DATAFRAME WITH MDM OLD VALUE FULL TRACE
    if (huemulBigDataGov.GlobalSettings.MDM_SaveOldValueTrace) {
      LocalControl.NewStep("Ref & Master: MDM Old Value Full Trace")
      OldValueTrace_save("__FullJoin", huemulBigDataGov.ProcessNameCall, LocalControl)
    }

    
    if (this._TableType == HuemulTypeTables.Reference || this._TableType == HuemulTypeTables.Master || IsSelectiveUpdate) {
      LocalControl.NewStep("Save: Drop ActionType column")
   
      if (OnlyInsert && !IsSelectiveUpdate) {
        DF_Final = DF_Final.where("___ActionType__ = 'NEW'") 
      } else if (this.getStorageType == HuemulTypeStorageType.HBASE){
        //exclude "EQUAL" to improve write performance on HBase
        DF_Final = DF_Final.where("___ActionType__ != 'EQUAL'")
      }
      
      DF_Final = DF_Final.drop("___ActionType__").drop("SameHashKey") //from 2.1: always this columns exist on reference and master tables
      if (OnlyInsert) {
      
        if (RegisterOnlyInsertInDQ) {
          val dt_start = huemulBigDataGov.getCurrentDateTimeJava
          DF_Final.createOrReplaceTempView("__tempnewtodq")
          val numRowsDQ = DF_Final.count()
          val dt_end = huemulBigDataGov.getCurrentDateTimeJava
          val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
          
          //from 2.1: insertOnlyNew as DQ issue 
          val Values = new HuemulDQRecord(huemulBigDataGov)
          Values.Table_Name =TableName
          Values.BBDD_Name =this.getDataBase(this._DataBase)
          Values.DF_Alias = ""
          Values.ColumnName = null
          Values.DQ_Name =s"OnlyInsert"
          Values.DQ_Description =s"new values inserted to master or reference table"
          Values.DQ_QueryLevel = HuemulTypeDQQueryLevel.Row // IsAggregate =false
          Values.DQ_Notification = HuemulTypeDQNotification.WARNING// RaiseError =true
          Values.DQ_SQLFormula =""
          Values.DQ_ErrorCode = 1055
          Values.DQ_toleranceError_Rows = 0L
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
                          , haveField = false             //haveField
                          , null              //fieldname
                          , Values.DQ_Id
                          , HuemulTypeDQNotification.WARNING //dq_error_notification
                          , Values.DQ_ErrorCode //error_code
                          , s"(1055) huemul_Table Warning: new values inserted to master or reference table"// dq_error_description
                          ,LocalControl
                          )
        }
      }
      
    }
      
    
    try {
      if (getPartitionList.length == 0 || this._TableType == HuemulTypeTables.Reference || this._TableType == HuemulTypeTables.Master ){
        
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
        
       
        if (this.getStorageType == HuemulTypeStorageType.HBASE){
          if (DF_Final.count() > 0) {
            val huemulDriver = new HuemulTableConnector(huemulBigDataGov, LocalControl)
            huemulDriver.saveToHBase(DF_Final
                                    ,getHBaseNamespace(HuemulTypeInternalTableType.Normal)
                                    ,getHBaseTableName(HuemulTypeInternalTableType.Normal)
                                    ,this.getNumPartitions //numPartitions
                                    ,OnlyInsert
                                    ,if (_numPKColumns == 1) _HBase_PKColumn else hs_rowKeyInternalName2 //PKName
                                    ,getALLDeclaredFields_forHBase(getALLDeclaredFields())
                                    )
          }
        }
        else {
          if (huemulBigDataGov.DebugMode) DF_Final.printSchema()
          if (getPartitionList.length == 0) //save without partitions
            DF_Final.write.mode(localSaveMode).format(_getSaveFormat(this.getStorageType)).save(getFullNameWithPath)
          else  //save with partitions
            DF_Final.write.mode(localSaveMode).format(_getSaveFormat(this.getStorageType)).partitionBy(getPartitionListForSave:_*).save(getFullNameWithPath)
        }
        
        
        //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)       
        //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath), new FsPermission("770"))
        LocalControl.NewStep("Register Master Information ")
        Control.RegisterMASTER_CREATE_Use(this)
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
          this.PartitionValue = new ArrayBuffer[String]
          //get partitions value to drop
          LocalControl.NewStep("Save: Get partitions values to delete (distinct)")
          var DFDistinct_step = DF_Final.select(dropPartitions.map(name => col(name.getMyName(getStorageType))): _*).distinct()

          //add columns cast
          dropPartitions.foreach { x => 
            DFDistinct_step = DFDistinct_step.withColumn( x.getMyName(getStorageType), DF_Final.col(x.getMyName(getStorageType)).cast(StringType))
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
            val numDistinctValues = DFDistinct_step.select(x.getMyName(getStorageType)).distinct().count()
            if (x.getPartitionOneValuePerProcess && numDistinctValues > 1) {
              raiseError(s"huemul_Table Error: N° values in partition wrong for column ${x.getMyName(getStorageType)}!, expected: 1, real: $numDistinctValues",1015)
            }
          }

            //get data values
          DFDistinct.foreach { xData => 
            var pathToDelete: String = ""
            val whereToDelete: ArrayBuffer[String] = new ArrayBuffer[String]
            
            //get columns name in order to create path to delete
            dropPartitions.foreach { xPartitions =>
                val colPartitionName = huemulBigDataGov.getCaseType(this.getStorageType,xPartitions.getMyName(getStorageType))
                val colData = xData.getAs[String](colPartitionName)
                pathToDelete += s"/$colPartitionName=$colData"
                whereToDelete.append(s"$colPartitionName = '$colData'")
            }
            this.PartitionValue.append(pathToDelete)
            
            LocalControl.NewStep(s"Save: deleting partition $pathToDelete ")
            pathToDelete = getFullNameWithPath.concat(pathToDelete)

            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"deleting $pathToDelete")
            //create fileSystema link
            val FullPath = new org.apache.hadoop.fs.Path(pathToDelete)
            val fs = FullPath.getFileSystem(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)   
            
            if (this.getStorageType == HuemulTypeStorageType.DELTA) {
              //FROM 2.4 --> NEW DELETE FOR DATABRICKS             
              val FullPath = new org.apache.hadoop.fs.Path(getFullNameWithPath)
              if (fs.exists(FullPath)) {
                val strSQL_delete: String = s"DELETE FROM delta.`$getFullNameWithPath` WHERE ${whereToDelete.mkString(" and ")} "
                if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(strSQL_delete)
                huemulBigDataGov.spark.sql(strSQL_delete)
              }
            } else {      
              fs.delete(FullPath, true)
            }
          }
        }

        LocalControl.NewStep("Save: get Partition Detailes counts")
        //get statistics 2.6.1
        val groupNames: String = getPartitionList.map(name => col(name.getMyName(getStorageType))).mkString(",")
        var DFDistinctControl_p1 = huemulBigDataGov.spark.sql(s"SELECT $groupNames,cast(count(1) as Long) as numRows FROM ${DF_huemul.Alias} group by $groupNames")

        //cast to string
        getPartitionList.foreach { x =>
          DFDistinctControl_p1 = DFDistinctControl_p1.withColumn( x.getMyName(getStorageType), DF_Final.col(x.getMyName(getStorageType)).cast(StringType))
        }
        val DFDistinctControl = DFDistinctControl_p1.collect()

        LocalControl.NewStep("Save: get Partition Details: save numRows to control model")
        //for each row, save to tableUSes control model
        DFDistinctControl.foreach { xData =>
          var pathToSave: String = ""

          //get columns name in order to create path to delete
          getPartitionList.foreach { xPartitions =>
            val colPartitionName = xPartitions.getMyName(getStorageType)
            val colData = xData.getAs[String](colPartitionName)
            pathToSave += s"/${colPartitionName.toLowerCase()}=$colData"
          }
          val numRows:java.lang.Long = xData.getAs[java.lang.Long]("numRows")

          Control.RegisterMASTER_CREATE_UsePartition(this,pathToSave ,numRows )
        }

        if (this.getNumPartitions > 0) {
          LocalControl.NewStep("Save: Set num FileParts")
          DF_Final = DF_Final.repartition(this.getNumPartitions)
        }
          
        LocalControl.NewStep("Save: OverWrite partition with new data")
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: $getFullNameWithPath ")
          
        DF_Final.write.mode(SaveMode.Append).format(_getSaveFormat(this.getStorageType)).partitionBy(getPartitionListForSave:_*).save(getFullNameWithPath)

        //get staticstics from 2.6.1
        DF_Final.select(dropPartitions.map(name => col(name.getMyName(getStorageType))): _*).distinct()

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
        runSQL_dropExternalTable(HuemulTypeInternalTableType.Normal, onlyHBASE = false)
      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }
     
      
        
      try {
        //create table
        LocalControl.NewStep("Save: Create Table in Hive Metadata")
        CreateTableScript = DF_CreateTableScript() 
              
        if (getStorageType == HuemulTypeStorageType.HBASE) {
          //new from 2.3
          runSQLexternalTable(CreateTableScript, onlyHBASE = true)
          
        } else{
          //new from 2.3
          runSQLexternalTable(CreateTableScript, onlyHBASE = false)
        }
        
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableName : String = internalGetTable(HuemulTypeInternalTableType.Normal)
        if (getPartitionList.length > 0) {
          if (this.getStorageType != HuemulTypeStorageType.DELTA) {
            LocalControl.NewStep("Save: Repair Hive Metadata")
            val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableName}"
            if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(_refreshTable)
            //new from 2.3
            if (getStorageType == HuemulTypeStorageType.HBASE) {
              runSQLexternalTable(_refreshTable, onlyHBASE = true)
            } else {
              runSQLexternalTable(_refreshTable, onlyHBASE = false)
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
    DF_huemul.setDataFrame(DF_Final, DF_huemul.Alias, SaveInTemp = false)
    
    //from 2.0: update dq and ovt used
    Control.RegisterMASTER_UPDATE_isused(this)
    
      
      
     Result
    
  }
    
  
  
  /**
   * Save DQ Result data to disk
   */
  private def savePersist_OldValueTrace(LocalControl: HuemulControl, DF: DataFrame): Boolean = {
    val DF_Final = DF
    var Result: Boolean = true
    this._table_ovt_isused = 1
      
    try {      
      LocalControl.NewStep("Save: OldVT Result: Saving Old Value Trace result")
      if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: $getFullNameWithPath_OldValueTrace ")
      if (getStorageType_OldValueTrace == HuemulTypeStorageType.PARQUET || getStorageType_OldValueTrace == HuemulTypeStorageType.ORC ||
          getStorageType_OldValueTrace == HuemulTypeStorageType.DELTA   || getStorageType_OldValueTrace == HuemulTypeStorageType.AVRO){
        DF_Final.write.mode(SaveMode.Append).partitionBy(getNameForMDM_columnName).format(_getSaveFormat(getStorageType_OldValueTrace)).save(getFullNameWithPath_OldValueTrace)
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).partitionBy(getNameForMDM_columnName).format(getStorageType_OldValueTrace.toString()).save(getFullNameWithPath_OldValueTrace)
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).format(_StorageType_OldValueTrace).save(getFullNameWithPath_OldValueTrace)
      }
      else
        DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).option("delimiter", "\t").option("emptyValue", "").option("treatEmptyValuesAsNulls", "false").option("nullValue", "null").format(_getSaveFormat(getStorageType_OldValueTrace)).save(getFullNameWithPath_OldValueTrace)
      
    } catch {
      case e: Exception => 
        this.Error_isError = true
        this.Error_Text = s"huemul_Table OldVT Error: write in disk failed for Old Value Trace result. ${e.getMessage}"
        this.Error_Code = 1052
        Result = false
        LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
    }
    
    if (Result) {
      val sqlDrop01 = s"drop table if exists ${internalGetTable(HuemulTypeInternalTableType.OldValueTrace)}"
      LocalControl.NewStep("Save: OldVT Result: Drop Hive table Def")
      if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
      try {
        //new from 2.3
        runSQL_dropExternalTable(HuemulTypeInternalTableType.OldValueTrace, onlyHBASE = false)
      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }
     
      
        
      try {
        //create table
        LocalControl.NewStep("Save: OldVT Result: Create Table in Hive Metadata")
        val lscript = DF_CreateTable_OldValueTrace_Script()       
        //new from 2.3
        runSQLexternalTable(lscript, onlyHBASE = false)
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableNameOldValueTrace: String = internalGetTable(HuemulTypeInternalTableType.OldValueTrace)
        
        if (this.getStorageType_OldValueTrace != HuemulTypeStorageType.DELTA) {
          LocalControl.NewStep("Save: OldVT Result: Repair Hive Metadata")
          val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableNameOldValueTrace}"
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"RMSCK REPAIR TABLE ${_tableNameOldValueTrace}")
          //new from 2.3
          runSQLexternalTable(_refreshTable, onlyHBASE = false)
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
      
     Result
    
  }
  
  /**
   * Save DQ Result data to disk
   */
  def savePersist_DQ(LocalControl: HuemulControl, DF: DataFrame): Boolean = {
    val DF_Final = DF
    var Result: Boolean = true
    this._table_dq_isused = 1
      
    try {      
      LocalControl.NewStep("Save DQ Result: Saving new DQ result")
      //from 2.2 --> add HBase storage (from table definition) --> NOT SUPPORTED
      if (this.getStorageType_DQResult == HuemulTypeStorageType.HBASE) {
        val huemulDriver = new HuemulTableConnector(huemulBigDataGov, LocalControl)
        huemulDriver.saveToHBase(DF_Final
                                  ,getHBaseNamespace(HuemulTypeInternalTableType.DQ)
                                  ,getHBaseTableName(HuemulTypeInternalTableType.DQ)
                                  ,this.getNumPartitions //numPartitions
                                  ,isOnlyInsert = true
                                  ,if (_numPKColumns == 1) _HBase_PKColumn else hs_rowKeyInternalName2 //PKName
                                  ,getALLDeclaredFields_forHBase(getALLDeclaredFields())
                                  )      
      } else {
        if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: $getFullNameWithPath_DQ ")
        //DF_Final.coalesce(numPartitionsForDQFiles).write.mode(SaveMode.Append).format(this.getStorageType_DQResult.toString()).partitionBy("dq_control_id").save(getFullNameWithPath_DQ)
        DF_Final.write.mode(SaveMode.Append).format(_getSaveFormat(this.getStorageType_DQResult)).partitionBy("dq_control_id").save(getFullNameWithPath_DQ)
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
      val sqlDrop01 = s"drop table if exists ${internalGetTable(HuemulTypeInternalTableType.DQ)}"
      LocalControl.NewStep("Save: Drop Hive table Def")
      if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
      try {
        //new from 2.3
        runSQL_dropExternalTable(HuemulTypeInternalTableType.DQ, onlyHBASE = false)
          
      } catch {
        case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }
     
      
        
      try {
        //create table
        LocalControl.NewStep("Save: Create Table in Hive Metadata")
        val lscript = DF_CreateTable_DQ_Script()
       
        //new from 2.3
        runSQLexternalTable(lscript, onlyHBASE = false)
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        val _tableNameDQ: String = internalGetTable(HuemulTypeInternalTableType.DQ)
        
        if (this.getStorageType_DQResult != HuemulTypeStorageType.DELTA) {
          val _refreshTable: String = s"MSCK REPAIR TABLE ${_tableNameDQ}"
          LocalControl.NewStep("Save: Repair Hive Metadata")
          if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(_refreshTable)
          //new from 2.3
          runSQLexternalTable(_refreshTable, onlyHBASE = false)
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
      
     Result
    
  }
  
  /**
   * Return an Empty table with all columns
   */
  def getEmptyTable(Alias: String): DataFrame = {
    val Schema = getSchema
    val SchemaForEmpty = StructType(Schema.map { x => StructField(x.name, x.dataType, x.nullable) })
    val EmptyRDD = huemulBigDataGov.spark.sparkContext.emptyRDD[Row]
    val EmptyDF = huemulBigDataGov.spark.createDataFrame(EmptyRDD, SchemaForEmpty)
    EmptyDF.createOrReplaceTempView(Alias)
    
     EmptyDF
  }
  
  def DF_from_SQL(Alias: String, sql: String, SaveInTemp: Boolean = true, NumPartitions: Integer = null) {
    this.DataFramehuemul.DF_from_SQL(Alias, sql, SaveInTemp, NumPartitions) 
  }
  
  
  def DF_from_DF(DFFrom: DataFrame, AliasFrom: String, AliasTo: String, SaveInTemp: Boolean = true) {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava
    this.DataFramehuemul.setDataFrame(DFFrom, AliasTo, SaveInTemp)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava
    
    huemulBigDataGov.DF_SaveLineage(AliasTo
                                 , s"SELECT * FROM $AliasFrom" //sql
                                 , dt_start
                                 , dt_end
                                 , Control
                                 , null //FinalTable
                                 , isQuery = false //isQuery
                                 , isReferenced = true //isReferenced
                                 )
  }
  
  def DF_from_RAW(dataLake_RAW: HuemulDataLake, AliasTo: String) {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava
    this.DataFramehuemul.setDataFrame(dataLake_RAW.DataFramehuemul.DataFrame , AliasTo, huemulBigDataGov.DebugMode)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava
    
    huemulBigDataGov.DF_SaveLineage(AliasTo
                                 , s"SELECT * FROM ${dataLake_RAW.DataFramehuemul.Alias}" //sql
                                 , dt_start
                                 , dt_end
                                 , Control
                                 , null //FinalTable
                                 , isQuery = false //isQuery
                                 , isReferenced = true //isReferenced
                                 )
  }
  
  private def runSQL_dropExternalTable(tableType: HuemulTypeInternalTableType, onlyHBASE: Boolean) {
    //new from 2.3
    val sqlDrop01 = s"drop table if exists ${internalGetTable(tableType)}"
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
      
    if (onlyHBASE) {
      /**** D R O P   F O R   H B A S E ******/
      
      //FOR SPARK--> NOT SUPPORTED FOR SPARK
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_SPARK.getActiveForHBASE){
        //comment two next lines: get error when doesn't have serDe
        //val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(databaseName).collect()
        //  if (TablesListFromHive.filter { x => x.name.toLowerCase() == tableName.toLowerCase()  }.length > 0) 
            huemulBigDataGov.spark.sql(sqlDrop01)
      }
      
      //FOR HIVE
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getActiveForHBASE)
        huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov).ExecuteJDBC_NoResulSet(sqlDrop01)
        
      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HWC.getActiveForHBASE)
        huemulBigDataGov.getHive_HWC.execute_NoResulSet(sqlDrop01)
    } else {
      /**** D R O P   F O R    O T H E R S ******/
      
      //FOR SPARK
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_SPARK.getActive) {
        //comment two next lines: get error when doesn't have serDe
        //val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(databaseName).collect()
        //  if (TablesListFromHive.filter { x => x.name.toLowerCase() == tableName.toLowerCase()  }.length > 0) 
            huemulBigDataGov.spark.sql(sqlDrop01)
      }
      
      //FOR HIVE
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getActive)
        huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov).ExecuteJDBC_NoResulSet(sqlDrop01)
        
      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HWC.getActive)
        huemulBigDataGov.getHive_HWC.execute_NoResulSet(sqlDrop01)
    }
    
  }
  
  
  private def runSQLexternalTable(sqlSentence:String, onlyHBASE: Boolean) {
    //new from 2.3
    if (onlyHBASE) {
      /**** D R O P   F O R   H B A S E ******/
      
      //FOR SPARK --> NOT SUPPORTED FOR SPARK
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_SPARK.getActiveForHBASE)
        huemulBigDataGov.spark.sql(sqlSentence)
      
      //FOR HIVE
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getActiveForHBASE)
          huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov).ExecuteJDBC_NoResulSet(sqlSentence)
          
      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HWC.getActiveForHBASE)
          huemulBigDataGov.getHive_HWC.execute_NoResulSet(sqlSentence)
    } else {
      /**** D R O P   F O R   O T H E R ******/
      
      //FOR SPARK
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_SPARK.getActive)
        huemulBigDataGov.spark.sql(sqlSentence)
      
      //FOR HIVE
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getActive)
        huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HIVE.getJDBC_connection(huemulBigDataGov).ExecuteJDBC_NoResulSet(sqlSentence)
    
      //FOR HIVE WAREHOUSE CONNECTOR
      if (huemulBigDataGov.GlobalSettings.externalBBDD_conf.Using_HWC.getActive)
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

