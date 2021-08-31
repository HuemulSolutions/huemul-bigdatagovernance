package com.huemulsolutions.bigdata.common

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.common.huemulType_bigDataProvider._




class huemul_GlobalPath() extends Serializable {
    /**example: "prod, desa, qa"      
     **/
    var GlobalEnvironments: String = "prod, desa, qa" //prod: PRODUCCION, desa: DESARROLLO, qa: ambientes previos a producción
    var ImpalaEnabled: Boolean = false

    //from 2.6 --> reduce validation to use in notebooks
  /**set validation level of globalSettings
   *
   */
  private var validationLevel: String = "FULL"
  def setValidationLight(): Unit = {
    validationLevel = "LOW"
  }
  def getValidationLevel: String = validationLevel

  var CONTROL_Driver: String = "org.postgresql.Driver"
  val CONTROL_Setting: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()


  val IMPALA_Setting: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()


  //from 2.3 --> add
  var externalBBDD_conf: huemul_ExternalDB = new huemul_ExternalDB()

  //RAW
  val RAW_SmallFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val RAW_BigFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //MASTER
  val MASTER_SmallFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val MASTER_BigFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val MASTER_DataBase: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //DIM
  val DIM_SmallFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val DIM_BigFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val DIM_DataBase: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //REPORTING
  val REPORTING_SmallFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val REPORTING_BigFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val REPORTING_DataBase: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //ANALYTICS
  val ANALYTICS_SmallFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val ANALYTICS_BigFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val ANALYTICS_DataBase: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //SANDBOX
  val SANDBOX_SmallFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val SANDBOX_BigFiles_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val SANDBOX_DataBase: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //TEMPORAL
  val TEMPORAL_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //DQ_Error
  var DQ_SaveErrorDetails: Boolean = true
  val DQError_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val DQError_DataBase: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //Save Old Values traceability
  var MDM_SaveOldValueTrace: Boolean = true
  val MDM_OldValueTrace_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
  val MDM_OldValueTrace_DataBase: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //Save Master & Reference Backup
  var MDM_SaveBackup: Boolean = true
  val MDM_Backup_Path: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()

  //from 2.1
  //set > 1 to cache hive metadata
  var HIVE_HourToUpdateMetadata: Integer = 0

  //from 2.6.3
  private var _MaxMinutesWaitInSingleton: Integer = 720
  def getMaxMinutesWaitInSingleton: Integer =  _MaxMinutesWaitInSingleton

  /**
   * set max minutes to wait to singleton finish
   * if minutes to wait > waiting elapsed time, continue with execution
   * @param value max attempts
   */
  def setMaxMinutesWaitInSingleton(value: Integer): Unit = {
    _MaxMinutesWaitInSingleton = value
  }

  //from 2.6.3
  private var _MaxAttemptApplicationInUse: Integer = 5
  def getMaxAttemptApplicationInUse: Integer = _MaxAttemptApplicationInUse
  /**
   * set max attempt to check if applicationId is in use
   * @param value max attempts
   */
  def setMaxAttemptApplicationInUse(value: Integer): Unit = {
    _MaxAttemptApplicationInUse = value
  }

  //from 2.4 --> bigData provider for technical configuration
  private var _bigDataProvider: huemulType_bigDataProvider = huemulType_bigDataProvider.None
  def getBigDataProvider: huemulType_bigDataProvider =  _bigDataProvider
  def setBigDataProvider(value: huemulType_bigDataProvider) {
    _bigDataProvider = value
  }

  //FROM 2.2
  //Add Hbase available
  private var _HBase_available: Boolean = false
  def getHBase_available: Boolean =  _HBase_available
  def setHBase_available() {
    _HBase_available = true
  }
  /*
  private var _HBase_formatTable: String = "org.apache.spark.sql.execution.datasources.hbase"
  def setHBase_formatTable(value: String) {
    _HBase_formatTable = value
  }
  def getHBase_formatTable(): String =  _HBase_formatTable
  *
  */

  //FROM 2.5
  //ADD AVRO SUPPORT
  private var _avro_format: String = "com.databricks.spark.avro"
  def getAVRO_format: String =   _avro_format
  def setAVRO_format(value: String) {_avro_format = value}

  private var _avro_compression: String = "snappy"
  def getAVRO_compression: String =   _avro_compression
  def setAVRO_compression(value: String) {_avro_compression = value}

  /**
   Returns true if path has value, otherwise return false
   */
  def validPath(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): Boolean = {
    val Result = Division.filter { x => x.environment == ManualEnvironment }
     if (Result == null || Result.isEmpty) false else true
  }

  def getPath(huemulBigDataGov: huemul_BigDataGovernance, Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    val Result = Division.filter { x => x.environment == huemulBigDataGov.Environment }
    if (Result == null || Result.isEmpty)
      sys.error(s"DAPI Error: environment '${huemulBigDataGov.Environment}' must be set")

    Result(0).Value
  }

  /**
   * from 2.6.1, get user name for connections
   * @param huemulBigDataGov huemul instance
   * @param Division setting to get value
   * @return
   */
  def getUserName(huemulBigDataGov: huemul_BigDataGovernance, Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    val Result = Division.filter { x => x.environment == huemulBigDataGov.Environment }
    if (Result == null || Result.isEmpty)
      sys.error(s"DAPI Error: environment '${huemulBigDataGov.Environment}' must be set")

    Result(0).getUserName
  }

  /**
   * from 2.6.1, get password for connections
   * @param huemulBigDataGov huemul instance
   * @param Division setting to get value
   * @return
   */
  def getPassword(huemulBigDataGov: huemul_BigDataGovernance, Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    val Result = Division.filter { x => x.environment == huemulBigDataGov.Environment }
    if (Result == null || Result.isEmpty)
      sys.error(s"DAPI Error: environment '${huemulBigDataGov.Environment}' must be set")

    Result(0).getPassword
  }


  /**
   Get Path with manual environment setting
   */
  def getPath(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    val Result = Division.filter { x => x.environment == ManualEnvironment }
    if (Result == null || Result.isEmpty)
      sys.error(s"DAPI Error: environment '$ManualEnvironment' must be set")

    Result(0).Value
  }



  def getDataBase(huemulBigDataGov: huemul_BigDataGovernance, Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    getPath(huemulBigDataGov, Division)
  }


  /**
   Get DataBase Name with manual environment setting
   */
  def getDataBase(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    getPath(Division, ManualEnvironment)
  }



  //TEMP
  def getDebugTempPath(huemulBigDataGov: huemul_BigDataGovernance, function_name: String, table_name: String): String = {
    s"${getPath(huemulBigDataGov, TEMPORAL_Path)}$function_name/$table_name"
  }

  //to save DF directly from DF without DataGovernance
  def getPathForSaveTableWithoutDG(huemulBigDataGov: huemul_BigDataGovernance,globalPath: ArrayBuffer[huemul_KeyValuePath], localPath_name: String, table_name: String): String = {
    s"${getPath(huemulBigDataGov, globalPath)}$localPath_name/$table_name"
  }


  def raiseError(Environment: String) {
    sys.error(s"error, environment does't exist: '$Environment', must be '$GlobalEnvironments'  ")
  }
}