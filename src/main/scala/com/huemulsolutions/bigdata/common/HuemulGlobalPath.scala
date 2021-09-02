package com.huemulsolutions.bigdata.common

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.common.HuemulTypeBigDataProvider._




class HuemulGlobalPath() extends Serializable {
    /**example: "prod, desa, qa"      
     **/
    var globalEnvironments: String = "prod, desa, qa" //prod: PRODUCCION, desa: DESARROLLO, qa: ambientes previos a producción
    var impalaEnabled: Boolean = false

    //from 2.6 --> reduce validation to use in notebooks
  /**set validation level of globalSettings
   *
   */
  private var validationLevel: String = "FULL"
  def setValidationLight(): Unit = {
    validationLevel = "LOW"
  }
  def getValidationLevel: String = validationLevel

  var controlDriver: String = "org.postgresql.Driver"
  val controlSetting: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()


  val impalaSetting: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()


  //from 2.3 --> add
  var externalBbddConf: HuemulExternalDb = new HuemulExternalDb()

  //RAW
  val rawSmallFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val rawBigFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //MASTER
  val masterSmallFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val masterBigFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val masterDataBase: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //DIM
  val dimSmallFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val dimBigFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val dimDataBase: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //REPORTING
  val reportingSmallFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val reportingBigFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val reportingDataBase: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //ANALYTICS
  val analyticsSmallFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val analyticsBigFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val analyticsDataBase: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //SANDBOX
  val sandboxSmallFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val sandboxBigFilesPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val sandboxDataBase: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //TEMPORAL
  val temporalPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //DQ_Error
  var dqSaveErrorDetails: Boolean = true
  val dqErrorPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val dqErrorDataBase: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //Save Old Values traceability
  var mdmSaveOldValueTrace: Boolean = true
  val mdmOldValueTracePath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()
  val mdmOldValueTraceDataBase: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //Save Master & Reference Backup
  var mdmSaveBackup: Boolean = true
  val mdmBackupPath: ArrayBuffer[HuemulKeyValuePath] = new ArrayBuffer[HuemulKeyValuePath]()

  //from 2.1
  //set > 1 to cache hive metadata
  var hiveHourToUpdateMetadata: Integer = 0

  //from 2.6.3
  private var _maxMinutesWaitInSingleton: Integer = 720
  def getMaxMinutesWaitInSingleton: Integer =  _maxMinutesWaitInSingleton

  /**
   * set max minutes to wait to singleton finish
   * if minutes to wait > waiting elapsed time, continue with execution
   * @param value max attempts
   */
  def setMaxMinutesWaitInSingleton(value: Integer): Unit = {
    _maxMinutesWaitInSingleton = value
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
  private var _bigDataProvider: HuemulTypeBigDataProvider = HuemulTypeBigDataProvider.None
  def getBigDataProvider: HuemulTypeBigDataProvider =  _bigDataProvider
  def setBigDataProvider(value: HuemulTypeBigDataProvider) {
    _bigDataProvider = value
  }

  //FROM 2.2
  //Add Hbase available
  private var _hBaseAvailable: Boolean = false
  def getHBaseAvailable: Boolean =  _hBaseAvailable
  def setHBaseAvailable() {
    _hBaseAvailable = true
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
  private var _avroFormat: String = "com.databricks.spark.avro"
  def getAvroFormat: String =   _avroFormat
  def setAvroFormat(value: String) {_avroFormat = value}

  private var _avroCompression: String = "snappy"
  def getAvroCompression: String =   _avroCompression
  def setAvroCompression(value: String) {_avroCompression = value}

  /**
   Returns true if path has value, otherwise return false
   */
  def validPath(division: ArrayBuffer[HuemulKeyValuePath], manualEnvironment: String): Boolean = {
    val Result = division.filter { x => x.getEnvironment == manualEnvironment }
     if (Result == null || Result.isEmpty) false else true
  }

  def getPath(huemulBigDataGov: HuemulBigDataGovernance, division: ArrayBuffer[HuemulKeyValuePath]): String = {
    val result = division.filter { x => x.getEnvironment == huemulBigDataGov.environment }
    if (result == null || result.isEmpty)
      sys.error(s"DAPI Error: environment '${huemulBigDataGov.environment}' must be set")

    result(0).value
  }

  /**
   * from 2.6.1, get user name for connections
   * @param huemulBigDataGov huemul instance
   * @param division setting to get value
   * @return
   */
  def getUserName(huemulBigDataGov: HuemulBigDataGovernance, division: ArrayBuffer[HuemulKeyValuePath]): String = {
    val result = division.filter { x => x.getEnvironment == huemulBigDataGov.environment }
    if (result == null || result.isEmpty)
      sys.error(s"DAPI Error: environment '${huemulBigDataGov.environment}' must be set")

    result(0).getUserName
  }

  /**
   * from 2.6.1, get password for connections
   * @param huemulBigDataGov huemul instance
   * @param division setting to get value
   * @return
   */
  def getPassword(huemulBigDataGov: HuemulBigDataGovernance, division: ArrayBuffer[HuemulKeyValuePath]): String = {
    val result = division.filter { x => x.getEnvironment == huemulBigDataGov.environment }
    if (result == null || result.isEmpty)
      sys.error(s"DAPI Error: environment '${huemulBigDataGov.environment}' must be set")

    result(0).getPassword
  }


  /**
   Get Path with manual environment setting
   */
  def getPath(Division: ArrayBuffer[HuemulKeyValuePath], manualEnvironment: String): String = {
    val Result = Division.filter { x => x.getEnvironment == manualEnvironment }
    if (Result == null || Result.isEmpty)
      sys.error(s"DAPI Error: environment '$manualEnvironment' must be set")

    Result(0).value
  }



  def getDataBase(huemulBigDataGov: HuemulBigDataGovernance, division: ArrayBuffer[HuemulKeyValuePath]): String = {
    getPath(huemulBigDataGov, division)
  }


  /**
   Get DataBase Name with manual environment setting
   */
  def getDataBase(Division: ArrayBuffer[HuemulKeyValuePath], manualEnvironment: String): String = {
    getPath(Division, manualEnvironment)
  }



  //TEMP
  def getDebugTempPath(huemulBigDataGov: HuemulBigDataGovernance, functionName: String, tableName: String): String = {
    s"${getPath(huemulBigDataGov, temporalPath)}$functionName/$tableName"
  }

  //to save DF directly from DF without DataGovernance
  def getPathForSaveTableWithoutDG(huemulBigDataGov: HuemulBigDataGovernance, globalPath: ArrayBuffer[HuemulKeyValuePath], localPathName: String, tableName: String): String = {
    s"${getPath(huemulBigDataGov, globalPath)}$localPathName/$tableName"
  }


  def raiseError(environment: String) {
    sys.error(s"error, environment does't exist: '$environment', must be '$globalEnvironments'  ")
  }
}