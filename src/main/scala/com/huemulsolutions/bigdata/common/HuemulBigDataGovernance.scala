package com.huemulsolutions.bigdata.common

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.io._
import java.text.DateFormat
import java.util.Calendar
import java.util.TimeZone
import java.util.concurrent.ThreadLocalRandom
import java.text.SimpleDateFormat
import scala.io.{BufferedSource, Source}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.DecimalType
import com.huemulsolutions.bigdata.control.{HuemulControl, HuemulJdbcProperties}
import com.huemulsolutions.bigdata.tables.HuemulTable
import com.huemulsolutions.bigdata.sql_decode._
import org.apache.log4j.{Level, Logger}


        
      
/*
 * huemul_BigDataGovernance es la clase inicial de la librería huemul-bigdata
 * recibe los parámetros enviados por consola, y los traduce en parámetros de la librería y de cada módulo en particular
 * expone el método .spark
 * Environment: select path and databases to save data
 * Malla_Id: (optional) external Id from process scheduler
 * DebugMode: (optional) default false. show all information to screen (sql sentences, data example, df temporary, etc) 
 * HideLibQuery: (optional) default false. if true, show all sql sentences when DebugMode is true
 * SaveTempDF: (optional) default true. if DebuMode is true, save all DF to persistent HDFS
 */

/** huemul_BigDataGovernance es la clase inicial de la librería huemul-bigdata
 *
 *  @constructor create a new huemul BigData Governances instance
 *  @param appName nombre de la aplicación
 *  @param args argumentos de la aplicación
 *  @param customGlobalSettings configuración de rutas y Bases de datos
 *  @param localSparkSession(opcional) permite enviar una sesión de Spark ya iniciada.
 */
class HuemulBigDataGovernance(appName: String, args: Array[String], customGlobalSettings: HuemulGlobalPath, localSparkSession: SparkSession = null) extends Serializable  {
  val currentVersion: String = "3.0.0_2.12"
  //val globalSettings: HuemulGlobalPath = globalSettings
  val warehouseLocation: String = new File("spark-warehouse").getAbsolutePath
  //@transient lazy val log_info = org.apache.log4j.LogManager.getLogger(s"$appName [with huemul]")
  @transient lazy val log_info: Logger = org.apache.log4j.LogManager.getLogger(s"com.huemulsolutions")
  log_info.setLevel(Level.ALL)

  def globalSettings: HuemulGlobalPath = customGlobalSettings

  private val excludeWords:ArrayBuffer[String]  = new ArrayBuffer[String]()
  val huemulSqlDecode: HuemulSqlDecode = new HuemulSqlDecode(excludeWords,1)

  private var _ColumnsAndTables: ArrayBuffer[HuemulSqlTablesAndColumns] = new ArrayBuffer[HuemulSqlTablesAndColumns]()
  def getColumnsAndTables(OnlyRefreshTempTables: Boolean): ArrayBuffer[HuemulSqlTablesAndColumns] = {
    val inicio = this.getCurrentDateTimeJava

    //try to get hive metadata from cache
    //var getFromHive: Boolean = true
    val df_name: String = globalSettings.getDebugTempPath(this, "internal", "temp_hive_metadata") + ".parquet"

    //cache is set to true
    if (this.globalSettings.hiveHourToUpdateMetadata > 0 && getMetadataFromHive) {
      this.logMessageInfo(s"get Hive Metadata from cache")
      //get from DF
      try {
        //try to read cache
        val DF_get = this.spark.read.parquet(df_name).collect
        this.logMessageInfo(s"Hive num Rows from cache: ${DF_get.length}")
        //if any row, get datetime
        if (DF_get.length > 0) {
          val datetime_insert = DF_get(0).getAs[String]("datetime_insert")

          //println(datetime_insert)
          val start_hive_date = Calendar.getInstance
          start_hive_date.setTime(dateTimeFormat.parse(datetime_insert))
          val current_date = this.getCurrentDateTimeJava
          val diff_date = this.getDateTimeDiff(start_hive_date, current_date)
          //println(start_hive_date)
          //println(current_date)
          //println(diff_date)

          //if elapsed hour > set hour, then refresh from hive
          if (diff_date.days * 24 + diff_date.hour > this.globalSettings.hiveHourToUpdateMetadata) {
            this.logMessageInfo(s"Time elapsed, must refresh Hive Metadata... ${diff_date.days * 24 + diff_date.hour} (elapsed) > ${this.globalSettings.hiveHourToUpdateMetadata} (set)")
            getMetadataFromHive = true
          } else {
            _ColumnsAndTables = new ArrayBuffer[HuemulSqlTablesAndColumns]()
            DF_get.foreach { x =>
              val newreg = new HuemulSqlTablesAndColumns()
              newreg.databaseName = x.getAs[String]("databaseName")
              newreg.tableName = x.getAs[String]("tableName")
              newreg.columnName = x.getAs[String]("columnName")

              _ColumnsAndTables.append(newreg)
            }

            getMetadataFromHive = false
          }

        } else {
          getMetadataFromHive = true
        }
      } catch {
        case e: Exception =>
          println(e)
          getMetadataFromHive = true
      }
    }

    //get from hive if cache doesn't exists
    if (getMetadataFromHive) {
      this.logMessageInfo(s"get Hive Metadata from HIVE")
      if (OnlyRefreshTempTables)
        _ColumnsAndTables = _ColumnsAndTables.filter { x_fil => x_fil.databaseName != "__temporary" }
      else
        _ColumnsAndTables = new ArrayBuffer[HuemulSqlTablesAndColumns]()

      //spark.catalog.listTables().show(10000)
      //spark.catalog.listDatabases().show()
      var numRowDatabase = 0
      var allDatabases = spark.catalog.listDatabases().collect()

      //only get the first one
      if (OnlyRefreshTempTables)
        allDatabases = allDatabases.filter { x => x == allDatabases(0)  }


      allDatabases.foreach { x_database =>
        numRowDatabase += 1
        //println(s"x_database.name: ${x_database.name}")
        var resTables = spark.catalog.listTables(x_database.name).collect()
        if (numRowDatabase > 1)
          resTables = resTables.filter { x_fil => x_fil.database != null }
        else {
          //only get temp tables (null database)
          if (OnlyRefreshTempTables)
            resTables = resTables.filter { x_fil => x_fil.database == null }
        }
          //resTables.foreach { x_prin => println(x_prin) }



        resTables.foreach { x =>
          //get all columns
          //println(s"database: ${x.database}, table: ${x.name}")
          //spark.catalog.listColumns(x.database, x.name).show(10000)
          var listcols:org.apache.spark.sql.Dataset[org.apache.spark.sql.catalog.Column]= null

          try {
            if (x.database == null)
              listcols = spark.catalog.listColumns(x.name)
            else
              listcols = spark.catalog.listColumns(x.database, x.name)

            listcols.collect().foreach { y =>
              //println(s"database: ${x.database}, table: ${x.name}, column: ${y.name}")
              val newRow = new HuemulSqlTablesAndColumns()
              newRow.columnName = y.name
              newRow.databaseName = if (x.database == null) "__temporary" else x.database
              newRow.tableName = x.name
              _ColumnsAndTables.append(newRow)
            }
          } catch {
            case e: Exception =>
              println(s"Error reading HIVE metadata on table ${x.database}.${x.name}")
              println(e)

          }
        }
      }
    }



    if (this.globalSettings.hiveHourToUpdateMetadata > 0 && getMetadataFromHive) {
      this.logMessageInfo(s"Save Hive Metadata to cache")
      import spark.implicits._
      val ldt = this.getCurrentDateTime()
      val b = _ColumnsAndTables.map { x => TempHiveSchema(x.databaseName, x.tableName, x.columnName, ldt ) }.toList
      val bDF = b.toDF()
      bDF.repartition(1).write.mode(SaveMode.Overwrite).parquet(df_name )
    }


    val duration = this.getDateTimeDiff(inicio, this.getCurrentDateTimeJava)
    logMessageInfo(s"duration (hh:mm:ss): ${"%02d".format(duration.hour)}:${"%02d".format(duration.minute)}:${"%02d".format(duration.second)}")
    //println(s"duracion: ${duracion.hour}: ${duracion.minute} ${duracion.second} ")

    //_ColumnsAndTables.foreach { x => println(s"${x.databaseName}, ${x.tableName}, ${x.columnName}")}
    _ColumnsAndTables
  }

  /*
  private def CreateTempHiveSchema(): StructType = {
    //Fields
    val fieldsDetail : ArrayBuffer[StructField] = new ArrayBuffer[StructField]()
    fieldsDetail.append(StructField("databaseName", StringType, nullable = true) )
    fieldsDetail.append(StructField("tableName", StringType, nullable = true) )
    fieldsDetail.append(StructField("columnName", StringType, nullable = true) )
    StructType(fieldsDetail)
  }

   */

  def num_to_text(text_format: String, value: Any): String = {
    text_format.format(value)
  }

  def addColumnsAndTablesFromQuery(Alias: String, queryRes: ArrayBuffer[com.huemulsolutions.bigdata.sql_decode.HuemulSqlColumns]) {
    //filter alias if exists before
    _ColumnsAndTables = _ColumnsAndTables.filter { x => !(x.databaseName == "__temporary" && x.tableName.toUpperCase() == Alias.toUpperCase())}

    queryRes.foreach { x =>
      val newRow = new HuemulSqlTablesAndColumns()
          newRow.columnName = x.columnName
          newRow.databaseName = "__temporary"
          newRow.tableName = Alias
      _ColumnsAndTables.append(newRow)
    }
  }

  private var isEnableSQLDecode: Boolean = true
  def enableSQLDecode() { isEnableSQLDecode = true }
  def disableSQLDecode() {isEnableSQLDecode = false}
  def getIsEnableSQLDecode: Boolean = isEnableSQLDecode

  /**
   * logMessageDebug: Send {message} to log4j - Debug
   */
  def logMessageDebug(message: Any)  {
    log_info.debug(message)
  }

  /**
   * logMessageInfo: Send {message} to log4j - Info
   */
  def logMessageInfo(message: Any)  {
    log_info.info(message)
  }

  /**
   * logMessageWarn: Send {message} to log4j - Warning
   */
  def logMessageWarn(message: Any)  {
    log_info.warn(message)
  }

  /**
   * logMessageError: Send {message} to log4j - Error
   */
  def logMessageError(message: Any)  {
    log_info.error(message)
  }




  /*********************
   * ARGUMENTS
   *************************/
  logMessageInfo(s"huemul_BigDataGovernance version $currentVersion")

  val arguments: HuemulArgs = new HuemulArgs()
  arguments.setArgs(args)
  val environment: String = arguments.getValue("Environment", null, s"MUST be set environment parameter: '${globalSettings.globalEnvironments}' " )


   //Validating GlobalSettings
  logMessageInfo(s"Start Validating GlobalSetings (level: ${this.globalSettings.getValidationLevel})..")
  var ErrorGlobalSettings: String = ""
  if (this.globalSettings.getValidationLevel.equals("FULL")) {
    if (!this.globalSettings.validPath(globalSettings.rawSmallFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}RAW_SmallFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.rawBigFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}RAW_BigFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.masterSmallFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}MASTER_SmallFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.masterBigFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}MASTER_BigFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.masterDataBase, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}MASTER_DataBase"
    if (!this.globalSettings.validPath(globalSettings.dimSmallFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}DIM_SmallFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.dimBigFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}DIM_BigFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.dimDataBase, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}DIM_DataBase"
    if (!this.globalSettings.validPath(globalSettings.reportingSmallFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}REPORTING_SmallFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.reportingBigFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}REPORTING_BigFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.reportingDataBase, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}REPORTING_DataBase"
    if (!this.globalSettings.validPath(globalSettings.analyticsSmallFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}ANALYTICS_SmallFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.analyticsBigFilesPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}ANALYTICS_BigFiles_Path"
    if (!this.globalSettings.validPath(globalSettings.analyticsDataBase, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}ANALYTICS_DataBase"


    if (this.globalSettings.mdmSaveOldValueTrace) {
      if (!this.globalSettings.validPath(globalSettings.mdmOldValueTracePath, this.environment))
        ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}MDM_OldValueTrace_Path"
      if (!this.globalSettings.validPath(globalSettings.mdmOldValueTraceDataBase, this.environment))
        ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}MDM_OldValueTrace_DataBase"
    }

    if (this.globalSettings.mdmSaveBackup) {
      if (!this.globalSettings.validPath(globalSettings.mdmBackupPath, this.environment))
        ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}MDM_Backup_Path"
    }
  }

  if (!this.globalSettings.validPath(globalSettings.temporalPath, this.environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}TEMPORAL_Path"
  if (this.globalSettings.dqSaveErrorDetails) {
    if (!this.globalSettings.validPath(globalSettings.dqErrorPath, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}DQError_Path"
    if (!this.globalSettings.validPath(globalSettings.dqErrorDataBase, this.environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.nonEmpty) ", " else ""}DQError_DataBase"
  }
  logMessageInfo("End Validating GlobalSetings..")



  if (ErrorGlobalSettings.nonEmpty) {
    sys.error(s"Error: GlobalSettings incomplete!!, you must set $ErrorGlobalSettings ")
  }

  var getMetadataFromHive: Boolean = true
  try {
    getMetadataFromHive = arguments.getValue("getMetadataFromHive", "true" ).toBoolean
  } catch {
    case _: Exception => logMessageError("getMetadataFromHive: error values (true or false)")
  }

  val Malla_Id: String = arguments.getValue("Malla_Id", "" )
  var HideLibQuery: Boolean = false
  try {
    HideLibQuery = arguments.getValue("HideLibQuery", "false" ).toBoolean
  } catch {
    case _: Exception => logMessageError("HideLibQuery: error values (true or false)")
  }
  var SaveTempDF: Boolean = true
  try {
    SaveTempDF = arguments.getValue("SaveTempDF", "true" ).toBoolean
  } catch {
    case _: Exception => logMessageError("SaveTempDF: error values (true or false)")
  }

  var SaveTempTables: Boolean = true
  try {
    SaveTempTables = arguments.getValue("SaveTempTables", "true" ).toBoolean
  } catch {
    case _: Exception => logMessageError("SaveTempTables: error values (true or false)")
  }


  var impalaEnabled: Boolean = globalSettings.impalaEnabled
  try {
    impalaEnabled = arguments.getValue("ImpalaEnabled", s"${globalSettings.impalaEnabled}" ).toBoolean
  } catch {
    case _: Exception => logMessageError("ImpalaEnabled: error values (true or false)")
  }

  var getHiveMetadata: Boolean = true
  try {
    getHiveMetadata = arguments.getValue("getHiveMetadata", "true" ).toBoolean
  } catch {
    case _: Exception => logMessageError("getHiveMetadata: error values (true or false)")
  }

  /**
   * Setting Control/PostgreSQL conectivity
   */
  var RegisterInControl: Boolean = true
  try {
    RegisterInControl = arguments.getValue("RegisterInControl", "true" ).toBoolean
  } catch {
    case _: Exception => logMessageError("RegisterInControl: error values (true or false)")
  }

  val TestPlanMode: Boolean = arguments.getValue("TestPlanMode", "false" ).toBoolean
  if (TestPlanMode)
    RegisterInControl = false

   /***
   * True for show all messages
   */
  val standardDateFormat: String = "yyyy-MM-dd HH:mm:ss"
  val standardDateFormatMilisec: String = "yyyy-MM-dd HH:mm:ss:SSS"
  val debugMode : Boolean = arguments.getValue("debugmode","false").toBoolean
  val dateFormatNumeric: DateFormat = new SimpleDateFormat("yyyyMMdd")
  val dateTimeFormat: DateFormat = new SimpleDateFormat(standardDateFormat)
  val dateTimeText: String = "{{YYYY}}-{{MM}}-{{DD}} {{hh}}:{{mm}}:{{ss}}"
  val dateFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd")
  //var AutoInc: BigInt = 0

  val Invoker: Array[StackTraceElement] = new Exception().getStackTrace
  val ProcessNameCall: String = Invoker(1).getClassName.replace(".", "_").replace("$", "")

  /*********************
   * START SPARK AND CONTROL MODEL CONNECTION
   *************************/
  //from 2.6.2 add userName and password withou using connectionString
  val controlUserName: String = globalSettings.getUserName(this, globalSettings.controlSetting)
  val controlPassword: String = globalSettings.getPassword(this, globalSettings.controlSetting)
  @transient val CONTROL_connection: HuemulJdbcProperties = new HuemulJdbcProperties(this
    , globalSettings.getPath(this, globalSettings.controlSetting)
    , globalSettings.controlDriver, debugMode)
    .setUserName(controlUserName)
    .setPassword(controlPassword)

  @transient val impalaConnection = new HuemulJdbcProperties(this, globalSettings.getPath(this, globalSettings.impalaSetting),"com.cloudera.impala.jdbc4.Driver", debugMode)

  if (!TestPlanMode && RegisterInControl) {
    logMessageInfo(s"establishing connection with control model")
    CONTROL_connection.startConnection()
  }
  if (!TestPlanMode && impalaEnabled) {
    logMessageInfo(s"establishing connection with impala")
    impalaConnection.startConnection()
  }


  val spark: SparkSession = if (!TestPlanMode & localSparkSession == null)
                                      SparkSession.builder().appName(appName)
                                              //.master("local[*]")
                                              .config("spark.sql.warehouse.dir", warehouseLocation)
                                              .config("spark.sql.parquet.writeLegacyFormat",value = true)
                                              .enableHiveSupport()
                                              .getOrCreate()
                            else if (!TestPlanMode & localSparkSession != null)
                                      localSparkSession
                            else null

  if (!TestPlanMode) {
     //spark.conf.getAll.foreach(logMessage)
    spark.sparkContext.setLogLevel("WARN")
    //spark.debug.maxToStringFields
  }

  val IdPortMonitoring: String = if (!TestPlanMode) spark.sparkContext.uiWebUrl.get else ""

  if (!TestPlanMode) {
    //from 2.2: resolve BUG reading ORC, add set spark.sql.hive.convertMetastoreOrc=true according to SPARK-15705
    spark.sql("set spark.sql.hive.convertMetastoreOrc=true")
  }

  //spark.sql("set").show(10000, truncate = false)
  //val GetConfigSet = spark.sparkContext.getConf


  /*********************
   * GET UNIQUE IDENTIFY
   *************************/

  val IdApplication: String = if (!TestPlanMode) spark.sparkContext.applicationId else ""
  logMessageInfo(s"application_Id: $IdApplication")
  logMessageInfo(s"URL Monitoring: $IdPortMonitoring")

  /*********************
   * GET HIVE METADATA FOR COLUMNS TRACEABILITY
   *************************/
  if (!TestPlanMode && getHiveMetadata  && RegisterInControl) {
    logMessageInfo("Start get hive table metadata..")
    getColumnsAndTables(false)
    logMessageInfo("End get hive table metadata..")
  }

  val IdSparkPort: String = if (!TestPlanMode) spark.sql("set").filter("key='spark.driver.port'").select("value").collectAsList().get(0).getAs[String]("value") else ""
  logMessageInfo(s"Port_Id: $IdSparkPort")

  //Process Registry, check if port is still in use
  if (RegisterInControl) {
    val startWaitingTime: Calendar = Calendar.getInstance()
    var continueWaiting: Boolean = application_StillAlive(IdApplication)
    var numAttempt: Integer = 0
    while (continueWaiting) {
      Thread.sleep(10000)
      val minutesWait = this.getDateTimeDiff(startWaitingTime, Calendar.getInstance())
      val minutesWaiting = ((minutesWait.days * 24) + minutesWait.hour) * 60 + minutesWait.minute

      logMessageError(s"waiting for singleton ($minutesWaiting out of ${globalSettings.getMaxMinutesWaitInSingleton} minutes) Application Id in use: $IdApplication, maybe you're creating two times a spark connection")

      //from 2.6.3
      if (application_StillAlive(IdApplication)) {
        numAttempt = 0
        continueWaiting = minutesWaiting < globalSettings.getMaxMinutesWaitInSingleton
      } else {
        //numAttempt is consecutive
        if (numAttempt > globalSettings.getMaxAttemptApplicationInUse) {
          //Si no existe ejecución vigente, debe invocar proceso que limpia proceso
          application_closeAll(IdApplication, closeExecutors = false)
          continueWaiting = false
        } else
          numAttempt += 1
      }
    }

    CONTROL_connection.executeJdbcNoResultSet(s"""
                  insert into control_executors (application_id
                  							   , idsparkport
                  							   , idportmonitoring
                  							   , executor_dtstart
                  							   , executor_name)
                	VALUES( ${ReplaceSQLStringNulls(IdApplication)}
                		   , ${ReplaceSQLStringNulls(IdSparkPort)}
                		   , ${ReplaceSQLStringNulls(IdPortMonitoring)}
                		   , ${ReplaceSQLStringNulls(getCurrentDateTime())}
                		   , ${ReplaceSQLStringNulls(appName)}
                  )
        """)
  }

  private var hive_HWC: HuemulExternalHWC = _
  def getHiveHwc: HuemulExternalHWC = { hive_HWC}

  if (globalSettings.externalBbddConf.usingHwc.getActive  || globalSettings.externalBbddConf.usingHwc.getActiveForHBase ) {
    hive_HWC = new HuemulExternalHWC(this)
  }

  /*********************
   * START METHOD
   *************************/

  def getPath(pathFromGlobal: ArrayBuffer[HuemulKeyValuePath]): String = {
    globalSettings.getPath(this, pathFromGlobal)
  }

  def getDataBase(dataBaseFromGlobal: ArrayBuffer[HuemulKeyValuePath]): String = {
    globalSettings.getDataBase(this, dataBaseFromGlobal)
  }

  def close(stopSpark: Boolean) {
    //println(s"this.IdApplication: ${this.IdApplication}, IdApplication: ${IdApplication}")
    application_closeAll(IdApplication, closeExecutors = true)
    this.spark.catalog.clearCache()
    if (stopSpark) {
      this.spark.close()
      this.spark.stop()
    }
    if (RegisterInControl) this.CONTROL_connection.connection.close()
    if (impalaEnabled) this.impalaConnection.connection.close()

    if (globalSettings.externalBbddConf.usingHive.getActive  || globalSettings.externalBbddConf.usingHive.getActiveForHBase  ) {
      val connHIVE = globalSettings.externalBbddConf.usingHive.getJdbcConnection(this)
      if (connHIVE != null) {
        if (connHIVE.connection != null)
          connHIVE.connection.close()
      }
    }

    //FROM 2.5 --> ADD HORTONWORKS WAREHOSUE CONNECTOR
    if (globalSettings.externalBbddConf.usingHwc.getActive  || globalSettings.externalBbddConf.usingHwc.getActiveForHBase ) {
      if (getHiveHwc != null)
        getHiveHwc.close()
    }

  }

  def close() {
    close(if (globalSettings.getBigDataProvider == HuemulTypeBigDataProvider.databricks) false else true)

  }

  def application_closeAll(ApplicationInUse: String, closeExecutors: Boolean) {
    if (RegisterInControl) {
      CONTROL_connection.executeJdbcNoResultSet(s"""DELETE FROM control_singleton WHERE application_id = ${ReplaceSQLStringNulls(ApplicationInUse)}""")
      if (closeExecutors) {
        CONTROL_connection.executeJdbcNoResultSet(s"""DELETE FROM control_executors WHERE application_id = ${ReplaceSQLStringNulls(ApplicationInUse)}""")
      }
       //println(s"""DELETE FROM control_executors WHERE application_id = ${ReplaceSQLStringNulls(ApplicationInUse)}""")
    }

  }

  def application_StillAlive(ApplicationInUse: String): Boolean = {
    if (!RegisterInControl) return false

    val CurrentProcess = this.CONTROL_connection.executeJdbcWithResult(s"select * from control_executors where application_id = '$ApplicationInUse'")
    var IdAppFromDataFrame : String = ""
    var IdAppFromAPI: String = ""
    var URLMonitor: String = ""
    var StillAlive: Boolean = true

    if (CurrentProcess.resultSet == null || CurrentProcess.resultSet.length == 0) { //dosn't have records, was eliminted by other process (rarely)
      StillAlive = false
    } else {
      IdAppFromDataFrame = CurrentProcess.resultSet(0).getAs[String]("application_id".toLowerCase())
      URLMonitor = s"${CurrentProcess.resultSet(0).getAs[String]("idportmonitoring".toLowerCase())}/api/v1/applications/"

      //Get Id App from Spark URL Monitoring
      try {
        val (idFromURL2, result2) = this.getIdFromExecution(URLMonitor)

        //get OK
        if (result2 == 0)
          IdAppFromAPI = idFromURL2
        else
          logMessageWarn(s"can't get url: return: $result2")


      } catch {
        case _: Exception =>
          StillAlive = false
      }
    }

    //if URL Monitoring is for another execution
    if (debugMode) logMessageInfo(s"IdAppFromDataFrame: $IdAppFromDataFrame, IdAppFromAPI: $IdAppFromAPI")
    if (StillAlive && IdAppFromAPI != IdAppFromDataFrame)
        StillAlive = false

    /*
    //Si no existe ejecución vigente, debe invocar proceso que limpia proceso
    if (!StillAlive) {
      application_closeAll(ApplicationInUse)
    }
    */

    StillAlive
  }

  private val TableRegistered: ArrayBuffer[String] = new ArrayBuffer[String]()
  def IsTableRegistered(tableName: String): Boolean = {
    var IsRegister: Boolean = false
    if (TableRegistered.contains(tableName))
      IsRegister = true
    else {
      TableRegistered.append(tableName)
    }


    IsRegister
  }


  //var sc: org.apache.spark.SparkContext = spark.sparkContext
  def getMonth(Date: Calendar): Int = {Date.get(Calendar.MONTH)+1}
  def getDay(Date: Calendar): Int = {Date.get(Calendar.DAY_OF_MONTH)}
  def getYear(Date: Calendar): Int = {Date.get(Calendar.YEAR)}
  def getHour(Date: Calendar): Int = {Date.get(Calendar.HOUR)}

  def isAllDigits(x: String): Boolean = x forall Character.isDigit


  /***
   * setDateTime: returns calendar for parameters
   */
  def setDateTime (ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer): Calendar = {

    val result : Calendar = Calendar.getInstance()
    result.setTime(dateTimeFormat.parse(replaceWithParams(dateTimeText,ano
                                                                      , mes
                                                                      , if (dia == null) 1 else dia
                                                                      , if (hora == null) 0 else hora
                                                                      , if (min == null) 0 else min
                                                                      , if (seg == null) 0 else seg
                                                                      )))

    result
  }

  /***
   * ReplaceSQLStringNulls: returns text for SQL
   * from version 1.1
   */
  def ReplaceSQLStringNulls(text: String, len: Integer = null): String = {
    var result = text
    if (len != null && result != null && result.length() > len)
      result = result.substring(0,len)

    result = if (result == null) "null" else s"'${result.replace("'", "''")}'"

    result
  }

  /***
   * getCurrentDateTime: returns current datetime in specific format
   * from version 1.1
   */
  def getCurrentDateTime (format: String = standardDateFormatMilisec): String = {

    val dateTimeFormat: DateFormat = new SimpleDateFormat(format)
    val ActualDateTime: Calendar  = Calendar.getInstance()

    val Fecha: String = dateTimeFormat.format(ActualDateTime.getTime)

    Fecha
  }

  /***
   * getCurrentDateTime: returns current datetime
   * from version 1.1
   */
  def getCurrentDateTimeJava: java.util.Calendar = Calendar.getInstance()

  /**
   * Return day, hour, minute and second difference from two datetime
   */
  def getDateTimeDiff(dt_start: Calendar, dt_end: Calendar): HuemulDateTimePart = {
    val dif = dt_end.getTimeInMillis - dt_start.getTimeInMillis

    new HuemulDateTimePart(dif)
  }

  /***
   * setDate: returns calendar for parameters (StringDate = YYYY-MM-DD)
   */
  def setDate (StringDate: String): Calendar = {
    val lDay: String = StringDate.substring(8, 10)
    val lMonth: String = StringDate.substring(5, 7)
    val lYear: String = StringDate.substring(0, 4)
    val result : Calendar = Calendar.getInstance()
    result.setTime(dateTimeFormat.parse(replaceWithParams(dateTimeText,lYear.toInt
                                                                      , lMonth.toInt
                                                                      , lDay.toInt
                                                                      , 0
                                                                      , 0
                                                                      , 0
                                                                      )))

    result
  }


  /***
   Create a temp table in huemulBigDataGov_Temp (Hive), and persist on HDFS Temp folder
   NumPartitionCoalesce = null for automatically settings
   */
  def CreateTempTable(DF: DataFrame, Alias: String, CreateTable: Boolean, NumPartitionCoalesce: Integer ) {
    //Create parquet in temp folder
    if (CreateTable && SaveTempDF && SaveTempTables){
      val FileToTemp: String = globalSettings.getDebugTempPath(this, ProcessNameCall, Alias) + ".parquet"
      logMessageInfo(s"path result for table alias $Alias: $FileToTemp ")
      //version 1.3 --> prueba para optimizar escritura temporal
      //Se aplica coalesce en vez de repartition para evitar el shuffle interno
      if (NumPartitionCoalesce == null || NumPartitionCoalesce == 0)
        DF.write.mode(SaveMode.Overwrite).parquet(FileToTemp)
      else
        DF.coalesce(NumPartitionCoalesce).write.mode(SaveMode.Overwrite).parquet(FileToTemp)

      //DF.repartition(4).write.mode(SaveMode.Overwrite).parquet(FileToTemp)

      //val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      //fs.setPermission(new org.apache.hadoop.fs.Path(FileToTemp), new FsPermission("770"))
    }
  }

  def IsNumericType(Type: DataType): Boolean = {
     Type == ByteType ||
            Type == ShortType ||
            Type == IntegerType ||
            Type == LongType ||
            Type == FloatType ||
            Type == DoubleType ||
            Type == DecimalType
  }

  def IsDateType(Type: DataType): Boolean = {
    Type == TimestampType ||
            Type == DateType
  }

  /***
   * Reemplaza un texto con los datos de fecha y hora, según formato requerido
   * El texto debe ir con dos llaves
   * YYYY = año
   * MM = mes
   * DD = dia
   * hh = hora
   * mm = minuto
   * ss = segundo
   * Ejemplo: {{YYYY}}
   */
  def replaceWithParams(text: String, YYYY: Integer, MM: Integer, DD: Integer, hh: Integer, mm: Integer, ss: Integer, additionalParams: String = null) : String = {
    var textFinal : String = text
    if (YYYY != null) {
       textFinal = textFinal.replace("{{YYYY}}", YYYY.toString)
                       .replace("{{YY}}", YYYY.toString.substring(2,4))
    }

    if (MM != null) {
       textFinal = textFinal.replace("{{MM}}", MM.toString.reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{M}}", MM.toString)
    }

    if (DD != null) {
       textFinal = textFinal.replace("{{DD}}", DD.toString.reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{D}}", DD.toString)
    }

    if (hh != null) {
       textFinal = textFinal.replace("{{hh}}", hh.toString.reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{h}}", hh.toString)
    }

    if (mm != null) {
       textFinal = textFinal.replace("{{mm}}", mm.toString.reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{m}}", mm.toString)
    }

    if (ss != null) {
       textFinal = textFinal.replace("{{ss}}", ss.toString.reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{s}}", ss.toString)
    }

    if (additionalParams != null) {
      additionalParams.split(";").foreach { x =>

        val Pair = x.split("=")
        //Ajuste en caso que condición sea solo "{{algo}}=" en vez de "{{algo}}=valor"
        textFinal = textFinal.replace(Pair(0), if (Pair.length == 1) "" else Pair(1))
      }
    }


    textFinal
  }

  /**
   * HasName: determine if argument is not null and is not empty
   */
  def hasName(name: String): Boolean = {
    name != null && name != ""
  }

  def huemul_getDateForLog(): String = {
    val dateTimeFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSSS")
    val ActualDateTime: Calendar  = Calendar.getInstance()

    val Fecha: String = dateTimeFormat.format(ActualDateTime.getTime)

    Fecha
  }

  private var _autoIncId: Int = 0
  /**
   * Get UniqueId from datetime, random and arbitrary value
   */
  def huemul_GetUniqueId (): String = {
    val MyId: String = this.IdSparkPort

    _autoIncId += 1
    if (_autoIncId > 999)
      _autoIncId = 0

    //Get DateTime
    val dateTimeFormat: DateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSSSS")
    val ActualDateTime: Calendar  = Calendar.getInstance()
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"))

    val Fecha: String = dateTimeFormat.format(ActualDateTime.getTime)


    //Generate random
    val randomNum: Int  = ThreadLocalRandom.current().nextInt(1, 999998 + 1)
    //var Random: String  = ("000000".concat(Integer.toString(randomNum)))
    val Random: String  = "%06d".format(randomNum)
    val autoIncString: String = "%03d".format(_autoIncId)
    //System.out.logMessage(Random)
    //Random = Random.substring(Random.length()-3, Random.length())

    //System.out.logMessage(Random)
    val Final: String  = Fecha.concat(autoIncString.concat(Random.concat(MyId)))
    Final

    //return this.spark.sql(s"select data_control.fabric_GetUniqueId(${this.IdSparkPort}) as NewId ").first().getAs[String]("NewId")
  }

  private var _huemul_showDemoLines: Boolean = true
  def gethuemul_showDemoLines(): Boolean = _huemul_showDemoLines
  def huemul_showDemoLines(value: Boolean) {
    _huemul_showDemoLines = value
  }

  //replicated in huemul_columns
  def getCaseType(tableStorage: com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.HuemulTypeStorageType, value: String): String = {
    if (tableStorage == com.huemulsolutions.bigdata.tables.HuemulTypeStorageType.AVRO) value.toLowerCase() else value
  }

  def getMovedHRef(html: String): String = {
    val posIni: Int =  html.indexOf("""<a href="""")
    var urlFound: String = null

    if (posIni >= 0) {
      val html2: String = html.substring(posIni + 9,html.length)
      val posEnd: Int = html2.indexOf("""">""")

      if (posEnd >= 0) {
        urlFound = html2.substring(0,posEnd)
      }

    }

    urlFound
  }

  /**
   * Get execution Id from spark monitoring url
   *
   */
  def getIdFromExecution(url: String, iterator: Int = 0): (String, Int) = {
    if (iterator >= 3)
      return (null, -10)

    var result: Int = 1
    import spark.implicits._

    var idFromURL: String = ""

    //try to open api
    var html: BufferedSource = null
    try {
      html = Source.fromURL(url)
    } catch {
      case _ : java.net.ConnectException =>
        //no connection found, stillAlive = false
        if (debugMode) logMessageInfo("no connection found, stillAlive = false")
        return ("", -1)
      case e : Exception =>
        //other error, return 0
        logMessageInfo(s"getIdFromExecution error: ${e.getMessage}")
        return ("", 0)
    }

    //if connection, get Id
    try {
      val vals = spark.sparkContext.parallelize(html.mkString :: Nil)
      //spark.read.json(vals).show(truncate = false)
      idFromURL = spark.read.json(vals).select($"id").first().getString(0)

      if (debugMode) logMessageInfo("read from html to dataframe")
      if (debugMode) spark.read.json(vals).show()

      return (idFromURL, 0)
    } catch {
      case e : Exception =>
        if (debugMode) logMessageWarn(e)
        result = -2
    }

    //error to get Id, try to read from html redirect location
    val newURL = getMovedHRef(html.mkString)

    if (newURL != null) {
      //get new url, try to get Id
      getIdFromExecution(newURL, iterator + 1)
    } else {
      //not redirect url, throw error
      if (debugMode) logMessageWarn("new URL doesn't exists, error -3")
      ("", -3)
    }

    //(idFromURL, result)
  }


/*
  def ExecuteJDBC_onSpark2(ConnectionString: String, SQL: String,valor: Boolean = true): huemul_JDBCResult = {
    //OJO: este proceso ejecuta con spark.read.jdbc y devuelve los datos en un arreglo porque el comportamiento
    //al usar el DataFrame es distinto al esperado
    //cada vez que se acceder al DataFrame entregado, internamete Spark hace nuevamente la ejecución del SQL
    //por tanto si hago un GetAs("campo1") y getAs("campo"), Spark ejecutará dos veces el SQl
    //si el sql es un procedimiento almacenado que inserta datos, hará dos veces la insersión del registro.
    //PARA EVITAR ESTO, ESTAMOS RETORNANDO UN ARRAY[ROW]

    var Result: huemul_JDBCResult = new huemul_JDBCResult()

    val driver = "org.postgresql.Driver"
    val url = ""



      var connection:Connection = null
      try {

        Class.forName(driver)
        /*
        Class.forName(driver)
        connection = DriverManager.getConnection(ConnectionString)

        val statement = connection.createStatement()
        Result.ResultSet = statement.executeQuery(SQL)
        connection.close()
        */


        // Create a Properties() object to hold the parameters.

        val connectionProperties = new Properties()
        connectionProperties.setProperty("driver", "org.postgresql.Driver" )
        val SQLExec = if (valor) s"($SQL) aliasTemp" else s"$SQL"
        //spark
        val ResultSet = spark.read.jdbc(ConnectionString, SQLExec , connectionProperties)
        if (ResultSet != null)
          Result.ResultSet = ResultSet.collect()

        ResultSet.unpersist()


        //sqlResult.show()
      } catch {
        case e: Exception  =>
          if (DebugMode) logMessage(SQL)
          if (DebugMode) logMessage(s"JDBC Error: $e")
          if (DebugMode) logMessage(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => logMessage(x) }}")
          Result.ErrorDescription = s"JDBC Error: ${e}"
          Result.IsError = true
      }

    Result
  }
  *
  */

  def print_result(resFinal: com.huemulsolutions.bigdata.sql_decode.HuemulSqlDecodeResult, numciclo: Int) {
    println(s"RESULTADO CICLO $numciclo ${resFinal.aliasQuery} ***************************************")
     println("************ SQL FROM ************ ")
     println(resFinal.fromSql)
     println("************ SQL WHERE ************ ")
     println(resFinal.whereSql)

     println("   ")
     println("************ COLUMNS ************ ")
     resFinal.columns.foreach { x =>
           println (s"*** COLUMN NAME: ${x.columnName}")
           println (s"    column_sql: ${x.columnSql}")
           println ("     columns used:")
           x.columnOrigin.foreach { y => println(s"     ---- column_database: ${y.traceDatabaseName}, trace_tableName: ${y.traceTableName}, trace_tableAlias_name: ${y.traceTableAliasName}, traceColumnName: ${y.traceColumnName}") }
    }

    println("   ")
    println("************ TABLES ************ ")
    resFinal.tables.foreach { x => println (s"*** DATABASE NAME: ${x.databaseName}, TABLE NAME: ${x.tableName}, ALIAS: ${x.tableAliasName}") }

    println("   ")
    println("************ COLUMNS WHERE ************ ")
    resFinal.columnsWhere.foreach { x => println(s"Columns: ${x.traceColumnName}, Table: ${x.traceTableName}, Database: ${x.traceDatabaseName}") }

    println("   ")
    println("************ FINAL RESULTS ************ ")
    println(s"N° Errores: ${resFinal.numErrors}")
    println(s"N° subquerys: ${resFinal.autoIncSubQuery}")
    println(s"AliasDatabase: ${resFinal.aliasDatabase}")
    println(s"AliasQuery: ${resFinal.aliasQuery}")


    var numCiclo2 = numciclo
    resFinal.subQueryResult.foreach { x =>
      numCiclo2 += 1
      print_result(x,  numCiclo2)
    }

  }

  def dfSaveLineage(Alias: String, sql: String, dt_start: java.util.Calendar, dt_end: java.util.Calendar, control: HuemulControl, FinalTable: HuemulTable, isQuery: Boolean, isReferenced: Boolean ) {
    if (this.getIsEnableSQLDecode) {
      val res = huemulSqlDecode.decodeSql(sql, _ColumnsAndTables)

      if (debugMode)
        print_result(res,res.autoIncSubQuery)

      val duration = getDateTimeDiff(dt_start, dt_end)
      control.registerTraceDecode(res
                                 , Alias
                                 , -1 //NumRows
                                 , duration.hour // Duration_Hour
                                 , duration.minute //Duration_Minute
                                 , duration.second // Duration_Second
                                 , FinalTable
                                 , isQuery
                                 , isReferenced)
      
                                  
      //Add my result to __temporary                            
      addColumnsAndTablesFromQuery(Alias, res.columns)
    }
  }
  
  /**
   * Execute a SQL sentence, create a new alias and save de DF result into HDFS
   */
  def DF_ExecuteQuery(Alias: String, SQL: String, numPartitions: Integer): DataFrame = {
    if (this.debugMode && !HideLibQuery) logMessageDebug(SQL)
    var SQL_DF = this.spark.sql(SQL)            //Ejecuta Query
    if (numPartitions != null && numPartitions > 0)
      SQL_DF = SQL_DF.repartition(numPartitions)      
      
    
    if (this.debugMode) {
      this.logMessageDebug(s"alias: $Alias")
      SQL_DF.show()
    }
    //Change alias name
    SQL_DF.createOrReplaceTempView(Alias)         //crea vista sql

    this.CreateTempTable(SQL_DF, Alias, this.debugMode, null)      //crea tabla temporal para debug
    SQL_DF
  }
  
  /**
   * Execute a SQL sentence, create a new alias and save de DF result into HDFS
   */
  def dfExecuteQuery(Alias: String, SQL: String): DataFrame = {
    DF_ExecuteQuery(Alias, SQL, 0 )
  }
  
  def dfExecuteQuery(alias: String, sql: String, control: HuemulControl ): DataFrame = {
    val dt_start = this.getCurrentDateTimeJava
    val Result = DF_ExecuteQuery(alias, sql, 0)
    val dt_end = this.getCurrentDateTimeJava

    dfSaveLineage(alias
                , sql
                , dt_start
                , dt_end
                , control
                , null //FinalTable
                , isQuery = true //isQuery
                , isReferenced = false //isReferenced)
                  )
    
    Result
  }
  
  def RegisterError(ErrorCode: Integer, Message: String, Trace: String, FileName: String, MethodName: String, ClassName: String, LineNumber: Integer, WhoWriteError: String ) {
    
      
    if (RegisterInControl) {
      
      val Error_Id = huemul_GetUniqueId()
      
      var message = Message
      if (message == null)
        message = "null"
        
      var trace = Trace
      if (trace == null)
        trace = "null"
        
      var fileName = FileName
      if (fileName == null)
        fileName = "null"
        
      var methodName = MethodName
      if (methodName == null)
        methodName = "null"
        
      var className = ClassName
      if (className == null)
        className = "null"
        
      //Insert processExcec
      CONTROL_connection.executeJdbcNoResultSet(s"""
              insert into control_error (error_id
                                    ,error_message
                                    ,error_code
                                    ,error_trace
                                    ,error_classname
                                    ,error_filename
                                    ,error_linenumber
                                    ,error_methodname
                                    ,error_detail
                                    ,mdm_fhcrea
                                    ,mdm_processname)
        	VALUES( ${ReplaceSQLStringNulls(Error_Id)}
          		  ,${ReplaceSQLStringNulls(message.replace("'", "''"))}
                ,$ErrorCode
          		  ,${ReplaceSQLStringNulls(trace.replace("'", "''"))}
          		  ,${ReplaceSQLStringNulls(className.replace("'", "''"))}
          		  ,${ReplaceSQLStringNulls(fileName.replace("'", "''"))}
          		  ,${ReplaceSQLStringNulls(LineNumber.toString)}
          		  ,${ReplaceSQLStringNulls(methodName.replace("'", "''"))}
          		  ,''
          		  ,${ReplaceSQLStringNulls(getCurrentDateTime())}
          		  ,${ReplaceSQLStringNulls(WhoWriteError)}
          )
             """, CallErrorRegister = false)
     
    }
        
                               
  }
  
  /**
   * return true if path exists
   */
  def hdfsPath_exists(path: String): Boolean = {
    val lpath = new org.apache.hadoop.fs.Path(path)
    val fs = lpath.getFileSystem(this.spark.sparkContext.hadoopConfiguration)
     
    fs.exists(lpath)
  }
  
  /**
   * hiveTable_exists: return true if database.table exists
   */
  def hiveTable_exists(databaseName: String, tableName: String): Boolean = {
    var Result: Boolean = false
    try {
      val df_Result = this.spark.sql(s"select 1 from $databaseName.$tableName limit 1")
      if (df_Result == null)
        Result = false
      else 
        Result = true
    } catch {
      case _: Exception =>
        Result = false
    }
    
    Result
  }
  
  
  
  log_info.setLevel(Level.ALL)
}


    

