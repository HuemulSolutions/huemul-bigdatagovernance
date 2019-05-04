package com.huemulsolutions.bigdata.common

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import java.io._
import java.text.DateFormat
import java.sql.Connection
import java.util.Calendar
import java.util.TimeZone
import java.util.concurrent.ThreadLocalRandom
import java.util.Properties

import java.text.SimpleDateFormat
import scala.io.Source
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.permission.FsPermission
import com.huemulsolutions.bigdata.control.huemul_JDBCResult
//import scala.math.BigInt.int2bigInt

import scala.collection.mutable.ArrayBuffer
import java.sql.Types
import org.apache.spark.sql.types.DecimalType

import com.huemulsolutions.bigdata.control.huemul_JDBCProperties
import com.huemulsolutions.bigdata.tables.huemul_Table
import org.apache.spark.sql.catalyst.expressions.Coalesce

        
      
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
 *  @constructor create a new person with a name and age.
 *  @param appName nombre de la aplicación
 *  @param args argumentos de la aplicación
 */
class huemul_BigDataGovernance (appName: String, args: Array[String], globalSettings: huemul_GlobalPath) extends Serializable  {
  val GlobalSettings = globalSettings
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  
  /*********************
   * ARGUMENTS
   *************************/
  println("huemul_BigDataGovernance version 1.3.0 - sv01")
  
       
  val arguments: huemul_Args = new huemul_Args()
  arguments.setArgs(args)  
  val Environment: String = arguments.GetValue("Environment", null, s"MUST be set environment parameter: '${GlobalSettings.GlobalEnvironments}' " )
  
  
   //Validating GlobalSettings
  println("Start Validating GlobalSetings..")
  var ErrorGlobalSettings: String = ""
  if (!this.GlobalSettings.ValidPath(globalSettings.RAW_SmallFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}RAW_SmallFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.RAW_BigFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}RAW_BigFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.MASTER_SmallFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}MASTER_SmallFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.MASTER_BigFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}MASTER_BigFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.MASTER_DataBase, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}MASTER_DataBase"
  if (!this.GlobalSettings.ValidPath(globalSettings.DIM_SmallFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}DIM_SmallFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.DIM_BigFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}DIM_BigFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.DIM_DataBase, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}DIM_DataBase"
  if (!this.GlobalSettings.ValidPath(globalSettings.REPORTING_SmallFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}REPORTING_SmallFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.REPORTING_BigFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}REPORTING_BigFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.REPORTING_DataBase, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}REPORTING_DataBase"
  if (!this.GlobalSettings.ValidPath(globalSettings.ANALYTICS_SmallFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}ANALYTICS_SmallFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.ANALYTICS_BigFiles_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}ANALYTICS_BigFiles_Path"
  if (!this.GlobalSettings.ValidPath(globalSettings.ANALYTICS_DataBase, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}ANALYTICS_DataBase"
  if (!this.GlobalSettings.ValidPath(globalSettings.TEMPORAL_Path, this.Environment))
    ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}TEMPORAL_Path"
  
  if (this.GlobalSettings.DQ_SaveErrorDetails) {
    if (!this.GlobalSettings.ValidPath(globalSettings.DQError_Path, this.Environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}DQError_Path"
    if (!this.GlobalSettings.ValidPath(globalSettings.DQError_DataBase, this.Environment))
      ErrorGlobalSettings += s"${if (ErrorGlobalSettings.length() > 0) ", " else ""}DQError_DataBase"
  }
  println("End Validating GlobalSetings..")
  
  if (ErrorGlobalSettings.length()> 0) {
    sys.error(s"Error: GlobalSettings incomplete!!, you must set $ErrorGlobalSettings ")
  }
  
  
  val Malla_Id: String = arguments.GetValue("Malla_Id", "" )
  var HideLibQuery: Boolean = false
  try {
    HideLibQuery = arguments.GetValue("HideLibQuery", "false" ).toBoolean
  } catch {    
    case e: Exception => println("HideLibQuery: error values (true or false)")
  }
  var SaveTempDF: Boolean = true
  try {
    SaveTempDF = arguments.GetValue("SaveTempDF", "true" ).toBoolean
  } catch {    
    case e: Exception => println("SaveTempDF: error values (true or false)")
  }
  
  var ImpalaEnabled: Boolean = GlobalSettings.ImpalaEnabled
  try {
    ImpalaEnabled = arguments.GetValue("ImpalaEnabled", s"${GlobalSettings.ImpalaEnabled}" ).toBoolean
  } catch {    
    case e: Exception => println("ImpalaEnabled: error values (true or false)")
  }
  
  
  /**
   * Setting Control/PostgreSQL conectivity
   */
  var RegisterInControl: Boolean = true
  try {
    RegisterInControl = arguments.GetValue("RegisterInControl", "true" ).toBoolean
  } catch {    
    case e: Exception => println("RegisterInControl: error values (true or false)")
  }
  
  val TestPlanMode = arguments.GetValue("TestPlanMode", "false" ).toBoolean
  if (TestPlanMode)
    RegisterInControl = false
  
   /***
   * True for show all messages
   */
  val standardDateFormat: String = "yyyy-MM-dd HH:mm:ss"
  val standardDateFormatMilisec: String = "yyyy-MM-dd HH:mm:ss:SSS"
  val DebugMode : Boolean = arguments.GetValue("debugmode","false").toBoolean
  val dateFormatNumeric: DateFormat = new SimpleDateFormat("yyyyMMdd");
  val dateTimeFormat: DateFormat = new SimpleDateFormat(standardDateFormat);
  val dateTimeText: String = "{{YYYY}}-{{MM}}-{{DD}} {{hh}}:{{mm}}:{{ss}}"
  val dateFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd")
  //var AutoInc: BigInt = 0
  
  val Invoker = new Exception().getStackTrace()
  val ProcessNameCall: String = Invoker(1).getClassName().replace(".", "_").replace("$", "")

  /*********************
   * START SPARK AND POSGRES CONNECTION
   *************************/
  @transient val CONTROL_connection= new huemul_JDBCProperties(this, GlobalSettings.GetPath(this, GlobalSettings.CONTROL_Setting),GlobalSettings.CONTROL_Driver, DebugMode) // Connection = null
  @transient val impala_connection = new huemul_JDBCProperties(this, GlobalSettings.GetPath(this, GlobalSettings.IMPALA_Setting),"com.cloudera.impala.jdbc4.Driver", DebugMode) //Connection = null
  
  if (!TestPlanMode && RegisterInControl) 
    CONTROL_connection.StartConnection()
  if (!TestPlanMode && ImpalaEnabled)
      impala_connection.StartConnection()
  
  val spark: SparkSession = if (!TestPlanMode) SparkSession.builder().appName(appName)
                                              //.master("local[*]")
                                              .config("spark.sql.warehouse.dir", warehouseLocation)
                                              .config("spark.sql.parquet.writeLegacyFormat",true)
                                              .enableHiveSupport()
                                              .getOrCreate()
                            else null

  if (!TestPlanMode) {
     //spark.conf.getAll.foreach(println)
    spark.sparkContext.setLogLevel("ERROR")  
  }
  
  val IdPortMonitoring = if (!TestPlanMode) spark.sparkContext.uiWebUrl.get else "" 
  
  //spark.sql("set").show(10000, truncate = false)
  //val GetConfigSet = spark.sparkContext.getConf  
  
  
  /*********************
   * GET UNIQUE IDENTIFY
   *************************/
  
  val IdApplication = if (!TestPlanMode) spark.sparkContext.applicationId else ""
  println(s"application_Id: ${IdApplication}")  
  println(s"URL Monitoring: ${IdPortMonitoring}")
  
  val IdSparkPort = if (!TestPlanMode) spark.sql("set").filter("key='spark.driver.port'").select("value").collectAsList().get(0).getAs[String]("value") else ""
  println(s"Port_Id: ${IdSparkPort}")
  
  //Process Registry
  if (RegisterInControl) {
    while (application_StillAlive(IdApplication)) {
      println(s"waiting for singleton Application Id in use: ${IdApplication}, maybe you're creating two times a spark connection")
      Thread.sleep(10000)
    }
    val Result = CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
                  insert into control_executors (application_id
                  							   , idsparkport
                  							   , idportmonitoring
                  							   , executor_dtstart
                  							   , executor_name) 	
                	SELECT ${ReplaceSQLStringNulls(IdApplication)}
                		   , ${ReplaceSQLStringNulls(IdSparkPort)}
                		   , ${ReplaceSQLStringNulls(IdPortMonitoring)}
                		   , ${ReplaceSQLStringNulls(getCurrentDateTime())}
                		   , ${ReplaceSQLStringNulls(appName)}
        """)
  }
                
  
  
  
  /*********************
   * START METHOD
   *************************/
  
  def close() {
    application_closeAll(this.IdApplication)
    this.spark.catalog.clearCache()
    this.spark.close()
    if (RegisterInControl) this.CONTROL_connection.connection.close()
    if (ImpalaEnabled) this.impala_connection.connection.close()
    
  }
  
  def application_closeAll(ApplicationInUse: String) {
    if (RegisterInControl) {
       val ExecResult1 = CONTROL_connection.ExecuteJDBC_NoResulSet(s"""DELETE FROM control_singleton WHERE application_id = ${ReplaceSQLStringNulls(ApplicationInUse)}""")
       val ExecResult2 = CONTROL_connection.ExecuteJDBC_NoResulSet(s"""DELETE FROM control_executors WHERE application_id = ${ReplaceSQLStringNulls(ApplicationInUse)}""")
    }
      
  }
  
  def application_StillAlive(ApplicationInUse: String): Boolean = {
    if (!RegisterInControl) return false
    
    val CurrentProcess = this.CONTROL_connection.ExecuteJDBC_WithResult(s"select * from control_executors where application_id = '${ApplicationInUse}'")
    var IdAppFromDataFrame : String = ""
    var IdAppFromAPI: String = ""
    var URLMonitor: String = ""
    var StillAlive: Boolean = true
    
    if (CurrentProcess.ResultSet == null || CurrentProcess.ResultSet.length == 0) { //dosn't have records, was eliminted by other process (rarely)
      StillAlive = false            
    } else {
      IdAppFromDataFrame = CurrentProcess.ResultSet(0).getAs[String]("application_id".toLowerCase())
      URLMonitor = s"${CurrentProcess.ResultSet(0).getAs[String]("idportmonitoring".toLowerCase())}/api/v1/applications/"            
    
      //Get Id App from Spark URL Monitoring          
      try {
        IdAppFromAPI = this.GetIdFromExecution(URLMonitor)    
      } catch {
        case f: Exception => {
          StillAlive = false
        }                
      }
    }
                        
    //if URL Monitoring is for another execution
    if (StillAlive && IdAppFromAPI != IdAppFromDataFrame)
        StillAlive = false
                
    //Si no existe ejecución vigente, debe invocar proceso que limpia proceso
    if (!StillAlive) {
      application_closeAll(ApplicationInUse)
    }
    
    return StillAlive
  }
  
  private val TableRegistered: ArrayBuffer[String] = new ArrayBuffer[String]() 
  def IsTableRegistered(tableName: String): Boolean = {
    var IsRegister: Boolean = false
    if (TableRegistered.filter { x => x == tableName }.length > 0)
      IsRegister = true
    else {
      TableRegistered.append(tableName)
    }
    
    
    return IsRegister
  }
  
  
  //var sc: org.apache.spark.SparkContext = spark.sparkContext
  def getMonth(Date: Calendar): Int = {return Date.get(Calendar.MONTH)+1}
  def getDay(Date: Calendar): Int = {return Date.get(Calendar.DAY_OF_MONTH)}
  def getYear(Date: Calendar): Int = {return Date.get(Calendar.YEAR)}
  def getHour(Date: Calendar): Int = {return Date.get(Calendar.HOUR)}
  
  def isAllDigits(x: String) = x forall Character.isDigit
  
  
  /***
   * setDateTime: returns calendar for parameters 
   */
  def setDateTime (ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer): Calendar = {
    
    var result : Calendar = Calendar.getInstance();
    result.setTime(dateTimeFormat.parse(ReplaceWithParams(dateTimeText,ano
                                                                      , mes
                                                                      , if (dia == null) 1 else dia
                                                                      , if (hora == null) 0 else hora
                                                                      , if (min == null) 0 else min
                                                                      , if (seg == null) 0 else seg
                                                                      )))
    
    return result
  }
  
  /***
   * ReplaceSQLStringNulls: returns text for SQL
   * from version 1.1
   */
  def ReplaceSQLStringNulls(ColumnName: String): String = {
    return if (ColumnName == null) "null" else s"'${ColumnName.replace("'", "''")}'"
  }
  
  /***
   * getCurrentDateTime: returns current datetime in specific format
   * from version 1.1
   */
  def getCurrentDateTime (format: String = standardDateFormatMilisec): String = {
    
    val dateTimeFormat: DateFormat = new SimpleDateFormat(format)  
    val ActualDateTime: Calendar  = Calendar.getInstance()
        
    val Fecha: String = dateTimeFormat.format(ActualDateTime.getTime())
    
    return Fecha
  }
  
  /***
   * getCurrentDateTime: returns current datetime
   * from version 1.1
   */
  def getCurrentDateTimeJava (): java.util.Calendar = {
    return Calendar.getInstance()
  }
  
  /**
   * Return day, hour, minute and second difference from two datetime
   */
  def getDateTimeDiff(dt_start: Calendar, dt_end: Calendar): huemul_DateTimePart = {
    val dif = dt_end.getTimeInMillis - dt_start.getTimeInMillis 
      
    return new huemul_DateTimePart(dif)
  }
 
  /***
   * setDate: returns calendar for parameters (StringDate = YYYY-MM-DD) 
   */
  def setDate (StringDate: String): Calendar = {
    val lDay: String = StringDate.substring(8, 10)
    val lMonth: String = StringDate.substring(5, 7)
    val lYear: String = StringDate.substring(0, 4)
    var result : Calendar = Calendar.getInstance();
    result.setTime(dateTimeFormat.parse(ReplaceWithParams(dateTimeText,lYear.toInt
                                                                      , lMonth.toInt
                                                                      , lDay.toInt
                                                                      , 0
                                                                      , 0
                                                                      , 0
                                                                      )))
    
    return result
  }
  
  
  /***
   Create a temp table in huemulBigDataGov_Temp (Hive), and persist on HDFS Temp folder
   NumPartitionCoalesce = null for automatically settings
   */
  def CreateTempTable(DF: DataFrame, Alias: String, CreateTable: Boolean, NumPartitionCoalesce: Integer ) {
    //Create parquet in temp folder
    if (CreateTable && SaveTempDF){
      val FileToTemp: String = GlobalSettings.GetDebugTempPath(this, ProcessNameCall, Alias) + ".parquet"      
      println(s"path result for table alias $Alias: $FileToTemp ")
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
    return (Type == ByteType || 
            Type == ShortType ||
            Type == IntegerType ||
            Type == LongType ||
            Type == FloatType ||
            Type == DoubleType ||
            Type == DecimalType )
  }
  
  def IsDateType(Type: DataType): Boolean = {
    return (Type == TimestampType || 
            Type == DateType )
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
  def ReplaceWithParams(text: String, YYYY: Integer, MM: Integer, DD: Integer, hh: Integer, mm: Integer, ss: Integer, AdditionalParams: String = null) : String = {
    var textFinal : String = text
    if (YYYY != null) {
       textFinal = textFinal.replace("{{YYYY}}", YYYY.toString())
                       .replace("{{YY}}", YYYY.toString().substring(2,4))
    }
    
    if (MM != null) {
       textFinal = textFinal.replace("{{MM}}", MM.toString().reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{M}}", MM.toString())
    }
    
    if (DD != null) {
       textFinal = textFinal.replace("{{DD}}", DD.toString().reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{D}}", DD.toString())
    }
    
    if (hh != null) {
       textFinal = textFinal.replace("{{hh}}", hh.toString().reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{h}}", hh.toString())
    }
    
    if (mm != null) {
       textFinal = textFinal.replace("{{mm}}", mm.toString().reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{m}}", mm.toString())
    }
    
    if (ss != null) {
       textFinal = textFinal.replace("{{ss}}", ss.toString().reverse.padTo(2, "0").reverse.mkString)
                       .replace("{{s}}", ss.toString())
    }
    
    if (AdditionalParams != null) {
      AdditionalParams.split(";").foreach { x =>
        
        val Pair = x.split("=")
        //Ajuste en caso que condición sea solo "{{algo}}=" en vez de "{{algo}}=valor"
        textFinal = textFinal.replace(Pair(0), if (Pair.length == 1) "" else Pair(1))
      }
    }
    
            
    return textFinal
  }
    
  /**
   * HasName: determine if argument is not null and is not empty
   */
  def HasName(name: String): Boolean = {
    return name != null && name != ""
  }
  
  def huemul_getDateForLog(): String = {
    val dateTimeFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSSS")  
    val ActualDateTime: Calendar  = Calendar.getInstance()
        
    val Fecha: String = dateTimeFormat.format(ActualDateTime.getTime())
    
    return Fecha
  }
  
  /**
   * Get UniqueId from datetime, random and arbitrary value
   */
  def huemul_GetUniqueId (): String = {
    val MyId: String = this.IdSparkPort
      
    //Get DateTime
    val dateTimeFormat: DateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSSSS")  
    val ActualDateTime: Calendar  = Calendar.getInstance()
    dateTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"))
    
    val Fecha: String = dateTimeFormat.format(ActualDateTime.getTime())
    
    
    //Generate random
    val randomNum: Int  = ThreadLocalRandom.current().nextInt(1, 999998 + 1)
    var Random: String  = ("000000".concat(Integer.toString(randomNum)))
    //System.out.println(Random);
    Random = Random.substring(Random.length()-6, Random.length())
    //System.out.println(Random);
    val Final: String  = Fecha.concat(Random.concat(MyId))
    return Final;
    
    //return this.spark.sql(s"select data_control.fabric_GetUniqueId(${this.IdSparkPort}) as NewId ").first().getAs[String]("NewId")
  }
  
 
  /**
   * Get execution Id from spark monitoring url
   */
 
  def GetIdFromExecution(url: String): String = {
    import spark.implicits._
    val html = Source.fromURL(url)
    val vals = spark.sparkContext.parallelize(
               html.mkString :: Nil)
               
    //spark.read.json(vals).show(truncate = false)
    return spark.read.json(vals).select($"id").first().getString(0)
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
          if (DebugMode) println(SQL)
          if (DebugMode) println(s"JDBC Error: $e")
          if (DebugMode) println(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => println(x) }}")
          Result.ErrorDescription = s"JDBC Error: ${e}"
          Result.IsError = true
      }
      
    return Result
  }
  * 
  */
  
  
  /**
   * Execute a SQL sentence, create a new alias and save de DF result into HDFS
   */
  def DF_ExecuteQuery(Alias: String, SQL: String): DataFrame = {
    if (this.DebugMode && !HideLibQuery) println(SQL)        
    val SQL_DF = this.spark.sql(SQL)            //Ejecuta Query
    if (this.DebugMode) SQL_DF.show()
    //Change alias name
    SQL_DF.createOrReplaceTempView(Alias)         //crea vista sql

    this.CreateTempTable(SQL_DF, Alias, this.DebugMode, null)      //crea tabla temporal para debug
    return SQL_DF
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
      CONTROL_connection.ExecuteJDBC_NoResulSet(s"""
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
        	SELECT ${ReplaceSQLStringNulls(Error_Id)}
          		  ,${ReplaceSQLStringNulls(message.replace("'", "''"))}
                ,${ErrorCode}
          		  ,${ReplaceSQLStringNulls(trace.replace("'", "''"))}
          		  ,${ReplaceSQLStringNulls(className.replace("'", "''"))}
          		  ,${ReplaceSQLStringNulls(fileName.replace("'", "''"))}
          		  ,${ReplaceSQLStringNulls(LineNumber.toString())}
          		  ,${ReplaceSQLStringNulls(methodName.replace("'", "''"))}
          		  ,''
          		  ,${ReplaceSQLStringNulls(getCurrentDateTime())}
          		  ,${ReplaceSQLStringNulls(WhoWriteError)}
             """, false)            
     
    }
        
                               
  }
  
  /**
   * Valid if Hive is alive
   */
  def testHive(): Boolean = {
    var Resultado:Boolean = false
    try {
      val DFNow = this.spark.sql("select now() as IsAlive")
      this.spark.sql("show tables")
      
      if (DFNow.first().getAs[String]("IsAlive").length() >= 10)
        Resultado = true
        
    } catch {
      case e: Exception => e.printStackTrace() // TODO: handle error
      Resultado = false
    }
           
    return Resultado
  }
  
  /**
   * Valid if HDFS is alive
   */
  def testHDFS(path: String): Boolean = {
    var Resultado:Boolean = false
    try {
      val DFWrite = this.spark.sql("select now() as IsAlive")
      val WriteValue = DFWrite.first().getAs[String]("IsAlive")
      
      val fullpath = s"${path}/testHDFS_IsAlive.parquet"
      println(s"test: save HDFS to path: ${fullpath}")
      
      DFWrite.write.mode(SaveMode.Overwrite).format("parquet").save(fullpath)
      DFWrite.unpersist()
      
      //try to open data and read
      val DFRead = this.spark.read.format("parquet").load(fullpath)
      val ReadValue = DFWrite.first().getAs[String]("IsAlive")
     
      if (ReadValue == WriteValue)
        Resultado = true
        
    } catch {
      case e: Exception => e.printStackTrace() // TODO: handle error
      Resultado = false
    }
           
    return Resultado
  }
  
  
}


