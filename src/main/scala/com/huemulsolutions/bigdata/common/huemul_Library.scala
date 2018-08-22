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
import scala.math.BigInt.int2bigInt
      
/*
 * huemul_Library es la clase inicial de la librería huemul-bigdata
 * recibe los parámetros enviados por consola, y los traduce en parámetros de la librería y de cada módulo en particular
 * expone el método .spark
 * Environment: select path and databases to save data
 * Malla_Id: (optional) external Id from process scheduler
 * DebugMode: (optional) default false. show all information to screen (sql sentences, data example, df temporary, etc) 
 * HideLibQuery: (optional) default false. if true, show all sql sentences when DebugMode is true
 * SaveTempDF: (optional) default true. if DebuMode is true, save all DF to persistent HDFS
 */

class huemul_Library (appName: String, args: Array[String], globalSettings: huemul_GlobalPath) extends Serializable  {
  val GlobalSettings = globalSettings
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  
  /*********************
   * ARGUMENTS
   *************************/
  
  val arguments: huemul_Args = new huemul_Args()
  arguments.setArgs(args)  
  val Environment: String = arguments.GetValue("Environment", null, s"MUST be set environment parameter: '${GlobalSettings.GlobalEnvironments}' " )
  val Malla_Id: String = arguments.GetValue("Malla_Id", "0" )
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
  val DebugMode : Boolean = arguments.GetValue("debugmode","false").toBoolean
  val dateFormatNumeric: DateFormat = new SimpleDateFormat("yyyyMMdd");
  val dateTimeFormat: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  val dateTimeText: String = "{{YYYY}}-{{MM}}-{{DD}} {{hh}}:{{mm}}:{{ss}}"
  //var AutoInc: BigInt = 0
  
  val Invoker = new Exception().getStackTrace()
  val ProcessNameCall: String = Invoker(1).getClassName().replace(".", "_").replace("$", "")

  /*********************
   * START SPARK
   *************************/
  
  val spark: SparkSession = if (!TestPlanMode) SparkSession.builder().appName(appName)
                                              //.master("local[*]")
                                              .config("spark.sql.warehouse.dir", warehouseLocation)
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
  val JDBCTXT = GlobalSettings.GetPath(this, GlobalSettings.POSTGRE_Setting) 
  
  val IdSparkPort = if (!TestPlanMode) spark.sql("set").filter("key='spark.driver.port'").select("value").first().getAs[String]("value") else ""
  println(s"Port_Id: ${IdSparkPort}")
  
  //Process Registry
  if (RegisterInControl) {
    val Result = ExecuteJDBC(JDBCTXT,s""" SELECT control_executors_add(
                    '${IdApplication}' -- p_application_Id
                    ,'${IdSparkPort}'  --as p_IdSparkPort
                    ,'${IdPortMonitoring}' --as p_IdPortMonitoring
                    ,'${appName}'  --as p_Executor_Name
                  ) 
        """)
  }
                
  
  
  /*********************
   * START METHOD
   *************************/
  
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
   Create a temp table in huemulLib_Temp (Hive), and persist on HDFS Temp folder
   */
  def CreateTempTable(DF: DataFrame, Alias: String, CreateTable: Boolean) {
    //Create parquet in temp folder
    if (CreateTable && SaveTempDF){
      val FileToTemp: String = GlobalSettings.GetDebugTempPath(this, ProcessNameCall, Alias) + ".parquet"      
      println(s"path result for table alias $Alias: $FileToTemp ")
      DF.write.mode(SaveMode.Overwrite).parquet(FileToTemp)
           
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)       
      fs.setPermission(new org.apache.hadoop.fs.Path(FileToTemp), new FsPermission("770"))
    }
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
  
  def ExecuteJDBC(ConnectionString: String, SQL: String,valor: Boolean = true): huemul_JDBCResult = {
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
  
  /**
   * Execute a SQL sentence, create a new alias and save de DF result into HDFS
   */
  def DF_ExecuteQuery(Alias: String, SQL: String): DataFrame = {
    if (this.DebugMode && !HideLibQuery) println(SQL)        
    val SQL_DF = this.spark.sql(SQL)            //Ejecuta Query
    if (this.DebugMode) SQL_DF.show()
    //Change alias name
    SQL_DF.createOrReplaceTempView(Alias)         //crea vista sql

    this.CreateTempTable(SQL_DF, Alias, this.DebugMode)      //crea tabla temporal para debug
    return SQL_DF
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