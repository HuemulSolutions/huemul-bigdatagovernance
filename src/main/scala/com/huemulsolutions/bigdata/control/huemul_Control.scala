package com.huemulsolutions.bigdata.control


import org.apache.spark.sql.types._
import java.util.Calendar;
import com.huemulsolutions.bigdata.datalake.huemul_DataLake
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.tables._

class huemul_Control (phuemulLib: huemul_Library, ControlParent: huemul_Control, IsSingleton: Boolean = true, RegisterInControlLog: Boolean = true) extends Serializable  {
  val huemulLib = phuemulLib
  val Control_Id: String = huemulLib.huemul_GetUniqueId() 
  
  val Invoker = new Exception().getStackTrace()
  
  val Control_IdParent: String = if (ControlParent == null) null else ControlParent.Control_Id
  val Control_ClassName: String = Invoker(1).getClassName().replace("$", "")
  val Control_ProcessName: String = Invoker(1).getMethodName().replace("$", "")
  val Control_FileName: String = Invoker(1).getFileName.replace("$", "")
  
  var Control_Start_dt: Calendar = Calendar.getInstance()
  var Control_Stop_dt: Calendar = null
  val Control_Error: huemul_ControlError = new huemul_ControlError(huemulLib)
  val Control_Params: scala.collection.mutable.ListBuffer[huemul_LibraryParams] = new scala.collection.mutable.ListBuffer[huemul_LibraryParams]() 
  private var LocalIdStep: String = ""
  private var Step_IsDQ: Boolean = false
  
  //Find process name in control_process
  
  if (RegisterInControlLog && huemulLib.RegisterInControl)
  huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s""" SELECT
  control_process_addOrUpd(
                  '${Control_ClassName}' -- process_id
                  ,''  --as area_id
                  ,'${Control_ClassName}' --as process_name
                  ,'${Control_FileName}' --as process_FileName
                  ,''  --as process_description
                  ,''  --as process_owner
                  ,'${Control_ClassName}' --as mdm_processname
                ) 
      """)
  
  
  //Insert processExcec
  if (RegisterInControlLog && huemulLib.RegisterInControl) {
    println(s"ProcessExec_Id: ${Control_Id}, processName: ${Control_ClassName}")
  huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_processExec_add (
            '${Control_Id}'  --p_processExec_id
           ,'${Control_IdParent}'  --p_processExec_idParent
           ,'${Control_ClassName}'  --p_process_id
           ,'${huemulLib.Malla_Id}'  --p_Malla_id
           ,'${huemulLib.IdApplication}'  --p_Application_id
           , 'user' --p_processExec_WhosRun
           , ${huemulLib.DebugMode} --p_processExec_DebugMode
           , '${huemulLib.Environment}' --p_processExec_Environment
           ,'${Control_ClassName}'  --p_MDM_ProcessName
          )
          """)
          }
  
  //Insert new record
  if (RegisterInControlLog)
  NewStep("Start")
  
  //*****************************************
  //Start Singleton
  //*****************************************  
  if (IsSingleton && huemulLib.RegisterInControl) {
    NewStep("SET SINGLETON MODE")
    var NumCycle: Int = 0
    var ContinueInLoop: Boolean = true
    
    
    while (ContinueInLoop) {
      //Try to add Singleton Mode      
      val Ejec =  huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_singleton_Add (
                        '${Control_ClassName}'  --p_singleton_id
                       ,'${huemulLib.IdApplication}'  --p_application_Id
                       ,'${Control_ClassName}'  --p_singleton_name
                      )
                      """)

      var ApplicationInUse: String = null
      ApplicationInUse = Ejec.ResultSet(0).getString(0)        
      
      //if don't have error, exit
      if (!Ejec.IsError && ApplicationInUse == null)
        ContinueInLoop = false
      else {      
        if (huemulLib.DebugMode) println(s"waiting for Singleton... (class: $Control_ClassName, appId: ${huemulLib.IdApplication} )")
        //if has error, verify other process still alive
        if (NumCycle == 1) //First cicle don't wait
          Thread.sleep(10000)
        
        NumCycle = 1
        // Obtiene procesos pendientes
        val CurrentProcess = huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"select * from control_executors where application_Id = '${ApplicationInUse}'")
        var IdAppFromDataFrame : String = ""
        var IdAppFromAPI: String = ""
        var URLMonitor: String = ""
        var StillAlive: Boolean = true
        
        if (CurrentProcess.ResultSet.length == 0) { //dosn't have records, was eliminted by other process (rarely)
          StillAlive = false            
        } else {
          IdAppFromDataFrame = CurrentProcess.ResultSet(0).getAs[String]("application_id")
          URLMonitor = s"${CurrentProcess.ResultSet(0).getAs[String]("idportmonitoring")}/api/v1/applications/"            
        
          //Get Id App from Spark URL Monitoring          
          try {
            IdAppFromAPI = huemulLib.GetIdFromExecution(URLMonitor)    
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
          val a = huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""SELECT control_executors_remove ('${ApplicationInUse}' )""")
        }
      }              
    }
  }
  
  //*****************************************
  //END Start Singleton
  //*****************************************  

  
  
  def Init_CreateTables() {
    //control_Executors
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""CREATE TABLE IF NOT EXISTS control_Executors (
                                              application_Id         VARCHAR(100)
                                             ,IdSparkPort            VARCHAR(50)
                                             ,IdPortMonitoring       VARCHAR(500)
                                             ,Executor_dtStart       TimeStamp
                                             ,Executor_Name          VARCHAR(100)
                                             ,PRIMARY KEY (application_Id)
                                            )
                             """)
                             
                             
    //control_Singleton
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""CREATE TABLE IF NOT EXISTS control_Singleton (
                                              Singleton_Id         VARCHAR(100)
                                             ,application_Id       VARCHAR(100)
                                             ,Singleton_Name       VARCHAR(100)
                                             ,MDM_fhCreate         TimeStamp
                                             ,PRIMARY KEY (Singleton_Id)
                                            )
                             """)
                             

    //control_Area
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""CREATE TABLE IF NOT EXISTS control_Area (
                                              Area_Id              VARCHAR(50)
                                             ,Area_name            VARCHAR(100)
                                             ,MDM_fhCreate         TimeStamp
                                             ,MDM_ProcessName      VARCHAR(200)
                                             ,PRIMARY KEY (Area_Id)
                                            )
                             """)
                    
    //control_Process
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_Process ( 
                                              process_id           VARCHAR(200)
                                             ,Area_Id              VARCHAR(50)
                                             ,process_name         VARCHAR(200)
                                             ,process_FileName     VARCHAR(200)
                                             ,process_Description  VARCHAR(1000)
                                             ,process_Owner        VARCHAR(200)
                                             ,MDM_ManualChange           Boolean default false
                                             ,MDM_fhCreate         TimeStamp
                                             ,MDM_ProcessName      VARCHAR(200)
                                             ,PRIMARY KEY (process_id)
                                            )
                             
                    """) 

    //control_ProcessExec
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_ProcessExec ( 
                                              processExec_id          VARCHAR(50)
                                             ,processExec_idParent    VARCHAR(50)
                                             ,process_id              VARCHAR(200)
                                             ,Malla_id                VARCHAR(50)
                                             ,application_Id          VARCHAR(100)
                                             ,processExec_IsStart     boolean
                                             ,processExec_IsCancelled boolean
                                             ,processExec_IsEndError  boolean
                                             ,processExec_IsEndOK     boolean
                                             ,processExec_dtStart     timeStamp
                                             ,processExec_dtEnd       TimeStamp
                                             ,processExec_Durhour     int
                                             ,processExec_DurMin      int
                                             ,processExec_DurSec      int
                                             ,processExec_WhosRun     VARCHAR(200)
                                             ,processExec_DebugMode   boolean
                                             ,processExec_Environment VARCHAR(200)
                                             ,Error_Id                VARCHAR(50)
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (processExec_id) 
                                            )
                             
                    """)
                    
     //control_ProcessExecParams
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_ProcessExecParams ( 
                                              processExec_id          VARCHAR(50)
                                             ,processExecParams_Name  VARCHAR(1000)
                                             ,processExecParams_Value VARCHAR(8000)
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (processExec_id, processExecParams_Name) 
                                            )
                    """)               
                    
    //control_ProcessExecStep
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_ProcessExecStep ( 
                                              processExecStep_id      VARCHAR(50)
                                             ,processExec_id          VARCHAR(50)
                                             ,processExecStep_Name        VARCHAR(200)
                                             ,processExecStep_Status      VARCHAR(20)  --running, error, ok
                                             ,processExecStep_dtStart     timeStamp
                                             ,processExecStep_dtEnd       TimeStamp
                                             ,processExecStep_Durhour     int
                                             ,processExecStep_DurMin      int
                                             ,processExecStep_DurSec      int
                                             ,Error_Id                VARCHAR(50)
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (processExecStep_id) 
                                            )
                    """)                    
                    
    //control_RAWFiles
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_RAWFiles ( 
                                              RAWFiles_id             VARCHAR(50)
                                             ,Area_Id                 VARCHAR(50)
                                             ,RAWFiles_LogicalName    VARCHAR(1000)
                                             ,RAWFiles_GroupName      VARCHAR(1000)
                                             ,RAWFiles_Description    VARCHAR(1000)
                                             ,RAWFiles_Owner          VARCHAR(200)
                                             ,RAWFiles_Frecuency      VARCHAR(200)
                                             ,MDM_ManualChange           Boolean default false
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (RAWFiles_id) 
                                            )
                    """)

    //control_RAWFilesDet
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""                    
                     CREATE TABLE IF NOT EXISTS control_RAWFilesDet ( 
                                              RAWFilesDet_id                            VARCHAR(50)
                                             ,RAWFiles_id                               VARCHAR(50)
                                             ,RAWFilesDet_StartDate                     TimeStamp
                                             ,RAWFilesDet_EndDate                       TimeStamp
                                             ,RAWFilesDet_FileName                      VARCHAR(1000)
                                             ,RAWFilesDet_LocalPath                     VARCHAR(1000)
                                             ,RAWFilesDet_GlobalPath                    VARCHAR(1000)
                                             ,RAWFilesDet_Data_ColSeparatorType         VARCHAR(50)
                                             ,RAWFilesDet_Data_ColSeparator             VARCHAR(50)
                                             ,RAWFilesDet_Data_HeaderColumnsString      VARCHAR(8000)
                                             ,RAWFilesDet_Log_ColSeparatorType          VARCHAR(50)
                                             ,RAWFilesDet_Log_ColSeparator              VARCHAR(50)
                                             ,RAWFilesDet_Log_HeaderColumnsString       VARCHAR(8000)
                                             ,RAWFilesDet_Log_NumRowsFieldName          VARCHAR(200)
                                             ,RAWFilesDet_ContactName                   VARCHAR(200)
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (RAWFilesDet_id) 
                                            )
                    """)
                    
    //control_RAWFilesDet
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""                                        
             CREATE TABLE IF NOT EXISTS control_RAWFilesDetFields ( 
                                              RAWFilesDet_id                  VARCHAR(50)
                                             ,RAWFilesDetFields_name          VARCHAR(200)
                                             ,RAWFilesDetFields_Position      INT
                                             ,RAWFilesDetFields_PosIni        VARCHAR(50)
                                             ,RAWFilesDetFields_PosFin        VARCHAR(50)
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (RAWFilesDet_id, RAWFilesDetFields_name) 
                                            )
                    """)
                    
                   
    //control_RAWFilesUse
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_RAWFilesUse ( 
                                              RAWFilesUse_id          VARCHAR(50)
                                             ,RAWFiles_id             VARCHAR(50)
                                             ,Process_Id              VARCHAR(200)
                                             ,ProcessExec_Id          VARCHAR(50)
                                             ,RAWFiles_fullName       VARCHAR(1000)
                                             ,RAWFiles_fullPath       VARCHAR(1000)
                                             ,RAWFiles_numRows        VARCHAR(50)
                                             ,RAWFiles_HeaderLine     VARCHAR(8000)
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (RAWFilesUse_id) 
                                            )
                    """)                    
                    
    //control_Tables
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_Tables ( 
                                              Table_id                VARCHAR(50)
                                             ,Area_Id                 VARCHAR(50)
                                             ,Table_BBDDName          VARCHAR(200)
                                             ,Table_Name              VARCHAR(200)
                                             ,Table_Description       VARCHAR(1000)
                                             ,Table_BusinessOwner     VARCHAR(200)
                                             ,Table_ITOwner           VARCHAR(200)
                                             ,Table_PartitionField    VARCHAR(200)
                                             ,Table_TableType         VARCHAR(50)  --reference, master, transactional
                                             ,Table_StorageType       VARCHAR(50)
                                             ,Table_LocalPath         VARCHAR(1000)
                                             ,Table_GlobalPath        VARCHAR(1000)
                                             ,Table_SQLCreate         VARCHAR(8000)
                                             ,MDM_ManualChange           Boolean default false
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)                                             
                                             ,PRIMARY KEY (Table_id) 
                                            )
                    """)
                    
    //control_TablesFK
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_TablesRel (                   
                                              TableRel_id               VARCHAR(50)
                                             ,Table_idPK                VARCHAR(50)
                                             ,Table_idFK                VARCHAR(50)
                                             ,TableFK_NameRelationship  VARCHAR(100)
                                             ,TableRel_ValidNull      boolean
                                             ,MDM_ManualChange           Boolean default false
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)                                             
                                             ,PRIMARY KEY (TableRel_id) 
                                            )
                    """) 
                    
    //control_TablesRelCol
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_TablesRelCol ( 
                                              TableRel_id                VARCHAR(50)
                                             ,Column_idFK                VARCHAR(50)
                                             ,Column_idPK                VARCHAR(50)
                                             ,MDM_ManualChange           Boolean default false
                                             ,MDM_fhCreate               TimeStamp
                                             ,MDM_ProcessName            VARCHAR(200)
                                             ,PRIMARY KEY (TableRel_id, Column_idFK) 
                                            )
                                                                       
                    """)
                    
    //control_Columns
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_Columns ( 
                                              Column_id                  VARCHAR(50)
                                             ,Table_id                   VARCHAR(50)
                                             ,Column_Position            int
                                             ,Column_Name                VARCHAR(200)
                                             ,Column_Description         VARCHAR(1000)
                                             ,Column_Formula             VARCHAR(1000)
                                             ,Column_DataType            VARCHAR(50)
                                             ,Column_SensibleData        boolean
                                             ,Column_EnableDTLog         boolean
                                             ,Column_EnableOldValue      boolean
                                             ,Column_EnableProcessLog    boolean
                                             ,Column_DefaultValue        VARCHAR(1000)
                                             ,Column_SecurityLevel       VARCHAR(200)
                                             ,Column_Encrypted           VARCHAR(200)
                                             ,Column_ARCO                VARCHAR(200)
                                             ,Column_Nullable            boolean
                                             ,Column_IsPK                boolean
                                             ,Column_IsUnique            boolean
                                             ,Column_DQ_MinLen           int
                                             ,Column_DQ_MaxLen           int 
                                             ,Column_DQ_MinDecimalValue  Decimal(30,10) 
                                             ,Column_DQ_MaxDecimalValue  Decimal(30,10) 
                                             ,Column_DQ_MinDateTimeValue VARCHAR(50) 
                                             ,Column_DQ_MaxDateTimeValue VARCHAR(50)
                                             ,MDM_Active                 Boolean default false
                                             ,MDM_ManualChange           Boolean default false
                                             ,MDM_fhCreate               TimeStamp
                                             ,MDM_ProcessName            VARCHAR(200)
                                             ,PRIMARY KEY (Column_id) 
                                            )
                                                                       
                    """)
    //control_TablesUse
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_TablesUse ( 
                                              Table_id                VARCHAR(50)
                                             ,Process_Id              VARCHAR(200)
                                             ,ProcessExec_Id          VARCHAR(50)
                                             ,TableUse_Read           boolean
                                             ,TableUse_Write          boolean
                                             ,TableUse_numRowsNew     BIGINT
                                             ,TableUse_numRowsUpdate  BIGINT
                                             ,TableUse_numRowsMarkDelete  BIGINT
                                             ,TableUse_numRowsTotal   BIGINT
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (Process_Id, Table_id, ProcessExec_Id) 
                                            )
                    """)
                    
    //control_DQ
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                            CREATE TABLE IF NOT EXISTS control_DQ (
                                              DQ_Id                   VARCHAR(50) 
                                             ,Table_id                VARCHAR(50) 
                                             ,Process_Id              VARCHAR(200)
                                             ,ProcessExec_Id          VARCHAR(50)
                                             ,Column_Id               VARCHAR(50)
                                             ,Column_Name             VARCHAR(200)
                                             ,Dq_AliasDF              VARCHAR(200)
                                             ,DQ_Name                 VARCHAR(200)
                                             ,DQ_Description          VARCHAR(1000)
                                             ,DQ_IsAggregate          Boolean
                                             ,DQ_RaiseError           Boolean
                                             ,DQ_SQLFormula           VARCHAR(8000)
                                             ,DQ_Error_MaxNumRows     BigInt
                                             ,DQ_Error_MaxPercent     Decimal(30,10)
                                             ,DQ_IsError              Boolean
                                             ,DQ_ResultDQ             VARCHAR(8000)
                                             ,DQ_ErrorCode            Integer
                                             ,DQ_NumRowsOK            BigInt
                                             ,DQ_NumRowsError         BigInt
                                             ,DQ_NumRowsTotal         BigInt
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (DQ_Id) 
                                            )
                    """)
                    
                    
    //control_RAWFilesUseDQ
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
        CREATE TABLE IF NOT EXISTS control_RAWFilesUseDQ ( 
                                                      RAWFilesUseDQ_id                 VARCHAR(50)
                                                     ,RAWFilesUse_id                   VARCHAR(50)
                                                     ,RAWFilesUseDQ_Column             VARCHAR(200)
                                                     ,RAWFilesUseDQ_Description        VARCHAR(1000)
                                                     ,RAWFilesUseDQ_IsError            Boolean
                                                     ,RAWFilesUseDQ_ErrorMsg           VARCHAR(8000)
                                                     ,RAWFilesUseDQ_RealNumTotal       Decimal(20,10)
                                                     ,RAWFilesUseDQ_RealNumOk          Decimal(20,10)
                                                     ,RAWFilesUseDQ_RealNumError       Decimal(20,10)
                                                     ,RAWFilesUseDQ_RealPercOK         Decimal(20,10)
                                                     ,RAWFilesUseDQ_TargetAbsMaxOK     Decimal(20,10)
                                                     ,RAWFilesUseDQ_TargetAbsMinOK     Decimal(20,10)
                                                     ,RAWFilesUseDQ_TargetPercMaxOK    Decimal(20,10)
                                                     ,RAWFilesUseDQ_TargetPercMinOK    Decimal(20,10)
        
                                                     ,MDM_fhCreate            TimeStamp
                                                     ,MDM_ProcessName         VARCHAR(200)
                                                     ,PRIMARY KEY (RAWFilesUseDQ_id) 
                                                    )
                    """)
                    
    
                    
     //control_Error
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_Error ( 
                                              Error_Id          VARCHAR(50)
                                             ,Error_Message                text
                                             ,Error_Code           Integer
                                             ,Error_Trace                text
                                             ,Error_ClassName                text
                                             ,Error_FileName                text
                                             ,Error_LIneNumber                text
                                             ,Error_MethodName                text
                                             ,Error_Detail                text
                                             ,MDM_fhCrea            TimeStamp
                                             ,MDM_ProcessName         varchar(1000)
                                             ,PRIMARY KEY (Error_Id)
                                            )
                                                                       
                    """)
                    
                
      //control_date
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
               CREATE TABLE IF NOT EXISTS control_Date ( 
                                      Date_Id		DATE
                                      ,Date_year	INT
                                      ,Date_Month	INT
                                      ,Date_Day	INT
                                      ,Date_DayOfWeek	INT
                                      ,Date_DayName	VARCHAR(20)
                                      ,Date_MonthName	VARCHAR(20)
                                      ,Date_Quarter	INT
                                      ,Date_Week	INT
                                      ,Date_isWeekEnd	BOOLEAN
                                      ,Date_IsWorkDay	BOOLEAN
                                      ,Date_IsBankWorkDay	BOOLEAN
                                      ,Date_NumWorkDay		INT
                                      ,Date_NumWorkDayRev	INT
                                      ,MDM_fhCreate            TimeStamp default current_timestamp
                                      ,MDM_ProcessName         VARCHAR(200) default ''
                                      ,PRIMARY KEY (Date_Id) 
                                      )
                                                                       
                    """)
                    
    //control_ProcessExec
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
                CREATE TABLE IF NOT EXISTS control_TestPlan ( 
                                              testPlan_Id             VARCHAR(200)
                                             ,testPlanGroup_Id        VARCHAR(200)
                                             ,processExec_id          VARCHAR(200)
                                             ,process_id              VARCHAR(200)
                                             ,testPlan_name           VARCHAR(200)
                                             ,testPlan_description    VARCHAR(1000)
                                             ,testPlan_resultExpected VARCHAR(1000)  
                                             ,testPlan_resultReal     VARCHAR(1000)
                                             ,testPlan_IsOK           boolean
                                             ,MDM_fhCreate            TimeStamp
                                             ,MDM_ProcessName         VARCHAR(200)
                                             ,PRIMARY KEY (testPlan_Id) 
                                            )
                             
                    """)
                    
       //create functions
                    
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""                    
CREATE OR REPLACE FUNCTION control_TestPlan_add (   p_testPlan_Id              VARCHAR(200)
                       , p_testPlanGroup_Id          VARCHAR(200)
                       , p_processExec_id          VARCHAR(200)
                       , p_process_id    VARCHAR(200)
                       , p_testPlan_name    VARCHAR(200)
                       , p_testPlan_description    VARCHAR(1000)
                       , p_testPlan_resultExpected  VARCHAR(1000)
                       , p_testPlan_resultReal    varchar(1000)
                       , p_testPlan_IsOK            boolean    
                       , p_Executor_Name     			VARCHAR(100))  
RETURNS VOID AS
$$
BEGIN
	INSERT INTO control_TestPlan ( testPlan_Id             
                                ,testPlanGroup_Id        
                                ,processExec_id          
                                ,process_id              
                                ,testPlan_name           
                                ,testPlan_description    
                                ,testPlan_resultExpected   
                                ,testPlan_resultReal     
                                ,testPlan_IsOK           
                                ,MDM_fhCreate            
                                ,MDM_ProcessName         
                              )
  SELECT p_testPlan_Id
       , p_testPlanGroup_Id
       , p_processExec_id
       , p_process_id
       , p_testPlan_name
       , p_testPlan_description
       , p_testPlan_resultExpected
       , p_testPlan_resultReal
       , p_testPlan_IsOK
       , current_timestamp
       , p_Executor_Name;
       
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")


    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""                    
CREATE OR REPLACE FUNCTION control_process_addOrUpd (   p_process_id              VARCHAR(200)
										   , p_area_id                 VARCHAR(50)
										   , p_process_name            VARCHAR(200)
                       , p_process_FileName        VARCHAR(200)
										   , p_process_description     VARCHAR(1000)
										   , p_process_owner           VARCHAR(200)
										   , p_mdm_processname         VARCHAR(200))  
RETURNS VOID AS
$$
BEGIN
	-- first try to update the key
	UPDATE control_process 
  SET area_id = CASE WHEN MDM_ManualChange THEN area_id ELSE p_area_id END
	   , process_name			= CASE WHEN MDM_ManualChange THEN process_name ELSE p_process_name END
     , process_FileName = CASE WHEN MDM_ManualChange THEN process_FileName ELSE p_process_FileName END
	   , process_description	= CASE WHEN MDM_ManualChange THEN process_description ELSE p_process_description END
	   , process_owner			= CASE WHEN MDM_ManualChange THEN process_owner ELSE p_process_owner END
	WHERE process_id = p_process_id;

	IF found THEN
		RETURN;
	END IF;

	-- not there, so try to insert the key
	-- if someone else inserts the same key concurrently,
	-- we could get a unique-key failure

	INSERT INTO control_process (process_id
							   , area_id
							   , process_name
                 , process_FileName
							   , process_description
							   , process_owner
							   , mdm_fhcreate
							   , mdm_processname) 	
	SELECT   p_process_id
		   , p_area_id
		   , p_process_name
       , p_process_FileName
		   , p_process_description
		   , p_process_owner
		   , current_timestamp
		   , p_mdm_processname;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

  huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_singleton_Add (p_singleton_id                 VARCHAR(100)
										   , p_application_Id              VARCHAR(100)
										   , p_singleton_name                 VARCHAR(100))  
RETURNS VARCHAR(100) AS
$$
DECLARE
	V_application_id VARCHAR(100);
BEGIN
	V_application_id := NULL;
	BEGIN
		INSERT INTO control_singleton (singleton_id
								   , application_id
								   , singleton_name
								   , mdm_fhcreate) 	
		SELECT   p_singleton_id
			   , p_application_Id
			   , p_singleton_name
			   , current_timestamp;
			   		
	EXCEPTION
		WHEN unique_violation THEN V_application_id := (SELECT application_id FROM control_singleton WHERE singleton_id = p_singleton_id);
		WHEN others THEN V_application_id := (SELECT application_id FROM control_singleton WHERE singleton_id = p_singleton_id);
	END;
	RETURN V_application_id;
END;
$$
LANGUAGE plpgsql;
""")

huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_singleton_remove (p_singleton_id                 VARCHAR(100))  
RETURNS VOID AS
$$
BEGIN
	DELETE FROM control_singleton 
  WHERE singleton_id = p_singleton_id;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

  huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_executors_remove (   p_application_Id              VARCHAR(100))  
RETURNS VOID AS
$$
BEGIN
	DELETE FROM control_singleton where application_id = p_application_Id;
	DELETE FROM control_executors where application_id = p_application_Id;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

  huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""                    
CREATE OR REPLACE FUNCTION control_executors_add (   p_application_Id              VARCHAR(100)
										   , p_IdSparkPort                 VARCHAR(50)
										   , p_IdPortMonitoring            VARCHAR(500)
										   , p_Executor_Name     			VARCHAR(100))  
RETURNS VOID AS
$$
BEGIN
	INSERT INTO control_executors (application_Id
							   , IdSparkPort
							   , IdPortMonitoring
							   , Executor_dtStart
							   , Executor_Name) 	
	SELECT   p_application_Id
		   , p_IdSparkPort
		   , p_IdPortMonitoring
		   , current_timestamp
		   , p_Executor_Name;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")


    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_processExec_add (     p_processExec_id          VARCHAR(50)
                                             ,p_processExec_idParent    VARCHAR(50)
                                             ,p_process_id              VARCHAR(200)
                                             ,p_Malla_id                VARCHAR(50)
											                       ,p_application_Id			VARCHAR(100)
                                             ,p_processExec_WhosRun     VARCHAR(200)
                                             ,p_processExec_DebugMode   boolean
                                             ,p_processExec_Environment VARCHAR(200)
                                             ,p_MDM_ProcessName         VARCHAR(200))
RETURNS VOID AS
$$
BEGIN
	INSERT INTO control_processExec (
                           processexec_id
                          ,processexec_idparent
                          ,process_id
                          ,malla_id
						              ,application_Id
                          ,processexec_isstart
                          ,processexec_iscancelled
                          ,processexec_isenderror
                          ,processexec_isendok
                          ,processexec_dtstart
                          ,processexec_dtend
                          ,processexec_durhour
                          ,processexec_durmin
                          ,processexec_dursec
                          ,processexec_whosrun
                          ,processexec_debugmode
                          ,processexec_environment
                          ,error_id
                          ,mdm_fhcreate
                          ,mdm_processname)
     SELECT    p_processexec_id
			  ,p_processexec_idparent
			  ,p_process_id
			  ,p_malla_id
			  ,p_application_Id
			  ,true
			  ,false
			  ,false
			  ,false
			  ,current_timestamp
			  ,null
			  ,0
			  ,0
			  ,0
			  ,p_processexec_whosrun
			  ,p_processexec_debugmode
			  ,p_processexec_environment
			  ,null
			  ,current_timestamp
			  ,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_processExec_Finish (p_processExec_id          VARCHAR(50)
										 	 ,p_processExecStep_id		VARCHAR(50)
                                             ,p_error_id              VARCHAR(50))
RETURNS VOID AS
$$
BEGIN
	UPDATE control_processExecStep
	SET processexecstep_status = case when p_error_id is null or p_error_id = '' then 'OK' else 'ERROR' end
	   ,processexecstep_dtend = current_timestamp
     ,error_id				  = p_error_id
	WHERE processExecStep_id = p_processExecStep_id;
	
	UPDATE control_processExec
	SET processexec_iscancelled = false
	  ,processexec_isenderror = case when p_error_id is null or p_error_id = '' then false else true end
	  ,processexec_isendok	  = case when p_error_id is null or p_error_id = '' then true else false end
	  ,processexec_dtend	  = current_timestamp
	  ,processexec_durhour	  = 0
	  ,processexec_durmin	  = 0
	  ,processexec_dursec	  = 0
	  ,error_id				  = p_error_id
	WHERE processexec_id = P_processexec_id;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_processExecStep_add ( p_processExecStep_id      VARCHAR(50)
											 ,p_processExecStep_idAnt   VARCHAR(50)
                                             ,p_processExec_id          VARCHAR(50)
                                             ,p_processExecStep_Name        VARCHAR(200)
                                             ,p_MDM_ProcessName         VARCHAR(200))
RETURNS VOID AS
$$
BEGIN
	IF (p_processExecStep_idAnt IS NOT NULL) THEN
		UPDATE control_processExecStep
    SET processexecstep_status = 'OK'
           ,processexecstep_dtend = current_timestamp
		WHERE processExecStep_id = p_processExecStep_idAnt;
	END IF;
	
	INSERT INTO control_processExecStep (  processexecstep_id
										  ,processexec_id
										  ,processexecstep_name
										  ,processexecstep_status
										  ,processexecstep_dtstart
										  ,processexecstep_dtend
										  ,processexecstep_durhour
										  ,processexecstep_durmin
										  ,processexecstep_dursec
										  ,error_id
										  ,mdm_fhcreate
										  ,mdm_processname)
     SELECT    p_processexecstep_id
			  ,p_processexec_id
			  ,p_processexecstep_name
			  ,'RUNNING'
			  ,current_timestamp
			  ,null
			  ,0
			  ,0
			  ,0
			  ,null
			  ,current_timestamp
			  ,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")


    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_DQ_add (  p_DQ_Id                   VARCHAR(50) 
								 ,p_Table_name              VARCHAR(200) 
								 ,p_BBDD_name               VARCHAR(200) 
								 ,p_Process_Id              VARCHAR(200)
								 ,p_ProcessExec_Id          VARCHAR(50)
								 ,p_Column_Name             VARCHAR(200)
								 ,p_Dq_AliasDF              VARCHAR(200)
								 ,p_DQ_Name                 VARCHAR(200)
								 ,p_DQ_Description          VARCHAR(1000)
								 ,p_DQ_IsAggregate          Boolean
								 ,p_DQ_RaiseError           Boolean
								 ,p_DQ_SQLFormula           VARCHAR(8000)
								 ,p_DQ_Error_MaxNumRows     BigInt
								 ,p_DQ_Error_MaxPercent     Decimal(30,10)
								 ,p_DQ_IsError              Boolean
								 ,p_DQ_ResultDQ             VARCHAR(8000)
                 ,p_DQ_ErrorCode            Integer
								 ,p_DQ_NumRowsOK            BigInt
								 ,p_DQ_NumRowsError         BigInt
								 ,p_DQ_NumRowsTotal         BigInt
								 ,p_MDM_ProcessName         VARCHAR(200))
RETURNS VOID AS
$$
DECLARE
	LocalTable_id VARCHAR(50);
	LocalColumn_Id VARCHAR(50);
BEGIN
	LocalTable_id := (select Table_id from control_Tables where Table_BBDDName = p_BBDD_name and Table_Name = p_Table_name);
	LocalColumn_Id := (select Column_id from control_Columns where Table_Id = LocalTable_id and Column_Name = p_Column_Name);
	
	INSERT INTO control_DQ (
                      dq_id
                     ,table_id
                     ,process_id
                     ,processexec_id
                     ,column_id
                     ,column_name
                     ,dq_aliasdf
                     ,dq_name
                     ,dq_description
                     ,dq_isaggregate
                     ,dq_raiseerror
                     ,dq_sqlformula
                     ,dq_error_maxnumrows
                     ,dq_error_maxpercent
                     ,dq_iserror
                     ,dq_resultdq
                     ,dq_ErrorCode
                     ,dq_numrowsok
                     ,dq_numrowserror
                     ,dq_numrowstotal
                     ,mdm_fhcreate
                     ,mdm_processname)
	SELECT    p_dq_id
			 ,LocalTable_id
			 ,p_process_id
			 ,p_processexec_id
			 ,LocalColumn_Id
			 ,p_column_name
			 ,p_dq_aliasdf
			 ,p_dq_name
			 ,p_dq_description
			 ,p_dq_isaggregate
			 ,p_dq_raiseerror
			 ,p_dq_sqlformula
			 ,p_dq_error_maxnumrows
			 ,p_dq_error_maxpercent
			 ,p_dq_iserror
			 ,p_dq_resultdq
       ,p_dq_ErrorCode
			 ,p_dq_numrowsok
			 ,p_dq_numrowserror
			 ,p_dq_numrowstotal
			 ,current_timestamp
			 ,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")


    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_rawFiles_add (    p_RAWFiles_id             VARCHAR(50)
										 ,p_RAWFiles_LogicalName    VARCHAR(1000)
										 ,p_RAWFiles_GroupName      VARCHAR(1000)
										 ,p_RAWFiles_Description    VARCHAR(1000)
										 ,p_RAWFiles_Owner          VARCHAR(200)
										 ,p_RAWFiles_Frecuency      VARCHAR(200)
										 ,p_MDM_ProcessName         VARCHAR(200))
RETURNS VOID AS
$$
BEGIN
	UPDATE control_rawFiles
  SET rawfiles_description	= case when MDM_ManualChange then rawfiles_description	else p_rawfiles_description	end
		 ,rawfiles_owner		= case when MDM_ManualChange then rawfiles_owner		else p_rawfiles_owner		end
		 ,rawfiles_frecuency	= case when MDM_ManualChange then rawfiles_frecuency	else p_rawfiles_frecuency	end
	WHERE rawfiles_logicalname = p_rawfiles_logicalname
  and   rawfiles_groupname   = p_rawfiles_groupname;
	
	IF FOUND THEN
		RETURN;
	END IF;
	
	INSERT INTO control_rawFiles (rawfiles_id
								 ,area_id
								 ,rawfiles_logicalname
								 ,rawfiles_groupname
								 ,rawfiles_description
								 ,rawfiles_owner
								 ,rawfiles_frecuency
								 ,MDM_ManualChange
								 ,mdm_fhcreate
								 ,mdm_processname)
	SELECT    p_rawfiles_id
			 ,''
			 ,p_rawfiles_logicalname
			 ,p_rawfiles_groupname
			 ,p_rawfiles_description
			 ,p_rawfiles_owner
			 ,p_rawfiles_frecuency
			 ,false as MDM_ManualChange
			 ,current_timestamp
			 ,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")


    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""

CREATE or replace FUNCTION control_RAWFilesDet_add (     p_RAWFilesDet_id                            VARCHAR(50)
                                             ,p_rawfiles_logicalname                      VARCHAR(1000)
                                             ,p_rawfiles_groupname                        VARCHAR(1000)
                                             ,p_RAWFilesDet_StartDate                     TimeStamp
                                             ,p_RAWFilesDet_EndDate                       TimeStamp
                                             ,p_RAWFilesDet_FileName                      VARCHAR(1000)
                                             ,p_RAWFilesDet_LocalPath                     VARCHAR(1000)
                                             ,p_RAWFilesDet_GlobalPath                    VARCHAR(1000)
                                             ,p_RAWFilesDet_Data_ColSeparatorType         VARCHAR(50)
                                             ,p_RAWFilesDet_Data_ColSeparator             VARCHAR(50)
                                             ,p_RAWFilesDet_Data_HeaderColumnsString      VARCHAR(8000)
                                             ,p_RAWFilesDet_Log_ColSeparatorType          VARCHAR(50)
                                             ,p_RAWFilesDet_Log_ColSeparator              VARCHAR(50)
                                             ,p_RAWFilesDet_Log_HeaderColumnsString       VARCHAR(8000)
                                             ,p_RAWFilesDet_Log_NumRowsFieldName          VARCHAR(200)
                                             ,p_RAWFilesDet_ContactName                   VARCHAR(200)
                                             ,p_MDM_ProcessName         VARCHAR(200))
RETURNS VOID AS
$$
DECLARE
	Localrawfilesdet_id VARCHAR(50);
  LocalRAWFiles_id  VARCHAR(50);
BEGIN

  LocalRAWFiles_id := ( SELECT RAWFiles_Id
                        FROM control_rawFiles
                        WHERE rawfiles_logicalname = p_rawfiles_logicalname
                        and   rawfiles_groupname   = p_rawfiles_groupname
                      );

	UPDATE control_RAWFilesDet
  SET rawfilesdet_enddate      = p_rawfilesdet_enddate						
	   ,rawfilesdet_filename					= p_rawfilesdet_filename					
	   ,rawfilesdet_localpath					= p_rawfilesdet_localpath					
	   ,rawfilesdet_globalpath					= p_rawfilesdet_globalpath					
	   ,rawfilesdet_data_colseparatortype		= p_rawfilesdet_data_colseparatortype		
	   ,rawfilesdet_data_colseparator			= p_rawfilesdet_data_colseparator			
	   ,rawfilesdet_data_headercolumnsstring	= p_rawfilesdet_data_headercolumnsstring	
	   ,rawfilesdet_log_colseparatortype		= p_rawfilesdet_log_colseparatortype		
	   ,rawfilesdet_log_colseparator			= p_rawfilesdet_log_colseparator			
	   ,rawfilesdet_log_headercolumnsstring		= p_rawfilesdet_log_headercolumnsstring		
	   ,rawfilesdet_log_numrowsfieldname		= p_rawfilesdet_log_numrowsfieldname		
	   ,rawfilesdet_contactname					= p_rawfilesdet_contactname					
	WHERE RAWFiles_id 			= LocalRAWFiles_id
	AND   RAWFilesDet_StartDate = p_RAWFilesDet_StartDate;
	
	IF FOUND THEN
		Localrawfilesdet_id := (SELECT rawfilesdet_id FROM control_RAWFilesDet WHERE RAWFiles_id = LocalRAWFiles_id AND RAWFilesDet_StartDate = p_RAWFilesDet_StartDate);
		DELETE FROM control_RAWFilesDetFields WHERE rawfilesdet_id = Localrawfilesdet_id;
		RETURN;
	END IF;
	
	INSERT INTO control_RAWFilesDet (   rawfilesdet_id
									   ,rawfiles_id
									   ,rawfilesdet_startdate
									   ,rawfilesdet_enddate
									   ,rawfilesdet_filename
									   ,rawfilesdet_localpath
									   ,rawfilesdet_globalpath
									   ,rawfilesdet_data_colseparatortype
									   ,rawfilesdet_data_colseparator
									   ,rawfilesdet_data_headercolumnsstring
									   ,rawfilesdet_log_colseparatortype
									   ,rawfilesdet_log_colseparator
									   ,rawfilesdet_log_headercolumnsstring
									   ,rawfilesdet_log_numrowsfieldname
									   ,rawfilesdet_contactname
									   ,mdm_fhcreate
									   ,mdm_processname)
	SELECT  p_rawfilesdet_id
		   ,LocalRAWFiles_id
		   ,p_rawfilesdet_startdate
		   ,p_rawfilesdet_enddate
		   ,p_rawfilesdet_filename
		   ,p_rawfilesdet_localpath
		   ,p_rawfilesdet_globalpath
		   ,p_rawfilesdet_data_colseparatortype
		   ,p_rawfilesdet_data_colseparator
		   ,p_rawfilesdet_data_headercolumnsstring
		   ,p_rawfilesdet_log_colseparatortype
		   ,p_rawfilesdet_log_colseparator
		   ,p_rawfilesdet_log_headercolumnsstring
		   ,p_rawfilesdet_log_numrowsfieldname
		   ,p_rawfilesdet_contactname
		   ,current_timestamp
		   ,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;

""")

    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE or replace FUNCTION control_RAWFilesDetFields_add (
                              p_rawfiles_logicalname            VARCHAR(1000)
                             ,p_rawfiles_groupname              VARCHAR(1000)
                             ,p_RAWFilesDet_StartDate           TimeStamp
														 ,P_RAWFilesDetFields_name          VARCHAR(200)
														 ,P_RAWFilesDetFields_Position      INT
														 ,P_RAWFilesDetFields_PosIni        VARCHAR(50)
														 ,P_RAWFilesDetFields_PosFin        VARCHAR(50)
														 ,P_MDM_ProcessName                 VARCHAR(200))
RETURNS VOID AS
$$
DECLARE
  Localrawfiles_id VARCHAR(50);
  Localrawfilesdet_id VARCHAR(50);
BEGIN
  Localrawfiles_id := (SELECT RawFiles_Id
                          FROM control_RAWFiles
                          WHERE rawfiles_logicalname = p_rawfiles_logicalname
                          AND   rawfiles_groupname = p_rawfiles_groupname);

  Localrawfilesdet_id := (SELECT rawfilesdet_id
                          FROM control_RAWFiles
                          WHERE RawFiles_Id = Localrawfiles_id
                          AND   RAWFilesDet_StartDate = p_RAWFilesDet_StartDate);

	INSERT INTO control_RAWFilesDetFields (  rawfilesdet_id
											,rawfilesdetfields_name
											,rawfilesdetfields_position
											,rawfilesdetfields_posini
											,rawfilesdetfields_posfin
											,mdm_fhcreate
											,mdm_processname)
	SELECT   Localrawfilesdet_id
			,p_rawfilesdetfields_name
			,p_rawfilesdetfields_position
			,p_rawfilesdetfields_posini
			,p_rawfilesdetfields_posfin
			,current_timestamp
			,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE or replace FUNCTION control_rawFilesUse_add (p_rawfiles_logicalname            VARCHAR(1000)
												 ,p_rawfiles_groupname              VARCHAR(1000)
												 ,p_RAWFilesUse_id          VARCHAR(50)
												 ,p_Process_Id              VARCHAR(200)
												 ,p_ProcessExec_Id          VARCHAR(50)
												 ,p_RAWFiles_fullName       VARCHAR(1000)
												 ,p_RAWFiles_fullPath       VARCHAR(1000)
												 ,p_RAWFiles_numRows        VARCHAR(50)
												 ,p_RAWFiles_HeaderLine     VARCHAR(8000)
												 ,p_MDM_ProcessName         VARCHAR(200))
RETURNS VOID AS
$$
DECLARE
  Localrawfiles_id VARCHAR(50);
BEGIN
  Localrawfiles_id := (SELECT RawFiles_Id
                          FROM control_RAWFiles
                          WHERE rawfiles_logicalname = p_rawfiles_logicalname
                          AND   rawfiles_groupname = p_rawfiles_groupname);

	INSERT INTO control_rawFilesUse (  rawfilesuse_id
									 ,rawfiles_id
									 ,process_id
									 ,processexec_id
									 ,rawfiles_fullname
									 ,rawfiles_fullpath
									 ,rawfiles_numrows
									 ,rawfiles_headerline
									 ,mdm_fhcreate
									 ,mdm_processname)
	SELECT    p_rawfilesuse_id
			 ,Localrawfiles_id
			 ,p_process_id
			 ,p_processexec_id
			 ,p_rawfiles_fullname
			 ,p_rawfiles_fullpath
			 ,p_rawfiles_numrows
			 ,p_rawfiles_headerline
			 ,current_timestamp
			 ,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_ProcessExecParams_add (    p_processExec_id          VARCHAR(50)
                                             ,p_processExecParams_Name  VARCHAR(1000)
                                             ,p_processExecParams_Value VARCHAR(8000)
                                             ,p_MDM_ProcessName         VARCHAR(200))
RETURNS VOID AS
$$
BEGIN
	INSERT INTO control_ProcessExecParams (processexec_id
								  ,processexecparams_name
								  ,processexecparams_value
								  ,mdm_fhcreate
								  ,mdm_processname)
	SELECT p_processexec_id
		  ,p_processexecparams_name
		  ,p_processexecparams_value
		  ,current_timestamp
		  ,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""") 


huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_TablesUse_add (    p_Table_Name		VARCHAR(200)
												  	 ,p_Table_BBDDName		VARCHAR(200)
												  	 ,p_Process_Id              VARCHAR(200)
													 ,p_ProcessExec_Id          VARCHAR(50)
													 ,p_TableUse_Read           boolean
													 ,p_TableUse_Write          boolean
													 ,p_TableUse_numRowsNew     BIGINT
													 ,p_TableUse_numRowsUpdate  BIGINT
													 ,p_TableUse_numRowsMarkDelete  BIGINT
													 ,p_TableUse_numRowsTotal   BIGINT
													 ,p_MDM_ProcessName         VARCHAR(200)
												 )
RETURNS VOID AS
$$
DECLARE
	LocalTable_id VARCHAR(50);
BEGIN
	LocalTable_id := (SELECT Table_Id FROM Control_Tables WHERE Table_Name = p_Table_Name and Table_BBDDName = p_Table_BBDDName);

	INSERT INTO control_TablesUse ( table_id
								   ,process_id
								   ,processexec_id
								   ,tableuse_read
								   ,tableuse_write
								   ,tableuse_numrowsnew
								   ,tableuse_numrowsupdate
								   ,tableuse_numrowsmarkdelete
								   ,tableuse_numrowstotal
								   ,mdm_fhcreate
								   ,mdm_processname)
	SELECT  LocalTable_id
		   ,p_process_id
		   ,p_processexec_id
		   ,p_tableuse_read
		   ,p_tableuse_write
		   ,p_tableuse_numrowsnew
		   ,p_tableuse_numrowsupdate
		   ,p_tableuse_numrowsmarkdelete
		   ,p_tableuse_numrowstotal
		   ,current_timestamp
		   ,p_mdm_processname
		   ;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""") 

huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_TablesRel_add (   p_TableRel_id				VARCHAR(50)
										   , p_Table_NamePK               VARCHAR(50)
										   , p_Table_BBDDPK               VARCHAR(50)
										   , p_Table_NameFK               VARCHAR(50)
										   , p_Table_BBDDFK               VARCHAR(50)
										   , p_TableFK_NameRelationship         VARCHAR(200)
										   , p_mdm_processname         VARCHAR(200))  
RETURNS VARCHAR(50) AS
$$
DECLARE
	lTableRel_id VARCHAR(50);
	FK_Id VARCHAR(50);
	PK_ID VARCHAR(50);
BEGIN
	PK_ID := (SELECT Table_Id FROM control_Tables 
			  WHERE Table_Name = p_Table_NamePK
			  AND   Table_BBDDName = p_Table_BBDDPK
			 );
			 
	FK_Id := (SELECT Table_Id FROM control_Tables 
			  WHERE Table_Name = p_Table_NameFK
			  AND   Table_BBDDName = p_Table_BBDDFK
			 );
			 
	lTableRel_id := (SELECT TableRel_id 
					 FROM control_TablesRel 
					 WHERE Table_idPK = PK_ID
					 AND   Table_idFK = FK_Id
					 AND   TableFK_NameRelationship = p_TableFK_NameRelationship
					);
			
	IF (lTableRel_id IS NULL) THEN
		lTableRel_id := P_TableRel_id;
		INSERT INTO control_TablesRel (TableRel_id
							   , Table_idPK
							   , Table_idFK
							   , TableFK_NameRelationship
							   , MDM_fhCreate
							   , mdm_processname) 	
		SELECT   P_TableRel_id
			   , PK_ID
			   , FK_Id
			   , p_TableFK_NameRelationship
			   , current_timestamp
			   , p_mdm_processname;
	ELSE
		DELETE FROM control_TablesRelCol WHERE TableRel_id = lTableRel_id;
	END IF;
	
	

	
	
	RETURN lTableRel_id;
END;
$$
LANGUAGE plpgsql;
""")

  huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_TablesRelCol_add ( p_TableRel_id				VARCHAR(50)
										   , p_Table_NamePK               VARCHAR(50)
										   , p_Table_BBDDPK               VARCHAR(50)
										   , p_ColumnName_PK               VARCHAR(50)			 
										   , p_Table_NameFK               VARCHAR(50)
										   , p_Table_BBDDFK               VARCHAR(50)
										   , p_ColumnName_FK               VARCHAR(50)			 
										   , p_mdm_processname         VARCHAR(200))  
RETURNS VOID AS
$$
DECLARE
	FK_Id VARCHAR(50);
	PK_ID VARCHAR(50);
	ColumnPK VARCHAR(50);
	ColumnFK VARCHAR(50);
BEGIN
	PK_ID := (SELECT Table_Id FROM control_Tables 
			  WHERE Table_Name = p_Table_NamePK
			  AND   Table_BBDDName = p_Table_BBDDPK
			 );
			 
	FK_Id := (SELECT Table_Id FROM control_Tables 
			  WHERE Table_Name = p_Table_NameFK
			  AND   Table_BBDDName = p_Table_BBDDFK
			 );
			 
	ColumnPK := (SELECT Column_Id FROM control_columns
				 WHERE table_id = PK_ID
				 AND   column_name = p_ColumnName_PK
				);
				
	ColumnFK := (SELECT Column_Id FROM control_columns
				 WHERE table_id = FK_ID
				 AND   column_name = p_ColumnName_FK
				);
			 
	INSERT INTO control_TablesRelCol (TableRel_id
									 ,Column_idFK
									 ,Column_idPK
									 ,MDM_fhCreate
									 ,MDM_ProcessName
									 )
	SELECT p_TableRel_id
		  ,ColumnFK
		  ,ColumnPK
		  ,current_timestamp
		  ,p_mdm_processname;
	
END;
$$
LANGUAGE plpgsql;
""")

  huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_Tables_addOrUpd (  p_Table_id                VARCHAR(50)
													 ,p_Area_Id                 VARCHAR(50)
													 ,p_Table_BBDDName          VARCHAR(200)
													 ,p_Table_Name              VARCHAR(200)
													 ,p_Table_Description       VARCHAR(1000)
													 ,p_Table_BusinessOwner     VARCHAR(200)
													 ,p_Table_ITOwner           VARCHAR(200)
													 ,p_Table_PartitionField    VARCHAR(200)
													 ,p_Table_TableType         VARCHAR(50)  --reference, master, transactional
													 ,p_Table_StorageType       VARCHAR(50)
													 ,p_Table_LocalPath         VARCHAR(1000)
													 ,p_Table_GlobalPath        VARCHAR(1000)
													 ,p_Table_SQLCreate         VARCHAR(8000)
													 ,p_MDM_ProcessName         VARCHAR(200) )  
RETURNS VOID AS
$$
DECLARE 
	LocalTable_Id VARCHAR(50);
BEGIN
	-- first try to update the key
	UPDATE control_Tables 
  SET table_description = CASE WHEN MDM_ManualChange THEN table_description		ELSE P_table_description	END	 
		,table_businessowner	= CASE WHEN MDM_ManualChange THEN table_businessowner	ELSE P_table_businessowner	END
		,table_itowner			= CASE WHEN MDM_ManualChange THEN table_itowner			ELSE P_table_itowner		END
		,table_partitionfield	= P_table_partitionfield	
		,table_tabletype		= P_table_tabletype		
		,table_storagetype		= P_table_storagetype		
		,table_localpath		= P_table_localpath		
		,table_globalpath		= P_table_globalpath		
		,table_sqlcreate		= P_table_sqlcreate		
	WHERE table_bbddname = p_table_bbddname
	and   table_name	 = p_table_name;

	IF found THEN
		LocalTable_Id := (SELECT Table_Id 
						  FROM control_Tables
						  WHERE table_bbddname = p_table_bbddname
						  and   table_name	 = p_table_name);
						  
		UPDATE Control_Columns SET MDM_Active = false WHERE Table_Id = LocalTable_Id;
		
		RETURN;
	END IF;

	-- not there, so try to insert the key
	-- if someone else inserts the same key concurrently,
	-- we could get a unique-key failure

	INSERT INTO control_Tables ( table_id
								,area_id
								,table_bbddname
								,table_name
								,table_description
								,table_businessowner
								,table_itowner
								,table_partitionfield
								,table_tabletype
								,table_storagetype
								,table_localpath
								,table_globalpath
								,table_sqlcreate
								,mdm_fhcreate
								,mdm_processname) 	
	SELECT   p_table_id
			,p_area_id
			,p_table_bbddname
			,p_table_name
			,p_table_description
			,p_table_businessowner
			,p_table_itowner
			,p_table_partitionfield
			,p_table_tabletype
			,p_table_storagetype
			,p_table_localpath
			,p_table_globalpath
			,p_table_sqlcreate
			,current_timestamp
			,p_mdm_processname;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_Columns_addOrUpd ( p_Column_id                  VARCHAR(50)
													 ,p_Table_Name               VARCHAR(200)
													 ,p_Table_BBDDName           VARCHAR(200)
													 ,p_Column_Position            int
													 ,p_Column_Name                VARCHAR(200)
													 ,p_Column_Description         VARCHAR(1000)
													 ,p_Column_Formula             VARCHAR(1000)
													 ,p_Column_DataType            VARCHAR(50)
													 ,p_Column_SensibleData        boolean
													 ,p_Column_EnableDTLog         boolean
													 ,p_Column_EnableOldValue      boolean
													 ,p_Column_EnableProcessLog    boolean
													 ,p_Column_DefaultValue        VARCHAR(1000)
													 ,p_Column_SecurityLevel       VARCHAR(200)
													 ,p_Column_Encrypted           VARCHAR(200)
													 ,p_Column_ARCO                VARCHAR(200)
													 ,p_Column_Nullable            boolean
													 ,p_Column_IsPK                boolean
													 ,p_Column_IsUnique            boolean
													 ,p_Column_DQ_MinLen           int
													 ,p_Column_DQ_MaxLen           int 
													 ,p_Column_DQ_MinDecimalValue  Decimal(30,10) 
													 ,p_Column_DQ_MaxDecimalValue  Decimal(30,10) 
													 ,p_Column_DQ_MinDateTimeValue VARCHAR(50) 
													 ,p_Column_DQ_MaxDateTimeValue VARCHAR(50)
													 ,p_MDM_ProcessName            VARCHAR(200) )  
RETURNS VOID AS
$$
DECLARE 
	LocalTable_Id VARCHAR(50);
BEGIN
	LocalTable_Id := (SELECT Table_Id 
					  FROM control_Tables
					  WHERE table_bbddname = p_table_bbddname
					  and   table_name	 = p_table_name);
						  
	-- first try to update the key
	UPDATE control_Columns 
  SET column_position	 = p_column_position				
		 ,column_description			= CASE WHEN MDM_ManualChange THEN column_description ELSE p_column_description	END
		 ,column_formula				= CASE WHEN MDM_ManualChange THEN column_formula ELSE p_column_formula	END			
		 ,column_datatype				= p_column_datatype				
		 ,column_sensibledata			= CASE WHEN MDM_ManualChange THEN column_sensibledata ELSE p_column_sensibledata	END		
		 ,column_enabledtlog			= p_column_enabledtlog			
		 ,column_enableoldvalue			= p_column_enableoldvalue			
		 ,column_enableprocesslog		= p_column_enableprocesslog		
		 ,column_defaultvalue			= p_column_defaultvalue			
		 ,column_securitylevel			= CASE WHEN MDM_ManualChange THEN column_securitylevel ELSE p_column_securitylevel	END		
		 ,column_encrypted				= p_column_encrypted				
		 ,column_arco					= CASE WHEN MDM_ManualChange THEN column_arco ELSE p_column_arco	END				
		 ,column_nullable				= p_column_nullable				
		 ,column_ispk					= p_column_ispk					
		 ,column_isunique				= p_column_isunique				
		 ,column_dq_minlen				= p_column_dq_minlen				
		 ,column_dq_maxlen				= p_column_dq_maxlen				
		 ,column_dq_mindecimalvalue		= p_column_dq_mindecimalvalue		
		 ,column_dq_maxdecimalvalue		= p_column_dq_maxdecimalvalue		
		 ,column_dq_mindatetimevalue	= p_column_dq_mindatetimevalue	
		 ,column_dq_maxdatetimevalue	= p_column_dq_maxdatetimevalue	
		 ,MDM_Active					= true
	WHERE Table_Id = LocalTable_Id
	AND   Column_Name = p_column_name;

	IF found THEN
		RETURN;
	END IF;

	-- not there, so try to insert the key
	-- if someone else inserts the same key concurrently,
	-- we could get a unique-key failure

	INSERT INTO control_Columns ( column_id
								 ,table_id
								 ,column_position
								 ,column_name
								 ,column_description
								 ,column_formula
								 ,column_datatype
								 ,column_sensibledata
								 ,column_enabledtlog
								 ,column_enableoldvalue
								 ,column_enableprocesslog
								 ,column_defaultvalue
								 ,column_securitylevel
								 ,column_encrypted
								 ,column_arco
								 ,column_nullable
								 ,column_ispk
								 ,column_isunique
								 ,column_dq_minlen
								 ,column_dq_maxlen
								 ,column_dq_mindecimalvalue
								 ,column_dq_maxdecimalvalue
								 ,column_dq_mindatetimevalue
								 ,column_dq_maxdatetimevalue
								 ,mdm_fhcreate
								 ,mdm_processname) 	
	SELECT    p_column_id
			 ,LocalTable_Id
			 ,p_column_position
			 ,p_column_name
			 ,p_column_description
			 ,p_column_formula
			 ,p_column_datatype
			 ,p_column_sensibledata
			 ,p_column_enabledtlog
			 ,p_column_enableoldvalue
			 ,p_column_enableprocesslog
			 ,p_column_defaultvalue
			 ,p_column_securitylevel
			 ,p_column_encrypted
			 ,p_column_arco
			 ,p_column_nullable
			 ,p_column_ispk
			 ,p_column_isunique
			 ,p_column_dq_minlen
			 ,p_column_dq_maxlen
			 ,p_column_dq_mindecimalvalue
			 ,p_column_dq_maxdecimalvalue
			 ,p_column_dq_mindatetimevalue
			 ,p_column_dq_maxdatetimevalue
			 ,current_timestamp
			 ,p_mdm_processname;
	
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
CREATE OR REPLACE FUNCTION control_Error_finish (p_processExec_id          VARCHAR(50)
										 	 	,p_processExecStep_id		VARCHAR(50)
                                             	,p_Error_Id          VARCHAR(50)
												 ,p_Error_Message                text
                         ,p_Error_Code                Integer
												 ,p_Error_Trace                text
												 ,p_Error_ClassName                text
												 ,p_Error_FileName                text
												 ,p_Error_LIneNumber                text
												 ,p_Error_MethodName                text
												 ,p_Error_Detail                text
												 ,p_MDM_ProcessName         varchar(1000))
RETURNS VOID AS
$$
BEGIN
	INSERT INTO Control_Error (error_id
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
	SELECT p_error_id
		  ,p_error_message
      ,p_error_code
		  ,p_error_trace
		  ,p_error_classname
		  ,p_error_filename
		  ,p_error_linenumber
		  ,p_error_methodname
		  ,p_error_detail
		  ,current_timestamp
		  ,p_mdm_processname;
		  
	PERFORM control_processExec_Finish(p_processExec_id, p_processExecStep_id, p_error_id);
		  
	RETURN;
END;
$$
LANGUAGE plpgsql;
""")

/*
    huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,"""
""") 
*/
  }
  
  
 def AddParamInfo(name: String, value: String) {
    val NewParam = new huemul_LibraryParams()
    NewParam.param_name = name
    NewParam.param_value = value
    NewParam.param_type = "function"
    Control_Params += NewParam
          
    //Insert processExcec
    if (huemulLib.RegisterInControl) {
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_ProcessExecParams_add (
                           '${this.Control_Id}'  --processexec_id
                         , '${NewParam.param_name}' --as processExecParams_Name
                         , '${NewParam.param_value}' --as processExecParams_Value
                         ,'${Control_ClassName}'  --process_id
                        )
                      """)      
    }

    if (huemulLib.DebugMode){
      println(s"Param num: ${Control_Params.length}, name: $name, value: $value")
    }
  }
    
  def FinishProcessOK {
    if (Control_IdParent == null) println("FINISH ALL OK")
    
    if (huemulLib.RegisterInControl) {
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_processExec_Finish (
                             '${Control_Id}'  --p_processExec_id
                           , '${this.LocalIdStep}' --as p_processExecStep_id
                           ,  null --as p_error_id
                           )
                        """)   
                        
      if (this.IsSingleton) {
          huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_singleton_remove (
                             '${Control_ClassName}'  --p_processExec_id
                           )
                        """)  
        }
    }
  }
  
  def FinishProcessError() {
    if (Control_IdParent == null) println("FINISH ERROR")
    val Error_Id = huemulLib.huemul_GetUniqueId()
      
    if (huemulLib.RegisterInControl) {
      //Insert processExcec
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_Error_finish (
                          '${this.Control_Id}'  --p_processExec_id
                         , '${this.LocalIdStep}'  --p_processExecStep_id
                         , '${Error_Id}'  --Error_Id
                         , '${Control_Error.ControlError_Message.replace("'", "''")}' --as Error_Message
                         , ${Control_Error.ControlError_ErrorCode} --as error_code
                         , '${Control_Error.ControlError_Trace.replace("'", "''")}' --as Error_Trace
                         , '${Control_Error.ControlError_ClassName.replace("'", "''")}' --as Error_ClassName
                         , '${Control_Error.ControlError_FileName.replace("'", "''")}' --as Error_FileName
                         , '${Control_Error.ControlError_LineNumber}' --as Error_LIneNumber
                         , '${Control_Error.ControlError_MethodName.replace("'", "''")}' --as Error_MethodName
                         , '' --as Error_Detail
                         ,'${Control_ClassName}'  --process_id
                    )
                      """)            
      if (this.IsSingleton) {
        huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_singleton_remove (
                           '${Control_ClassName}'  --p_processExec_id
                         )
                      """)  
      }
    }
        
                               
  }
  
  
  
  def NewStep(StepName: String) {
    println(s"Step: $StepName")
    
   
    //New Step add
    val PreviousLocalIdStep = LocalIdStep
    LocalIdStep = huemulLib.huemul_GetUniqueId()
    
    if (huemulLib.RegisterInControl) {
      //Insert processExcec
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""SELECT control_processExecStep_add (
                  '${LocalIdStep}'  --p_processExecStep_id
                 ,'${PreviousLocalIdStep}'  --p_processExecStep_idAnt
                 ,'${this.Control_Id}'  --p_processExec_id
                 , '${StepName}' --p_processExecStep_Name
                 ,'${Control_ClassName}'  --p_MDM_ProcessName
                )
          """)
    }
  }
  
  def RegisterTestPlan(p_testPlanGroup_Id: String
                        ,p_testPlan_name: String
                        ,p_testPlan_description: String
                        ,p_testPlan_resultExpected: String
                        ,p_testPlan_resultReal: String
                        ,p_testPlan_IsOK: Boolean) {
    //Create New Id
    val testPlan_Id = huemulLib.huemul_GetUniqueId()
    
    //if (!p_testPlan_IsOK) {
      println(s"TestPlan ${if (p_testPlan_IsOK) "" else "ERROR " }: testPlan_name: ${p_testPlan_name}, resultExpected: ${p_testPlan_resultExpected}, resultReal: ${p_testPlan_resultReal} ")
    //}
                     
    if (huemulLib.RegisterInControl) {
       //Insert processExcec
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""SELECT control_TestPlan_add (
                        '${testPlan_Id}'  --as p_testPlan_Id
                         , '${p_testPlanGroup_Id}'  --as p_testPlanGroup_Id
                         , '${this.Control_Id}'  --p_processExec_id
                         , '${this.Control_ClassName}' --as p_process_id
                         , '${p_testPlan_name.replace("'", "''")}' --p_testPlan_name
                         , '${p_testPlan_description.replace("'", "''")}' --p_testPlan_description
                         , '${p_testPlan_resultExpected.replace("'", "''")}' --p_testPlan_resultExpected
                         , '${p_testPlan_resultReal.replace("'", "''")}' --p_testPlan_resultReal
                         , ${p_testPlan_IsOK} --p_testPlan_IsOK
                         , '${Control_ClassName}' --p_Executor_Name
                         
                        )""")
    }
  }
  
  def RegisterDQuality (Table_Name: String
                             , BBDD_Name: String
                             , DF_Alias: String
                             , ColumnName: String
                             , DQ_Name: String
                             , DQ_Description: String
                             , DQ_IsAggregate: Boolean
                             , DQ_RaiseError: Boolean
                             , DQ_SQLFormula: String
                             , DQ_Error_MaxNumRows: Long
                             , DQ_Error_MaxPercent: Decimal
                             , DQ_ResultDQ: String
                             , DQ_ErrorCode: Integer
                             , DQ_NumRowsOK: Long
                             , DQ_NumRowsError: Long
                             , DQ_NumRowsTotal: Long) {
                
    //Create New Id
    val DQId = huemulLib.huemul_GetUniqueId()

    if (huemulLib.RegisterInControl) {
      //Insert processExcec
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""SELECT control_DQ_add (
                        '${DQId}'  --as p_DQ_Id
                         , '${Table_Name}'  --as Table_name
                         , '${BBDD_Name}'  --asp_BBDD_name
                         , '${this.Control_ClassName}' --as p_Process_Id
                         , '${this.Control_Id}' -- p_ProcessExec_Id
                         , '${ColumnName}' --Column_Name
                         , '${DF_Alias}' --p_Dq_AliasDF
                         , '${DQ_Name}' --p_DQ_Name
                         , '${DQ_Description}' --DQ_Description
                         , ${DQ_IsAggregate} --DQ_IsAggregate
                         , ${DQ_RaiseError} --DQ_RaiseError
                         , '${DQ_SQLFormula.replace("'", "''")}' --DQ_SQLFormula
                         , ${DQ_Error_MaxNumRows} --DQ_Error_MaxNumRows
                         , ${DQ_Error_MaxPercent} --DQ_Error_MaxPercent
                         , ${!(DQ_ResultDQ == null || DQ_ResultDQ == "")} --DQ_IsError
                         , '${if (DQ_ResultDQ == null) "" else DQ_ResultDQ.replace("'", "''")}' --DQ_ResultDQ
                         , ${DQ_ErrorCode} --DQ_ErrorCode
                         , ${DQ_NumRowsOK} --DQ_NumRowsOK
                         , ${DQ_NumRowsError} --DQ_NumRowsError
                         , ${DQ_NumRowsTotal} --DQ_NumRowsTotal
                         ,'${Control_ClassName}'  --process_id
                        )""")
    }
    
  }
  
  def RegisterRAW_USE(dapi_raw: huemul_DataLake) {        
    dapi_raw.setrawFiles_id(huemulLib.huemul_GetUniqueId())
    
    if (huemulLib.RegisterInControl) {
      //Insert processExcec
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""
        select control_rawFiles_add ( '${dapi_raw.getrawFiles_id}'  --p_RAWFiles_id
                         , '${dapi_raw.LogicalName}' --p_RAWFiles_LogicalName
                         , '${dapi_raw.GroupName}' --p_RAWFiles_GroupName
                         , '${dapi_raw.Description}' -- p_RAWFiles_Description
                         , '${dapi_raw.SettingInUse.ContactName}' --p_RAWFiles_Owner
                         ,''  --p_RAWFiles_Frecuency
                         ,'${Control_ClassName}'  --MDM_ProcessName
                      )
                      """)
                      
      //Insert Config Details
      dapi_raw.SettingByDate.foreach { x => 
        //Insert processExcec
        val RAWFilesDet_id = huemulLib.huemul_GetUniqueId()
        huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""SELECT control_RAWFilesDet_add (
                               '${RAWFilesDet_id}' --as RAWFilesDet_id
                             , '${dapi_raw.LogicalName}' --p_RAWFiles_LogicalName
                             , '${dapi_raw.GroupName}' --p_RAWFiles_GroupName
                             ,'${huemulLib.dateTimeFormat.format(x.StartDate.getTime) }'  --RAWFilesDet_StartDate
                             ,'${huemulLib.dateTimeFormat.format(x.EndDate.getTime) }'  --RAWFilesDet_EndDate
                             ,'${x.FileName }'  --RAWFilesDet_FileName
                             ,'${x.LocalPath }'  --RAWFilesDet_LocalPath
                             ,'${x.GlobalPath }'  --RAWFilesDet_GlobalPath
                             ,'${x.DataSchemaConf.ColSeparatorType }'  --RAWFilesDet_Data_ColSeparatorType
                             ,'${x.DataSchemaConf.ColSeparator }'  --RAWFilesDet_Data_ColSeparator
                             ,'${x.DataSchemaConf.HeaderColumnsString }'  --RAWFilesDet_Data_HeaderColumnsString
                             ,'${x.LogSchemaConf.ColSeparatorType }'  --RAWFilesDet_Log_ColSeparatorType
                             ,'${x.LogSchemaConf.ColSeparator }'  --RAWFilesDet_Log_ColSeparator
                             ,'${x.LogSchemaConf.HeaderColumnsString }'  --RAWFilesDet_Log_HeaderColumnsString
                             ,'${x.LogNumRows_FieldName }'  --RAWFilesDet_Log_NumRowsFieldName
                             ,'${x.ContactName }'  --RAWFilesDet_ContactName
                             ,'${Control_ClassName}'  --process_id
                            )
                          """)   
         
         if (x.DataSchemaConf.ColumnsPosition != null) {
           var pos: Integer = 0
           x.DataSchemaConf.ColumnsPosition.foreach { y =>
                 huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""SELECT control_RAWFilesDetFields_add(
                                 '${dapi_raw.LogicalName}' --p_RAWFiles_LogicalName
                               , '${dapi_raw.GroupName}' --p_RAWFiles_GroupName
                               ,'${huemulLib.dateTimeFormat.format(x.StartDate.getTime) }'  --RAWFilesDet_StartDate
                               ,'${y(0)}'  --RAWFilesDetFields_name
                               ,${pos }  --RAWFilesDetFields_Position
                               ,'${y(1) }'  --RAWFilesDetFields_PosIni
                               ,'${y(2) }'  --RAWFilesDetFields_PosFin
                               ,'${Control_ClassName}'  --process_id
                          )
                            """)   
                            
                 pos += 1
             }
         }
      }
    
      //Insert control_rawFilesUse
      val rawfilesuse_id = huemulLib.huemul_GetUniqueId()
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_rawFilesUse_add (
                           '${dapi_raw.LogicalName}' --p_RAWFiles_LogicalName
                         , '${dapi_raw.GroupName}' --p_RAWFiles_GroupName
                         ,'${rawfilesuse_id }'  --rawfilesuse_id
                         ,'${this.Control_ClassName}'  --process_id
                         ,'${this.Control_Id}'          --processExec_Id
                         ,'${dapi_raw.FileName}' --RAWFiles_FullName
                         ,'${dapi_raw.SettingInUse.GetFullNameWithPath()}' --RAWFiles_FullPath
                         ,'${dapi_raw.DataFramehuemul.getNumRows}' --RAWFiles_NumRows
                         , '' --RAWFiles_HeaderLine
                         ,'${Control_ClassName}'  --process_id
                        )
                      """)
    }
  }
  
  def RegisterMASTER_USE(DefMaster: huemul_Table) {
    
    //Insert control_TablesUse
    if (huemulLib.RegisterInControl) {
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_TablesUse_add (
                                    '${DefMaster.TableName}'
                                   ,'${DefMaster.GetDataBase(DefMaster.DataBase) }'  
                                   , '${Control_ClassName}' --as Process_Id
                                   , '${Control_Id}' --as ProcessExec_Id
                                   , true --as TableUse_Read
                                   , false --as TableUse_Write
                                   , null --as TableUse_numRowsInsert
                                   , null --as TableUse_numRowsUpdate
                                   , null --as TableUse_numRowsMarkDelete
                                   , null --as TableUse_numRowsTotal
                                   ,'${Control_ClassName}'  --process_id
                                  )
                      """)     
    }
  }

  def RegisterMASTER_CREATE_Basic(DefMaster: huemul_Table) {
    val LocalNewTable_id = huemulLib.huemul_GetUniqueId()
          
    if (huemulLib.RegisterInControl) {
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s""" select control_Tables_addOrUpd(
                            '${LocalNewTable_id}'  --Table_id
                           ,null  --Area_Id
                           , '${DefMaster.GetDataBase(DefMaster.DataBase)}' --as Table_BBDDName
                           , '${DefMaster.TableName}' --as Table_Name
                           , '${DefMaster.Description}' --as Table_Description
                           , '${DefMaster.Business_ResponsibleName}' --as Table_BusinessOwner
                           , '${DefMaster.IT_ResponsibleName}' --as Table_ITOwner
                           , '${DefMaster.PartitionField}' --as Table_PartitionField
                           , '${DefMaster.TableType}' --as Table_TableType
                           , '${DefMaster.StorageType}' --as Table_StorageType
                           , '${DefMaster.LocalPath}' --as Table_LocalPath
                           , '${DefMaster.GlobalPath}' --as Table_GlobalPath
                           , '' --as Table_SQLCreate
                           ,'${Control_ClassName}'  --process_id
                          )
      """)
    
      
      //Insert control_Columns
      var i: Integer = 0
      var localDatabaseName = DefMaster.GetDataBase(DefMaster.DataBase)
      DefMaster.GetColumns().foreach { x => 
        val Column_Id = huemulLib.huemul_GetUniqueId()
  
        huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""SELECT control_Columns_addOrUpd (
        
                            '${Column_Id}' --Column_Id
                           , '${DefMaster.TableName}' --as Table_Name
                           , '${localDatabaseName}' --as Table_BBDDName
                           , ${i} --as Column_Position
                           , '${x.get_MyName()}' --as Column_Name
                           , '${x.Description}' --as Column_Description
                           , null --as Column_Formula
                           , '${x.DataType.sql}' --as Column_DataType
                           , false --as Column_SensibleData
                           , ${x.MDM_EnableDTLog} --as Column_EnableDTLog
                           , ${x.MDM_EnableOldValue} --as Column_EnableOldValue
                           , ${x.MDM_EnableProcessLog} --as Column_EnableProcessLog
                           , '${x.DefaultValue.replace("'", "''")}' --as Column_DefaultValue
                           , '${x.SecurityLevel}' --as Column_SecurityLevel
                           , '${x.EncryptedType}' --as Column_Encrypted
                           , '${x.ARCO_Data}' --as Column_ARCO
                           , ${x.Nullable} --as Column_Nullable
                           , ${x.IsPK} --as Column_IsPK
                           , ${x.IsUnique} --as Column_IsUnique
                           , ${x.DQ_MinLen} --as Column_DQ_MinLen
                           , ${x.DQ_MaxLen} --as Column_DQ_MaxLen
                           , ${x.DQ_MinDecimalValue} --as Column_DQ_MinValue
                           , ${x.DQ_MaxDecimalValue} --as Column_DQ_MaxValue
                           , '${x.DQ_MinDateTimeValue}' --as Column_DQ_MinDateTimeValue
                           , '${x.DQ_MaxDateTimeValue}' --as Column_DQ_MaxDateTimeValue
                           ,'${Control_ClassName}'  --process_id
                      )""")        
          i += 1
        }
    }
  }
  
  def RegisterMASTER_CREATE_Use(DefMaster: huemul_Table) {     
      
    if (huemulLib.RegisterInControl) {
      //Insert control_TablesUse
      huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_TablesUse_add (
                                  '${DefMaster.TableName}'
                                 ,'${DefMaster.GetDataBase(DefMaster.DataBase) }'  
                                 , '${Control_ClassName}' --as Process_Id
                                 , '${Control_Id}' --as ProcessExec_Id
                                 , false --as TableUse_Read
                                 , true --as TableUse_Write
                                 , ${DefMaster.NumRows_New()} -- as TableUse_numRowsInsert
                                 , ${DefMaster.NumRows_Update()} -- as TableUse_numRowsUpdate
                                 , ${DefMaster.NumRows_Delete()} -- as TableUse_numRowsMarkDelete
                                 , ${DefMaster.NumRows_Total()} -- as TableUse_numRowsTotal
                                 ,'${Control_ClassName}'  --process_id
                                )
                    """)
                    
      //Insert control_tablesrel_add
      DefMaster.GetForeingKey().foreach { x =>       
        val p_tablerel_id = huemulLib.huemul_GetUniqueId()
        
        val localDatabaseName = x._Class_TableName.asInstanceOf[huemul_Table].GetDataBase(x._Class_TableName.asInstanceOf[huemul_Table].DataBase)
        val Resultado = 
        huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_tablesrel_add (
                                    '${p_tablerel_id}'    --p_tablerel_id
                                   ,'${x._Class_TableName.asInstanceOf[huemul_Table].TableName }'  --p_table_Namepk
                                   ,'${localDatabaseName }'  --p_table_BBDDpk
  
                                   ,'${DefMaster.TableName }'  --p_table_NameFK
                                   ,'${DefMaster.GetDataBase(DefMaster.DataBase) }'  --p_table_BBDDFK
  
                                   ,'${x.MyName }'  --p_TableFK_NameRelationship
  
                                   ,'${Control_ClassName}'  --process_id
                                  )
                      """)
                      
         val IdRel = Resultado.ResultSet(0).getString(0)
  
         x.Relationship.foreach { y => 
            huemulLib.ExecuteJDBC(huemulLib.JDBCTXT,s"""select control_TablesRelCol_add (
                                      '${IdRel}'    --p_tablerel_id
                                     ,'${x._Class_TableName.asInstanceOf[huemul_Table].TableName }'  --p_table_Namepk
                                     ,'${localDatabaseName }'  --p_table_BBDDpk
                                     ,'${y.PK.get_MyName() }'  --p_ColumnName_PK
  
                                     ,'${DefMaster.TableName }'  --p_table_NameFK
                                     ,'${DefMaster.GetDataBase(DefMaster.DataBase) }'  --p_table_BBDDFK
                                     ,'${y.FK.get_MyName() }'  --p_ColumnName_FK    
  
                                     ,'${Control_ClassName}'  --process_id
                                    )
                        """)
          }
      }
    }
                        
  }
 
  
  def RaiseError(txt: String) {
    sys.error(txt)
  }
  
  
}