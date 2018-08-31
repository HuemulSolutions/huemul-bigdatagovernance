package com.huemulsolutions.bigdata.control


import org.apache.spark.sql.types._
import java.util.Calendar;
import com.huemulsolutions.bigdata.datalake.huemul_DataLake
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel._

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
  huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s""" SELECT
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
    println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] ProcessExec_Id: ${Control_Id}, processName: ${Control_ClassName}")
  huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_processExec_add (
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
      val Ejec =  huemulLib.postgres_connection.ExecuteJDBC_WithResult(s"""select control_singleton_Add (
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
        if (huemulLib.DebugMode) println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] waiting for Singleton... (class: $Control_ClassName, appId: ${huemulLib.IdApplication} )")
        //if has error, verify other process still alive
        if (NumCycle == 1) //First cicle don't wait
          Thread.sleep(10000)
        
        NumCycle = 1
        // Obtiene procesos pendientes
        val CurrentProcess = huemulLib.postgres_connection.ExecuteJDBC_WithResult(s"select * from control_executors where application_Id = '${ApplicationInUse}'")
        var IdAppFromDataFrame : String = ""
        var IdAppFromAPI: String = ""
        var URLMonitor: String = ""
        var StillAlive: Boolean = true
        
        if (CurrentProcess.ResultSet == null || CurrentProcess.ResultSet.length == 0) { //dosn't have records, was eliminted by other process (rarely)
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
          val a = huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""SELECT control_executors_remove ('${ApplicationInUse}' )""")
        }
      }              
    }
  }
  
  //*****************************************
  //END Start Singleton
  //*****************************************  

  
  
  
 def AddParamInfo(name: String, value: String) {
    val NewParam = new huemul_LibraryParams()
    NewParam.param_name = name
    NewParam.param_value = value
    NewParam.param_type = "function"
    Control_Params += NewParam
          
    //Insert processExcec
    if (huemulLib.RegisterInControl) {
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_ProcessExecParams_add (
                           '${this.Control_Id}'  --processexec_id
                         , '${NewParam.param_name}' --as processExecParams_Name
                         , '${NewParam.param_value}' --as processExecParams_Value
                         ,'${Control_ClassName}'  --process_id
                        )
                      """)      
    }

    if (huemulLib.DebugMode){
      println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] Param num: ${Control_Params.length}, name: $name, value: $value")
    }
  }
    
  def FinishProcessOK {
    
    if (huemulLib.RegisterInControl) {
      if (!huemulLib.HasName(Control_IdParent)) println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] FINISH ALL OK")
      println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] FINISH ProcessExec_Id: ${Control_Id}, processName: ${Control_ClassName}")
    
    
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_processExec_Finish (
                             '${Control_Id}'  --p_processExec_id
                           , '${this.LocalIdStep}' --as p_processExecStep_id
                           ,  null --as p_error_id
                           )
                        """)   
                        
      if (this.IsSingleton) {
          huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_singleton_remove (
                             '${Control_ClassName}'  --p_processExec_id
                           )
                        """)  
        }
    }
  }
  
  def FinishProcessError() {
    
      
    if (huemulLib.RegisterInControl) {
      if (Control_IdParent == null) println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] FINISH ERROR")
      println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] FINISH ProcessExec_Id: ${Control_Id}, processName: ${Control_ClassName}")
    
      val Error_Id = huemulLib.huemul_GetUniqueId()
    
      //Insert processExcec
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_Error_finish (
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
        huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_singleton_remove (
                           '${Control_ClassName}'  --p_processExec_id
                         )
                      """)  
      }
    }
        
                               
  }
  
  
  
  def NewStep(StepName: String) {
    println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] Step: $StepName")
    
   
    //New Step add
    val PreviousLocalIdStep = LocalIdStep
    LocalIdStep = huemulLib.huemul_GetUniqueId()
    
    if (huemulLib.RegisterInControl) {
      //Insert processExcec
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""SELECT control_processExecStep_add (
                  '${LocalIdStep}'  --p_processExecStep_id
                 ,'${PreviousLocalIdStep}'  --p_processExecStep_idAnt
                 ,'${this.Control_Id}'  --p_processExec_id
                 , '${StepName}' --p_processExecStep_Name
                 ,'${Control_ClassName}'  --p_MDM_ProcessName
                )
          """)
    }
  }
  
  def RegisterTestPlanFeature(p_Feature_Id: String
                             ,p_TestPlan_Id: String) {
    if (huemulLib.RegisterInControl) {
       //Insert processExcec
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""SELECT control_TestPlanFeature_add (
                            '${p_Feature_Id.replace("'", "''")}'  --as p_Feature_Id
                         , '${p_TestPlan_Id}'  --as p_testPlan_Id
                         ,'${Control_ClassName}'  --p_MDM_ProcessName
                                                  
                        )""")
    }
    
    
  }
  
  /**
   * Return TestPlan ID
   */
  def RegisterTestPlan(p_testPlanGroup_Id: String
                        ,p_testPlan_name: String
                        ,p_testPlan_description: String
                        ,p_testPlan_resultExpected: String
                        ,p_testPlan_resultReal: String
                        ,p_testPlan_IsOK: Boolean): String = {
    //Create New Id
    val testPlan_Id = huemulLib.huemul_GetUniqueId()
    
    //if (!p_testPlan_IsOK) {
      println(s"HuemulControlLog: [${huemulLib.huemul_getDateForLog()}] TestPlan ${if (p_testPlan_IsOK) "OK " else "ERROR " }: testPlan_name: ${p_testPlan_name}, resultExpected: ${p_testPlan_resultExpected}, resultReal: ${p_testPlan_resultReal} ")
    //}
                     
    if (huemulLib.RegisterInControl) {
       //Insert processExcec
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""SELECT control_TestPlan_add (
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
    
    return testPlan_Id
  }
  
  def RegisterDQuality (Table_Name: String
                             , BBDD_Name: String
                             , DF_Alias: String
                             , ColumnName: String
                             , DQ_Name: String
                             , DQ_Description: String
                             , DQ_QueryLevel: huemulType_DQQueryLevel //DQ_IsAggregate: Boolean
                             , DQ_Notification: huemulType_DQNotification ////DQ_RaiseError: Boolean
                             , DQ_SQLFormula: String
                             , DQ_toleranceError_Rows: java.lang.Long
                             , DQ_toleranceError_Percent: Decimal
                             , DQ_ResultDQ: String
                             , DQ_ErrorCode: Integer
                             , DQ_NumRowsOK: Long
                             , DQ_NumRowsError: Long
                             , DQ_NumRowsTotal: Long
                             , DQ_IsError: Boolean) {
                
    //Create New Id
    val DQId = huemulLib.huemul_GetUniqueId()

    if (huemulLib.RegisterInControl) {
      //Insert processExcec
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""SELECT control_DQ_add (
                        '${DQId}'  --as p_DQ_Id
                         , '${Table_Name}'  --as Table_name
                         , '${BBDD_Name}'  --asp_BBDD_name
                         , '${this.Control_ClassName}' --as p_Process_Id
                         , '${this.Control_Id}' -- p_ProcessExec_Id
                         , '${ColumnName}' --Column_Name
                         , '${DF_Alias}' --p_Dq_AliasDF
                         , '${DQ_Name}' --p_DQ_Name
                         , '${DQ_Description}' --DQ_Description
                         , '${DQ_QueryLevel}' --DQ_IsAggregate
                         , '${DQ_Notification}' --DQ_RaiseError
                         , '${DQ_SQLFormula.replace("'", "''")}' --DQ_SQLFormula
                         , ${DQ_toleranceError_Rows} --DQ_Error_MaxNumRows
                         , ${DQ_toleranceError_Percent} --DQ_Error_MaxPercent
                         , '${if (DQ_ResultDQ == null) "" else DQ_ResultDQ.replace("'", "''")}' --DQ_ResultDQ
                         , ${DQ_ErrorCode} --DQ_ErrorCode
                         , ${DQ_NumRowsOK} --DQ_NumRowsOK
                         , ${DQ_NumRowsError} --DQ_NumRowsError
                         , ${DQ_NumRowsTotal} --DQ_NumRowsTotal
                         , ${DQ_IsError} --DQ_IsError
                         ,'${Control_ClassName}'  --process_id
                        )""")
    }
    
  }
  
  def RegisterRAW_USE(dapi_raw: huemul_DataLake) {        
    dapi_raw.setrawFiles_id(huemulLib.huemul_GetUniqueId())
    
    if (huemulLib.RegisterInControl) {
      //Insert processExcec
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""
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
        huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""SELECT control_RAWFilesDet_add (
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
                 huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""SELECT control_RAWFilesDetFields_add(
                                 '${dapi_raw.LogicalName}' --p_RAWFiles_LogicalName
                               , '${dapi_raw.GroupName}' --p_RAWFiles_GroupName
                               ,'${huemulLib.dateTimeFormat.format(x.StartDate.getTime) }'  --RAWFilesDet_StartDate
                               ,'${y(0)}'  --RAWFilesDetFields_ITName
                               , '' as RAWFilesDetFields_LogicalName
                               , '' as RAWFilesDetFields_description
                               , '' as RAWFilesDetFields_DataType
                               ,${pos }  --RAWFilesDetFields_Position
                               ,'${y(1) }'  --RAWFilesDetFields_PosIni
                               ,'${y(2) }'  --RAWFilesDetFields_PosFin
                               , -1 --,RAWFilesDetFields_ApplyTrim     
                               , -1 --,RAWFilesDetFields_ConvertNull   
                               ,'${Control_ClassName}'  --process_id
                          )
                            """)   
                            
                 pos += 1
             }
         }
      }
    
      //Insert control_rawFilesUse
      val rawfilesuse_id = huemulLib.huemul_GetUniqueId()
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_rawFilesUse_add (
                           '${dapi_raw.LogicalName}' --p_RAWFiles_LogicalName
                         , '${dapi_raw.GroupName}' --p_RAWFiles_GroupName
                         ,'${rawfilesuse_id }'  --rawfilesuse_id
                         ,'${this.Control_ClassName}'  --process_id
                         ,'${this.Control_Id}'          --processExec_Id
                         , ${dapi_raw.SettingInUse.getuse_year} -- RAWFilesUse_Year
                         , ${dapi_raw.SettingInUse.getuse_month} -- ,RAWFilesUse_Month
                         , ${dapi_raw.SettingInUse.getuse_day} -- RAWFilesUse_Day 
                         , ${dapi_raw.SettingInUse.getuse_hour} -- RAWFilesUse_Hour
                         , ${dapi_raw.SettingInUse.getuse_minute} -- RAWFilesUse_Miute
                         , ${dapi_raw.SettingInUse.getuse_second} -- RAWFilesUse_Second
                         , '${dapi_raw.SettingInUse.getuse_params}' -- RAWFilesUse_params
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
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_TablesUse_add (
                                    '${DefMaster.TableName}'
                                   ,'${DefMaster.GetCurrentDataBase() }'  
                                   , '${Control_ClassName}' --as Process_Id
                                   , '${Control_Id}' --as ProcessExec_Id
                                   , '${LocalIdStep}' --as ProcessExecStep_Id
                                   , null -- TableUse_Year
                                   , null -- TableUse_Month
                                   , null -- TableUse_Day  
                                   , null -- TableUse_Hour 
                                   , null -- TableUse_Miute
                                   , null -- TableUse_Second
                                   , null -- TableUse_params
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
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s""" select control_Tables_addOrUpd(
                            '${LocalNewTable_id}'  --Table_id
                           ,null  --Area_Id
                           , '${DefMaster.GetCurrentDataBase()}' --as Table_BBDDName
                           , '${DefMaster.TableName}' --as Table_Name
                           , '${DefMaster.getDescription}' --as Table_Description
                           , '${DefMaster.getBusiness_ResponsibleName}' --as Table_BusinessOwner
                           , '${DefMaster.getIT_ResponsibleName}' --as Table_ITOwner
                           , '${DefMaster.getPartitionField}' --as Table_PartitionField
                           , '${DefMaster.getTableType}' --as Table_TableType
                           , '${DefMaster.getStorageType}' --as Table_StorageType
                           , '${DefMaster.getLocalPath}' --as Table_LocalPath
                           , '${DefMaster.getGlobalPaths}' --as Table_GlobalPath
                           , '' --as Table_SQLCreate
                           ,'${Control_ClassName}'  --process_id
                          )
      """)
    
      
      //Insert control_Columns
      var i: Integer = 0
      var localDatabaseName = DefMaster.GetCurrentDataBase()
      DefMaster.GetColumns().foreach { x => 
        val Column_Id = huemulLib.huemul_GetUniqueId()
    
        huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""SELECT control_Columns_addOrUpd (
        
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
      huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_TablesUse_add (
                                  '${DefMaster.TableName}'
                                 ,'${DefMaster.GetCurrentDataBase() }'  
                                 , '${Control_ClassName}' --as Process_Id
                                 , '${Control_Id}' --as ProcessExec_Id
                                 , '${LocalIdStep}' --as ProcessExecStep_Id
                                   , null -- TableUse_Year
                                   , null -- TableUse_Month
                                   , null -- TableUse_Day  
                                   , null -- TableUse_Hour 
                                   , null -- TableUse_Miute
                                   , null -- TableUse_Second
                                   , null -- TableUse_params
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
        
        val localDatabaseName = x._Class_TableName.asInstanceOf[huemul_Table].GetCurrentDataBase()
        val Resultado = 
        huemulLib.postgres_connection.ExecuteJDBC_WithResult(s"""select control_tablesrel_add (
                                    '${p_tablerel_id}'    --p_tablerel_id
                                   ,'${x._Class_TableName.asInstanceOf[huemul_Table].TableName }'  --p_table_Namepk
                                   ,'${localDatabaseName }'  --p_table_BBDDpk
  
                                   ,'${DefMaster.TableName }'  --p_table_NameFK
                                   ,'${DefMaster.GetCurrentDataBase() }'  --p_table_BBDDFK
  
                                   ,'${x.MyName }'  --p_TableFK_NameRelationship
  
                                   ,'${Control_ClassName}'  --process_id
                                  )
                      """)
                      
         val IdRel = Resultado.ResultSet(0).getString(0)
  
         x.Relationship.foreach { y => 
            huemulLib.postgres_connection.ExecuteJDBC_NoResulSet(s"""select control_TablesRelCol_add (
                                      '${IdRel}'    --p_tablerel_id
                                     ,'${x._Class_TableName.asInstanceOf[huemul_Table].TableName }'  --p_table_Namepk
                                     ,'${localDatabaseName }'  --p_table_BBDDpk
                                     ,'${y.PK.get_MyName() }'  --p_ColumnName_PK
  
                                     ,'${DefMaster.TableName }'  --p_table_NameFK
                                     ,'${DefMaster.GetCurrentDataBase() }'  --p_table_BBDDFK
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