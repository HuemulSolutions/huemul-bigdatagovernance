package com.huemulsolutions.bigdata.common

import scala.collection.mutable.ArrayBuffer

class huemul_KeyValuePath(Environment: String, PathOrDataBase: String) extends Serializable {
  /**example: "prod, desa, qa"      
     **/
  val environment: String = Environment
  /** Value: Path for Files, DataBase Name for hive tables 
   */
  val Value: String = PathOrDataBase
}

class huemul_GlobalPath() extends Serializable {
    /**example: "prod, desa, qa"      
     **/
    var GlobalEnvironments: String = "prod, desa, qa" //prod: PRODUCCION, desa: DESARROLLO, qa: ambientes previos a producción
    var ImpalaEnabled: Boolean = false
    
    var CONTROL_Driver: String = "org.postgresql.Driver"
    val CONTROL_Setting: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
    val IMPALA_Setting: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
    
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

    
    
    /**
     Returns true if path has value, otherwise return false
     */
    def ValidPath(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): Boolean = {
      val Result = Division.filter { x => x.environment == ManualEnvironment }
      return if (Result == null || Result.length == 0) false else true
    }
    
    def GetPath(huemulBigDataGov: huemul_BigDataGovernance, Division: ArrayBuffer[huemul_KeyValuePath]): String = {
      val Result = Division.filter { x => x.environment == huemulBigDataGov.Environment }
      if (Result == null || Result.length == 0)
        sys.error(s"DAPI Error: environment '${huemulBigDataGov.Environment}' must be set")
        
      return Result(0).Value
    }
    
    /**
     Get Path with manual environment setting
     */
    def GetPath(huemulBigDataGov: huemul_BigDataGovernance, Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
      val Result = Division.filter { x => x.environment == ManualEnvironment }
      if (Result == null || Result.length == 0)
        sys.error(s"DAPI Error: environment '${ManualEnvironment}' must be set")
        
      return Result(0).Value
    }
    
    def GetDataBase(huemulBigDataGov: huemul_BigDataGovernance, Division: ArrayBuffer[huemul_KeyValuePath]): String = {
      return GetPath(huemulBigDataGov, Division)
    }
    
    /**
     Get DataBase Name with manual environment setting
     */
    def GetDataBase(huemulBigDataGov: huemul_BigDataGovernance, Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
      return GetPath(huemulBigDataGov, Division, ManualEnvironment)
    }
    
    
    //TEMP
    def GetDebugTempPath(huemulBigDataGov: huemul_BigDataGovernance, function_name: String, table_name: String): String = {        
      return s"${GetPath(huemulBigDataGov, TEMPORAL_Path)}$function_name/$table_name"
    }
    
    //to save DF directly from DF without DataGovernance
    def GetPathForSaveTableWithoutDG(huemulBigDataGov: huemul_BigDataGovernance,globalPath: ArrayBuffer[huemul_KeyValuePath], localPath_name: String, table_name: String): String = {        
      return s"${GetPath(huemulBigDataGov, globalPath)}$localPath_name/$table_name"
    }
    
    
    def RaiseError(Environment: String) {
      sys.error(s"error, environment does't exist: '$Environment', must be '$GlobalEnvironments'  ")
    }
    
    
}