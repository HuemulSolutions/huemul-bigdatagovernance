package com.huemul.bigdata.common

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
    
    val POSTGRE_Setting: ArrayBuffer[huemul_KeyValuePath] = new ArrayBuffer[huemul_KeyValuePath]()
    
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
    
    def GetPath(huemulLib: huemul_Library, Division: ArrayBuffer[huemul_KeyValuePath]): String = {
      val Result = Division.filter { x => x.environment == huemulLib.Environment }
      if (Result == null || Result.length == 0)
        sys.error(s"DAPI Error: environment '${huemulLib.Environment}' must be set")
        
      return Result(0).Value
    }
    
    /**
     Get Path with manual environment setting
     */
    def GetPath(huemulLib: huemul_Library, Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
      val Result = Division.filter { x => x.environment == ManualEnvironment }
      if (Result == null || Result.length == 0)
        sys.error(s"DAPI Error: environment '${ManualEnvironment}' must be set")
        
      return Result(0).Value
    }
    
    def GetDataBase(huemulLib: huemul_Library, Division: ArrayBuffer[huemul_KeyValuePath]): String = {
      return GetPath(huemulLib, Division)
    }
    
    /**
     Get DataBase Name with manual environment setting
     */
    def GetDataBase(huemulLib: huemul_Library, Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
      return GetPath(huemulLib, Division, ManualEnvironment)
    }
    
    
    //TEMP
    def GetDebugTempPath(huemulLib: huemul_Library, function_name: String, table_name: String): String = {        
      return s"${GetPath(huemulLib, TEMPORAL_Path)}$function_name/$table_name"
    }
    
    
    def RaiseError(Environment: String) {
      sys.error(s"error, environment does't exist: '$Environment', must be '$GlobalEnvironments'  ")
    }
    
    
}