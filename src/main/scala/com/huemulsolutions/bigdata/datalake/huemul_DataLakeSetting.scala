package com.huemulsolutions.bigdata.datalake

import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.common.huemul_Library
import com.huemulsolutions.bigdata.common.huemul_KeyValuePath
import huemulType_FileType._

class huemul_DataLakeSetting(huemulLib: huemul_Library) extends Serializable {
  /***
   * Start date for this configuration
   */
  var StartDate : Calendar = null 
  /***
   * End date for this configuration
   */
  var EndDate : Calendar = null
  /***
   * Type (example: TEXT_FILE, EXCEL_FILE)
   */
  var FileType: huemulType_FileType = null
  /***
   * File Name (example: "PLAN-CTAS.TXT")
   */
  var FileName    : String = ""  
  /***
   * Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  var LocalPath   : String= ""
  /***
   * from Global path
   */
  var GlobalPath  : ArrayBuffer[huemul_KeyValuePath] = null
  /***
   * Responsible contact to resolve doubt or errors
   */
  var ContactName :  String= ""
  
  /***
   * Data Schema configuration
   */
  var DataSchemaConf: huemul_DataLakeSchemaConf = new  huemul_DataLakeSchemaConf()
  
  /***
   * Log Schema configuration
   */
  var LogSchemaConf: huemul_DataLakeSchemaConf = new  huemul_DataLakeSchemaConf()
  
  
  /***
   * FieldName for N° Rows in control line
   */
  var LogNumRows_FieldName: String = ""
  
  
  private var use_year: Integer = null
  def getuse_year: Integer = {return use_year}
  private var use_month: Integer = null
  def getuse_month: Integer = {return use_month}
  private var use_day: Integer = null
  def getuse_day: Integer = {return use_day}
  private var use_hour: Integer = null
  def getuse_hour: Integer = {return use_hour}
  private var use_minute: Integer = null
  def getuse_minute: Integer = {return use_minute}
  private var use_second: Integer = null
  def getuse_second: Integer = {return use_second}
  private var use_params: String = null
  def getuse_params: String = {return use_params}
  
  def SetParamsInUse(ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer, AdditionalParams: String){
    use_year = ano
    use_month = mes
    use_day = dia
    use_hour = hora
    use_minute = min
    use_second = seg
    use_params = AdditionalParams
  }
  
  
  def GetFullNameWithPath() : String = {
    return GetPath(GlobalPath) + LocalPath + FileName
  }
  
  
  def GetDataBase(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    return huemulLib.GlobalSettings.GetDataBase(huemulLib, Division)  
  }
  
  def GetDataBase(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    return huemulLib.GlobalSettings.GetDataBase(huemulLib, Division, ManualEnvironment)  
  }
  
  def GetPath(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    return huemulLib.GlobalSettings.GetPath(huemulLib, Division)  
  }
  
  def GetPath(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    return huemulLib.GlobalSettings.GetPath(huemulLib, Division, ManualEnvironment)  
  }
  
}