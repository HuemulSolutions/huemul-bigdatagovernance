package com.huemul.bigdata.datalake

import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import com.huemul.bigdata.common.huemul_Library
import com.huemul.bigdata.common.huemul_KeyValuePath
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