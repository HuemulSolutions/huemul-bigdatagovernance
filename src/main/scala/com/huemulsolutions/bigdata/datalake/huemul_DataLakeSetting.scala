package com.huemulsolutions.bigdata.datalake

import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.common.huemul_BigDataGovernance
import com.huemulsolutions.bigdata.common.huemul_KeyValuePath
import huemulType_FileType._

class huemul_DataLakeSetting(huemulBigDataGov: huemul_BigDataGovernance) extends Serializable {
  /***
   * Start date for this configuration
   */
  var StartDate : Calendar = _
  def getStartDate: Calendar = StartDate
  /*** set Start date for this configuration
   * @param value: date
   */
  def setStartDate(value: Calendar): huemul_DataLakeSetting = {
    StartDate = value
    this
  }
  /*** set Start date for this configuration
   * @param year: Int
   * @param month: Int
   * @param day: Int
   * @param hour: Int
   * @param minute: Int
   * @param second: Int
   */
  def setStartDate(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): huemul_DataLakeSetting = {
    StartDate = huemulBigDataGov.setDateTime(year, month, day, hour, minute, second)
    this
  }
  /***
   * End date for this configuration
   */
  var EndDate : Calendar = _
  def getEndDate: Calendar = EndDate
  /*** set end date for this configuration
   * @param value: date
   */
  def setEndDate(value: Calendar): huemul_DataLakeSetting = {
    EndDate = value
    this
  }
  /*** set end date for this configuration
   * @param year: Int
   * @param month: Int
   * @param day: Int
   * @param hour: Int
   * @param minute: Int
   * @param second: Int
   */
  def setEndDate(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): huemul_DataLakeSetting = {
    EndDate = huemulBigDataGov.setDateTime(year, month, day, hour, minute, second)
    this
  }

  /***
   * Type (example: TEXT_FILE, EXCEL_FILE)
   */
  var FileType: huemulType_FileType = _
  def getFileType: huemulType_FileType = FileType
  /*** set FileType to read
   * Type (example: TEXT_FILE, EXCEL_FILE)
   */
  def setFileType(value: huemulType_FileType): huemul_DataLakeSetting = {
    FileType = value
    this
  }

  /***
   * File Name (example: "PLAN-CTAS.TXT")
   */
  var FileName    : String = ""
  def getFileName: String = FileName
  /*** File Name (example: "PLAN-CTAS.TXT")
   * {{YYYY}} = replace 4 digits year
   * {{YY}} = replace 2 digits year
   * {{MM}} = replace with month
   * {{DD}} = replace 2 digits day
   */
  def setFileName(value: String): huemul_DataLakeSetting = {
    FileName = value
    this
  }
  /*** Local path (example "SBIF\\{{YYYY}}{{MM}}\\"
   * * {{YYYY}} = replace 4 digits year
   * * {{YY}} = replace 2 digits year
   * * {{MM}} = replace with month
   * * {{DD}} = replace 2 digits day
   */
  var LocalPath   : String= ""
  def getLocalPath: String = LocalPath
  def setLocalPath(value: String): huemul_DataLakeSetting = {
    LocalPath = value
    this
  }
  /***
   * from Global path
   */
  var GlobalPath  : ArrayBuffer[huemul_KeyValuePath] = _
  def getGlobalPath: ArrayBuffer[huemul_KeyValuePath] = GlobalPath
  def setGlobalPath(value: ArrayBuffer[huemul_KeyValuePath]): huemul_DataLakeSetting = {
    GlobalPath = value
    this
  }
  /***
   * Responsible contact to resolve doubt or errors
   */
  var ContactName :  String= ""
  def getContactName: String = ContactName
  def setContactName(value: String): huemul_DataLakeSetting = {
    ContactName = value
    this
  }
  
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
  var LogNumRows_FieldName2: String = ""
  def getLogNumRowsColumnName: String = LogNumRows_FieldName2
  /*** for Header, set column name that have the rows count
   *
   */
  def setLogNumRowsColumnName(value: String ): huemul_DataLakeSetting = {
    LogNumRows_FieldName2 = value
    this
  }
  
  //FROM 2.4 
  /***
   * character for row delimited in PDF files
   */
  var rowDelimiterForPDF: String = "\\n"
  def getRowDelimiterForPDF: String = rowDelimiterForPDF
  def setRowDelimiterForPDF(value: String): huemul_DataLakeSetting = {
    rowDelimiterForPDF = value
    this
  }
  
  
  private var use_year: Integer = _
  def getuse_year: Integer =  use_year
  private var use_month: Integer = _
  def getuse_month: Integer =  use_month
  private var use_day: Integer = _
  def getuse_day: Integer =  use_day
  private var use_hour: Integer = _
  def getuse_hour: Integer =  use_hour
  private var use_minute: Integer = _
  def getuse_minute: Integer =  use_minute
  private var use_second: Integer = _
  def getuse_second: Integer =  use_second
  private var use_params: String = ""
  def getuse_params: String =  use_params
  
  def SetParamsInUse(year: Integer, month: Integer, day: Integer, hour: Integer, min: Integer, sec: Integer, AdditionalParams: String){
    use_year = year
    use_month = month
    use_day = day
    use_hour = hour
    use_minute = min
    use_second = sec
    use_params = AdditionalParams
  }
  
  
  def GetFullNameWithPath() : String = {
    GetPath(getGlobalPath) + getLocalPath + getFileName
  }
  
  
  def GetDataBase(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    huemulBigDataGov.GlobalSettings.GetDataBase(huemulBigDataGov, Division)
  }
  
  def GetDataBase(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    huemulBigDataGov.GlobalSettings.GetDataBase(huemulBigDataGov, Division, ManualEnvironment)
  }
  
  def GetPath(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov, Division)
  }
  
  def GetPath(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov, Division, ManualEnvironment)
  }
  
}