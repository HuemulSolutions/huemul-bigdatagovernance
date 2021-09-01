package com.huemulsolutions.bigdata.datalake

import java.util.Calendar

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import com.huemulsolutions.bigdata.common.HuemulKeyValuePath
import com.huemulsolutions.bigdata.datalake.HuemulTypeSeparator.HuemulTypeSeparator
import HuemulTypeFileType._
import org.apache.spark.sql.types.{DataType, StringType}

class HuemulDataLakeSetting(huemulBigDataGov: HuemulBigDataGovernance) extends Serializable {
  /***
   * Start date for this configuration
   */
  var StartDate : Calendar = _
  def getStartDate: Calendar = StartDate
  /*** set Start date for this configuration
   * @param value: date
   */
  def setStartDate(value: Calendar): HuemulDataLakeSetting = {
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
  def setStartDate(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): HuemulDataLakeSetting = {
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
  def setEndDate(value: Calendar): HuemulDataLakeSetting = {
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
  def setEndDate(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): HuemulDataLakeSetting = {
    EndDate = huemulBigDataGov.setDateTime(year, month, day, hour, minute, second)
    this
  }

  /***
   * Type (example: TEXT_FILE, EXCEL_FILE)
   */
  var FileType: HuemulTypeFileType = _
  def getFileType: HuemulTypeFileType = FileType
  /*** set FileType to read
   * Type (example: TEXT_FILE, EXCEL_FILE)
   */
  def setFileType(value: HuemulTypeFileType): HuemulDataLakeSetting = {
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
  def setFileName(value: String): HuemulDataLakeSetting = {
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
  def setLocalPath(value: String): HuemulDataLakeSetting = {
    LocalPath = value
    this
  }
  /***
   * from Global path
   */
  var GlobalPath  : ArrayBuffer[HuemulKeyValuePath] = _
  def getGlobalPath: ArrayBuffer[HuemulKeyValuePath] = GlobalPath
  def setGlobalPath(value: ArrayBuffer[HuemulKeyValuePath]): HuemulDataLakeSetting = {
    GlobalPath = value
    this
  }
  /***
   * Responsible contact to resolve doubt or errors
   */
  var ContactName :  String= ""
  def getContactName: String = ContactName
  def setContactName(value: String): HuemulDataLakeSetting = {
    ContactName = value
    this
  }

  /***
   * Data Schema configuration
   */
  var DataSchemaConf: HuemulDataLakeSchemaConf = new  HuemulDataLakeSchemaConf()

  /**
   * set column delimiter type
   * @param value: POSITION,CHARACTER, NONE
   * @return
   */
  def setColumnDelimiterType(value: HuemulTypeSeparator): HuemulDataLakeSetting = {
    DataSchemaConf.ColSeparatorType = value
    this
  }

  def setColumnDelimiter(value: String): HuemulDataLakeSetting = {
    DataSchemaConf.ColSeparator = value
    this
  }

  /**
   * set columns name from strings: ex: column1;column2;column3....
   * @param columnsList column list ex: column1;column2;column3...
   * @return
   */
  def setColumnsString(columnsList: String): HuemulDataLakeSetting = {
    DataSchemaConf.setHeaderColumnsString(columnsList)
    this
  }



  /**
   * add columns definition
   * @param columnNameBusiness column business name (official name)
   * @param columnNameTI column technical name (for lineage)
   * @param dataType column data Type
   * @param description column description
   * @param posStart start position (only for POSITION delimiter type, start on cero)
   * @param posEnd end  position (only for POSITION delimiter type)
   * @param applyTrim default false, true for auto trim
   * @param convertToNull default false, true for convert text "null" to null
   * @return
   */
  def addColumn(columnNameBusiness: String
                 , columnNameTI: String = null
                 , dataType: DataType = StringType
                 , description: String = "[[missing description]]"
                 , posStart: Integer = null
                 , posEnd: Integer = null
                 , applyTrim: Boolean = false
                 , convertToNull: Boolean = false
                ): HuemulDataLakeSetting = {
    DataSchemaConf.AddColumns(columnNameBusiness
            , columnNameTI
            , dataType
            , description
            , posStart
            , posEnd
            , applyTrim
            , convertToNull)
    this
  }

  /***
   * Log Schema configuration
   */
  var LogSchemaConf: HuemulDataLakeSchemaConf = new  HuemulDataLakeSchemaConf()

  /**
   * set column delimiter type for Header (if exists)
   * @param value ex: POSITION, CHARACTER, NONE
   * @return
   */
  def setHeaderColumnDelimiterType(value: HuemulTypeSeparator): HuemulDataLakeSetting = {
    LogSchemaConf.ColSeparatorType = value
    this
  }

  /**
   * set column delimiter for header
   * @param value ex: \\|, \t, ;
   * @return
   */
  def setHeaderColumnDelimiter(value: String): HuemulDataLakeSetting = {
    LogSchemaConf.ColSeparator = value
    this
  }

  /**
   * set columns name for header
   * @param columnsList ex: headerInfo1;headerInfo2;headerInfo3...
   * @return
   */
  def setHeaderColumnsString(columnsList: String): HuemulDataLakeSetting = {
    LogSchemaConf.setHeaderColumnsString(columnsList)
    this
  }

  /***
   * FieldName for N° Rows in control line
   */
  var LogNumRows_FieldName: String = ""
  def getLogNumRowsColumnName: String = LogNumRows_FieldName
  /*** for Header, set column name that have the rows count
   *
   */
  def setLogNumRowsColumnName(value: String ): HuemulDataLakeSetting = {
    LogNumRows_FieldName = value
    this
  }

  //FROM 2.4
  /***
   * character for row delimited in PDF files
   */
  var rowDelimiterForPDF: String = "\\n"
  def getRowDelimiterForPDF: String = rowDelimiterForPDF
  def setRowDelimiterForPDF(value: String): HuemulDataLakeSetting = {
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
  
  
  def GetDataBase(Division: ArrayBuffer[HuemulKeyValuePath]): String = {
    huemulBigDataGov.GlobalSettings.getDataBase(huemulBigDataGov, Division)
  }
  
  def GetDataBase(Division: ArrayBuffer[HuemulKeyValuePath], ManualEnvironment: String): String = {
    huemulBigDataGov.GlobalSettings.getDataBase( Division, ManualEnvironment)
  }
  
  def GetPath(Division: ArrayBuffer[HuemulKeyValuePath]): String = {
    huemulBigDataGov.GlobalSettings.getPath(huemulBigDataGov, Division)
  }
  
  def GetPath(Division: ArrayBuffer[HuemulKeyValuePath], ManualEnvironment: String): String = {
    huemulBigDataGov.GlobalSettings.getPath( Division, ManualEnvironment)
  }
  
}