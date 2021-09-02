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
  var startDate : Calendar = _
  def getStartDate: Calendar = startDate
  /*** set Start date for this configuration
   * @param value: date
   */
  def setStartDate(value: Calendar): HuemulDataLakeSetting = {
    startDate = value
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
    startDate = huemulBigDataGov.setDateTime(year, month, day, hour, minute, second)
    this
  }
  /***
   * End date for this configuration
   */
  var endDate : Calendar = _
  def getEndDate: Calendar = endDate
  /*** set end date for this configuration
   * @param value: date
   */
  def setEndDate(value: Calendar): HuemulDataLakeSetting = {
    endDate = value
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
    endDate = huemulBigDataGov.setDateTime(year, month, day, hour, minute, second)
    this
  }

  /***
   * Type (example: TEXT_FILE, EXCEL_FILE)
   */
  var fileType: HuemulTypeFileType = _
  def getFileType: HuemulTypeFileType = fileType
  /*** set FileType to read
   * Type (example: TEXT_FILE, EXCEL_FILE)
   */
  def setFileType(value: HuemulTypeFileType): HuemulDataLakeSetting = {
    fileType = value
    this
  }

  /***
   * File Name (example: "PLAN-CTAS.TXT")
   */
  var fileName    : String = ""
  def getFileName: String = fileName
  /*** File Name (example: "PLAN-CTAS.TXT")
   * {{YYYY}} = replace 4 digits year
   * {{YY}} = replace 2 digits year
   * {{MM}} = replace with month
   * {{DD}} = replace 2 digits day
   */
  def setFileName(value: String): HuemulDataLakeSetting = {
    fileName = value
    this
  }
  /*** Local path (example "SBIF\\{{YYYY}}{{MM}}\\"
   * * {{YYYY}} = replace 4 digits year
   * * {{YY}} = replace 2 digits year
   * * {{MM}} = replace with month
   * * {{DD}} = replace 2 digits day
   */
  var localPath   : String= ""
  def getLocalPath: String = localPath
  def setLocalPath(value: String): HuemulDataLakeSetting = {
    localPath = value
    this
  }
  /***
   * from Global path
   */
  var globalPath  : ArrayBuffer[HuemulKeyValuePath] = _
  def getGlobalPath: ArrayBuffer[HuemulKeyValuePath] = globalPath
  def setGlobalPath(value: ArrayBuffer[HuemulKeyValuePath]): HuemulDataLakeSetting = {
    globalPath = value
    this
  }
  /***
   * Responsible contact to resolve doubt or errors
   */
  var contactName :  String= ""
  def getContactName: String = contactName
  def setContactName(value: String): HuemulDataLakeSetting = {
    contactName = value
    this
  }

  /***
   * Data Schema configuration
   */
  var dataSchemaConf: HuemulDataLakeSchemaConf = new  HuemulDataLakeSchemaConf()

  /**
   * set column delimiter type
   * @param value: POSITION,CHARACTER, NONE
   * @return
   */
  def setColumnDelimiterType(value: HuemulTypeSeparator): HuemulDataLakeSetting = {
    dataSchemaConf.colSeparatorType = value
    this
  }

  def setColumnDelimiter(value: String): HuemulDataLakeSetting = {
    dataSchemaConf.colSeparator = value
    this
  }

  /**
   * set columns name from strings: ex: column1;column2;column3....
   * @param columnsList column list ex: column1;column2;column3...
   * @return
   */
  def setColumnsString(columnsList: String): HuemulDataLakeSetting = {
    dataSchemaConf.setHeaderColumnsString(columnsList)
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
    dataSchemaConf.addColumns(columnNameBusiness
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
  var logSchemaConf: HuemulDataLakeSchemaConf = new  HuemulDataLakeSchemaConf()

  /**
   * set column delimiter type for Header (if exists)
   * @param value ex: POSITION, CHARACTER, NONE
   * @return
   */
  def setHeaderColumnDelimiterType(value: HuemulTypeSeparator): HuemulDataLakeSetting = {
    logSchemaConf.colSeparatorType = value
    this
  }

  /**
   * set column delimiter for header
   * @param value ex: \\|, \t, ;
   * @return
   */
  def setHeaderColumnDelimiter(value: String): HuemulDataLakeSetting = {
    logSchemaConf.colSeparator = value
    this
  }

  /**
   * set columns name for header
   * @param columnsList ex: headerInfo1;headerInfo2;headerInfo3...
   * @return
   */
  def setHeaderColumnsString(columnsList: String): HuemulDataLakeSetting = {
    logSchemaConf.setHeaderColumnsString(columnsList)
    this
  }

  /***
   * FieldName for N° Rows in control line
   */
  var logNumRowsFieldName: String = ""
  def getLogNumRowsColumnName: String = logNumRowsFieldName
  /*** for Header, set column name that have the rows count
   *
   */
  def setLogNumRowsColumnName(value: String ): HuemulDataLakeSetting = {
    logNumRowsFieldName = value
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
  
  
  private var useYear: Integer = _
  def getUseYear: Integer =  useYear
  private var useMonth: Integer = _
  def getUseMonth: Integer =  useMonth
  private var useDay: Integer = _
  def getUseDay: Integer =  useDay
  private var useHour: Integer = _
  def getUseHour: Integer =  useHour
  private var useMinute: Integer = _
  def getUseMinute: Integer =  useMinute
  private var useSecond: Integer = _
  def getUseSecond: Integer =  useSecond
  private var useParams: String = ""
  def getUseParams: String =  useParams
  
  def setParamsInUse(year: Integer, month: Integer, day: Integer, hour: Integer, min: Integer, sec: Integer, AdditionalParams: String){
    useYear = year
    useMonth = month
    useDay = day
    useHour = hour
    useMinute = min
    useSecond = sec
    useParams = AdditionalParams
  }
  
  
  def getFullNameWithPath: String = {
    getPath(getGlobalPath) + getLocalPath + getFileName
  }


  def getDataBase(division: ArrayBuffer[HuemulKeyValuePath]): String = {
    huemulBigDataGov.globalSettings.getDataBase(huemulBigDataGov, division)
  }

  def getDataBase(division: ArrayBuffer[HuemulKeyValuePath], manualEnvironment: String): String = {
    huemulBigDataGov.globalSettings.getDataBase( division, manualEnvironment)
  }

  def getPath(Division: ArrayBuffer[HuemulKeyValuePath]): String = {
    huemulBigDataGov.globalSettings.getPath(huemulBigDataGov, Division)
  }
  
  def getPath(division: ArrayBuffer[HuemulKeyValuePath], manualEnvironment: String): String = {
    huemulBigDataGov.globalSettings.getPath( division, manualEnvironment)
  }
  
}