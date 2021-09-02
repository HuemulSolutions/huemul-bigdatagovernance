package com.huemulsolutions.bigdata.datalake

import org.apache.spark.rdd._
import java.util.Calendar
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import scala.collection.mutable._
//import com.huemulsolutions.bigdata.tables._
//import com.huemulsolutions.bigdata.tables.huemulType_Tables._
import com.huemulsolutions.bigdata.control.HuemulTypeFrequency._


class HuemulDataLake(huemulBigDataGov: HuemulBigDataGovernance, control: HuemulControl) extends Serializable {
  /***
   * Id of data (example: PlanCuentas)
   */
  val logicalName : String= this.getClass.getSimpleName.replace("$", "") // ""
  /***
   * Group or System (Example: SBIF)
   */
  var groupName : String= ""
  /***
   * Description of content
   */
  var description: String= ""
  /***
   * Configuration Details for dates
   */
  var settingByDate: ArrayBuffer[HuemulDataLakeSetting] = new ArrayBuffer[HuemulDataLakeSetting]()

  /**when text = null, change to null
   *
   */
  var stringNullAsNull: Boolean = true

  var applyTrim: Boolean = true

  /***
   * Information about interfaces
   */
  var settingInUse: HuemulDataLakeSetting = _

  /***
   * get execution's info about rows and DF
   */
  var dataFrameHuemul: HuemulDataFrame = new HuemulDataFrame(huemulBigDataGov, control)
  var log: HuemulDataLakeLogInfo = new HuemulDataLakeLogInfo()


  var fileName: String = _ //Name who was read
  var startReadDt: Calendar = _
  var stopReadDt: Calendar = _

  var error: HuemulControlError = new HuemulControlError(huemulBigDataGov)
  var errorIsError: Boolean = false
  //var Error_Text: String = ""
  private var rawFilesId: String = ""
  def setRawFilesId(id: String) {rawFilesId = id}
  def getRawFilesId: String =  rawFilesId
  //RAW_OpenFile(RAW_File_Info, year, mes, day, hora, min, seg, AdditionalParams)

  var dataRdd: RDD[String] = _
  var dataRawDf: DataFrame = _ // Store the loaded data from Avro or other future format

  //FROM 2.5
  //ADD AVRO SUPPORT
  private var _avroFormat: String = huemulBigDataGov.globalSettings.getAvroFormat
  def getAvroFormat: String =   _avroFormat
  def setAvroFormat(value: String) {_avroFormat = value}

  private var _avroCompression: String = huemulBigDataGov.globalSettings.getAvroCompression
  def getAvroCompression: String =   _avroCompression
  def setAvroCompression(value: String) {_avroCompression = value}

  //from 2.4
  /**
   * get extended info for PDF Files
   * pos 0: line number
   * pos 1: line length
   * pos 2: line length with trim
   * pos 3: text line
   */
  var dataRddExtended: RDD[(Int, Int, Int, String)] = _

  //from 2.4
  /**
   * get metadata info from PDF Files
   * pos 0: attribute name
   * pos 1: attribute value
   */
  var DataRddMetadata: Array[(String, String)] = _


  private val _allColumnsAsString: Boolean = true
  //def allColumnsAsString(value: Boolean) {_allColumnsAsString = value}

  def setFrequency(value: HuemulTypeFrequency) {
    _frequency = value
  }
  private var _frequency: HuemulTypeFrequency = HuemulTypeFrequency.ANY_MOMENT
  def getFrequency: HuemulTypeFrequency =  _frequency

  def raiseErrorRaw(txt: String, errorCode: Integer) {
    error.controlErrorMessage = txt
    error.controlErrorErrorCode = errorCode
    errorIsError = true
    control.raiseError(txt)

  }

  /**
   * RAW_to_DF: Create DF from RDD, save at Data.DataDF
   */
  def dfFromRaw(rowRdd: RDD[Row], alias: String) {
    dataFrameHuemul.dfFromRAW(rowRdd, alias)
    this.stopReadDt = huemulBigDataGov.getCurrentDateTimeJava
    //Register use in control
    control.registerRawUse(this, alias)

  }

   /**
   * Register DF to Data.DataDF, and assign alias (NEW 2.5)
   *
   * @param rowData   DataFrame Data
   * @param alias     DataFrame Alias
   */
  def dfFromRaw(rowData: DataFrame, alias: String): Unit = {
    dataFrameHuemul.setDataFrame(rowData,alias,SaveInTemp = false)
    this.stopReadDt = huemulBigDataGov.getCurrentDateTimeJava
    //Register use in control
    control.registerRawUse(this, alias)
  }

  /***
   * ConvertSchemaLocal: Transforma un string en un objeto RAW
   * SchemaConf: Schema configuration in definition
   * Schema: get from Data or Log (ex: RAW_Exec.Data.DataSchema)
   * Return: objeto RAW
   */

  private def convertSchemaLocal(schemaConf: HuemulDataLakeSchemaConf, row : String, localAllColumnsAsString: Boolean, customColumn: String) : Row = {
    val schema: StructType = createSchema(schemaConf, localAllColumnsAsString)
    var dataArrayDest : Array[Any] = null
    val customColumnNum: Integer = if (customColumn == null) 0 else 1

    if (schemaConf.colSeparatorType == HuemulTypeSeparator.CHARACTER) {
      //Get separator and numCols from params
      val separator: String = schemaConf.colSeparator
      val numCols: Integer = schema.length

      if (numCols == 0 || numCols == null){
        this.raiseErrorRaw("huemul_DataLake Error: Schema not defined",3002)
      }

      //declare variables for transform
      dataArrayDest = new Array[Any](numCols + customColumnNum)
      val dataArrayOrig = row.split(separator,numCols)

      dataArrayOrig.indices.foreach { i =>
          var temp1 = dataArrayOrig(i)
          val fieldSchema = schemaConf.columnsDef(i)

          if (applyTrim || fieldSchema.getApplyTrim  )
          temp1 = temp1.trim()

          if ((stringNullAsNull || fieldSchema.getConvertToNull) && temp1.toLowerCase() == "null")
            temp1 = null

          dataArrayDest(i) = if (_allColumnsAsString) temp1
                              else if (fieldSchema.getDataType == StringType) temp1
                              else if (fieldSchema.getDataType == IntegerType) temp1.toInt
                              else if (fieldSchema.getDataType.toString.toLowerCase().contains("decimal")) Decimal.apply(temp1)
                              else if (fieldSchema.getDataType == LongType) temp1.toLong
                              else if (fieldSchema.getDataType == DoubleType) temp1.toDouble
                              else if (fieldSchema.getDataType == ShortType) temp1.toShort
                              else if (fieldSchema.getDataType == ByteType) temp1.toByte
                              else if (fieldSchema.getDataType == FloatType) temp1.toFloat

                              else if (fieldSchema.getDataType == BooleanType) temp1.toBoolean

                              else if (fieldSchema.getDataType == DateType) temp1
                              else if (fieldSchema.getDataType == TimestampType) temp1

                              else if (fieldSchema.getDataType == ArrayType) temp1.toArray

                              //else if (FieldSchema.getDataType == BinaryType) temp1.to
                              else temp1

      }
    }
    else if (schemaConf.colSeparatorType == HuemulTypeSeparator.POSITION) {
      dataArrayDest = new Array[Any](schemaConf.columnsDef.length + customColumnNum)


      schemaConf.columnsDef.indices.foreach { i =>
        val dataInfo = schemaConf.columnsDef(i)
        //#40: allow dynamic last position
        var lPosFin = dataInfo.getPosFin.toInt
        if (lPosFin == -1)
          lPosFin = row.length()
        var temp1 = row.substring(dataInfo.getPosIni.toInt, lPosFin)
        val FieldSchema = schemaConf.columnsDef(i)
        if (applyTrim || FieldSchema.getApplyTrim  )
          temp1 = temp1.trim()

        if ((stringNullAsNull || FieldSchema.getConvertToNull) && temp1.toLowerCase() == "null")
          temp1 = null


        dataArrayDest(i) =   if (_allColumnsAsString) temp1
                              else if (FieldSchema.getDataType == StringType) temp1
                              else if (FieldSchema.getDataType == IntegerType) temp1.toInt
                              else if (FieldSchema.getDataType.toString.toLowerCase().contains("decimal")) Decimal.apply(temp1)
                              else if (FieldSchema.getDataType == LongType) temp1.toLong
                              else if (FieldSchema.getDataType == DoubleType) temp1.toDouble
                              else if (FieldSchema.getDataType == ShortType) temp1.toShort
                              else if (FieldSchema.getDataType == ByteType) temp1.toByte
                              else if (FieldSchema.getDataType == FloatType) temp1.toFloat

                              else if (FieldSchema.getDataType == BooleanType) temp1.toBoolean

                              else if (FieldSchema.getDataType == DateType) temp1
                              else if (FieldSchema.getDataType == TimestampType) temp1

                              else if (FieldSchema.getDataType == ArrayType) temp1.toArray

                              //else if (FieldSchema.getDataType == BinaryType) temp1.to
                              else temp1
       }

    }

    if (customColumnNum == 1) {
      dataArrayDest(dataArrayDest.length-1) = customColumn
    }

    Row.fromSeq(dataArrayDest)
  }

  /***
   * ConvertSchema: Transforma un string en un objeto ROW
   * Return: objeto Row
   */

  def convertSchema(row : String) : Row = {
     convertSchemaLocal(this.settingInUse.dataSchemaConf, row, _allColumnsAsString, null)
    //SchemaConf: huemul_DataLakeSchemaConf, Schema: StructType,
  }

  /***
   * ConvertSchema: Transforma un string en un objeto ROW
   * incluye parámetro de texto adicional.
   * Return: objeto Row
   */

  def convertSchema(row : String, customColumn: String) : Row = {
     convertSchemaLocal(this.settingInUse.dataSchemaConf, row, _allColumnsAsString, customColumn)
    //SchemaConf: huemul_DataLakeSchemaConf, Schema: StructType,
  }

  /***
   * Open the file with huemulBigDataGov.spark.sparkContext.textFile <br>
   * and set de DataRDD attribute
   */
  def openFile(year: Integer, month: Integer, day: Integer, hour: Integer, min: Integer, sec: Integer, additionalParams: String = null): Boolean = {
    //Ask for definition in date
    val dateProcess = huemulBigDataGov.setDateTime(year, month, day, hour, min, sec)
    var localErrorCode: Integer = null
    if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug("N° array config: " + this.settingByDate.length.toString)
    val dataResult = this.settingByDate.filter { x => dateProcess.getTimeInMillis >= x.getStartDate.getTimeInMillis && dateProcess.getTimeInMillis <= x.getEndDate.getTimeInMillis  }

    if (dataResult.isEmpty) {
      localErrorCode = 3003
      raiseErrorRaw(s"huemul_DataLake Error: No definition found in ${this.logicalName} (${this.settingByDate.length.toString})",localErrorCode)
    } else if (dataResult.length > 1) {
      localErrorCode = 3004
      raiseErrorRaw(s"huemul_DataLake Error: Multiple definitions found in ${this.logicalName} (${this.settingByDate.length.toString})", localErrorCode )
    } else if (dataResult.length == 1) {
       this.settingInUse = dataResult(0)
    }

    this.settingInUse.setParamsInUse(year, month, day, hour, min, sec, additionalParams)

    try {
        this.startReadDt = huemulBigDataGov.getCurrentDateTimeJava
        /************************************************************************/
        /********   GET  FIELDS   ********************************/
      /** **********************************************************************/
      //Fields
      val fieldsDetail: StructType = createSchema(this.settingInUse.dataSchemaConf, _allColumnsAsString)

        this.dataFrameHuemul.SetDataSchema(fieldsDetail)
        if (this.huemulBigDataGov.debugMode) {
          huemulBigDataGov.logMessageDebug("printing DataSchema from settings: ")
          this.dataFrameHuemul.getDataSchema.printTreeString()
        }

        huemulBigDataGov.logMessageInfo("N° Columns in RowFile: " + this.dataFrameHuemul.getNumCols.toString)

        /************************************************************************/
        /********   OPEN FILE   *********************************/
        /************************************************************************/
        //Open File
        this.fileName = huemulBigDataGov.replaceWithParams(this.settingInUse.getFullNameWithPath, year, month, day, hour, min, sec, additionalParams)
        //DQ: Validate name special characters
        if (this.fileName.contains("{{") || this.fileName.contains("}}")) {
          localErrorCode = 3005
          this.raiseErrorRaw("huemul_DataLake Error: FileName contains incorrect characters {{ or }}: " + this.fileName, localErrorCode)
        }
        huemulBigDataGov.logMessageInfo("Reading File: " + this.fileName)

        if (this.settingInUse.getFileType == HuemulTypeFileType.TEXT_FILE) {
          this.dataRdd = huemulBigDataGov.spark.sparkContext.textFile(this.fileName)

          if (this.dataRdd  == null) {
            localErrorCode = 3009
            this.raiseErrorRaw(s"huemul_DataLake Error: File doesn't exist $fileName",localErrorCode)
          }

          if (huemulBigDataGov.gethuemul_showDemoLines() ) huemulBigDataGov.logMessageInfo("2 first line example of file: " + this.fileName)
          this.dataRdd.take(2).foreach { x =>
            if (huemulBigDataGov.gethuemul_showDemoLines() )
              huemulBigDataGov.logMessageInfo(x)
            }
        } else if (this.settingInUse.getFileType == HuemulTypeFileType.PDF_FILE) {
          val _pdfFile = huemulBigDataGov.spark.sparkContext.binaryFiles(this.fileName).collect()

          val pdfResult = new HuemulDataLakePDF()
          pdfResult.openPdf(_pdfFile, this.settingInUse.getRowDelimiterForPDF)

          this.dataRdd = huemulBigDataGov.spark.sparkContext.parallelize(pdfResult.rddBase)
          this.dataRddExtended = huemulBigDataGov.spark.sparkContext.parallelize(pdfResult.rddPdfData)
          this.DataRddMetadata = pdfResult.rddPdfMetadata
        } else if (this.settingInUse.getFileType == HuemulTypeFileType.AVRO_FILE) {
          huemulBigDataGov.logMessageInfo("Opening AVRO data file and building DF DataRaw")

          this.dataRawDf= huemulBigDataGov.spark.read
            .format(this.getAvroFormat)
            .option(if (this.getAvroCompression == null) "" else "compression",if (this.getAvroCompression == null) "" else this.getAvroCompression)
            //.schema( this.DataFramehuemul.getDataSchema())
            .load(this.fileName)

        } else if (this.settingInUse.getFileType == HuemulTypeFileType.DELTA_FILE ||
                   this.settingInUse.getFileType == HuemulTypeFileType.ORC_FILE ||
                   this.settingInUse.getFileType == HuemulTypeFileType.PARQUET_FILE ) {
          huemulBigDataGov.logMessageInfo(s"Opening ${this.settingInUse.getFileType} data file and building DF DataRaw")

          val _format = this.settingInUse.getFileType.toString.replace("_FILE", "")

          this.dataRawDf= huemulBigDataGov.spark.read
            .format(_format)
            .load(this.fileName)

        } else {
          localErrorCode = 3006
          this.raiseErrorRaw("huemul_DataLake Error: FileType missing (add this.FileType setting in DataLake definition)",localErrorCode)
        }


        /************************************************************************/
        /********   GET   LOG   FIELDS   ********************************/
        /************************************************************************/
        //Log Fields
        if (this.settingInUse.logSchemaConf.colSeparatorType == HuemulTypeSeparator.NONE) {
          this.log.dataFirstRow = ""
          this.log.logIsRead = false
          this.log.logIsInfoRows = false
        }
        else if (this.settingInUse.logSchemaConf.colSeparatorType == HuemulTypeSeparator.POSITION ||
                 this.settingInUse.logSchemaConf.colSeparatorType == HuemulTypeSeparator.CHARACTER
                ) {
          this.log.dataFirstRow = this.dataRdd.first()

          val fieldsLogSchema = createSchema(this.settingInUse.logSchemaConf, allColumnsAsString = true)
          if (fieldsLogSchema == null || fieldsLogSchema.length == 0) {
            localErrorCode = 3007
            this.raiseErrorRaw("huemul_DataLake Error: Don't have header information for Detail, see fieldsSeparatorType field ", localErrorCode)
          }

          this.log.logSchema = fieldsLogSchema

          this.log.logIsRead = true
          this.log.logIsInfoRows = true

          val logRdd =  huemulBigDataGov.spark.sparkContext.parallelize(List(this.log.dataFirstRow))
          val rowRdd =  logRdd.map { x =>  convertSchemaLocal(this.settingInUse.logSchemaConf, x, localAllColumnsAsString = true,null)}

          //Create DataFrame
          if (huemulBigDataGov.debugMode) {
            huemulBigDataGov.logMessageDebug("Demo DF Log: " + this.fileName)
            huemulBigDataGov.logMessageDebug(rowRdd.take(2).foreach { x => huemulBigDataGov.logMessageDebug(x) })
          }
          this.log.logDF = huemulBigDataGov.spark.createDataFrame(rowRdd, this.log.logSchema)

          if (huemulBigDataGov.debugMode) this.log.logDF.show()
          if (this.settingInUse.getLogNumRowsColumnName != null) {
            this.log.dataNumRows = this.log.logDF.first().getAs[String](this.settingInUse.getLogNumRowsColumnName).toLong
            huemulBigDataGov.logMessageDebug("N° Rows according Log: " + this.log.dataNumRows.toString)
          }

        }


    } catch {
      case e: Exception =>
        huemulBigDataGov.logMessageError("Error Code")
        huemulBigDataGov.logMessageError(localErrorCode)
        huemulBigDataGov.logMessageError(e.getMessage)

        if (localErrorCode == null)
          localErrorCode = 3001
        this.errorIsError = true
        this.error.setError(e, this.getClass.getName, this, localErrorCode)
    }

     !this.errorIsError
  }

  def createSchema(schemaConf: HuemulDataLakeSchemaConf, allColumnsAsString: Boolean = false): StructType = {
    //Fields
    var fieldsDetail : ArrayBuffer[StructField] = null
    fieldsDetail = schemaConf.columnsDef.map (fieldName => StructField(fieldName.getColumnNameBusiness, if (allColumnsAsString) StringType else fieldName.getDataType, nullable = true) )

    if (fieldsDetail == null) {
      this.raiseErrorRaw("huemul_DataLake Error: Don't have header information for Detail, see fieldsSeparatorType field ", 3008)
    }

    //from 2.4 --> add custom columns at the end
    val localCustomColumn = schemaConf.getCustomColumn
    if ( localCustomColumn != null) {
      fieldsDetail.append(StructField(localCustomColumn.getColumnNameBusiness, if (allColumnsAsString) StringType else localCustomColumn.getDataType, nullable = true))
    }
    
    StructType(fieldsDetail)
  }
   

}
