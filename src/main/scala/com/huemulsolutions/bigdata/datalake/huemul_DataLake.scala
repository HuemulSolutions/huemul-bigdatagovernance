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
import com.huemulsolutions.bigdata.control.huemulType_Frequency._


class huemul_DataLake(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends Serializable {
  /***
   * Id of data (example: PlanCuentas)
   */
  val LogicalName : String= this.getClass.getSimpleName.replace("$", "") // ""
  /***
   * Group or System (Example: SBIF)
   */
  var GroupName : String= ""
  /***
   * Description of content
   */
  var Description: String= ""
  /***
   * Configuration Details for dates
   */
  var SettingByDate: ArrayBuffer[huemul_DataLakeSetting] = new ArrayBuffer[huemul_DataLakeSetting]()
  
  /**when text = null, change to null
   * 
   */
  var StringNull_as_Null: Boolean = true
  
  var ApplyTrim: Boolean = true
  
  /***
   * Information about interfaces
   */
  var SettingInUse: huemul_DataLakeSetting = _
  
  /***
   * get execution's info about rows and DF
   */
  var DataFramehuemul: huemul_DataFrame = new huemul_DataFrame(huemulBigDataGov, Control)
  var Log: huemul_DataLakeLogInfo = new huemul_DataLakeLogInfo()
  
   
  var FileName: String = _ //Name who was read
  var StartRead_dt: Calendar = _
  var StopRead_dt: Calendar = _

  var Error: huemul_ControlError = new huemul_ControlError(huemulBigDataGov)
  var Error_isError: Boolean = false
  //var Error_Text: String = ""  
  private var rawFiles_id: String = ""
  def setrawFiles_id(id: String) {rawFiles_id = id}
  def getrawFiles_id(): String =  rawFiles_id
  //RAW_OpenFile(RAW_File_Info, year, mes, day, hora, min, seg, AdditionalParams)
  
  var DataRDD: RDD[String] = _
  var DataRawDF: DataFrame = _ // Store the loaded data from Avro or other future format
  
  //FROM 2.5 
  //ADD AVRO SUPPORT
  private var _avro_format: String = huemulBigDataGov.GlobalSettings.getAVRO_format
  def getAVRO_format: String =   _avro_format
  def setAVRO_format(value: String) {_avro_format = value} 
  
  private var _avro_compression: String = huemulBigDataGov.GlobalSettings.getAVRO_compression
  def getAVRO_compression: String =   _avro_compression
  def setAVRO_compression(value: String) {_avro_compression = value} 
  
  //from 2.4
  /**
   * get extended info for PDF Files
   * pos 0: line number
   * pos 1: line length
   * pos 2: line length with trim
   * pos 3: text line
   */
  var DataRDD_extended: RDD[(Int, Int, Int, String)] = _
  
  //from 2.4
  /**
   * get metadata info from PDF Files
   * pos 0: attribute name
   * pos 1: attribute value
   */
  var DataRDD_Metadata: Array[(String, String)] = _


  private val _allColumnsAsString: Boolean = true
  //def allColumnsAsString(value: Boolean) {_allColumnsAsString = value}
  
  def setFrequency(value: huemulType_Frequency) {
    _Frequency = value
  }
  private var _Frequency: huemulType_Frequency = huemulType_Frequency.ANY_MOMENT
  def getFrequency: huemulType_Frequency =  _Frequency
  
  def RaiseError_RAW(txt: String, Error_Code: Integer) {
    Error.ControlError_Message = txt
    Error.ControlError_ErrorCode = Error_Code
    Error_isError = true
    Control.RaiseError(txt)
    
  } 
  
  /**
   * RAW_to_DF: Create DF from RDD, save at Data.DataDF
   */
  def DF_from_RAW(rowRDD: RDD[Row], Alias: String) {
    DataFramehuemul.DF_from_RAW(rowRDD, Alias)
    this.StopRead_dt = huemulBigDataGov.getCurrentDateTimeJava
    //Register use in control
    Control.RegisterRAW_USE(this, Alias)
    
  }
  
   /**
   * Register DF to Data.DataDF, and assign alias (NEW 2.5)
   *
   * @param rowData   DataFrame Data
   * @param Alias     DataFrame Alias
   */
  def DF_from_RAW(rowData: DataFrame, Alias: String): Unit = {
    DataFramehuemul.setDataFrame(rowData,Alias,SaveInTemp = false)
    this.StopRead_dt = huemulBigDataGov.getCurrentDateTimeJava
    //Register use in control
    Control.RegisterRAW_USE(this, Alias)
  }
  
  /***
   * ConvertSchemaLocal: Transforma un string en un objeto RAW
   * SchemaConf: Schema configuration in definition
   * Schema: get from Data or Log (ex: RAW_Exec.Data.DataSchema)
   * Return: objeto RAW
   */
  
  private def ConvertSchemaLocal(SchemaConf: huemul_DataLakeSchemaConf, row : String, local_allColumnsAsString: Boolean, customColumn: String) : Row = { 
    val Schema: StructType = CreateSchema(SchemaConf, local_allColumnsAsString)
    var DataArray_Dest : Array[Any] = null
    val customColumnNum: Integer = if (customColumn == null) 0 else 1
    
    if (SchemaConf.ColSeparatorType == huemulType_Separator.CHARACTER) {
      //Get separator and numCols from params
      val separator: String = SchemaConf.ColSeparator
      
      val numCols: Integer = Schema.length 
      
      if (numCols == 0 || numCols == null){
        this.RaiseError_RAW("huemul_DataLake Error: Schema not defined",3002)
      }
      
      //declare variables for transform
      DataArray_Dest = new Array[Any](numCols + customColumnNum)      
      val DataArray_Orig = row.split(separator,numCols)       
          
      DataArray_Orig.indices.foreach { i => 
          var temp1 = DataArray_Orig(i) 
          val FieldSchema = SchemaConf.ColumnsDef(i)
          
          if (ApplyTrim || FieldSchema.getApplyTrim  )
          temp1 = temp1.trim()
            
          if ((StringNull_as_Null || FieldSchema.getConvertToNull) && temp1.toLowerCase() == "null") 
            temp1 = null
            
          DataArray_Dest(i) = if (_allColumnsAsString) temp1
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
    else if (SchemaConf.ColSeparatorType == huemulType_Separator.POSITION) {      
      DataArray_Dest = new Array[Any](SchemaConf.ColumnsDef.length + customColumnNum)
      
     
      SchemaConf.ColumnsDef.indices.foreach { i => 
        val dataInfo = SchemaConf.ColumnsDef(i)
        //#40: allow dynamic last position
        var lPosFin = dataInfo.getPosFin.toInt
        if (lPosFin == -1)
          lPosFin = row.length()        
        var temp1 = row.substring(dataInfo.getPosIni.toInt, lPosFin) 
        val FieldSchema = SchemaConf.ColumnsDef(i)
        if (ApplyTrim || FieldSchema.getApplyTrim  )
          temp1 = temp1.trim()
        
        if ((StringNull_as_Null || FieldSchema.getConvertToNull) && temp1.toLowerCase() == "null") 
          temp1 = null
        
          
        DataArray_Dest(i) =   if (_allColumnsAsString) temp1
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
      DataArray_Dest(DataArray_Dest.length-1) = customColumn
    }
     
    Row.fromSeq(DataArray_Dest) 
  }
  
  /***
   * ConvertSchema: Transforma un string en un objeto ROW
   * Return: objeto Row
   */
  
  def ConvertSchema(row : String) : Row = {
     ConvertSchemaLocal(this.SettingInUse.DataSchemaConf, row, _allColumnsAsString, null)
    //SchemaConf: huemul_DataLakeSchemaConf, Schema: StructType,
  }
  
  /***
   * ConvertSchema: Transforma un string en un objeto ROW
   * incluye parámetro de texto adicional.
   * Return: objeto Row
   */
  
  def ConvertSchema(row : String, customColumn: String) : Row = {
     ConvertSchemaLocal(this.SettingInUse.DataSchemaConf, row, _allColumnsAsString, customColumn)
    //SchemaConf: huemul_DataLakeSchemaConf, Schema: StructType,
  }
  
  /***
   * Open the file with huemulBigDataGov.spark.sparkContext.textFile <br>
   * and set de DataRDD attribute
   */
  def OpenFile(year: Integer, month: Integer, day: Integer, hour: Integer, min: Integer, sec: Integer, AdditionalParams: String = null): Boolean = {    
    //Ask for definition in date
    val DateProcess = huemulBigDataGov.setDateTime(year, month, day, hour, min, sec)
    var LocalErrorCode: Integer = null
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug("N° array config: " + this.SettingByDate.length.toString)
    val DataResult = this.SettingByDate.filter { x => DateProcess.getTimeInMillis >= x.getStartDate.getTimeInMillis && DateProcess.getTimeInMillis <= x.getEndDate.getTimeInMillis  }
    
    if (DataResult.isEmpty) {
      LocalErrorCode = 3003 
      RaiseError_RAW(s"huemul_DataLake Error: No definition found in ${this.LogicalName} (${this.SettingByDate.length.toString})",LocalErrorCode)
    } else if (DataResult.length > 1) {
      LocalErrorCode = 3004
      RaiseError_RAW(s"huemul_DataLake Error: Multiple definitions found in ${this.LogicalName} (${this.SettingByDate.length.toString})", LocalErrorCode )
    } else if (DataResult.length == 1) {
       this.SettingInUse = DataResult(0)
    }         
    
    this.SettingInUse.SetParamsInUse(year, month, day, hour, min, sec, AdditionalParams)
    
    try {
        this.StartRead_dt = huemulBigDataGov.getCurrentDateTimeJava
        /************************************************************************/
        /********   GET  FIELDS   ********************************/
      /** **********************************************************************/
      //Fields
      val fieldsDetail: StructType = CreateSchema(this.SettingInUse.DataSchemaConf, _allColumnsAsString)
           
        this.DataFramehuemul.SetDataSchema(fieldsDetail)
        if (this.huemulBigDataGov.DebugMode) {
          huemulBigDataGov.logMessageDebug("printing DataSchema from settings: ")
          this.DataFramehuemul.getDataSchema.printTreeString()
        }
        
        huemulBigDataGov.logMessageInfo("N° Columns in RowFile: " + this.DataFramehuemul.getNumCols.toString)
      
        /************************************************************************/
        /********   OPEN FILE   *********************************/
        /************************************************************************/
        //Open File 
        this.FileName = huemulBigDataGov.ReplaceWithParams(this.SettingInUse.GetFullNameWithPath(), year, month, day, hour, min, sec, AdditionalParams)
        //DQ: Validate name special characters 
        if (this.FileName.contains("{{") || this.FileName.contains("}}")) {
          LocalErrorCode = 3005
          this.RaiseError_RAW("huemul_DataLake Error: FileName contains incorrect characters {{ or }}: " + this.FileName, LocalErrorCode)
        }
        huemulBigDataGov.logMessageInfo("Reading File: " + this.FileName)
        
        if (this.SettingInUse.getFileType == huemulType_FileType.TEXT_FILE) {
          this.DataRDD = huemulBigDataGov.spark.sparkContext.textFile(this.FileName)
          
          if (this.DataRDD  == null) {
            LocalErrorCode = 3009
            this.RaiseError_RAW(s"huemul_DataLake Error: File doesn't exist $FileName",LocalErrorCode)
          }     
          
          if (huemulBigDataGov.gethuemul_showDemoLines() ) huemulBigDataGov.logMessageInfo("2 first line example of file: " + this.FileName)
          this.DataRDD.take(2).foreach { x =>
            if (huemulBigDataGov.gethuemul_showDemoLines() )
              huemulBigDataGov.logMessageInfo(x) 
            }
        } else if (this.SettingInUse.getFileType == huemulType_FileType.PDF_FILE) {
          val _PDFFile = huemulBigDataGov.spark.sparkContext.binaryFiles(this.FileName).collect()
                
          val pdfResult = new huemul_DataLakePDF()
          pdfResult.openPDF(_PDFFile, this.SettingInUse.getRowDelimiterForPDF)
          
          this.DataRDD = huemulBigDataGov.spark.sparkContext.parallelize(pdfResult.RDD_Base)
          this.DataRDD_extended = huemulBigDataGov.spark.sparkContext.parallelize(pdfResult.RDDPDF_Data)
          this.DataRDD_Metadata = pdfResult.RDDPDF_Metadata
        } else if (this.SettingInUse.getFileType == huemulType_FileType.AVRO_FILE) {
          huemulBigDataGov.logMessageInfo("Opening AVRO data file and building DF DataRaw")

          this.DataRawDF= huemulBigDataGov.spark.read
            .format(this.getAVRO_format)
            .option(if (this.getAVRO_compression == null) "" else "compression",if (this.getAVRO_compression == null) "" else this.getAVRO_compression)
            //.schema( this.DataFramehuemul.getDataSchema())
            .load(this.FileName)

        } else if (this.SettingInUse.getFileType == huemulType_FileType.DELTA_FILE ||
                   this.SettingInUse.getFileType == huemulType_FileType.ORC_FILE ||
                   this.SettingInUse.getFileType == huemulType_FileType.PARQUET_FILE ) {
          huemulBigDataGov.logMessageInfo(s"Opening ${this.SettingInUse.getFileType} data file and building DF DataRaw")

          val _format = this.SettingInUse.getFileType.toString.replace("_FILE", "")
          
          this.DataRawDF= huemulBigDataGov.spark.read
            .format(_format)
            .load(this.FileName)

        } else {
          LocalErrorCode = 3006
          this.RaiseError_RAW("huemul_DataLake Error: FileType missing (add this.FileType setting in DataLake definition)",LocalErrorCode)
        }
          

        /************************************************************************/
        /********   GET   LOG   FIELDS   ********************************/
        /************************************************************************/ 
        //Log Fields
        if (this.SettingInUse.LogSchemaConf.ColSeparatorType == huemulType_Separator.NONE) {
          this.Log.DataFirstRow = ""
          this.Log.Log_isRead = false
          this.Log.Log_isInfoRows = false
        }
        else if (this.SettingInUse.LogSchemaConf.ColSeparatorType == huemulType_Separator.POSITION ||
                 this.SettingInUse.LogSchemaConf.ColSeparatorType == huemulType_Separator.CHARACTER    
                ) {
          this.Log.DataFirstRow = this.DataRDD.first()
          
          val fieldsLogSchema = CreateSchema(this.SettingInUse.LogSchemaConf, allColumnsAsString = true)
          if (fieldsLogSchema == null || fieldsLogSchema.length == 0) {
            LocalErrorCode = 3007
            this.RaiseError_RAW("huemul_DataLake Error: Don't have header information for Detail, see fieldsSeparatorType field ", LocalErrorCode)
          }
          
          this.Log.LogSchema = fieldsLogSchema         
          
          this.Log.Log_isRead = true
          this.Log.Log_isInfoRows = true
       
          val LogRDD =  huemulBigDataGov.spark.sparkContext.parallelize(List(this.Log.DataFirstRow))
          val rowRDD =  LogRDD.map { x =>  ConvertSchemaLocal(this.SettingInUse.LogSchemaConf, x, local_allColumnsAsString = true,null)}

          //Create DataFrame
          if (huemulBigDataGov.DebugMode) {
            huemulBigDataGov.logMessageDebug("Demo DF Log: " + this.FileName)
            huemulBigDataGov.logMessageDebug(rowRDD.take(2).foreach { x => huemulBigDataGov.logMessageDebug(x) })
          }
          this.Log.LogDF = huemulBigDataGov.spark.createDataFrame(rowRDD, this.Log.LogSchema)
          
          if (huemulBigDataGov.DebugMode) this.Log.LogDF.show()
          if (this.SettingInUse.getLogNumRowsColumnName != null) {
            this.Log.DataNumRows = this.Log.LogDF.first().getAs[String](this.SettingInUse.getLogNumRowsColumnName).toLong
            huemulBigDataGov.logMessageDebug("N° Rows according Log: " + this.Log.DataNumRows.toString)
          }
          
        }
        
                            
    } catch {
      case e: Exception =>
        huemulBigDataGov.logMessageError("Error Code")
        huemulBigDataGov.logMessageError(LocalErrorCode)
        huemulBigDataGov.logMessageError(e.getMessage)
        
        if (LocalErrorCode == null)
          LocalErrorCode = 3001
        this.Error_isError = true
        this.Error.GetError(e, this.getClass.getName, this, LocalErrorCode)
    }
    
     !this.Error_isError
  }
  
  def CreateSchema(SchemaConf: huemul_DataLakeSchemaConf, allColumnsAsString: Boolean = false): StructType = {
    //Fields
    var fieldsDetail : ArrayBuffer[StructField] = null
    fieldsDetail = SchemaConf.ColumnsDef.map ( fieldName => StructField(fieldName.getcolumnName_Business, if (allColumnsAsString) StringType else fieldName.getDataType, nullable = true) )
    
    if (fieldsDetail == null) {
      this.RaiseError_RAW("huemul_DataLake Error: Don't have header information for Detail, see fieldsSeparatorType field ", 3008)
    }
    
    //from 2.4 --> add custom columns at the end
    val localCustomColumn = SchemaConf.getCustomColumn
    if ( localCustomColumn != null) {
      fieldsDetail.append(StructField(localCustomColumn.getcolumnName_Business, if (allColumnsAsString) StringType else localCustomColumn.getDataType, nullable = true))
    }
    
    StructType(fieldsDetail)
  }
   

}
