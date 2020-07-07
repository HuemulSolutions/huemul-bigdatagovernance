package com.huemulsolutions.bigdata.datalake

import org.apache.spark.rdd._
import java.util.Calendar
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import scala.collection.mutable._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.tables.huemulType_Tables._
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
  private var _avro_format: String = huemulBigDataGov.GlobalSettings.getAVRO_format()
  def getAVRO_format: String =   _avro_format
  def setAVRO_format(value: String) {_avro_format = value} 
  
  private var _avro_compression: String = huemulBigDataGov.GlobalSettings.getAVRO_compression()
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
    this.StopRead_dt = huemulBigDataGov.getCurrentDateTimeJava()
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
    this.StopRead_dt = huemulBigDataGov.getCurrentDateTimeJava()
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
        this.StartRead_dt = huemulBigDataGov.getCurrentDateTimeJava()
        /************************************************************************/
        /********   GET  FIELDS   ********************************/
      /** **********************************************************************/
      //Fields
      val fieldsDetail: StructType = CreateSchema(this.SettingInUse.DataSchemaConf, _allColumnsAsString)
           
        this.DataFramehuemul.SetDataSchema(fieldsDetail)
        if (this.huemulBigDataGov.DebugMode) {
          huemulBigDataGov.logMessageDebug("printing DataSchema from settings: ")
          this.DataFramehuemul.getDataSchema().printTreeString()
        }
        
        huemulBigDataGov.logMessageInfo("N° Columns in RowFile: " + this.DataFramehuemul.getNumCols().toString)
      
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
   
  /** Genera el codigo inicial para una tabla y el proceso que masteriza dicha tabla
   *
   *  @param PackageBase es el package base (ejemplo: your.application)
   *  @param PackageProject es la ruta del package del proyecto (ejemplo "sbif")
   *  @param NewObjectName es el nombre de objeto que tendra tu masterizacion "process_[modulo]_[entidad]" (ejemplo process_comun_institucion )
   *  @param NewTableName es el nombre de la tabla "tbl_[modulo]_[entidad]" (ejemplo tbl_comun_institucion)
   *  @param TableType es el tipo de tabla (master y reference para tablas maestras, Transaction para tablas particionadas por periodo con informacion transaccional)
   *  @param Frecuency indica si la tabla transaccional tiene particion mensual, diaria. Además indica la periocididad de actualización del proceso
   *  @param AutoMapping true para indicar que los nombres de columnas en raw son iguales a la tabla, indicar false si los nombres de columnas en raw son distintos a tabla
   */
  def GenerateInitialCode(PackageBase: String, PackageProject: String, NewObjectName: String, NewTableName: String, TableType: huemulType_Tables , Frecuency: huemulType_Frequency, AutoMapping: Boolean = true) {
      val Symbol: String = "$"
      val Comas: String = "\"\"\""
      val Coma: String = "\""
      val LocalPath: String = PackageProject.replace(".", "/").concat("/")
      //reemplaza caracteres no deseados a nombre de la clase.
      val param_ObjectName = NewObjectName.replace("$", "")
      val param_PackageBase = PackageBase.replace("$", "")


      //val Param_PackageModule: String = getClass.getPackage.getName.replace("$", "").replace(Param_PackageBase.concat("."), "")
      var LocalFields: String = ""
      var LocalMapping: String = ""
      var LocalColumns: String = ""

      this.SettingByDate(0).DataSchemaConf.ColumnsDef.foreach { x =>
        LocalFields += s"                                     ,${x.getcolumnName_Business}\n"
        LocalMapping += s"      huemulTable.${x.getcolumnName_Business}.SetMapping(${Coma}${x.getcolumnName_Business}${Coma})\n"

        LocalColumns += s"  val ${x.getcolumnName_Business} = new huemul_Columns (${x.getDataType}, true, ${Coma}${x.getDescription}${Coma})"

        if (TableType == huemulType_Tables.Master || TableType == huemulType_Tables.Reference) {
          LocalColumns += s".setMDM_EnableOldValue().setMDM_EnableDTLog().setMDM_EnableProcessLog()"
        }

        LocalColumns += s"  \n"

        if (huemulBigDataGov.IsNumericType(x.getDataType)) {
          LocalColumns += s"""  //${x.getcolumnName_Business}.setDQ_MinDecimalValue(Decimal.apply(0),"DQ_USER_ERROR_")  \n"""
          LocalColumns += s"""  //${x.getcolumnName_Business}.setDQ_MaxDecimalValue(Decimal.apply(200.34),"DQ_USER_ERROR_")  \n"""
        } else if (huemulBigDataGov.IsDateType(x.getDataType)) {
          LocalColumns += s"""  //${x.getcolumnName_Business}.setDQ_MinDateTimeValue("2018-01-01","DQ_USER_ERROR_")  \n"""
          LocalColumns += s"""  //${x.getcolumnName_Business}.setDQ_MaxDateTimeValue("2018-12-31","DQ_USER_ERROR_")  \n"""
        } else if (x.getDataType == StringType) {
          LocalColumns += s"""  //${x.getcolumnName_Business}.setDQ_MinLen(5,"DQ_USER_ERROR_") \n"""
          LocalColumns += s"""  //${x.getcolumnName_Business}.setDQ_MaxLen(100,"DQ_USER_ERROR_")  \n"""
        }

        LocalColumns += s"\n"

      }

      val PeriodName: String = if (Frecuency == huemulType_Frequency.MONTHLY) "month"
                                     else if (Frecuency == huemulType_Frequency.ANNUAL) "year"
                                     else if (Frecuency == huemulType_Frequency.DAILY) "day"
                                     else if (Frecuency == huemulType_Frequency.WEEKLY) "week"
                                     else "other"

      var Code: String = s"""

  /*******************************************************************************************************************/
  /***********************    T A B L E   C L A S S     **************************************************************/
  /*******************************************************************************************************************/

  /*
  instrucciones (parte a):
     1. Crear una clase en el packete que contiene las tablas ($param_PackageBase.tables.master) con el nombre "$NewTableName"
     2. copiar el codigo desde estas instrucciones hasta ***    M A S T E R   P R O C E S S     *** y pegarlo en "$NewTableName"
     3. Revisar detalladamente la configuracion de la tabla
        3.1 busque el texto "[[LLENAR ESTE CAMPO]]" y reemplace la descripcion segun corresponda
     4. seguir las instrucciones "parte b"
  */

  import com.huemulsolutions.bigdata.common._
  import com.huemulsolutions.bigdata.control._
  import com.huemulsolutions.bigdata.tables._
  import com.huemulsolutions.bigdata.dataquality._
  import org.apache.spark.sql.types._


  class $NewTableName(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {
    /**********   C O N F I G U R A C I O N   D E   L A   T A B L A   ****************************************/
    //Tipo de tabla, Master y Reference son catalogos sin particiones de periodo
    this.setTableType(huemulType_Tables.$TableType)
    //Base de Datos en HIVE donde sera creada la tabla
    this.setDataBase(huemulBigDataGov.GlobalSettings.MASTER_DataBase)
    //Tipo de archivo que sera almacenado en HDFS
    this.setStorageType(huemulType_StorageType.PARQUET)
    //Ruta en HDFS donde se guardara el archivo PARQUET
    this.setGlobalPaths(huemulBigDataGov.GlobalSettings.MASTER_SmallFiles_Path)
    //Ruta en HDFS especifica para esta tabla (Globalpaths / localPath)
    this.setLocalPath("$LocalPath")
    //Frecuencia de actualización
    this.setFrequency(huemulType_Frequency.$Frecuency)
    //permite asignar un código de error personalizado al fallar la PK
    this.setPK_externalCode("COD_ERROR")

    /**********   O P T I M I Z A C I O N  ****************************************/
    //nuevo desde version 2.0
    //Indica la cantidad de particiones al guardar un archivo, para archivos pequeños (menor al bloque de HDFS) se
    //recomienda el valor 1, mientras mayor la tabla la cantidad de particiones debe ser mayor para aprovechar el paralelismo
    //this.setNumPartitions(1)

    /**********   C O N T R O L   D E   C A M B I O S   Y   B A C K U P   ****************************************/
    //Permite guardar los errores y warnings en la aplicación de reglas de DQ, valor por default es true
    //this.setSaveDQResult(true)
    //Permite guardar backup de tablas maestras
    //this.setSaveBackup(true)  //default value = false


    ${
    if (TableType == huemulType_Tables.Transaction) {
    s"""  //columna de particion
    this.setPartitionField("period_$PeriodName")"""
    } else ""}
    /**********   S E T E O   I N F O R M A T I V O   ****************************************/
    //Nombre del contacto de TI
    this.setDescription("[[LLENAR ESTE CAMPO]]")
    //Nombre del contacto de negocio
    this.setBusiness_ResponsibleName("[[LLENAR ESTE CAMPO]]")
    //Nombre del contacto de TI
    this.setIT_ResponsibleName("[[LLENAR ESTE CAMPO]]")

    /**********   D A T A   Q U A L I T Y   ****************************************/
    //DataQuality: maximo numero de filas o porcentaje permitido, dejar comentado o null en caso de no aplicar
    //this.setDQ_MaxNewRecords_Num(null)  //ej: 1000 para permitir maximo 1.000 registros nuevos cada vez que se intenta insertar
    //this.setDQ_MaxNewRecords_Perc(null) //ej: 0.2 para limitar al 20% de filas nuevas

    /**********   S E G U R I D A D   ****************************************/
    //Solo estos package y clases pueden ejecutar en modo full, si no se especifica todos pueden invocar
    //this.WhoCanRun_executeFull_addAccess("$param_ObjectName", "$PackageBase.$PackageProject")
    //Solo estos package y clases pueden ejecutar en modo solo Insert, si no se especifica todos pueden invocar
    //this.WhoCanRun_executeOnlyInsert_addAccess("[[MyclassName]]", "[[my.package.path]]")
    //Solo estos package y clases pueden ejecutar en modo solo Update, si no se especifica todos pueden invocar
    //this.WhoCanRun_executeOnlyUpdate_addAccess("[[MyclassName]]", "[[my.package.path]]")


    /**********   C O L U M N A S   ****************************************/

    ${
    if (TableType == huemulType_Tables.Transaction) {
    s"""  //Columna de period
    val period_$PeriodName = new huemul_Columns (StringType, true,"periodo de los datos").setIsPK()
    """
    } else ""}

  $LocalColumns

    //**********Atributos adicionales de DataQuality
    /*
              .setIsPK()         //por default los campos no son PK
              .setIsUnique("COD_ERROR") //por default los campos pueden repetir sus valores
              .setNullable() //por default los campos no permiten nulos
              .setDQ_MinDecimalValue(Decimal.apply(0),"COD_ERROR")
              .setDQ_MaxDecimalValue(Decimal.apply(200.0),"COD_ERROR")
              .setDQ_MinDateTimeValue("2018-01-01","COD_ERROR")
              .setDQ_MaxDateTimeValue("2018-12-31","COD_ERROR")
              .setDQ_MinLen(5,"COD_ERROR")
              .setDQ_MaxLen(100,"COD_ERROR")
              .setDQ_RegExpresion("","COD_ERROR")                          //desde versión 2.0
    */
    //**********Atributos adicionales para control de cambios en los datos maestros
    /*
              .setMDM_EnableDTLog()
              .setMDM_EnableOldValue()
              .setMDM_EnableProcessLog()
              .setMDM_EnableOldValue_FullTrace()     //desde 2.0: guarda cada cambio de la tabla maestra en tabla de trace
    */
    //**********Otros atributos de clasificación
    /*
              .encryptedType("tipo")
              .setARCO_Data()
              .securityLevel(huemulType_SecurityLevel.Public)
              .setBusinessGlossary("CODIGO")           //desde 2.0: enlaza id de glosario de términos con campos de la tabla
    */
    //**********Otros atributos
    /*
              .setDefaultValues("'string'") // "10" // "'2018-01-01'"
              .encryptedType("tipo")
    */

    //**********Ejemplo para aplicar DataQuality de Integridad Referencial
    //val i[[tbl_PK]] = new [[tbl_PK]](huemulBigDataGov,Control)
    //val fk_[[tbl_PK]] = new huemul_Table_Relationship(i[[tbl_PK]], false)
    //fk_[[tbl_PK]].AddRelationship(i[[tbl_PK]].[[PK_Id]], [[LocalField]_Id)

    //**********Ejemplo para agregar reglas de DataQuality Avanzadas  -->ColumnXX puede ser null si la validacion es a nivel de tabla
    //**************Parametros
    //********************  ColumnXXColumna a la cual se aplica la validacion, si es a nivel de tabla poner null
    //********************  Descripcion de la validacion, ejemplo: "Consistencia: Campo1 debe ser mayor que campo 2"
    //********************  Formula SQL En Positivo, ejemplo1: campo1 > campo2  ;ejemplo2: sum(campo1) > sum(campo2)
    //********************  CodigoError: Puedes especificar un codigo para la captura posterior de errores, es un numero entre 1 y 999
    //********************  QueryLevel es opcional, por default es "row" y se aplica al ejemplo1 de la formula, para el ejmplo2 se debe indicar "Aggregate"
    //********************  Notification es opcional, por default es "error", y ante la aparicion del error el programa falla, si lo cambias a "warning" y la validacion falla, el programa sigue y solo sera notificado
    //********************  SaveErrorDetails es opcional, por default es "true", permite almacenar el detalle del error o warning en una tabla específica, debe estar habilitada la opción DQ_SaveErrorDetails en GlobalSettings
    //********************  DQ_ExternalCode es opcional, por default es "null", permite asociar un Id externo de DQ
    //val DQ_NombreRegla: huemul_DataQuality = new huemul_DataQuality(ColumnXX,"Descripcion de la validacion", "Campo_1 > Campo_2",1)
    //**************Adicionalmente, puedes agregar "tolerancia" a la validacion, es decir, puedes especiicar
    //************** numFilas = 10 para permitir 10 errores (al 11 se cae)
    //************** porcentaje = 0.2 para permitir una tolerancia del 20% de errores
    //************** ambos parametros son independientes (condicion o), cualquiera de las dos tolerancias que no se cumpla se gatilla el error o warning
    //DQ_NombreRegla.setTolerance(numfilas, porcentaje)
    //DQ_NombreRegla.setDQ_ExternalCode("Cod_001")

    this.ApplyTableDefinition()
  }



  /*******************************************************************************************************************/
  /***********************    M A S T E R   P R O C E S S     ********************************************************/
  /*******************************************************************************************************************/


  /*
  instrucciones (parte b):
     1. Crear un objeto en el packete de su aplicacion (ejemplo ${param_PackageBase}.${LocalPath.replace("/", ".")}) con un nombre segun su nomenclatura (ejemplo ${param_ObjectName})
     2. Copia el codigo desde MASTER PROCESS hasta el final y pégalo en el objeto creado en el paso 1
     3. agregar import del package que contiene GlobalSettings
     4. cambiar el import you.package.tables._ por el nombre del paquete que contiene la definicion de la tabla.
  */

  package $param_PackageBase.$PackageProject

  import com.huemulsolutions.bigdata.common._
  import com.huemulsolutions.bigdata.control._
  import java.util.Calendar;
  import org.apache.spark.sql.types._
  import $param_PackageBase.tables.master._
  import $param_PackageBase.$PackageProject.datalake._

  //import com.huemulsolutions.bigdata.tables._
  //import com.huemulsolutions.bigdata.dataquality._


  object $param_ObjectName {

    /**
     * Este codigo se ejecuta cuando se llama el JAR desde spark2-submit. el codigo esta preparado para hacer reprocesamiento masivo.
    */
    def main(args : Array[String]) {
      //Creacion API
      val huemulBigDataGov  = new huemul_BigDataGovernance(s"Masterizacion tabla $NewTableName - $Symbol{this.getClass.getSimpleName}", args, globalSettings.Global)

      /*************** PARAMETROS **********************/
      var param_year = huemulBigDataGov.arguments.GetValue("year", null, "Debe especificar el parametro año, ej: year=2017").toInt
      var param_month = huemulBigDataGov.arguments.GetValue("month", null, "Debe especificar el parametro month, ej: month=12").toInt
      ${if (Frecuency == huemulType_Frequency.MONTHLY) s"""
      var param_day = 1
      val param_numMonths = huemulBigDataGov.arguments.GetValue("num_months", "1").toInt

      /*************** CICLO REPROCESO MASIVO **********************/
      var i: Int = 1
      var FinOK: Boolean = true
      var Fecha = huemulBigDataGov.setDateTime(param_year, param_month, param_day, 0, 0, 0)

      while (i <= param_numMonths) {
        param_year = huemulBigDataGov.getYear(Fecha)
        param_month = huemulBigDataGov.getMonth(Fecha)
        huemulBigDataGov.logMessageInfo(s"Procesando Año ${Symbol}param_year, month ${Symbol}param_month (${Symbol}i de ${Symbol}param_numMonths)")

        //Ejecuta codigo
        var finControl = process_master(huemulBigDataGov, null, param_year, param_month)

        if (finControl.Control_Error.IsOK())
          i+=1
        else {
          huemulBigDataGov.logMessageError(s"ERROR Procesando Año ${Symbol}param_year, month ${Symbol}param_month (${Symbol}i de ${Symbol}param_numMonths)")
          i = param_numMonths + 1
        }

        Fecha.add(Calendar.MONTH, 1)
      }
      """
      else s"""
      var param_day = huemulBigDataGov.arguments.GetValue("day", null, "Debe especificar el parametro day, ej: day=31").toInt
      val param_numdays = huemulBigDataGov.arguments.GetValue("num_days", "1").toInt

      process_master(huemulBigDataGov, null, param_year, param_month, param_day)
      """
      }

      huemulBigDataGov.close
    }

    /**
      masterizacion de archivo [[CAMBIAR]] <br>
      param_year: año de los datos  <br>
      param_month: mes de los datos  <br>
     */
    def process_master(huemulBigDataGov: huemul_BigDataGovernance, ControlParent: huemul_Control, param_year: Integer, param_month: Integer${if (Frecuency == huemulType_Frequency.DAILY) ",param_day: Integer" else "" }): huemul_Control = {
      val Control = new huemul_Control(huemulBigDataGov, ControlParent, huemulType_Frequency.${Frecuency})

      try {
        /*************** AGREGAR PARAMETROS A CONTROL **********************/
        Control.AddParamYear("param_year", param_year)
        Control.AddParamMonth("param_month", param_month)
        ${if (Frecuency == huemulType_Frequency.DAILY) s"""Control.AddParamDay("param_day", param_day)""" else ""}
        //Control.AddParamInformation("param_oters", param_otherparams)

        /*************** ABRE RAW DESDE DATALAKE **********************/
        Control.NewStep("Abre DataLake")
        var DF_RAW =  new ${this.getClass.getSimpleName.replace("_test", "")}(huemulBigDataGov, Control)
        if (!DF_RAW.open("DF_RAW", Control, param_year, param_month, ${if (Frecuency == huemulType_Frequency.DAILY) "param_day" else "1" }, 0, 0, 0))
          Control.RaiseError(s"error encontrado, abortar: ${Symbol}{DF_RAW.Error.ControlError_Message}")


        /*********************************************************/
        /*************** LOGICAS DE NEGOCIO **********************/
        /*********************************************************/
        //instancia de clase ${NewTableName}
        val huemulTable = new ${NewTableName}(huemulBigDataGov, Control)

        Control.NewStep("Generar Logica de Negocio")
        huemulTable.DF_from_SQL("FinalRAW"
                            , s${Comas}SELECT TO_DATE("${Symbol}{param_year}-${Symbol}{param_month}-${if (Frecuency == huemulType_Frequency.DAILY) s"${Symbol}{param_day}" else "1"}") as period_${PeriodName}
  ${LocalFields}
                                 FROM DF_RAW${Comas})

        DF_RAW.DataFramehuemul.DataFrame.unpersist()

        //comentar este codigo cuando ya no sea necesario generar estadisticas de las columnas.
        Control.NewStep("QUITAR!!! Generar Estadisticas de las columnas SOLO PARA PRIMERA EJECUCION")
        huemulTable.DataFramehuemul.DQ_StatsAllCols(Control, huemulTable)

        Control.NewStep("Asocia columnas de la tabla con nombres de campos de SQL")
        ${if (AutoMapping) s"""huemulTable.setMappingAuto()"""
        else { s"""
        huemulTable.period_$PeriodName.SetMapping("period_${PeriodName}")
  $LocalMapping
        """
        }}

        // huemulTable.setApplyDistinct(false) //deshabilitar si DF tiene datos únicos, (está habilitado por default)

        Control.NewStep("Ejecuta Proceso")
        if (!huemulTable.executeFull("FinalSaved"))
          Control.RaiseError(s"User: Error al intentar masterizar instituciones (${Symbol}{huemulTable.Error_Code}): ${Symbol}{huemulTable.Error_Text}")

        Control.FinishProcessOK
      } catch {
        case e: Exception => {
          Control.Control_Error.GetError(e, this.getClass.getName, null)
          Control.FinishProcessError()
        }
      }

      Control
    }

  }

 """
 

   println(Code)
   //huemulBigDataGov.logMessageInfo(Code)
    
    
  }
}
