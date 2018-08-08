package com.huemulsolutions.bigdata.datalake

import org.apache.spark.rdd._
import java.util.Calendar
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import huemulType_FileType._

class huemul_DataLake(huemulLib: huemul_Library) extends Serializable {
  /***
   * Id of data (example: PlanCuentas)
   */
  var LogicalName : String= ""
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
  var SettingByDate: Array[huemul_DataLakeSetting] = null    
  
  /***
   * Information about interfaces
   */
  var SettingInUse: huemul_DataLakeSetting = null
  
  /***
   * get execution's info about rows and DF
   */
  var DataFramehuemul: huemul_DataFrame = new huemul_DataFrame(huemulLib)
  var Log: huemul_DataLakeLogInfo = new huemul_DataLakeLogInfo()
  
   
  var FileName: String = null //Name who was read
  var StartRead_dt: Calendar = null
  var StopRead_dt: Calendar = null

  var Error: huemul_ControlError = new huemul_ControlError(huemulLib)
  var Error_isError: Boolean = false
  //var Error_Text: String = ""  
  private var rawFiles_id: String = ""
  def setrawFiles_id(id: String) {rawFiles_id = id}
  def getrawFiles_id(): String = {return rawFiles_id}
  //RAW_OpenFile(RAW_File_Info, ano, mes, dia, hora, min, seg, AdditionalParams)
  
  var DataRDD: RDD[String] = null
  
  
  def RaiseError_RAW(txt: String) {
    Error.ControlError_Message = txt
    //Error_Text = txt
    Error_isError = true
    sys.error(txt)
  } 
  
  /**
   * RAW_to_DF: Create DF from RDD, save at Data.DataDF
   */
  def DF_from_RAW(rowRDD: RDD[Row], Alias: String, Control: huemul_Control) {
    DataFramehuemul.DF_from_RAW(rowRDD, Alias)
    //Register use in control
    if (Control != null) {
      Control.RegisterRAW_USE(this)
    }
  }
  
  
  /***
   * DAPI_ConvertSchema: Transforma un string en un objeto ROW
   * SchemaConf: Schema configuration in definition
   * Schema: get from Data or Log (ex: RAW_Exec.Data.DataSchema)
   * Return: objeto Row
   */
  
  private def ConvertSchemaLocal(SchemaConf: huemul_DataLakeSchemaConf, Schema: StructType, row : String, ApplyTrim: Boolean = true) : Row = {
    var DataArray_Dest : Array[Any] = null
    if (SchemaConf.ColSeparatorType == huemulType_Separator.CHARACTER) {
      //Get separator and numCols from params
      val separator: String = SchemaConf.ColSeparator
      val numCols: Integer = Schema.length
      
      if (numCols == 0 || numCols == null){
        this.RaiseError_RAW("Schema not defined")
      }
      //declare variables for transform
      DataArray_Dest = new Array[Any](numCols)      
      val DataArray_Orig = row.split(separator,numCols)
      
      if (ApplyTrim)
        DataArray_Orig.indices.foreach { i => DataArray_Dest(i) = DataArray_Orig(i).trim()  }
      else
        DataArray_Orig.indices.foreach { i => DataArray_Dest(i) = DataArray_Orig(i)  }
    }
    else if (SchemaConf.ColSeparatorType == huemulType_Separator.POSITION) {      
      DataArray_Dest = new Array[Any](SchemaConf.ColumnsPosition.length)
      
      if (ApplyTrim)
        SchemaConf.ColumnsPosition.indices.foreach { i => DataArray_Dest(i) = row.substring(SchemaConf.ColumnsPosition(i)(1).toInt, SchemaConf.ColumnsPosition(i)(2).toInt).trim() }
      else 
        SchemaConf.ColumnsPosition.indices.foreach { i => DataArray_Dest(i) = row.substring(SchemaConf.ColumnsPosition(i)(1).toInt, SchemaConf.ColumnsPosition(i)(2).toInt) }
    }
     
    Row.fromSeq(DataArray_Dest) 
  }
  
  /***
   * ConvertSchema: Transforma un string en un objeto ROW
   * Return: objeto Row
   */
  
  def ConvertSchema(row : String, ApplyTrim: Boolean = true) : Row = {
    return ConvertSchemaLocal(this.SettingInUse.DataSchemaConf, this.DataFramehuemul.getDataSchema(), row, ApplyTrim)
    //SchemaConf: huemul_DataLakeSchemaConf, Schema: StructType,
  }
  
  /***
   * Open the file with huemulLib.spark.sparkContext.textFile <br>
   * and set de DataRDD attribute
   */
  def OpenFile(ano: Integer, mes: Integer, dia: Integer, hora: Integer, min: Integer, seg: Integer, AdditionalParams: String = null): Boolean = {    
    //Ask for definition in date
    val DateProcess = huemulLib.setDateTime(ano, mes, dia, hora, min, seg)
    
    if (huemulLib.DebugMode) println("N° array config: " + this.SettingByDate.length.toString())
    for (Data <- this.SettingByDate) {
      if (DateProcess.getTimeInMillis >= Data.StartDate.getTimeInMillis && DateProcess.getTimeInMillis <= Data.EndDate.getTimeInMillis) {
        this.SettingInUse = Data
      }
    }
    
    if (this.SettingInUse == null) {
      RaiseError_RAW("Logical Error: No definition found in " + this.LogicalName + " (" + this.SettingByDate.length.toString() + ")" )
    }
    
    try {
        this.StartRead_dt = Calendar.getInstance()
        /************************************************************************/
        /********   OPEN FILE   *********************************/
        /************************************************************************/
        //Open File 
        this.FileName = huemulLib.ReplaceWithParams(this.SettingInUse.GetFullNameWithPath(), ano, mes, dia, hora, min, seg, AdditionalParams)
        //DQ: Validate name special characters 
        if (this.FileName.contains("{{") || this.FileName.contains("}}")) {
          sys.error("FileName contains incorrect characters {{ or }}: " + this.FileName)
        }
        println("Reading File: " + this.FileName)
        //val a = huemulLib.spark.sparkContext.textFile(this.FileName)
        
        if (this.SettingInUse.FileType == huemulType_FileType.TEXT_FILE) {
          this.DataRDD = huemulLib.spark.sparkContext.textFile(this.FileName)
        } else {
          sys.error("FileType missing (add this.FileType setting in DataLake definition)")
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
        else if (this.SettingInUse.LogSchemaConf.ColSeparatorType == huemulType_Separator.POSITION) {
          this.Log.DataFirstRow = this.DataRDD.first()
          
          //Create schema
          var fieldsLog : Array[StructField] = null
          fieldsLog = this.SettingInUse.LogSchemaConf.ColumnsPosition       
              .map(fieldName => StructField(fieldName(0), StringType, nullable = true))  
              
          if (fieldsLog == null)
            sys.error("Don't have header information for Detail, see fieldsSeparatorType field ")
                  
          this.Log.LogSchema = StructType(fieldsLog)          
          
          this.Log.Log_isRead = true
          this.Log.Log_isInfoRows = true
        }
        else if (this.SettingInUse.LogSchemaConf.ColSeparatorType == huemulType_Separator.CHARACTER) {
          this.Log.DataFirstRow = this.DataRDD.first()
          //detail fields
          val fieldsLog = this.SettingInUse.LogSchemaConf.HeaderColumnsString.split(";")       
              .map(fieldName => StructField(fieldName, StringType, nullable = true))
          this.Log.LogSchema = StructType(fieldsLog)
        }
        
        if (this.Log.LogSchema != null) {
          val LogRDD =  huemulLib.spark.sparkContext.parallelize(List(this.Log.DataFirstRow))
          val rowRDD =  LogRDD.map { x =>  ConvertSchemaLocal(this.SettingInUse.LogSchemaConf, this.Log.LogSchema, x)} 

          //Create DataFrame
          println("Demo DF Log: " + this.FileName)
          println(rowRDD.take(2).foreach { x => println(x) })
          
          this.Log.LogDF = huemulLib.spark.createDataFrame(rowRDD, this.Log.LogSchema)
          
          this.Log.LogDF.show()
          if (this.SettingInUse.LogNumRows_FieldName != null) {
            this.Log.DataNumRows = this.Log.LogDF.first().getAs[String](this.SettingInUse.LogNumRows_FieldName).toLong 
            println("N° Rows according Log: " + this.Log.DataNumRows.toString())
          }
          
        }
        
        
        /************************************************************************/
        /********   GET  FIELDS   ********************************/
        /************************************************************************/ 
        //Fields
        var fieldsDetail : Array[StructField] = null
        if (this.SettingInUse.DataSchemaConf.ColSeparatorType == huemulType_Separator.POSITION) {
          fieldsDetail = this.SettingInUse.DataSchemaConf.ColumnsPosition       
              .map(fieldName => StructField(fieldName(0), StringType, nullable = true))  
        }
        else if (this.SettingInUse.DataSchemaConf.ColSeparatorType == huemulType_Separator.CHARACTER) {
          //detail fields
          fieldsDetail = this.SettingInUse.DataSchemaConf.HeaderColumnsString.split(";")       
              .map(fieldName => StructField(fieldName, StringType, nullable = true))                             
        }
        
        if (fieldsDetail == null)
          sys.error("Don't have header information for Detail, see fieldsSeparatorType field ")
          
        
        this.DataFramehuemul.SetDataSchema(StructType(fieldsDetail))
        if (this.huemulLib.DebugMode) {
          println("printing DataSchema from settings: ")
          this.DataFramehuemul.getDataSchema().printTreeString()
        }
        
        println("N° Columns: " + this.DataFramehuemul.getNumCols.toString())                        
    } catch {
      case e: Exception => {
        this.Error_isError = true
        this.Error.GetError(e, this.getClass.getName)
        
              
      }
    }
    
    return !this.Error_isError
  }
   
  //TODO: Terminar el generador de código
def GenerateInitialCode(Param_ClassName: String, TableName: String, DataBase: String, Param_PackageBase: String) {
    val Symbol: String = "$"
    val Comas: String = "\"\"\""
    val Coma: String = "\""
    val Param_PackageModule: String = getClass.getPackage.getName.replace("$", "").replace(Param_PackageBase.concat("."), "")
    var LocalFields: String = ""
    var LocalMapping: String = ""
    var LocalColumns: String = ""
    //excluye raw_ del texto para obtener el nombre de la clase.
    val param_ClassName = Param_ClassName.substring(4,Param_ClassName.length()).replace("$", "")
      
    this.DataFramehuemul.getDataSchema.foreach { x => 
      LocalFields += s"                                     ,${x.name}\n"
      LocalMapping += s"      huemulTable.${x.name}.SetMapping(${Coma}${x.name}${Coma})\n"
       
      LocalColumns += s"    val ${x.name} = new huemul_Columns (StringType, true, ${Coma}[[DESCRIPCION]]${Coma}) \n"
      LocalColumns += s"    ${x.name}.IsPK = false \n"
      LocalColumns += s"    ${x.name}.Nullable = false \n"
      LocalColumns += s"    ${x.name}.IsUnique = false \n"
      LocalColumns += s"    ${x.name}.ARCO_Data = false  \n"
      LocalColumns += s"    ${x.name}.SecurityLevel = huemulType_SecurityLevel.Public  \n"
      LocalColumns += s"    ${x.name}.DQ_MinLen = 7 \n"
      LocalColumns += s"    ${x.name}.DQ_MaxLen = 7 \n\n"
    }
    
    
    var Code: String = s"""

/*******************************************************************************************************************/
/***********************    T A B L E   C L A S S     **************************************************************/
/*******************************************************************************************************************/

/*
instrucciones (parte a):
   1. Crear una clase en el packete you.package.tables con el nombre "${TableName}"
   2. copiar el código desde estas instrucciones hasta ***    M A S T E R   P R O C E S S     ***
   3. Revisar detalladamente la configuración de la tabla
      2.1 this.DataBase 
      2.2 busque el texto "[[LLENAR ESTE CAMPO]]" y reemplace la descripción según corresponda 
      2.3 revise si el seteo por default del campo TableType es correcto
   4. seguir las instrucciones "parte b"
*/

import bigdata.warehouse.huemul._
import bigdata.warehouse.control._
import bigdata.warehouse.tables._
import bigdata.warehouse.dataquality._
import org.apache.spark.sql.types._


class ${TableName}(huemulLib: huemul_Library, Control: huemul_Control) extends huemul_Table(huemulLib, Control) with Serializable {
    /********************************/
    /**** DEFINICION DE TABLA *******/
    /********************************/
    
    this.DataBase = huemulLib.GlobalSettings.MASTER_DataBase
    this.Description = "[[LLENAR ESTE CAMPO]]"
    this.IT_ResponsibleName = "[[LLENAR ESTE CAMPO]]"
    this.Business_ResponsibleName = "[[LLENAR ESTE CAMPO]]"
        
    this.PartitionField = "periodo_mes" //(example value: periodo_mes
    this.TableType  = huemulType_Tables.Transaction
    
    this.StorageType = "parquet" //example value: com.databricks.spark.csv 
    this.GlobalPaths = huemulLib.GlobalSettings.MASTER_BigFiles_Path
    this.LocalPath = "${Param_PackageModule.replace(".", "/")}/"  //example value: sbif/)
    
    //DAtaQuality for insert
    this.DQ_MaxNewRecords_Num = null
    this.DQ_MaxNewRecords_Perc = null
    
    //Security to execute huemul (not defined = all clasess can execute the command)
    this.WhoCanRun_DoFullhuemul.AddAccess("master_${param_ClassName}","${Param_PackageBase.concat(".").concat(Param_PackageModule)}")
    //this.WhoCanRun_DoOnlyInserthuemul.AddAccess([[changeclassname]],[[my.package.path]])
    //this.WhoCanRun_DoOnlyUpdatehuemul.AddAccess([[changeclassname]],[[my.package.path]])

    /********************************/
    /**** DEFINICION DE COLUMNAS ****/
    /********************************/

    val periodo_mes = new huemul_Columns (StringType, true,"periodo de los datos")
    periodo_mes.IsPK = true
    
${LocalColumns}

    //[[FIELDS]].DQ_MinDecimalValue = Decimal.apply(0)
    //[[FIELDS]].DQ_MaxDecimalValue = Decimal.apply(200)

   
    
    //FK EXAMPLE
    //var tbl_[[PK]] = new master_[[PK]]_Def(huemulLib,Control)
    //var fk_[[LocalField]] = new huemul_Table_Relationship(huemulLib,tbl_[[PK]], false)
    //fk_[[LocalField]].AddRelationship(tbl_[[PK]].[[PK_Id]], [[LocalField]_Id)
    
    /********************************/
    /**** DEF DATA QUALITY Y MDM ****/
    /********************************/
    
    //val DQ_ColumnXX: huemul_DataQuality = new huemul_DataQuality(ColumnXX,false,true,"Fecha solicitud debe ser anterior a fecha aprobacion", "to_date(ColumnXX) <= to_date(ColumnYY)")
    //DQ_ColumnXX.Error_Percent = Decimal.apply("0.15") //> 15% of rows with error, process fail
    //DQ_ColumnXX.Error_MaxNumRows = 100 //> 100 rows with error, process fail
    
    this.ApplyTableDefinition()
  }



/*******************************************************************************************************************/
/***********************    M A S T E R   P R O C E S S     ********************************************************/
/*******************************************************************************************************************/


/*
instrucciones (parte b):
   1. Crear un objeto en el packete ${Param_PackageBase.concat(".").concat(Param_PackageModule)} con el nombre master_${Param_ClassName}
   2. Copie el código desde MASTER PROCESS hasta el final
   3. agregar import del package que contiene GlobalSettings
   4. cambiar el import you.package.tables._ por el nombre del paquete que contiene la definición de la tabla.
   5. seguir las instrucciones del código Class que viene a continuación.
*/

package com.huemulsolutions.bigdata.datalake{Param_PackageBase.concat(".").concat(Param_PackageModule) }

import bigdata.warehouse.huemul._
import bigdata.warehouse.control._
import bigdata.warehouse.tables._
import bigdata.warehouse.dataquality._
import huemulType_Tables._
import java.util.Calendar;
import org.apache.spark.sql.types._
import ${Param_PackageBase.concat(".").concat(Param_PackageModule)}.tables._
import ${Param_PackageBase}._

object master_${param_ClassName} {
  
  /**
   * Este código se ejecuta cuando se llama el JAR desde spark2-submit. el código está preparado para hacer reprocesamiento masivo.
  /*
  def main(args : Array[String]) {
    //Creación API
    val huemulLib  = new huemul_Library(s"BigData Fabrics - ${Symbol}{this.getClass.getSimpleName}", args, GlobalSettings.Global)
    
    /*************** PARAMETROS **********************/
    var param_ano = huemulLib.arguments.GetValue("ano", null, "Debe especificar el parámetro año, ej: ano=2017").toInt
    var param_mes = huemulLib.arguments.GetValue("mes", null, "Debe especificar el parámetro mes, ej: mes=12").toInt
    val param_numMeses = huemulLib.arguments.GetValue("num_meses", "1").toInt
    
    
    /*************** CICLO REPROCESO MASIVO **********************/
    var i: Int = 1
    var FinOK: Boolean = true
    var Fecha = huemulLib.setDateTime(param_ano, param_mes, 1, 0, 0, 0)
    
    while (i <= param_numMeses) {
      param_ano = huemulLib.getYear(Fecha)
      param_mes = huemulLib.getMonth(Fecha)
      println(s"Procesando Año ${Symbol}param_ano, Mes ${Symbol}param_mes (${Symbol}i de ${Symbol}param_numMeses)")
      
      //Ejecuta código
      var FinOK = master_${param_ClassName}(huemulLib, null, param_ano, param_mes)
      
      if (FinOK)
        i+=1
      else
        i = param_numMeses + 1
        
      Fecha.add(Calendar.MONTH, 1)      
    }

  }
  
  /**
    masterización de archivo [[CAMBIAR]] <br>
    param_ano: año de los datos  <br>
    param_mes: mes de los datos  <br>
   */
  def master_${param_ClassName}(huemulLib: huemul_Library, ControlParent: huemul_Control, param_ano: Integer, param_mes: Integer): Boolean = {
    val Control = new huemul_Control(huemulLib, ControlParent)    
    
    try {             
      /*************** AGREGAR PARAMETROS A CONTROL **********************/
      Control.AddParamInfo("param_ano", param_ano.toString())
      Control.AddParamInfo("param_mes", param_mes.toString())
      
      
      /*********************************************************/
      /*************** ABRIR RAW Y MASTER **********************/
      /*********************************************************/      
      Control.NewStep("Abre RAW")
      
      //Inicializa clase RAW  
      var DF_RAW =  new raw_${param_ClassName}(huemulLib)
      DF_RAW.open("DF_RAW", Control, param_ano, param_mes, 0, 0, 0, 0)       
      
      if (DF_RAW.Error_isError) Control.RaiseError(s"error encontrado, abortar: ${Symbol}{DF_RAW.Error.ControlError_Message}")
      
      
      /*********************************************************/
      /*************** LOGICAS DE NEGOCIO **********************/
      /*********************************************************/
      //instancia de clase ${param_ClassName} 
      val huemulTable = new ${TableName}(huemulLib, Control)
      
      Control.NewStep("Generar Lógica de Negocio")
      huemulTable.DF_from_SQL("FinalRAW"
                          , s${Comas}SELECT TO_DATE(cast(unix_timestamp(concat(${Symbol}{param_ano},'-',${Symbol}{param_mes},'-',1), 'yyyy-MM-dd') as TimeStamp)) as periodo_mes
${LocalFields}
                               FROM DF_RAW${Comas})
      
      //Quita persistencia de RAW Data
      DF_RAW.DataFramehuemul.DataFrame.unpersist()
      
      //muestra estadistica de todas las columnas
      if(huemulLib.DebugMode) {
        Control.NewStep("Generar Estadísticas de las columnas")
        huemulTable.DataFramehuemul.DQ_StatsAllCols(Control, huemulTable)        
        //comentar este código cuando ya no sea necesario generar estadísticas de las columnas.
      }
      
      /*********************************************************/
      /*************** SETEAR CAMPOS Y GUARDA MDM **************/
      /*********************************************************/

      huemulTable.periodo_mes.SetMapping("periodo_mes")
${LocalMapping}
            
      /*********************************************************/
      /*************** FIN PROCESO *****************************/
      /*********************************************************/
      Control.NewStep("Crear Tabla")      
      huemulTable.doFullhuemul("FinalSaved")
            
      Control.FinishProcessOK
    } catch {
      case e: Exception => {
        Control.Control_Error.GetError(e, this.getClass.getName)
        Control.FinishProcessError()
      }
    }
    
    return Control.Control_Error.IsOK()   
  }
  
}

object master_${param_ClassName}_Migrar {
 
 def main(args : Array[String]) {
   //Creación API
    val huemulLib  = new huemul_Library(s"BigData Fabrics - ${Symbol}{this.getClass.getSimpleName}", args, GlobalSettings.Global)
    
    /*************** PARAMETROS **********************/
    var param_ano = huemulLib.arguments.GetValue("ano", null, "Debe especificar el parámetro año, ej: ano=2017").toInt
    var param_mes = huemulLib.arguments.GetValue("mes", null, "Debe especificar el parámetro mes, ej: mes=12").toInt
    var param_dia = 1
   
    var param = huemulLib.ReplaceWithParams("{{YYYY}}-{{MM}}-{{DD}}", param_ano, param_mes, param_dia, 0, 0, 0)
    
   val clase = new ${TableName}(huemulLib, null)
   clase.CopyToDest(param, "desa")
   
 }
 
}








  """
   
   println(Code)
    
    
  }
}