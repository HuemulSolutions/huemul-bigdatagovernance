package com.huemulsolutions.bigdata.common

import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import scala.collection.mutable._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification._
import com.huemulsolutions.bigdata.dataquality.huemul_DQRecord
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.storage.StorageLevel._

/**
 * Def_Fabric_DataInfo: Define method to improve DQ over DF
 */
class huemul_DataFrame(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends Serializable {
  private val numPartitionsForTempFiles: Integer = 2
  if (Control == null) 
    sys.error("Control is null in huemul_DataFrame")
    
  
  if (huemulBigDataGov == null) 
    sys.error("huemulBigDataGov is null in huemul_DataFrame")
  /**
   * Get and Set DataFrame
   */
  private var DataDF: DataFrame = null
  def DataFrame: DataFrame = {return DataDF}
  
  /**
   * Schema info
   */
  private var DataSchema: StructType = null
  def SetDataSchema(Schema: StructType) {
    DataSchema = Schema
    NumCols = Schema.length
  }
  def getDataSchema(): StructType = {return DataSchema  }
  
  /**
   * No Rows info
   */
  
  //private var NumRows: Long = -1
  private var _NumRows: Long = -1
  def getNumRows(): Long = {
    if (_NumRows == -1)
      _NumRows = DataFrame.count()
      
    return _NumRows
    
  }
  
  /**
   * num cols Info
   */
  private var NumCols: Integer = null
  def getNumCols(): Integer = {return NumCols}
  
  //var Data_isRead: java.lang.Boolean = false
  
  //Alias
  private var AliasDF: String = ""
  def Alias: String = {return AliasDF}
  //val TempDir: String = 
  
  private var DQ_Result: ArrayBuffer[huemul_DQRecord] = new ArrayBuffer[huemul_DQRecord]()
  def getDQResult(): ArrayBuffer[huemul_DQRecord] = {return DQ_Result}
  
  private def local_setDataFrame(DF: DataFrame, Alias: String, SaveInTemp: Boolean, createLinage: Boolean ) {
    //DF.persist(MEMORY_ONLY_SER)
    DataDF = DF
    DataSchema = DF.schema
    DataDF.createOrReplaceTempView(Alias)
    AliasDF = Alias
    
    //Set as Readed
    //NumRows = DataDF.count() //este parámetro se setea autmáticamente al intentar leer getNumRows
    NumCols = DataDF.columns.length
    //Data_isRead = true
    //TODO: ver como poner la fecha de término de lectura StopRead_dt = Calendar.getInstance()
    
    if (createLinage) {
      
    }
        
    if (SaveInTemp)
      huemulBigDataGov.CreateTempTable(DataDF, AliasDF, huemulBigDataGov.DebugMode, null)
  }
  
  def setDataFrame(DF: DataFrame, Alias: String, SaveInTemp: Boolean = true) {
    local_setDataFrame(DF, Alias, SaveInTemp, true /*create linage*/)
  }
  
  
  
  /**   
   Create DF from SQL (equivalent to spark.sql method)
   */
  def DF_from_SQL(Alias: String, sql: String, SaveInTemp: Boolean = true, NumPartitions: Integer = null) {
    //WARNING: ANY CHANGE ON THIS METHOD MUST BE REPLIATES TO _CreateFinalQuery
    //THE ONLY DIFFERENCE IS IN 
    //huemulBigDataGov.DF_SaveLinage(Alias, sql,dt_start, dt_end, Control)
    
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sql)
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
    //Cambio en v1.3: optimiza tiempo al aplicar repartition para archivos pequeños             
    val DFTemp = if (NumPartitions == null || NumPartitions <= 0) huemulBigDataGov.spark.sql(sql)
                 else huemulBigDataGov.spark.sql(sql).repartition(NumPartitions)
      
    
    local_setDataFrame(DFTemp, Alias, SaveInTemp, false)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    
    huemulBigDataGov.DF_SaveLinage(Alias
                                 , sql
                                 , dt_start
                                 , dt_end
                                 , Control
                                 , null //FinalTable
                                 , true //isQuery
                                 , false //isReferenced
                                 )
        
  }
  
  /**   
   Create DF from SQL (equivalent to spark.sql method)
   */
  def _CreateFinalQuery(Alias: String, sql: String, SaveInTemp: Boolean = true, NumPartitions: Integer = null, finalTable: huemul_Table) {
    //WARNING: ANY CHANGE ON DF_from_SQL MUST BE REPLIATE IN THIS METHOD
    
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sql)
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
    //Cambio en v1.3: optimiza tiempo al aplicar repartition para archivos pequeños             
    val DFTemp = if (NumPartitions == null || NumPartitions <= 0) huemulBigDataGov.spark.sql(sql)
                 else huemulBigDataGov.spark.sql(sql).repartition(NumPartitions)
      
    
    local_setDataFrame(DFTemp, Alias, SaveInTemp, false)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    
    huemulBigDataGov.DF_SaveLinage(Alias
                                 , sql
                                 , dt_start
                                 , dt_end
                                 , Control
                                 , finalTable
                                 , false //isQuery
                                 , false //isReferenced
                                 )
    
  }
  
    
  /**
   * RAW_to_DF: Create DF from RDD, save at Data.DataDF
   */
  def DF_from_RAW(rowRDD: RDD[Row], Alias: String) {
    //Create DataFrame 
    if (huemulBigDataGov.DebugMode) { println(rowRDD.take(2).foreach { x => println(x) }) }
    val DF = huemulBigDataGov.spark.createDataFrame(rowRDD, DataSchema)
    
    //Assign DataFrame to LocalDataFrame
    local_setDataFrame(DF, Alias, huemulBigDataGov.DebugMode, false)
    
    //Unpersisnt unused data
    rowRDD.unpersist(false)
    
    //Show sample data and structure
    if (huemulBigDataGov.DebugMode) { 
      DataDF.printSchema()
      DataDF.show()
    }
    
  }
  
  /**
   * DQ_NumRowsInterval: Test DQ for num Rows in DF
   */
  def DQ_NumRowsInterval(ObjectData: Object, NumMin: Long, NumMax: Long, DQ_ExternalCode:String = null): huemul_DataQualityResult = {

    val DQResult = new huemul_DataQualityResult()
    //var Rows = NumRows
    if (getNumRows >= NumMin && getNumRows <= NumMax ) {
      DQResult.isError = false
    } else if (getNumRows < NumMin) {
      DQResult.isError = true
      DQResult.Description = s"huemul_DataFrame Error: Rows($getNumRows) < MinDef($NumMin)"
      DQResult.Error_Code = 2001
    } else if (getNumRows > NumMax) {
      DQResult.isError = true
      DQResult.Description = s"huemul_DataFrame Error: Rows($getNumRows) > MaxDef($NumMax)"
      DQResult.Error_Code = 2002
    }
    
    var TableName: String = null
    var BBDDName: String = null
    if (ObjectData.isInstanceOf[huemul_Table]) {
      val Data = ObjectData.asInstanceOf[huemul_Table]
      TableName = Data.TableName
      BBDDName = Data.GetCurrentDataBase()
    }
    
    val Values = new huemul_DQRecord()
    Values.Table_Name =TableName
    Values.BBDD_Name =BBDDName
    Values.DF_Alias =AliasDF
    Values.ColumnName =null
    Values.DQ_Name ="DQ_NumRowsInterval"
    Values.DQ_Description =s"num rows between ${NumMin} and ${NumMax}"
    Values.DQ_QueryLevel = huemulType_DQQueryLevel.Aggregate 
    Values.DQ_Notification = huemulType_DQNotification.ERROR
    Values.DQ_SQLFormula =""
    Values.DQ_toleranceError_Rows =0
    Values.DQ_toleranceError_Percent =Decimal.apply(0)
    Values.DQ_ResultDQ =DQResult.Description
    Values.DQ_ErrorCode = DQResult.Error_Code
    Values.DQ_ExternalCode = DQ_ExternalCode
    Values.DQ_NumRowsOK =0
    Values.DQ_NumRowsError =0
    Values.DQ_NumRowsTotal =getNumRows
    Values.DQ_IsError = DQResult.isError
    
    this.DQ_Register(Values)    
    
    
    return DQResult
  }
  
  /****
   * DQ_NumRowsInterval: Test DQ for num Rows in DF
   */
  def DQ_NumRows(ObjectData: Object, NumRowsExpected: Long, DQ_ExternalCode:String = null): huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    //var Rows = NumRows
    if (getNumRows == NumRowsExpected ) {
      DQResult.isError = false
    } else if (getNumRows < NumRowsExpected) {
      DQResult.isError = true
      DQResult.Description = s"huemul_DataFrame Error: Rows($getNumRows) < NumExpected($NumRowsExpected)"
      DQResult.Error_Code = 2003
    } else if (getNumRows > NumRowsExpected) {
      DQResult.isError = true
      DQResult.Description = s"huemul_DataFrame Error: Rows($getNumRows) > NumExpected($NumRowsExpected)"
      DQResult.Error_Code = 2004
    }
    
    
    var TableName: String = null
    var BBDDName: String = null
    if (ObjectData.isInstanceOf[huemul_Table]) {
      val Data = ObjectData.asInstanceOf[huemul_Table]
      TableName = Data.TableName
      BBDDName = Data.GetCurrentDataBase()
    }
    
    val Values = new huemul_DQRecord()
    Values.Table_Name =TableName
    Values.BBDD_Name =BBDDName
    Values.DF_Alias =AliasDF
    Values.ColumnName =null
    Values.DQ_Name ="DQ_NumRows"
    Values.DQ_Description =s"num rows = ${NumRowsExpected} "
    Values.DQ_QueryLevel = huemulType_DQQueryLevel.Aggregate 
    Values.DQ_Notification = huemulType_DQNotification.ERROR
    Values.DQ_SQLFormula =""
    Values.DQ_toleranceError_Rows = 0
    Values.DQ_toleranceError_Percent =Decimal.apply(0)
    Values.DQ_ResultDQ =DQResult.Description
    Values.DQ_ErrorCode = DQResult.Error_Code
    Values.DQ_ExternalCode = DQ_ExternalCode
    Values.DQ_NumRowsOK =0
    Values.DQ_NumRowsError =0
    Values.DQ_NumRowsTotal =getNumRows
    Values.DQ_IsError = DQResult.isError
      
    this.DQ_Register(Values)
    
   
    
    return DQResult
  }
  
  /**
   * Compare actual DF with other DF (only columns exist in Actual DF and other DF)
   */
  def DQ_CompareDF(ObjectData: Object, DF_to_Compare: DataFrame, PKFields: String, DQ_ExternalCode:String = null) : huemul_DataQualityResult = {
    if (!huemulBigDataGov.HasName(PKFields)) {
      sys.error("HuemulError: PKFields must be set ")
    }
    DF_to_Compare.createOrReplaceTempView("__Compare")
    val DQResult = new huemul_DataQualityResult()
    DQResult.isError = false
    
    var NumColumns: Long = 0
    var NumColumnsError: Long = 0
    
    try {
      //Genera Select
      var SQL_Compare: String = ""
      var SQL_Resumen: String = "SELECT count(1) as total "
      this.DataSchema.foreach { x =>
        if (DF_to_Compare.schema.filter { y => y.name.toUpperCase() == x.name.toUpperCase() }.length == 1) {
          SQL_Compare = SQL_Compare.concat(s",CAST(CASE WHEN DF.${x.name} = __Compare.${x.name} or (DF.${x.name} IS NULL AND __Compare.${x.name} IS NULL ) THEN 1 ELSE 0 END as Integer) as Equal_${x.name} \n  ")
          SQL_Resumen = SQL_Resumen.concat(s",CAST(SUM(Equal_${x.name}) AS BigInt) AS Total_Equal_${x.name} \n")  
        }
        
      }
      
      //Genera Where
      var SQLWhere: String = ""
      var SQLPK: String = ""
      PKFields.split(",").foreach { x =>
        SQLPK = s"${SQLPK},coalesce(DF.${x},__Compare.${x}) as ${x} \n"
        SQLWhere = s" ${if (SQLWhere == "") "" else " and "} DF.${x} = __Compare.${x} \n"  
          
      }
      
      SQL_Compare = s"SELECT 'compare' as Operation\n ${SQLPK} ${SQL_Compare }"
      
      //Query Final
      SQL_Compare = s""" ${SQL_Compare} \n FROM ${this.AliasDF} DF FULL JOIN __Compare ON ${SQLWhere} """
      DQResult.dqDF = huemulBigDataGov.DF_ExecuteQuery("__DF_CompareResult", SQL_Compare)
      
      if (huemulBigDataGov.DebugMode) DQResult.dqDF.show()
      
      
      //Query Resume
      SQL_Resumen = s"${SQL_Resumen} FROM __DF_CompareResult "
      val DF_FinalResult = huemulBigDataGov.DF_ExecuteQuery("__DF_CompareResultRes",SQL_Resumen)
      if (huemulBigDataGov.DebugMode) DF_FinalResult.show()
      
      
      val  DF_FinalResultFirst = DF_FinalResult.first()
      val totalCount = DF_FinalResultFirst.getAs[Long]("total")
      this.DataSchema.foreach { x =>
        if (DF_to_Compare.schema.filter { y => y.name.toUpperCase() == x.name.toUpperCase() }.length == 1) {
       
          val currentColumn = DF_FinalResultFirst.getAs[Long](s"Total_Equal_${x.name}")
          if (currentColumn != totalCount) {
            DQResult.Description = s"huemul_DataFrame Error: Column ${x.name} have different values: Total rows ${totalCount}, num OK : ${currentColumn} "
            DQResult.isError = true
            DQResult.Error_Code = 2005
            huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] ${DQResult.Description}")
            DQResult.dqDF.where(s"Equal_${x.name} = 0").show()
            NumColumnsError += 1
          }
          
          NumColumns += 1
        }
        
        
      }
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulBigDataGov.DebugMode)
      }
    }
    
    
    
    var TableName: String = null
    var BBDDName: String = null
    if (ObjectData.isInstanceOf[huemul_Table]) {
      val Data = ObjectData.asInstanceOf[huemul_Table]
      TableName = Data.TableName
      BBDDName = Data.GetCurrentDataBase()
    }
    
    val Values = new huemul_DQRecord()
    Values.Table_Name =TableName
    Values.BBDD_Name =BBDDName
    Values.DF_Alias =AliasDF
    Values.ColumnName =null
    Values.DQ_Name ="COMPARE"
    Values.DQ_Description ="COMPARE TWO DATAFRAMES"
    Values.DQ_QueryLevel = huemulType_DQQueryLevel.Aggregate 
    Values.DQ_Notification = huemulType_DQNotification.ERROR
    Values.DQ_SQLFormula =""
    Values.DQ_toleranceError_Rows =0
    Values.DQ_toleranceError_Percent =Decimal.apply(0)
    Values.DQ_ResultDQ =DQResult.Description
    Values.DQ_ErrorCode = DQResult.Error_Code
    Values.DQ_ExternalCode = DQ_ExternalCode
    Values.DQ_NumRowsOK =NumColumns - NumColumnsError
    Values.DQ_NumRowsError =NumColumnsError
    Values.DQ_NumRowsTotal =NumColumns
    Values.DQ_IsError = DQResult.isError

    this.DQ_Register(Values)
      
    
    
    
    return DQResult
  }
  
  
  def DQ_StatsAllCols(ObjectData: Object) : huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    DQResult.isError = false
    
    this.DataSchema.foreach { x =>
      val Res = DQ_StatsByCol(ObjectData, x.name)
      
      if (Res.isError) {
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] ERROR DQ")
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] ${Res.Description}")
      }
      
      if (DQResult.dqDF != null)
        DQResult.dqDF =  Res.dqDF.union(DQResult.dqDF)
      else 
        DQResult.dqDF =  Res.dqDF
    }
    
    if (huemulBigDataGov.DebugMode) DQResult.dqDF.show()
    
    return DQResult
  }
  
  /**
   * DQ_StatsByCol: Get Stats by Column
   * Max, Min, avg, sum, count(distinct), count, max(length), min(length)
   */
  def DQ_StatsByCol(ObjectData: Object, Col: String) : huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    DQResult.isError = false
    
    //Get DataType
    val Colfield = this.DataSchema.fields.filter { x => x.name.toUpperCase() == Col.toUpperCase()  }
    var DataType: DataType  = null
    if (Colfield != null) {
      DataType = Colfield(0).dataType
    }
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] ${DataType}")
    var SQL : String = s""" SELECT "$Col"            as ColName
                                  ,cast(Min($Col) as String)        as min_Col
                                  ,cast(Max($Col) as String)         as max_Col
                                  ,${if (DataType == null || DataType == TimestampType || DataType == DateType || DataType == CalendarIntervalType) "cast('error' as String)" else s"cast(avg($Col) as String)" }    as avg_Col
                                  ,${if (DataType == null || DataType == TimestampType || DataType == DateType || DataType == CalendarIntervalType) "cast('error' as String)" else s"cast(sum($Col) as String)" }    as sum_Col
                                  ,count(distinct $Col)     as count_distinct_Col
                                  ,count($Col)        as count_all_Col
                                  ,min(length($Col))  as minlen_Col
                                  ,max(length($Col))  as maxlen_Col
                                  ,sum(CASE WHEN length($Col) = 0 and $Col is not null then 1 else 0 end)  as count_empty
                                  ,sum(CASE WHEN $Col is null then 1 else 0 end)  as count_null
                                  ,${if (huemulBigDataGov.IsNumericType(DataType)) s"cast(sum(CASE WHEN $Col = 0 then 1 else 0 end) as long)" else "cast(-1 as long)" }  as count_cero
                            FROM """ + AliasDF   
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(SQL)
    try {
      DQResult.dqDF = huemulBigDataGov.spark.sql(SQL)
            
      if (huemulBigDataGov.DebugMode) DQResult.dqDF.show()        
      huemulBigDataGov.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_StatsByCol_${Col}",huemulBigDataGov.DebugMode, numPartitionsForTempFiles)
      
      val FirstRow = DQResult.dqDF.first()
      DQResult.profilingResult.max_Col = FirstRow.getAs[String]("max_Col")   
      DQResult.profilingResult.min_Col = FirstRow.getAs[String]("min_Col")
      DQResult.profilingResult.avg_Col = FirstRow.getAs[String]("avg_Col")
      DQResult.profilingResult.sum_Col = FirstRow.getAs[String]("sum_Col")
      DQResult.profilingResult.count_distinct_Col = FirstRow.getAs[Long]("count_distinct_Col")
      DQResult.profilingResult.count_all_Col = FirstRow.getAs[Long]("count_all_Col")
      DQResult.profilingResult.maxlen_Col = FirstRow.getAs[Int]("maxlen_Col")
      DQResult.profilingResult.minlen_Col = FirstRow.getAs[Int]("minlen_Col")
      DQResult.profilingResult.count_empty = FirstRow.getAs[Long]("count_empty")
      DQResult.profilingResult.count_null = FirstRow.getAs[Long]("count_null")
      DQResult.profilingResult.count_cero = FirstRow.getAs[Long]("count_cero")
      
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulBigDataGov.DebugMode)
      }
    }
    
        
    return DQResult
  }
  
  /**
   * DQ_StatsByFunction: Returns all columns using "function" aggregate function
   * function: sum, max, min, count, count_distinct, max_length, min_length, 
   */
  def DQ_StatsByFunction(ObjectData: Object, function: String) : huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    DQResult.isError = false
    var SQL : String = s""" SELECT "$function" AS __function__ """
    
    if (function.toUpperCase() == "count_distinct".toUpperCase() || function.toUpperCase() == "max_length".toUpperCase() || function.toUpperCase() == "min_length".toUpperCase() )
      DataSchema.fields.foreach { x => SQL = SQL + " ,Cast(" + function.replace("_", "(") + "(" + x.name + ")) as Long)   as " + x.name + " "   }
    else
      DataSchema.fields.foreach { x => SQL = SQL + " ," + function + "(" + x.name + ")   as " + x.name + " "   }
    
    SQL = SQL + " FROM " + AliasDF   

    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(SQL)
    try {
      DQResult.dqDF = huemulBigDataGov.spark.sql(SQL)
      
      if (huemulBigDataGov.DebugMode) {
        DQResult.dqDF.printSchema()
        DQResult.dqDF.show()
      }
      huemulBigDataGov.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_StatsByFunction_${function}",huemulBigDataGov.DebugMode, numPartitionsForTempFiles)
      
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulBigDataGov.DebugMode)
      }
    }
    
       
    return DQResult
  }
  
   /**
   * DQ_DuplicateValues: validate duplicate rows for ColDuplicate
   *  
   */
  def DQ_DuplicateValues2(ObjectData: Object, ColDuplicate: String, colMaxMin: String, TempFileName: String = null, DQ_ExternalCode:String = null) : huemul_DataQualityResult = {
    var colMaxMin_local = colMaxMin
    if (colMaxMin_local == null)
      colMaxMin_local = "0"
    val DQResult = new huemul_DataQualityResult()
    val talias = AliasDF
    DQResult.isError = false
    var SQL : String = s""" SELECT "$ColDuplicate"            as ColName
                                  ,$ColDuplicate              
                                  ,max($colMaxMin_local)   as Max$colMaxMin_local
                                  ,min($colMaxMin_local)   as Min$colMaxMin_local                                  
                                  ,count(1)          as countDup
                            FROM $talias
                            GROUP BY $ColDuplicate
                            HAVING count(1) > 1
                            order by countDup desc
                        """   

    var DQDup: Long = 0
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(SQL)
    try {
      DQResult.dqDF = huemulBigDataGov.spark.sql(SQL) 
      if (huemulBigDataGov.DebugMode) {
        
        DQResult.dqDF.printSchema()
        DQResult.dqDF.show()
      
        var TempFileName_local = TempFileName
        if (TempFileName_local == null)
          TempFileName_local = ColDuplicate
        huemulBigDataGov.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_DupliVal_${TempFileName_local}",huemulBigDataGov.DebugMode, numPartitionsForTempFiles)
      }
      
      //duplicate rows found
      DQDup = DQResult.dqDF.count()
      if (DQDup > 0) {
        DQResult.Description = s"huemul_DataFrame Error: num Rows Duplicate in $ColDuplicate: " + DQDup.toString()
        DQResult.isError = true
        DQResult.Error_Code = 2006
        huemulBigDataGov.logMessageWarn(DQResult.Description)
        DQResult.dqDF.show()
      }
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulBigDataGov.DebugMode)
      }
    }
    
    var TableName: String = null
    var BBDDName: String = null
    if (ObjectData.isInstanceOf[huemul_Table]) {
      val Data = ObjectData.asInstanceOf[huemul_Table]
      TableName = Data.TableName
      BBDDName = Data.GetCurrentDataBase()
    }
    
    val Values = new huemul_DQRecord()
    Values.Table_Name =TableName
    Values.BBDD_Name =BBDDName
    Values.DF_Alias =AliasDF
    Values.ColumnName = if (TempFileName == "PK") null else ColDuplicate
    Values.DQ_Name = if (TempFileName == "PK") "PK" else "UNIQUE"
    Values.DQ_Description =s"UNIQUE VALUES FOR FIELD $ColDuplicate"
    Values.DQ_QueryLevel = huemulType_DQQueryLevel.Aggregate 
    Values.DQ_Notification = huemulType_DQNotification.ERROR
    Values.DQ_SQLFormula =""
    Values.DQ_toleranceError_Rows =0
    Values.DQ_toleranceError_Percent =Decimal.apply(0)
    Values.DQ_ResultDQ =DQResult.Description
    Values.DQ_ErrorCode = DQResult.Error_Code
    Values.DQ_ExternalCode = DQ_ExternalCode
    Values.DQ_NumRowsOK =0
    Values.DQ_NumRowsError =DQDup
    Values.DQ_NumRowsTotal =getNumRows
    Values.DQ_IsError = DQResult.isError

    this.DQ_Register(Values)
    
    return DQResult
  }
  
   /**
   * DQ_NotNullValues: validate nulls rows
   *  
   */
  def DQ_NotNullValues(ObjectData: Object, Col: String, DQ_ExternalCode:String = null) : huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    val talias = AliasDF
    DQResult.isError = false
    var SQL : String = s""" SELECT *
                            FROM $talias
                            WHERE $Col is null
                        """   

    var DQDup: Long = 0
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(SQL)
    try {
      DQResult.dqDF = huemulBigDataGov.spark.sql(SQL) 
      if (huemulBigDataGov.DebugMode) {
        
        DQResult.dqDF.printSchema()
        DQResult.dqDF.show()
        
      }
      huemulBigDataGov.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_NotNullValues_${Col}",huemulBigDataGov.DebugMode, numPartitionsForTempFiles)
      
      //null rows found
      DQDup = DQResult.dqDF.count()
      if (DQDup > 0) {
        DQResult.Description = s"huemul_DataFrame Error: num Rows Null in $Col: " + DQDup.toString()
        DQResult.isError = true
        DQResult.Error_Code = 2007
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] ${DQResult.Description}")
        DQResult.dqDF.show()
      }
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulBigDataGov.DebugMode)
      }
    }
    
  
    var TableName: String = null
    var BBDDName: String = null
    if (ObjectData.isInstanceOf[huemul_Table]) {
      val Data = ObjectData.asInstanceOf[huemul_Table]
      TableName = Data.TableName
      BBDDName = Data.GetCurrentDataBase()
    }
    
    val Values = new huemul_DQRecord()
    Values.Table_Name =TableName
    Values.BBDD_Name =BBDDName
    Values.DF_Alias =AliasDF
    Values.ColumnName =Col
    Values.DQ_Name ="NOT NULL"
    Values.DQ_Description =s"NOT NULL VALUES FOR FIELD $Col"
    Values.DQ_QueryLevel = huemulType_DQQueryLevel.Row
    Values.DQ_Notification = huemulType_DQNotification.ERROR
    Values.DQ_SQLFormula =""
    Values.DQ_toleranceError_Rows =0
    Values.DQ_toleranceError_Percent =Decimal.apply(0)
    Values.DQ_ResultDQ =DQResult.Description
    Values.DQ_ErrorCode = DQResult.Error_Code
    Values.DQ_ExternalCode = DQ_ExternalCode
    Values.DQ_NumRowsOK =0
    Values.DQ_NumRowsError =DQDup
    Values.DQ_NumRowsTotal =0
    Values.DQ_IsError = DQResult.isError

    this.DQ_Register(Values)
    
            
    return DQResult
  }
  
  /**
   Create DQ List from DataDef Definition
   */
  private def getDataQualitySentences(OfficialDataQuality: ArrayBuffer[huemul_DataQuality], RulesDQ: ArrayBuffer[huemul_DataQuality]): ArrayBuffer[huemul_DataQuality] = {    
    var Result: ArrayBuffer[huemul_DataQuality] = new ArrayBuffer[huemul_DataQuality]()
    var i: Integer = 0
    
    if (OfficialDataQuality != null) {
      OfficialDataQuality.foreach { x =>
        //Get DQ
        
        x.setId(i)
        Result.append(x)
        i += 1
      }
    }
    
    if (RulesDQ != null) {
      RulesDQ.foreach { x =>
        //Get DQ
        
        x.setId(i)
        if (x.getMyName == null || x.getMyName == "")
          x.setMyName(x.getDescription)
        Result.append(x)
        i += 1
      }
    }
    
    return Result
  }
  
  
  def getSQL_DataQualityForRun(OfficialDataQuality: ArrayBuffer[huemul_DataQuality], RulesDQ: ArrayBuffer[huemul_DataQuality], AliasDF: String): String = {
    var SQLResult: String = ""
 
    getDataQualitySentences(OfficialDataQuality, RulesDQ).foreach { x =>  
        SQLResult += s",CAST(${if (x.getQueryLevel() == huemulType_DQQueryLevel.Aggregate) "" else "SUM"}(CASE WHEN ${x.getSQLFormula} THEN 1 ELSE 0 END) AS LONG) AS ___DQ_${x.getId} \n"        
    }
    
        
    if (SQLResult != "") {
      SQLResult  = s"SELECT count(1) as ___Total \n ${SQLResult} FROM ${AliasDF}"           
    }
         
    return SQLResult
  }
  
  
  /**
   Run DataQuality defined by user
   */
  def DF_RunDataQuality(ManualRules: ArrayBuffer[huemul_DataQuality]): huemul_DataQualityResult = {
    return DF_RunDataQuality(null, ManualRules, null, null)
  }
   
  
  /**
   Run DataQuality defined in Master
   */
  def DF_RunDataQuality(ManualRules: ArrayBuffer[huemul_DataQuality], DF_to_Query: String, dMaster: huemul_Table): huemul_DataQualityResult = {
    return DF_RunDataQuality(null, ManualRules, DF_to_Query, dMaster)
  }
   
  
  /**
   Run DataQuality defined in Master
   */
  def DF_RunDataQuality(OfficialDataQuality: ArrayBuffer[huemul_DataQuality], ManualRules: ArrayBuffer[huemul_DataQuality], DF_to_Query: String, dMaster: huemul_Table): huemul_DataQualityResult = {
    val AliasToQuery = if (DF_to_Query == null) this.AliasDF else DF_to_Query
     
    /*****************************************************************/
    /********** S Q L   F O R M U L A   F O R   D Q    ***************/
    /*****************************************************************/
    
    var NumTotalErrors: Integer = 0
    var txtTotalErrors: String = ""
    var ErrorLog: huemul_DataQualityResult = new huemul_DataQualityResult()
    var localErrorCode : Integer = null
    
    //DataQuality AdHoc
    val SQLDQ = getSQL_DataQualityForRun(OfficialDataQuality, ManualRules, AliasToQuery)
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) {
      huemulBigDataGov.logMessageDebug(s"HuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] DATA QUALITY ADHOC QUERY")
      huemulBigDataGov.logMessageDebug(SQLDQ)
    }
    var DF_ErrorDetails: DataFrame = null
    
    if (SQLDQ != ""){
      //Execute DQ
      val AliasDQ = s"${AliasToQuery}__DQ_p0"
      ErrorLog.dqDF = huemulBigDataGov.DF_ExecuteQuery(AliasDQ
                                        , SQLDQ)
      
      val FirstReg = ErrorLog.dqDF.collect()(0) // .first()
      val DQTotalRows = FirstReg.getAs[Long](s"___Total")
      
      //import java.util.Calendar;
      //Get DQ Result from DF
      
      getDataQualitySentences(OfficialDataQuality, ManualRules).foreach { x =>
        x.NumRowsOK = FirstReg.getAs[Long](s"___DQ_${x.getId}")
        x.NumRowsTotal = if (x.getQueryLevel() == huemulType_DQQueryLevel.Aggregate) 1 else DQTotalRows
        x.ResultDQ = ""
        
        val DQWithError = x.NumRowsTotal - x.NumRowsOK
        val DQWithErrorPerc = Decimal.apply(DQWithError) / Decimal.apply(x.NumRowsTotal)
        var IsError: Boolean = false
        
        //Validate max num rows with error vs definition 
        if (x.getToleranceError_Rows != null) {
           
          if (DQWithError > x.getToleranceError_Rows){
            x.ResultDQ = s"DQ Name ${if (IsError) s"${x.getNotification()} (code:${x.getErrorCode()})" else "OK"}: ${x.getMyName} (___DQ_${x.getId}), num OK: ${x.NumRowsOK}, num Total: ${x.NumRowsTotal}, Error: ${DQWithError}(tolerance:${x.getToleranceError_Rows}), Error %: ${DQWithErrorPerc * Decimal.apply(100)}%(tolerance:${if (x.getToleranceError_Percent == null) 0 else x.getToleranceError_Percent * Decimal.apply(100)}%)"
            IsError = true
          }
            
        } 
        
        //Validate % rows with error vs definition
        if (x.getToleranceError_Percent != null) {
           
          if (DQWithErrorPerc > x.getToleranceError_Percent) {
            x.ResultDQ = s"DQ Name ${if (IsError) s"${x.getNotification()} (code:${x.getErrorCode()})" else "OK"}: ${x.getMyName} (___DQ_${x.getId}), num OK: ${x.NumRowsOK}, num Total: ${x.NumRowsTotal}, Error: ${DQWithError}(tolerance:${x.getToleranceError_Rows}), Error %: ${DQWithErrorPerc * Decimal.apply(100)}%(tolerance:${if (x.getToleranceError_Percent == null) 0 else x.getToleranceError_Percent * Decimal.apply(100)}%)"
            IsError = true
          }
        }
        
        if (huemulBigDataGov.DebugMode || IsError) huemulBigDataGov.logMessageDebug(s"HuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] DQ Name ${if (IsError) s"${x.getNotification()} (code:${x.getErrorCode()})" else "OK"}: ${x.getMyName} (___DQ_${x.getId}), num OK: ${x.NumRowsOK}, num Total: ${x.NumRowsTotal}, Error: ${DQWithError}(tolerance:${x.getToleranceError_Rows}), Error %: ${DQWithErrorPerc * Decimal.apply(100)}%(tolerance:${if (x.getToleranceError_Percent == null) 0 else x.getToleranceError_Percent * Decimal.apply(100)}%)")
                       
        var dfTableName: String = null
        var dfDataBaseName: String = null
        if (dMaster != null) {
          dfTableName = dMaster.TableName
          dfDataBaseName = dMaster.GetCurrentDataBase()
        }
        
        val Values = new huemul_DQRecord()
        Values.Table_Name =dfTableName
        Values.BBDD_Name =dfDataBaseName
        Values.DF_Alias =AliasToQuery
        Values.ColumnName =if (x.getFieldName == null) null else x.getFieldName.get_MyName()
        Values.DQ_Name =x.getMyName()
        Values.DQ_Description =s"(Id ${x.getId}) ${x.getDescription}"
        Values.DQ_QueryLevel =x.getQueryLevel() // .getDQ_QueryLevel
        Values.DQ_Notification =x.getNotification()
        Values.DQ_SQLFormula =x.getSQLFormula
        Values.DQ_toleranceError_Rows =x.getToleranceError_Rows
        Values.DQ_toleranceError_Percent =x.getToleranceError_Percent
        Values.DQ_ResultDQ =x.ResultDQ
        Values.DQ_ExternalCode = x.getDQ_ExternalCode()
        Values.DQ_ErrorCode = if (IsError) x.getErrorCode() else null 
        Values.DQ_NumRowsOK =x.NumRowsOK
        Values.DQ_NumRowsError =DQWithError
        Values.DQ_NumRowsTotal =x.NumRowsTotal    
        Values.DQ_IsError = IsError 
       
        ErrorLog.appendDQResult(Values)
        this.DQ_Register(Values)
        
        if (x.getNotification() == huemulType_DQNotification.ERROR && IsError) {
          txtTotalErrors += s"\nHuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] DQ Name (${x.getErrorCode()}): ${x.getMyName} with error: ${x.ResultDQ}"
          NumTotalErrors += 1
          localErrorCode = x.getErrorCode()
        }
        
        //Save details to DF with errors
        if (dfTableName != null && huemulBigDataGov.GlobalSettings.DQ_SaveErrorDetails && IsError && x.getSaveErrorDetails()) {
          //Query to get detail errors
          //Control.NewStep(s"Step: DQ Result: Get detales for (Id ${x.getId}) ${x.getDescription} ") 
          val SQL_Detail = DQ_GenQuery(AliasToQuery
                                      ,!(x.getFieldName == null) //asField
                                      ,x.getFieldName.get_MyName() //fieldName
                                      ,x.getNotification()
                                      ,x.getErrorCode()
                                      ,x.getId
                                      ,x.getDescription
                                      ,x.getSQLFormula()
                                      )
                               
          //Execute query
          var DF_EDetail = huemulBigDataGov.DF_ExecuteQuery("temp_DQ", SQL_Detail)
                               
          if (DF_ErrorDetails == null)
            DF_ErrorDetails = DF_EDetail
          else
            DF_ErrorDetails = DF_ErrorDetails.union(DF_EDetail)
        }
      }
    }
    
    if (NumTotalErrors > 0){
      huemulBigDataGov.logMessageWarn (s"HuemulDataFrameLog: [${huemulBigDataGov.huemul_getDateForLog()}] num with errors: ${NumTotalErrors} ")
      ErrorLog.isError = true
      ErrorLog.Description = txtTotalErrors
      ErrorLog.Error_Code = localErrorCode 
    }
    
    ErrorLog.DetailErrorsDF = DF_ErrorDetails
    
    
    return ErrorLog
  }
  
  def DQ_GenQuery(AliasToQuery: String
                  ,asField: Boolean
                  ,fieldName: String
                  ,dq_error_notification: huemulType_DQNotification.huemulType_DQNotification
                  ,error_code: Integer
                  ,getId: Integer
                  ,getDescription: String
                  ,whereClause: String
                  ): String = {
    return s"""SELECT '${Control.Control_Id }' as dq_control_id
                                     ,'${if (asField) "all" else fieldName}' as dq_error_columnname
                                     ,'${dq_error_notification}' as dq_error_notification 
                                     ,'${error_code}' as dq_error_code
                                     ,'(Id ${getId}) ${getDescription}' as dq_error_descripcion
                                     , *
                               FROM  ${AliasToQuery}
                               ${if (whereClause == null) "" else "WHERE not (${whereClause})" }"""
  }
  
  def DQ_Register(DQ: huemul_DQRecord) {
    DQ_Result.append(DQ)
    Control.RegisterDQuality(DQ.Table_Name
        , DQ.BBDD_Name
        , DQ.DF_Alias
        , DQ.ColumnName
        , DQ.DQ_Name
        , DQ.DQ_Description
        , DQ.DQ_QueryLevel
        , DQ.DQ_Notification// _RaiseError
        , DQ.DQ_SQLFormula
        , DQ.DQ_toleranceError_Rows
        , DQ.DQ_toleranceError_Percent
        , DQ.DQ_ResultDQ
        , DQ.DQ_ErrorCode
        , DQ.DQ_ExternalCode
        , DQ.DQ_NumRowsOK
        , DQ.DQ_NumRowsError
        , DQ.DQ_NumRowsTotal 
        , DQ.DQ_IsError)
  }
  
}