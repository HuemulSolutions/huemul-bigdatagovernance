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
  
  private def local_setDataFrame(DF: DataFrame, Alias: String, SaveInTemp: Boolean ) {
    _NumRows = -1
    DataDF = DF
    DataSchema = DF.schema
    DataDF.createOrReplaceTempView(Alias)
    AliasDF = Alias
    
    //Set as Readed
    //NumRows = DataDF.count() //este parámetro se setea autmáticamente al intentar leer getNumRows
    NumCols = DataDF.columns.length
    //Data_isRead = true
    //TODO: ver como poner la fecha de término de lectura StopRead_dt = Calendar.getInstance()
    
            
    if (SaveInTemp)
      huemulBigDataGov.CreateTempTable(DataDF, AliasDF, huemulBigDataGov.DebugMode, null)
  }
  
  def setDataFrame(DF: DataFrame, Alias: String, SaveInTemp: Boolean = true) {
    local_setDataFrame(DF, Alias, SaveInTemp)
  }
  
  
  
  /**   
   Create DF from SQL (equivalent to spark.sql method)
   */
  def DF_from_SQL(Alias: String, sql: String, SaveInTemp: Boolean = true, NumPartitions: Integer = null) {
    //WARNING: ANY CHANGE ON THIS METHOD MUST BE REPLIATES TO _CreateFinalQuery
    //THE ONLY DIFFERENCE IS IN 
    //huemulBigDataGov.DF_SaveLineage(Alias, sql,dt_start, dt_end, Control)
    _NumRows = -1
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sql)
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
    //Cambio en v1.3: optimiza tiempo al aplicar repartition para archivos pequeños             
    val DFTemp = if (NumPartitions == null || NumPartitions <= 0) huemulBigDataGov.spark.sql(sql)
                 else huemulBigDataGov.spark.sql(sql).repartition(NumPartitions)
      
    
    local_setDataFrame(DFTemp, Alias, SaveInTemp)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    
    huemulBigDataGov.DF_SaveLineage(Alias
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
  def _CreateFinalQuery(Alias: String, sql: String, SaveInTemp: Boolean = true, NumPartitions: Integer = null, finalTable: huemul_Table, storageLevelOfDF: org.apache.spark.storage.StorageLevel) {
    //WARNING: ANY CHANGE ON DF_from_SQL MUST BE REPLICATE IN THIS METHOD
    
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sql)
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
    //Cambio en v1.3: optimiza tiempo al aplicar repartition para archivos pequeños             
    val DFTemp = if (NumPartitions == null || NumPartitions <= 0) huemulBigDataGov.spark.sql(sql)
                 else huemulBigDataGov.spark.sql(sql).repartition(NumPartitions)
      
    if (storageLevelOfDF != null) {
      huemulBigDataGov.logMessageDebug(s"DF to ${storageLevelOfDF}")
      DFTemp.persist(storageLevelOfDF)
    }
    
    local_setDataFrame(DFTemp, Alias, SaveInTemp)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    
    huemulBigDataGov.DF_SaveLineage(Alias
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
    _NumRows = -1
    if (huemulBigDataGov.DebugMode) { println(rowRDD.take(2).foreach { x => println(x) }) }
    val DF = huemulBigDataGov.spark.createDataFrame(rowRDD, DataSchema)
    
    //Assign DataFrame to LocalDataFrame
    local_setDataFrame(DF, Alias, huemulBigDataGov.DebugMode)
    
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
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
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
      BBDDName = Data.getCurrentDataBase()
    }
    
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
      
    val Values = new huemul_DQRecord(huemulBigDataGov)
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
    Values.DQ_IsWarning = DQResult.isWarning
    Values.DQ_duration_hour = duration.hour.toInt
    Values.DQ_duration_minute = duration.minute.toInt
    Values.DQ_duration_second = duration.second.toInt
    
    this.DQ_Register(Values)    
    
    
    return DQResult
  }
  
  /****
   * DQ_NumRowsInterval: Test DQ for num Rows in DF
   */
  def DQ_NumRows(ObjectData: Object, NumRowsExpected: Long, DQ_ExternalCode:String = null): huemul_DataQualityResult = {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
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
      BBDDName = Data.getCurrentDataBase()
    }
    
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
    
    val Values = new huemul_DQRecord(huemulBigDataGov)
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
    Values.DQ_IsWarning = DQResult.isWarning
    Values.DQ_duration_hour = duration.hour.toInt
    Values.DQ_duration_minute = duration.minute.toInt
    Values.DQ_duration_second = duration.second.toInt
      
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
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
    
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
            huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: ${DQResult.Description}")
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
      BBDDName = Data.getCurrentDataBase()
    }
    
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
    
    val Values = new huemul_DQRecord(huemulBigDataGov)
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
    Values.DQ_IsWarning = DQResult.isWarning
    Values.DQ_duration_hour = duration.hour.toInt
    Values.DQ_duration_minute = duration.minute.toInt
    Values.DQ_duration_second = duration.second.toInt
    
    this.DQ_Register(Values)
      
    
    
    
    return DQResult
  }
  
  
  def DQ_StatsAllCols(ObjectData: Object) : huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    DQResult.isError = false
    
    this.DataSchema.foreach { x =>
      val Res = DQ_StatsByCol(ObjectData, x.name)
      
      if (Res.isError) {
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: ERROR DQ")
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: ${Res.Description}")
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
    if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"HuemulDataFrameLog: ${DataType}")
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
            
      DQResult.dqDF.show()        
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
      }
      DQResult.dqDF.show()
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
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
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
      BBDDName = Data.getCurrentDataBase()
    }
    
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
    
    val Values = new huemul_DQRecord(huemulBigDataGov)
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
    Values.DQ_IsWarning = DQResult.isWarning
    Values.DQ_duration_hour = duration.hour.toInt
    Values.DQ_duration_minute = duration.minute.toInt
    Values.DQ_duration_second = duration.second.toInt
    
    this.DQ_Register(Values)
    
    return DQResult
  }
  
   /**
   * DQ_NotNullValues: validate nulls rows
   *  
   */
  def DQ_NotNullValues(ObjectData: Object, Col: String, DQ_ExternalCode:String = null) : huemul_DataQualityResult = {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
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
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: ${DQResult.Description}")
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
      BBDDName = Data.getCurrentDataBase()
    }
    
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)    
    
    val Values = new huemul_DQRecord(huemulBigDataGov)
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
    Values.DQ_IsWarning = DQResult.isWarning
    Values.DQ_duration_hour = duration.hour.toInt
    Values.DQ_duration_minute = duration.minute.toInt
    Values.DQ_duration_second = duration.second.toInt
    
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
    DF_RunDataQuality(OfficialDataQuality, ManualRules, DF_to_Query, dMaster, true)  
  }
  
  def DF_RunDataQuality(OfficialDataQuality: ArrayBuffer[huemul_DataQuality], ManualRules: ArrayBuffer[huemul_DataQuality], DF_to_Query: String, dMaster: huemul_Table, registerDQOnce: Boolean ): huemul_DataQualityResult = {
    val AliasToQuery = if (DF_to_Query == null) this.AliasDF else DF_to_Query
     
    /*****************************************************************/
    /********** S Q L   F O R M U L A   F O R   D Q    ***************/
    /*****************************************************************/
    
    var NumTotalErrors: Integer = 0
    var NumTotalWarnings: Integer = 0
    var txtTotalErrors: String = ""
    var ErrorLog: huemul_DataQualityResult = new huemul_DataQualityResult()
    var localErrorCode : Integer = null
    
    //DataQuality AdHoc
    val SQLDQ = getSQL_DataQualityForRun(OfficialDataQuality, ManualRules, AliasToQuery)
    if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) {
      huemulBigDataGov.logMessageDebug(s"HuemulDataFrameLog: DATA QUALITY ADHOC QUERY")
      huemulBigDataGov.logMessageDebug(SQLDQ)
    }
    var DF_ErrorDetails: DataFrame = null
    
    if (SQLDQ != ""){
      val dt_start = huemulBigDataGov.getCurrentDateTimeJava()
      //Execute DQ
      val AliasDQ = s"${AliasToQuery}__DQ_p0"
      ErrorLog.dqDF = huemulBigDataGov.DF_ExecuteQuery(AliasDQ
                                        , SQLDQ)
      
      val FirstReg = ErrorLog.dqDF.collect()(0) // .first()
      val DQTotalRows = FirstReg.getAs[Long](s"___Total")
      
      val dt_end = huemulBigDataGov.getCurrentDateTimeJava()
      val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
      //import java.util.Calendar;
      //Get DQ Result from DF
      
      Control.NewStep(s"Step: DQ Result: Start analyzing the DQ result") 
      
      if (DQTotalRows == 0) {
        Control.NewStep(s"Step: DQ Result: 0 rows in DF, nothing to evaluate") 
        huemulBigDataGov.logMessageWarn("0 rows in DF, nothing to evaluate")
      } else {
        getDataQualitySentences(OfficialDataQuality, ManualRules).foreach { x =>
          x.NumRowsOK = FirstReg.getAs[Long](s"___DQ_${x.getId}")
          x.NumRowsTotal = if (x.getQueryLevel() == huemulType_DQQueryLevel.Aggregate) 1 else DQTotalRows
          x.ResultDQ = ""
          
          val DQWithError = x.NumRowsTotal - x.NumRowsOK
          val DQWithErrorPerc = Decimal.apply(DQWithError) / Decimal.apply(x.NumRowsTotal)
          var IsError: Boolean = false
          
          //Validate max num rows with error vs definition 
          if (x.getToleranceError_Rows != null) {
            if (DQWithError > x.getToleranceError_Rows)
              IsError = true
          } 
          
          //Validate % rows with error vs definition
          if (x.getToleranceError_Percent != null) {
            if (DQWithErrorPerc > x.getToleranceError_Percent) 
              IsError = true
          }
          
          //set user message
          x.ResultDQ = s"DQ ${if (IsError) s"${x.getNotification()} (code:${x.getErrorCode()})" else "OK"}, Name: ${x.getMyName} (___DQ_${x.getId}), num OK: ${x.NumRowsOK}, num Total: ${x.NumRowsTotal}, Error: ${DQWithError}(tolerance:${x.getToleranceError_Rows}), Error %: ${DQWithErrorPerc * Decimal.apply(100)}%(tolerance:${if (x.getToleranceError_Percent == null) 0 else x.getToleranceError_Percent * Decimal.apply(100)}%)"
          if (huemulBigDataGov.DebugMode || IsError) huemulBigDataGov.logMessageDebug(x.ResultDQ)
                         
          var dfTableName: String = null
          var dfDataBaseName: String = null
          if (dMaster != null) {
            dfTableName = dMaster.TableName
            dfDataBaseName = dMaster.getCurrentDataBase()
          }
          
          
          
          val Values = new huemul_DQRecord(huemulBigDataGov)
          Values.Table_Name =dfTableName
          Values.BBDD_Name =dfDataBaseName
          Values.DF_Alias =AliasToQuery
          Values.ColumnName =if (x.getFieldName == null) null else x.getFieldName.get_MyName(dMaster.getStorageType)
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
          Values.DQ_IsError = x.getNotification() == huemulType_DQNotification.ERROR && IsError //IsError 
          Values.DQ_IsWarning = (x.getNotification() == huemulType_DQNotification.WARNING || x.getNotification() == huemulType_DQNotification.WARNING_EXCLUDE) && IsError
          Values.DQ_duration_hour = duration.hour.toInt
          Values.DQ_duration_minute = duration.minute.toInt
          Values.DQ_duration_second = duration.second.toInt
      
      
          ErrorLog.appendDQResult(Values)
          this.DQ_Register(Values)
          
          if (Values.DQ_IsWarning)
            NumTotalWarnings += 1
            
          if (Values.DQ_IsError) {
            txtTotalErrors += s"\nHuemulDataFrameLog: DQ Name (${x.getErrorCode()}): ${x.getMyName} with error: ${x.ResultDQ}"
            NumTotalErrors += 1
            localErrorCode = x.getErrorCode()
          }
          
          //Save details to DF with errors
          if (dfTableName != null && huemulBigDataGov.GlobalSettings.DQ_SaveErrorDetails && IsError && x.getSaveErrorDetails()) {
            //Query to get detail errors
            Control.NewStep(s"Step: DQ Result: Get detail error for (Id ${x.getId}) ${x.getDescription}) ") 
            val SQL_Detail = DQ_GenQuery(AliasToQuery
                                        ,s"not (${x.getSQLFormula()})"
                                        ,!(x.getFieldName == null) //asField
                                        ,if (x.getFieldName == null) "all" else x.getFieldName.get_MyName(dMaster.getStorageType) //fieldName
                                        ,Values.DQ_Id
                                        ,x.getNotification()
                                        ,x.getErrorCode()
                                        ,s"(Id ${x.getId}) ${x.getDescription}"
                                        )
                                 
            //Execute query
            //Control.NewStep(s"Step: DQ Result: Get detail error for (Id ${x.getId}) ${x.getDescription}, save to DF) ") 
            var DF_EDetail = huemulBigDataGov.DF_ExecuteQuery("temp_DQ", SQL_Detail)
                                 
            if (dMaster != null && !registerDQOnce) {
              //if call is from huemul_Table, save detail to disk
              //Save errors to disk
              if (huemulBigDataGov.GlobalSettings.DQ_SaveErrorDetails && DF_EDetail != null && dMaster.getSaveDQResult) {
                Control.NewStep("Start Save DQ Error Details ")                
                if (!dMaster.savePersist_DQ(Control, DF_EDetail)){
                  huemulBigDataGov.logMessageWarn("Warning: DQ error can't save to disk")
                }
              }
            } else {
              //acumulate DF, if call is outside huemul_Table
              if (DF_ErrorDetails == null)
                DF_ErrorDetails = DF_EDetail
              else
                DF_ErrorDetails = DF_ErrorDetails.union(DF_EDetail)
            }
          }
        }
      }
    }
    
    
    ErrorLog.isWarning = NumTotalWarnings > 0 
    if (NumTotalErrors > 0){
      huemulBigDataGov.logMessageWarn (s"HuemulDataFrameLog: num with errors: ${NumTotalErrors} ")
      ErrorLog.isError = true
      ErrorLog.Description = txtTotalErrors
      ErrorLog.Error_Code = localErrorCode 
    }
    
    ErrorLog.DetailErrorsDF = DF_ErrorDetails
    
    
    return ErrorLog
  }
  
  def DQ_GenQuery(fromSQL: String
                  ,whereSQL: String
                  ,haveField: Boolean
                  ,fieldName: String
                  ,dq_id: String
                  ,dq_error_notification: huemulType_DQNotification.huemulType_DQNotification
                  ,error_code: Integer
                  ,dq_error_description: String
                  ): String = {
    return s"""SELECT '${Control.Control_Id }' as dq_control_id
                                     ,'${dq_id }' as dq_dq_id
                                     ,'${if (haveField) fieldName else "all"}' as dq_error_columnname
                                     ,'${dq_error_notification}' as dq_error_notification 
                                     ,'${error_code}' as dq_error_code
                                     ,'${dq_error_description}' as dq_error_descripcion
                                     , *
                               FROM  ${fromSQL}
                               ${if (whereSQL == null || whereSQL == "") "" else s"WHERE ${whereSQL}" }"""
  }
  
  
  
  /**
   * Create table script to save DF to disk
   */
  private def DF_CreateTable_Script(parquetLocation: String, fullTableName: String): String = {
    //create structu
    var ColumnsCreateTable : String = ""
    var coma: String = ""
    
    this.DataDF.schema.fields.foreach { x => 
      ColumnsCreateTable += s"$coma${x.name} ${x.dataType.sql} \n"
      coma = ","
    }
    
    
    //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
    val lCreateTableScript = s"""
                                 CREATE EXTERNAL TABLE IF NOT EXISTS ${fullTableName} (${ColumnsCreateTable })
                                 STORED AS PARQUET                            
                                 LOCATION '${parquetLocation}'"""
                                 
    if (huemulBigDataGov.DebugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: ${lCreateTableScript} ")
      
    return lCreateTableScript    
  }
  
  
  
  /**
   * Save Result data to disk
   */
  def savePersistToDisk(OverriteIfExist: Boolean, tableNameInHive: String, localPath: String, globalPath: ArrayBuffer[huemul_KeyValuePath] = huemulBigDataGov.GlobalSettings.SANDBOX_BigFiles_Path, databaseName: ArrayBuffer[huemul_KeyValuePath] = huemulBigDataGov.GlobalSettings.SANDBOX_DataBase ): Boolean = {
    var Result: Boolean = true
    val tempPath = huemulBigDataGov.GlobalSettings.GetPathForSaveTableWithoutDG(huemulBigDataGov, globalPath, localPath, tableNameInHive)
    
    try {      
      Control.NewStep("Saving DF to Disk")
      if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"saving path: ${tempPath} ")
      if (OverriteIfExist)
        this.DataDF.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
      else 
        this.DataDF.write.mode(SaveMode.Append).format("parquet").save(tempPath)
      
    } catch {
      case e: Exception => 
        Result = false
        Control.Control_Error.GetError(e, getClass.getSimpleName, 2008)
    }
    
    if (Result) {
      //if (CreateInHive ) {
        val ddbbName: String = huemulBigDataGov.getDataBase(databaseName)
        val tabName: String = s"$ddbbName.$tableNameInHive"
        val sqlDrop01 = s"drop table if exists ${tabName}"
        Control.NewStep("Save: Drop Hive table Def")
        if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
        try {
          val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(ddbbName).collect()
          if (TablesListFromHive.filter { x => x.name.toUpperCase() == tableNameInHive.toUpperCase() }.length > 0) 
            huemulBigDataGov.spark.sql(sqlDrop01)
            
        } catch {
          case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
        }
       
      //}
        
      try {
        //create table
        //if (CreateInHive ) {
          Control.NewStep("Save: Create Table in Hive Metadata")
          val lscript = DF_CreateTable_Script(tempPath, tabName) 
          huemulBigDataGov.spark.sql(lscript)
        //}
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        //Control.NewStep("Save: Repair Hive Metadata")
        //if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"MSCK REPAIR TABLE ${tabName}")
        //huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${tabName}")
        
        if (huemulBigDataGov.ImpalaEnabled) {
          Control.NewStep("Save: refresh Impala Metadata")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"invalidate metadata ${tabName}")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"refresh ${tabName}")
        }
      } catch {
        case e: Exception => 
          Result = false
          Control.Control_Error.GetError(e, getClass.getSimpleName,2009)
      }
    }
      
    return Result
    
  }
  
  def DQ_Register(DQ: huemul_DQRecord) {
    DQ_Result.append(DQ)
    Control.RegisterDQuality(DQ.Table_Name
        , DQ.BBDD_Name
        , DQ.DF_Alias
        , DQ.ColumnName
        , DQ.DQ_Id
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
        , DQ.DQ_IsError
        , DQ.DQ_IsWarning
        , DQ.DQ_duration_hour
        , DQ.DQ_duration_minute
        , DQ.DQ_duration_second)
  }
  
}