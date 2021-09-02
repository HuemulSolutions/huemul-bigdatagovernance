package com.huemulsolutions.bigdata.common

import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import scala.collection.mutable._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.dataquality.HuemulDqRecord

/**
 * Def_Fabric_DataInfo: Define method to improve DQ over DF
 */
class HuemulDataFrame(huemulBigDataGov: HuemulBigDataGovernance, control: HuemulControl) extends Serializable {
  private val numPartitionsForTempFiles: Integer = 2
  if (control == null)
    sys.error("Control is null in huemul_DataFrame")


  if (huemulBigDataGov == null)
    sys.error("huemulBigDataGov is null in huemul_DataFrame")
  /**
   * Get and Set DataFrame
   */
  private var DataDF: DataFrame = _
  def dataFrame: DataFrame = DataDF

  /**
   * Schema info
   */
  private var DataSchema: StructType = _
  def SetDataSchema(Schema: StructType) {
    DataSchema = Schema
    NumCols = Schema.length
  }
  def getDataSchema: StructType =  DataSchema

  /**
   * No Rows info
   */

  //private var NumRows: Long = -1
  private var _NumRows: Long = -1
  def getNumRows: Long = {
    if (_NumRows == -1)
      _NumRows = dataFrame.count()

     _NumRows

  }

  /**
   * num cols Info
   */
  private var NumCols: Integer = _
  def getNumCols: Integer =  NumCols

  //var Data_isRead: java.lang.Boolean = false

  //Alias
  private var AliasDF: String = ""
  def alias: String =  AliasDF
  //val TempDir: String =

  private val DQ_Result: ArrayBuffer[HuemulDqRecord] = new ArrayBuffer[HuemulDqRecord]()
  def getDqResult: ArrayBuffer[HuemulDqRecord] =  DQ_Result

  private def local_setDataFrame(DF: DataFrame, Alias: String, SaveInTemp: Boolean ) {
    _NumRows = -1
    DataDF = DF
    DataSchema = DF.schema
    DataDF.createOrReplaceTempView(Alias)
    AliasDF = Alias

    //Set as Readed
    //NumRows = DataDF.count() //este parámetro se setea automáticamente al intentar leer getNumRows
    NumCols = DataDF.columns.length
    //Data_isRead = true
    //TODO: ver como poner la fecha de término de lectura StopRead_dt = Calendar.getInstance()


    if (SaveInTemp)
      huemulBigDataGov.CreateTempTable(DataDF, AliasDF, huemulBigDataGov.debugMode, null)
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
    if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sql)
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava
    //Cambio en v1.3: optimiza tiempo al aplicar repartition para archivos pequeños
    val DFTemp = if (NumPartitions == null || NumPartitions <= 0) huemulBigDataGov.spark.sql(sql)
                 else huemulBigDataGov.spark.sql(sql).repartition(NumPartitions)


    local_setDataFrame(DFTemp, Alias, SaveInTemp)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava

    huemulBigDataGov.dfSaveLineage(Alias
                                 , sql
                                 , dt_start
                                 , dt_end
                                 , control
                                 , null //FinalTable
                                 , isQuery = true //isQuery
                                 , isReferenced = false //isReferenced
                                 )

  }

  /**
   Create DF from SQL (equivalent to spark.sql method)
   */
  def _createFinalQuery(Alias: String, sql: String, SaveInTemp: Boolean = true, NumPartitions: Integer = null, finalTable: HuemulTable, storageLevelOfDF: org.apache.spark.storage.StorageLevel) {
    //WARNING: ANY CHANGE ON DF_from_SQL MUST BE REPLICATE IN THIS METHOD

    if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sql)
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava
    //Cambio en v1.3: optimiza tiempo al aplicar repartition para archivos pequeños
    val DFTemp = if (NumPartitions == null || NumPartitions <= 0) huemulBigDataGov.spark.sql(sql)
                 else huemulBigDataGov.spark.sql(sql).repartition(NumPartitions)

    if (storageLevelOfDF != null) {
      huemulBigDataGov.logMessageDebug(s"DF to $storageLevelOfDF")
      DFTemp.persist(storageLevelOfDF)
    }

    local_setDataFrame(DFTemp, Alias, SaveInTemp)
    val dt_end = huemulBigDataGov.getCurrentDateTimeJava

    huemulBigDataGov.dfSaveLineage(Alias
                                 , sql
                                 , dt_start
                                 , dt_end
                                 , control
                                 , finalTable
                                 , isQuery = false //isQuery
                                 , isReferenced = false //isReferenced
                                 )

  }


  /**
   * RAW_to_DF: Create DF from RDD, save at Data.DataDF
   */
  def dfFromRAW(rowRDD: RDD[Row], Alias: String) {
    //Create DataFrame
    _NumRows = -1
    if (huemulBigDataGov.debugMode) { println(rowRDD.take(2).foreach { x => println(x) }) }
    val DF = huemulBigDataGov.spark.createDataFrame(rowRDD, DataSchema)

    //Assign DataFrame to LocalDataFrame
    local_setDataFrame(DF, Alias, huemulBigDataGov.debugMode)

    //Unpersisnt unused data
    rowRDD.unpersist(false)

    //Show sample data and structure
    if (huemulBigDataGov.debugMode) {
      DataDF.printSchema()
      DataDF.show()
    }

  }

  /**
   * DQ_NumRowsInterval: Test DQ for num Rows in DF
   */
  def DQ_NumRowsInterval(objectData: Object, NumMin: Long, NumMax: Long, DQ_ExternalCode:String = null): HuemulDataQualityResult = {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava
    val DQResult = new HuemulDataQualityResult()
    //var Rows = NumRows
    if (getNumRows >= NumMin && getNumRows <= NumMax ) {
      DQResult.isError = false
    } else if (getNumRows < NumMin) {
      DQResult.isError = true
      DQResult.description = s"huemul_DataFrame Error: Rows($getNumRows) < MinDef($NumMin)"
      DQResult.errorCode = 2001
    } else if (getNumRows > NumMax) {
      DQResult.isError = true
      DQResult.description = s"huemul_DataFrame Error: Rows($getNumRows) > MaxDef($NumMax)"
      DQResult.errorCode = 2002
    }

    var TableName: String = null
    var BBDDName: String = null
    objectData match {
      case data: HuemulTable =>
        TableName = data.tableName
        BBDDName = data.getCurrentDataBase
      case _ =>
    }

    val dt_end = huemulBigDataGov.getCurrentDateTimeJava
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)

    val Values = new HuemulDqRecord(huemulBigDataGov)
    Values.tableName =TableName
    Values.bbddName =BBDDName
    Values.dfAlias =AliasDF
    Values.columnName =null
    Values.dqName ="DQ_NumRowsInterval"
    Values.dqDescription =s"num rows between $NumMin and $NumMax"
    Values.dqQueryLevel = HuemulTypeDqQueryLevel.Aggregate
    Values.dqNotification = HuemulTypeDqNotification.ERROR
    Values.dqSqlFormula =""
    Values.dqToleranceError_Rows =0L
    Values.dqToleranceError_Percent =Decimal.apply(0)
    Values.dqResultDq =DQResult.description
    Values.dqErrorCode = DQResult.errorCode
    Values.dqExternalCode = DQ_ExternalCode
    Values.dqNumRowsOk =0
    Values.dqNumRowsError =0
    Values.dqNumRowsTotal =getNumRows
    Values.dqIsError = DQResult.isError
    Values.dqIsWarning = DQResult.isWarning
    Values.dqDurationHour = duration.hour.toInt
    Values.dqDurationMinute = duration.minute.toInt
    Values.dqDurationSecond = duration.second.toInt

    this.DqRegister(Values)


     DQResult
  }

  /****
   * DQ_NumRowsInterval: Test DQ for num Rows in DF
   */
  def DQ_NumRows(objectData: Object, NumRowsExpected: Long, DQ_ExternalCode:String = null): HuemulDataQualityResult = {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava
    val DQResult = new HuemulDataQualityResult()
    //var Rows = NumRows
    if (getNumRows == NumRowsExpected ) {
      DQResult.isError = false
    } else if (getNumRows < NumRowsExpected) {
      DQResult.isError = true
      DQResult.description = s"huemul_DataFrame Error: Rows($getNumRows) < NumExpected($NumRowsExpected)"
      DQResult.errorCode = 2003
    } else if (getNumRows > NumRowsExpected) {
      DQResult.isError = true
      DQResult.description = s"huemul_DataFrame Error: Rows($getNumRows) > NumExpected($NumRowsExpected)"
      DQResult.errorCode = 2004
    }


    var TableName: String = null
    var BBDDName: String = null
    objectData match {
      case data: HuemulTable =>
        TableName = data.tableName
        BBDDName = data.getCurrentDataBase
      case _ =>
    }

    val dt_end = huemulBigDataGov.getCurrentDateTimeJava
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)

    val Values = new HuemulDqRecord(huemulBigDataGov)
    Values.tableName =TableName
    Values.bbddName =BBDDName
    Values.dfAlias =AliasDF
    Values.columnName =null
    Values.dqName ="DQ_NumRows"
    Values.dqDescription =s"num rows = $NumRowsExpected "
    Values.dqQueryLevel = HuemulTypeDqQueryLevel.Aggregate
    Values.dqNotification = HuemulTypeDqNotification.ERROR
    Values.dqSqlFormula =""
    Values.dqToleranceError_Rows = 0L
    Values.dqToleranceError_Percent =Decimal.apply(0)
    Values.dqResultDq =DQResult.description
    Values.dqErrorCode = DQResult.errorCode
    Values.dqExternalCode = DQ_ExternalCode
    Values.dqNumRowsOk =0
    Values.dqNumRowsError =0
    Values.dqNumRowsTotal =getNumRows
    Values.dqIsError = DQResult.isError
    Values.dqIsWarning = DQResult.isWarning
    Values.dqDurationHour = duration.hour.toInt
    Values.dqDurationMinute = duration.minute.toInt
    Values.dqDurationSecond = duration.second.toInt

    this.DqRegister(Values)



     DQResult
  }

  /**
   * Compare actual DF with other DF (only columns exist in Actual DF and other DF)
   */
  def DQ_CompareDF(objectData: Object, DF_to_Compare: DataFrame, PKFields: String, DQ_ExternalCode:String = null) : HuemulDataQualityResult = {
    if (!huemulBigDataGov.hasName(PKFields)) {
      sys.error("HuemulError: PKFields must be set ")
    }
    DF_to_Compare.createOrReplaceTempView("__Compare")
    val DQResult = new HuemulDataQualityResult()
    DQResult.isError = false

    var NumColumns: Long = 0
    var NumColumnsError: Long = 0
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava

    try {
      //Genera Select
      var SQL_Compare: String = ""
      var SQL_Resumen: String = "SELECT count(1) as total "
      this.DataSchema.foreach { x =>
        if (DF_to_Compare.schema.count { y => y.name.toUpperCase() == x.name.toUpperCase() } == 1) {
          SQL_Compare = SQL_Compare.concat(s",CAST(CASE WHEN DF.${x.name} = __Compare.${x.name} or (DF.${x.name} IS NULL AND __Compare.${x.name} IS NULL ) THEN 1 ELSE 0 END as Integer) as Equal_${x.name} \n  ")
          SQL_Resumen = SQL_Resumen.concat(s",CAST(SUM(Equal_${x.name}) AS BigInt) AS Total_Equal_${x.name} \n")
        }

      }

      //Genera Where
      var SQLWhere: String = ""
      var SQLPK: String = ""
      PKFields.split(",").foreach { x =>
        SQLPK = s"$SQLPK,coalesce(DF.$x,__Compare.$x) as $x \n"
        SQLWhere = s" ${if (SQLWhere == "") "" else " and "} DF.$x = __Compare.$x \n"

      }

      SQL_Compare = s"SELECT 'compare' as Operation\n $SQLPK $SQL_Compare "

      //Query Final
      SQL_Compare = s""" $SQL_Compare \n FROM ${this.AliasDF} DF FULL JOIN __Compare ON $SQLWhere """
      DQResult.dqDF = huemulBigDataGov.dfExecuteQuery("__DF_CompareResult", SQL_Compare)

      if (huemulBigDataGov.debugMode) DQResult.dqDF.show()


      //Query Resume
      SQL_Resumen = s"$SQL_Resumen FROM __DF_CompareResult "
      val DF_FinalResult = huemulBigDataGov.dfExecuteQuery("__DF_CompareResultRes",SQL_Resumen)
      if (huemulBigDataGov.debugMode) DF_FinalResult.show()


      val  DF_FinalResultFirst = DF_FinalResult.first()
      val totalCount = DF_FinalResultFirst.getAs[Long]("total")
      this.DataSchema.foreach { x =>
        if (DF_to_Compare.schema.count { y => y.name.toUpperCase() == x.name.toUpperCase() } == 1) {

          val currentColumn = DF_FinalResultFirst.getAs[Long](s"Total_Equal_${x.name}")
          if (currentColumn != totalCount) {
            DQResult.description = s"huemul_DataFrame Error: Column ${x.name} have different values: Total rows $totalCount, num OK : $currentColumn "
            DQResult.isError = true
            DQResult.errorCode = 2005
            huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: ${DQResult.description}")
            DQResult.dqDF.where(s"Equal_${x.name} = 0").show()
            NumColumnsError += 1
          }

          NumColumns += 1
        }


      }
    } catch {
      case e: Exception =>
        DQResult.GetError(e,huemulBigDataGov.debugMode)

    }



    var TableName: String = null
    var BBDDName: String = null
    objectData match {
      case data: HuemulTable =>
        TableName = data.tableName
        BBDDName = data.getCurrentDataBase
      case _ =>
    }

    val dt_end = huemulBigDataGov.getCurrentDateTimeJava
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)

    val Values = new HuemulDqRecord(huemulBigDataGov)
    Values.tableName =TableName
    Values.bbddName =BBDDName
    Values.dfAlias =AliasDF
    Values.columnName =null
    Values.dqName ="COMPARE"
    Values.dqDescription ="COMPARE TWO DATAFRAMES"
    Values.dqQueryLevel = HuemulTypeDqQueryLevel.Aggregate
    Values.dqNotification = HuemulTypeDqNotification.ERROR
    Values.dqSqlFormula =""
    Values.dqToleranceError_Rows =0L
    Values.dqToleranceError_Percent =Decimal.apply(0)
    Values.dqResultDq =DQResult.description
    Values.dqErrorCode = DQResult.errorCode
    Values.dqExternalCode = DQ_ExternalCode
    Values.dqNumRowsOk =NumColumns - NumColumnsError
    Values.dqNumRowsError =NumColumnsError
    Values.dqNumRowsTotal =NumColumns
    Values.dqIsError = DQResult.isError
    Values.dqIsWarning = DQResult.isWarning
    Values.dqDurationHour = duration.hour.toInt
    Values.dqDurationMinute = duration.minute.toInt
    Values.dqDurationSecond = duration.second.toInt

    this.DqRegister(Values)




     DQResult
  }


  def DQ_StatsAllCols() : HuemulDataQualityResult = {
    val DQResult = new HuemulDataQualityResult()
    DQResult.isError = false

    this.DataSchema.foreach { x =>
      val Res = DQ_StatsByCol(x.name)

      if (Res.isError) {
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: ERROR DQ")
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: ${Res.description}")
      }

      if (DQResult.dqDF != null)
        DQResult.dqDF =  Res.dqDF.union(DQResult.dqDF)
      else
        DQResult.dqDF =  Res.dqDF
    }

    if (huemulBigDataGov.debugMode) DQResult.dqDF.show()

     DQResult
  }

  /**
   * DQ_StatsByCol: Get Stats by Column
   * Max, Min, avg, sum, count(distinct), count, max(length), min(length)
   */
  def DQ_StatsByCol(Col: String) : HuemulDataQualityResult = {
    val DQResult = new HuemulDataQualityResult()
    DQResult.isError = false

    //Get DataType
    val Colfield = this.DataSchema.fields.filter { x => x.name.toUpperCase() == Col.toUpperCase()  }
    var DataType: DataType  = null
    if (Colfield != null) {
      DataType = Colfield(0).dataType
    }
    if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"HuemulDataFrameLog: $DataType")
    val SQL : String = s""" SELECT "$Col"            as ColName
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
    if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(SQL)
    try {
      DQResult.dqDF = huemulBigDataGov.spark.sql(SQL)

      DQResult.dqDF.show()
      huemulBigDataGov.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_StatsByCol_$Col",huemulBigDataGov.debugMode, numPartitionsForTempFiles)

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
      case e: Exception =>
        DQResult.GetError(e,huemulBigDataGov.debugMode)

    }


     DQResult
  }

  /**
   * DQ_StatsByFunction: Returns all columns using "function" aggregate function
   * function: sum, max, min, count, count_distinct, max_length, min_length,
   */
  def DQ_StatsByFunction(function: String) : HuemulDataQualityResult = {
    val DQResult = new HuemulDataQualityResult()
    DQResult.isError = false
    var SQL : String = s""" SELECT "$function" AS __function__ """

    if (function.toUpperCase() == "count_distinct".toUpperCase() || function.toUpperCase() == "max_length".toUpperCase() || function.toUpperCase() == "min_length".toUpperCase() )
      DataSchema.fields.foreach { x => SQL = SQL + " ,Cast(" + function.replace("_", "(") + "(" + x.name + ")) as Long)   as " + x.name + " "   }
    else
      DataSchema.fields.foreach { x => SQL = SQL + " ," + function + "(" + x.name + ")   as " + x.name + " "   }

    SQL = SQL + " FROM " + AliasDF

    if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(SQL)
    try {
      DQResult.dqDF = huemulBigDataGov.spark.sql(SQL)

      if (huemulBigDataGov.debugMode) {
        DQResult.dqDF.printSchema()
      }
      DQResult.dqDF.show()
      huemulBigDataGov.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_StatsByFunction_$function",huemulBigDataGov.debugMode, numPartitionsForTempFiles)

    } catch {
      case e: Exception =>
        DQResult.GetError(e,huemulBigDataGov.debugMode)

    }


    DQResult
  }

   /**
   * DQ_DuplicateValues: validate duplicate rows for ColDuplicate
   *
   */
  def DQ_DuplicateValues2(objectData: Object, ColDuplicate: String, colMaxMin: String, TempFileName: String = null, DQ_ExternalCode:String = null) : HuemulDataQualityResult = {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava
    var colMaxMin_local = colMaxMin
    if (colMaxMin_local == null)
      colMaxMin_local = "0"
    val DQResult = new HuemulDataQualityResult()
    val talias = AliasDF
    DQResult.isError = false
    val SQL : String = s""" SELECT "$ColDuplicate"            as ColName
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
    if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(SQL)
    try {
      DQResult.dqDF = huemulBigDataGov.spark.sql(SQL)
      if (huemulBigDataGov.debugMode) {

        DQResult.dqDF.printSchema()
        DQResult.dqDF.show()

        var TempFileName_local = TempFileName
        if (TempFileName_local == null)
          TempFileName_local = ColDuplicate
        huemulBigDataGov.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_DupliVal_$TempFileName_local",huemulBigDataGov.debugMode, numPartitionsForTempFiles)
      }

      //duplicate rows found
      DQDup = DQResult.dqDF.count()
      if (DQDup > 0) {
        DQResult.description = s"huemul_DataFrame Error: num Rows Duplicate in $ColDuplicate: " + DQDup.toString
        DQResult.isError = true
        DQResult.errorCode = 2006
        huemulBigDataGov.logMessageWarn(DQResult.description)
        DQResult.dqDF.show()
      }
    } catch {
      case e: Exception =>
        DQResult.GetError(e,huemulBigDataGov.debugMode)

    }

    var TableName: String = null
    var BBDDName: String = null
    objectData match {
      case data: HuemulTable =>
        TableName = data.tableName
        BBDDName = data.getCurrentDataBase
      case _ =>
    }

    val dt_end = huemulBigDataGov.getCurrentDateTimeJava
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)

    val Values = new HuemulDqRecord(huemulBigDataGov)
    Values.tableName =TableName
    Values.bbddName =BBDDName
    Values.dfAlias =AliasDF
    Values.columnName = if (TempFileName == "PK") null else ColDuplicate
    Values.dqName = if (TempFileName == "PK") "PK" else "UNIQUE"
    Values.dqDescription =s"UNIQUE VALUES FOR FIELD $ColDuplicate"
    Values.dqQueryLevel = HuemulTypeDqQueryLevel.Aggregate
    Values.dqNotification = HuemulTypeDqNotification.ERROR
    Values.dqSqlFormula =""
    Values.dqToleranceError_Rows =0L
    Values.dqToleranceError_Percent =Decimal.apply(0)
    Values.dqResultDq =DQResult.description
    Values.dqErrorCode = DQResult.errorCode
    Values.dqExternalCode = DQ_ExternalCode
    Values.dqNumRowsOk =0
    Values.dqNumRowsError =DQDup
    Values.dqNumRowsTotal =getNumRows
    Values.dqIsError = DQResult.isError
    Values.dqIsWarning = DQResult.isWarning
    Values.dqDurationHour = duration.hour.toInt
    Values.dqDurationMinute = duration.minute.toInt
    Values.dqDurationSecond = duration.second.toInt

    this.DqRegister(Values)

    DQResult
  }

   /**
   * DQ_NotNullValues: validate nulls rows
   *
   */
  def DQ_NotNullValues(objectData: Object, Col: String, DQ_ExternalCode:String = null) : HuemulDataQualityResult = {
    val dt_start = huemulBigDataGov.getCurrentDateTimeJava
    val DQResult = new HuemulDataQualityResult()
    val talias = AliasDF
    DQResult.isError = false
    val SQL : String = s""" SELECT *
                            FROM $talias
                            WHERE $Col is null
                        """

    var DQDup: Long = 0
    if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(SQL)
    try {
      DQResult.dqDF = huemulBigDataGov.spark.sql(SQL)
      if (huemulBigDataGov.debugMode) {

        DQResult.dqDF.printSchema()
        DQResult.dqDF.show()

      }
      huemulBigDataGov.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_NotNullValues_$Col",huemulBigDataGov.debugMode, numPartitionsForTempFiles)

      //null rows found
      DQDup = DQResult.dqDF.count()
      if (DQDup > 0) {
        DQResult.description = s"huemul_DataFrame Error: num Rows Null in $Col: " + DQDup.toString
        DQResult.isError = true
        DQResult.errorCode = 2007
        huemulBigDataGov.logMessageWarn(s"HuemulDataFrameLog: ${DQResult.description}")
        DQResult.dqDF.show()
      }
    } catch {
      case e: Exception =>
        DQResult.GetError(e,huemulBigDataGov.debugMode)

    }


    var TableName: String = null
    var BBDDName: String = null
    objectData match {
      case data: HuemulTable =>
        TableName = data.tableName
        BBDDName = data.getCurrentDataBase
      case _ =>
    }

    val dt_end = huemulBigDataGov.getCurrentDateTimeJava
    val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)

    val Values = new HuemulDqRecord(huemulBigDataGov)
    Values.tableName =TableName
    Values.bbddName =BBDDName
    Values.dfAlias =AliasDF
    Values.columnName =Col
    Values.dqName ="NOT NULL"
    Values.dqDescription =s"NOT NULL VALUES FOR FIELD $Col"
    Values.dqQueryLevel = HuemulTypeDqQueryLevel.Row
    Values.dqNotification = HuemulTypeDqNotification.ERROR
    Values.dqSqlFormula =""
    Values.dqToleranceError_Rows =0L
    Values.dqToleranceError_Percent =Decimal.apply(0)
    Values.dqResultDq =DQResult.description
    Values.dqErrorCode = DQResult.errorCode
    Values.dqExternalCode = DQ_ExternalCode
    Values.dqNumRowsOk =0
    Values.dqNumRowsError =DQDup
    Values.dqNumRowsTotal =0
    Values.dqIsError = DQResult.isError
    Values.dqIsWarning = DQResult.isWarning
    Values.dqDurationHour = duration.hour.toInt
    Values.dqDurationMinute = duration.minute.toInt
    Values.dqDurationSecond = duration.second.toInt

    this.DqRegister(Values)


    DQResult
  }

  /**
   Create DQ List from DataDef Definition
   */
  private def getDataQualitySentences(OfficialDataQuality: ArrayBuffer[HuemulDataQuality], RulesDQ: ArrayBuffer[HuemulDataQuality]): ArrayBuffer[HuemulDataQuality] = {
    val Result: ArrayBuffer[HuemulDataQuality] = new ArrayBuffer[HuemulDataQuality]()
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

    Result
  }


  def getSQL_DataQualityForRun(OfficialDataQuality: ArrayBuffer[HuemulDataQuality], RulesDQ: ArrayBuffer[HuemulDataQuality], AliasDF: String): String = {
    var SQLResult: String = ""

    getDataQualitySentences(OfficialDataQuality, RulesDQ).foreach { x =>
        SQLResult += s",CAST(${if (x.getQueryLevel == HuemulTypeDqQueryLevel.Aggregate) "" else "SUM"}(CASE WHEN ${x.getSqlFormula} THEN 1 ELSE 0 END) AS LONG) AS ___DQ_${x.getId} \n"
    }


    if (SQLResult != "") {
      SQLResult  = s"SELECT count(1) as ___Total \n $SQLResult FROM $AliasDF"
    }

    SQLResult
  }


  /**
   Run DataQuality defined by user
   */
  def DF_RunDataQuality(ManualRules: ArrayBuffer[HuemulDataQuality]): HuemulDataQualityResult = {
    DF_RunDataQuality(null, ManualRules, null, null)
  }


  /**
   Run DataQuality defined in Master
   */
  def DF_RunDataQuality(ManualRules: ArrayBuffer[HuemulDataQuality], DF_to_Query: String, dMaster: HuemulTable): HuemulDataQualityResult = {
    DF_RunDataQuality(null, ManualRules, DF_to_Query, dMaster)
  }


  /**
   Run DataQuality defined in Master
   */
  def DF_RunDataQuality(OfficialDataQuality: ArrayBuffer[HuemulDataQuality], ManualRules: ArrayBuffer[HuemulDataQuality], DF_to_Query: String, dMaster: HuemulTable): HuemulDataQualityResult = {
    DF_RunDataQuality(OfficialDataQuality, ManualRules, DF_to_Query, dMaster, registerDQOnce = true)
  }

  def DF_RunDataQuality(OfficialDataQuality: ArrayBuffer[HuemulDataQuality], ManualRules: ArrayBuffer[HuemulDataQuality], DF_to_Query: String, dMaster: HuemulTable, registerDQOnce: Boolean ): HuemulDataQualityResult = {
    val AliasToQuery = if (DF_to_Query == null) this.AliasDF else DF_to_Query

    /*****************************************************************/
    /********** S Q L   F O R M U L A   F O R   D Q    ***************/
    /*****************************************************************/

    var NumTotalErrors: Integer = 0
    var NumTotalWarnings: Integer = 0
    var txtTotalErrors: String = ""
    val ErrorLog: HuemulDataQualityResult = new HuemulDataQualityResult()
    var localErrorCode : Integer = null

    //DataQuality AdHoc
    val SQLDQ = getSQL_DataQualityForRun(OfficialDataQuality, ManualRules, AliasToQuery)
    if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) {
      huemulBigDataGov.logMessageDebug(s"HuemulDataFrameLog: DATA QUALITY ADHOC QUERY")
      huemulBigDataGov.logMessageDebug(SQLDQ)
    }
    var DF_ErrorDetails: DataFrame = null

    if (SQLDQ != ""){
      val dt_start = huemulBigDataGov.getCurrentDateTimeJava
      //Execute DQ
      val AliasDQ = s"${AliasToQuery}__DQ_p0"
      ErrorLog.dqDF = huemulBigDataGov.dfExecuteQuery(AliasDQ
                                        , SQLDQ)

      val FirstReg = ErrorLog.dqDF.collect()(0) // .first()
      val DQTotalRows = FirstReg.getAs[Long](s"___Total")

      val dt_end = huemulBigDataGov.getCurrentDateTimeJava
      val duration = huemulBigDataGov.getDateTimeDiff(dt_start, dt_end)
      //import java.util.Calendar;
      //Get DQ Result from DF

      control.newStep(s"Step: DQ Result: Start analyzing the DQ result")

      if (DQTotalRows == 0) {
        control.newStep(s"Step: DQ Result: 0 rows in DF, nothing to evaluate")
        huemulBigDataGov.logMessageWarn("0 rows in DF, nothing to evaluate")
      } else {
        getDataQualitySentences(OfficialDataQuality, ManualRules).foreach { x =>
          x.NumRowsOK = FirstReg.getAs[Long](s"___DQ_${x.getId}")
          x.NumRowsTotal = if (x.getQueryLevel == HuemulTypeDqQueryLevel.Aggregate) 1L else DQTotalRows
          x.ResultDQ = ""

          val DQWithError = x.NumRowsTotal - x.NumRowsOK
          val DQWithErrorPerc = Decimal.apply(DQWithError) / Decimal.apply(x.NumRowsTotal)
          var IsError: Boolean = false

          //Validate max num rows with error vs definition
          if (x.getToleranceErrorRows != null) {
            if (DQWithError > x.getToleranceErrorRows)
              IsError = true
          }

          //Validate % rows with error vs definition
          if (x.getToleranceErrorPercent != null) {
            if (DQWithErrorPerc > x.getToleranceErrorPercent)
              IsError = true
          }

          //set user message
          x.ResultDQ = s"DQ ${if (IsError) s"${x.getNotification} (code:${x.getErrorCode})" else "OK"}, Name: ${x.getMyName} (___DQ_${x.getId}), num OK: ${x.NumRowsOK}, num Total: ${x.NumRowsTotal}, Not meets: $DQWithError(tolerance:${x.getToleranceErrorRows}), Not meets %: ${DQWithErrorPerc * Decimal.apply(100)}%(tolerance:${if (x.getToleranceErrorPercent == null) 0 else x.getToleranceErrorPercent * Decimal.apply(100)}%)"
          if (huemulBigDataGov.debugMode || IsError) huemulBigDataGov.logMessageDebug(x.ResultDQ)

          var dfTableName: String = null
          var dfDataBaseName: String = null
          if (dMaster != null) {
            dfTableName = dMaster.tableName
            dfDataBaseName = dMaster.getCurrentDataBase
          }



          val Values = new HuemulDqRecord(huemulBigDataGov)
          Values.tableName =dfTableName
          Values.bbddName =dfDataBaseName
          Values.dfAlias =AliasToQuery
          Values.columnName =if (x.getFieldName == null) null else x.getFieldName.getMyName(dMaster.getStorageType)
          Values.dqName =x.getMyName
          Values.dqDescription =s"(Id ${x.getId}) ${x.getDescription}"
          Values.dqQueryLevel =x.getQueryLevel // .getDQ_QueryLevel
          Values.dqNotification =x.getNotification
          Values.dqSqlFormula =x.getSqlFormula
          Values.dqToleranceError_Rows =x.getToleranceErrorRows
          Values.dqToleranceError_Percent =x.getToleranceErrorPercent
          Values.dqResultDq =x.ResultDQ
          Values.dqExternalCode = x.getDqExternalCode
          Values.dqErrorCode = if (IsError) x.getErrorCode else null
          Values.dqNumRowsOk =x.NumRowsOK
          Values.dqNumRowsError =DQWithError
          Values.dqNumRowsTotal =x.NumRowsTotal
          Values.dqIsError = x.getNotification == HuemulTypeDqNotification.ERROR && IsError //IsError
          Values.dqIsWarning = (x.getNotification == HuemulTypeDqNotification.WARNING || x.getNotification == HuemulTypeDqNotification.WARNING_EXCLUDE) && IsError
          Values.dqDurationHour = duration.hour.toInt
          Values.dqDurationMinute = duration.minute.toInt
          Values.dqDurationSecond = duration.second.toInt


          ErrorLog.appendDQResult(Values)
          this.DqRegister(Values)

          if (Values.dqIsWarning)
            NumTotalWarnings += 1

          if (Values.dqIsError) {
            txtTotalErrors += s"\nHuemulDataFrameLog: DQ Name (${x.getErrorCode}): ${x.getMyName} with error: ${x.ResultDQ}"
            NumTotalErrors += 1
            localErrorCode = x.getErrorCode
          }

          //Save details to DF with errors
          if (dfTableName != null && huemulBigDataGov.globalSettings.dqSaveErrorDetails && IsError && x.getSaveErrorDetails) {
            //Query to get detail errors
            control.newStep(s"Step: DQ Result: Get detail error for (Id ${x.getId}) ${x.getDescription}) ")
            val SQL_Detail = DqGenQuery(AliasToQuery
                                        ,s"not (${x.getSqlFormula})"
                                        ,!(x.getFieldName == null) //asField
                                        ,if (x.getFieldName == null) "all" else x.getFieldName.getMyName(dMaster.getStorageType) //fieldName
                                        ,Values.dqId
                                        ,x.getNotification
                                        ,x.getErrorCode
                                        ,s"(Id ${x.getId}) ${x.getDescription}"
                                        )

            //Execute query
            //Control.NewStep(s"Step: DQ Result: Get detail error for (Id ${x.getId}) ${x.getDescription}, save to DF) ")
            val DF_EDetail = huemulBigDataGov.dfExecuteQuery("temp_DQ", SQL_Detail)

            if (dMaster != null && !registerDQOnce) {
              //if call is from huemul_Table, save detail to disk
              //Save errors to disk
              if (huemulBigDataGov.globalSettings.dqSaveErrorDetails && DF_EDetail != null && dMaster.getSaveDQResult) {
                control.newStep("Start Save DQ Error Details ")
                if (!dMaster.savePersistDq(control, DF_EDetail)){
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
      huemulBigDataGov.logMessageWarn (s"HuemulDataFrameLog: num with errors: $NumTotalErrors ")
      ErrorLog.isError = true
      ErrorLog.description = txtTotalErrors
      ErrorLog.errorCode = localErrorCode
    }

    ErrorLog.detailErrorsDF = DF_ErrorDetails


    ErrorLog
  }

  def DqGenQuery(fromSQL: String
                 , whereSQL: String
                 , haveField: Boolean
                 , fieldName: String
                 , dq_id: String
                 , dq_error_notification: HuemulTypeDqNotification.HuemulTypeDqNotification
                 , error_code: Integer
                 , dq_error_description: String
                  ): String = {
    s"""SELECT '${control.Control_Id }' as dq_control_id
                                     ,'$dq_id' as dq_dq_id
                                     ,'${if (haveField) fieldName else "all"}' as dq_error_columnname
                                     ,'$dq_error_notification' as dq_error_notification
                                     ,'$error_code' as dq_error_code
                                     ,'$dq_error_description' as dq_error_descripcion
                                     , *
                               FROM  $fromSQL
                               ${if (whereSQL == null || whereSQL == "") "" else s"WHERE $whereSQL" }"""
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
                                 CREATE EXTERNAL TABLE IF NOT EXISTS $fullTableName ($ColumnsCreateTable)
                                 STORED AS PARQUET
                                 LOCATION '$parquetLocation'"""

    if (huemulBigDataGov.debugMode)
      huemulBigDataGov.logMessageDebug(s"Create Table sentence: $lCreateTableScript ")

    lCreateTableScript
  }



  /**
   * Save Result data to disk
   */
  def savePersistToDisk(OverriteIfExist: Boolean, tableNameInHive: String, localPath: String, globalPath: ArrayBuffer[HuemulKeyValuePath] = huemulBigDataGov.globalSettings.sandboxBigFilesPath, databaseName: ArrayBuffer[HuemulKeyValuePath] = huemulBigDataGov.globalSettings.sandboxDataBase ): Boolean = {
    var Result: Boolean = true
    val tempPath = huemulBigDataGov.globalSettings.getPathForSaveTableWithoutDG(huemulBigDataGov, globalPath, localPath, tableNameInHive)

    try {
      control.newStep("Saving DF to Disk")
      if (huemulBigDataGov.debugMode) huemulBigDataGov.logMessageDebug(s"saving path: $tempPath ")
      if (OverriteIfExist)
        this.DataDF.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
      else
        this.DataDF.write.mode(SaveMode.Append).format("parquet").save(tempPath)

    } catch {
      case e: Exception =>
        Result = false
        control.controlError.setError(e, getClass.getSimpleName, 2008)
    }

    if (Result) {
      //if (CreateInHive ) {
        val ddbbName: String = huemulBigDataGov.getDataBase(databaseName)
        val tabName: String = s"$ddbbName.$tableNameInHive"
        val sqlDrop01 = s"drop table if exists $tabName"
        control.newStep("Save: Drop Hive table Def")
        if (huemulBigDataGov.debugMode && !huemulBigDataGov.HideLibQuery) huemulBigDataGov.logMessageDebug(sqlDrop01)
        try {
          val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(ddbbName).collect()
          if (TablesListFromHive.exists { x => x.name.toUpperCase() == tableNameInHive.toUpperCase() })
            huemulBigDataGov.spark.sql(sqlDrop01)

        } catch {
          case t: Throwable => huemulBigDataGov.logMessageError(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
        }

      //}

      try {
        //create table
        //if (CreateInHive ) {
          control.newStep("Save: Create Table in Hive Metadata")
          val lscript = DF_CreateTable_Script(tempPath, tabName)
          huemulBigDataGov.spark.sql(lscript)
        //}

        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        //Control.NewStep("Save: Repair Hive Metadata")
        //if (huemulBigDataGov.DebugMode) huemulBigDataGov.logMessageDebug(s"MSCK REPAIR TABLE ${tabName}")
        //huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${tabName}")

        if (huemulBigDataGov.impalaEnabled) {
          control.newStep("Save: refresh Impala Metadata")
          huemulBigDataGov.impalaConnection.executeJdbcNoResultSet(s"invalidate metadata $tabName")
          huemulBigDataGov.impalaConnection.executeJdbcNoResultSet(s"refresh $tabName")
        }
      } catch {
        case e: Exception =>
          Result = false
          control.controlError.setError(e, getClass.getSimpleName,2009)
      }
    }

    Result

  }

  def DqRegister(DQ: HuemulDqRecord) {
    DQ_Result.append(DQ)
    control.RegisterDQuality(DQ.tableName
        , DQ.bbddName
        , DQ.dfAlias
        , DQ.columnName
        , DQ.dqId
        , DQ.dqName
        , DQ.dqDescription
        , DQ.dqQueryLevel
        , DQ.dqNotification// _RaiseError
        , DQ.dqSqlFormula
        , DQ.dqToleranceError_Rows
        , DQ.dqToleranceError_Percent
        , DQ.dqResultDq
        , DQ.dqErrorCode
        , DQ.dqExternalCode
        , DQ.dqNumRowsOk
        , DQ.dqNumRowsError
        , DQ.dqNumRowsTotal
        , DQ.dqIsError
        , DQ.dqIsWarning
        , DQ.dqDurationHour
        , DQ.dqDurationMinute
        , DQ.dqDurationSecond)
  }
  
}