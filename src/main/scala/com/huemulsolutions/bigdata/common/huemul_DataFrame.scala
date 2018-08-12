package com.huemulsolutions.bigdata.common

import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.sql.types._
import scala.collection.mutable._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.tables._
import com.huemulsolutions.bigdata.control._


/**
 * Def_Fabric_DataInfo: Define method to improve DQ over DF
 */
class huemul_DataFrame(huemulLib: huemul_Library, Control: huemul_Control) extends Serializable {
  
  if (Control == null) 
    sys.error("Control is null in huemul_DataFrame")
    
  
  if (huemulLib == null) 
    sys.error("huemulLib is null in huemul_DataFrame")
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
   * N° Rows info
   */
  private var NumRows: Long = -1
  def getNumRows(): Long = {return NumRows}
  
  /**
   * N° cols Info
   */
  private var NumCols: Integer = null
  def getNumCols(): Integer = {return NumCols}
  
  //var Data_isRead: java.lang.Boolean = false
  
  //Alias
  private var AliasDF: String = ""
  def Alias: String = {return AliasDF}
  //val TempDir: String = 
  
  private var DQ_List: ArrayBuffer[huemul_DQRecord] = new ArrayBuffer[huemul_DQRecord]()
  def getDQ(): ArrayBuffer[huemul_DQRecord] = {return DQ_List}
  
  def setDataFrame(DF: DataFrame, Alias: String, SaveInTemp: Boolean = true) {
    DataDF = DF
    DataSchema = DF.schema
    DataDF.createOrReplaceTempView(Alias)
    AliasDF = Alias
    
    //Set as Readed
    NumRows = DataDF.count()
    NumCols = DataDF.columns.length
    //Data_isRead = true
    //TODO: ver como poner la fecha de término de lectura StopRead_dt = Calendar.getInstance()
        
    if (SaveInTemp)
      huemulLib.CreateTempTable(DataDF, AliasDF, huemulLib.DebugMode)
  }
  
  /**   
   Create DF from SQL (equivalent to spark.sql method)
   */
  def DF_from_SQL(Alias: String, sql: String, SaveInTemp: Boolean = true) {
    if (huemulLib.DebugMode && !huemulLib.HideLibQuery) println(sql)
    val DFTemp = huemulLib.spark.sql(sql)
    
    setDataFrame(DFTemp, Alias, SaveInTemp)
        
  }
    
  /**
   * RAW_to_DF: Create DF from RDD, save at Data.DataDF
   */
  def DF_from_RAW(rowRDD: RDD[Row], Alias: String) {
    //Create DataFrame 
    if (huemulLib.DebugMode) { println(rowRDD.take(2).foreach { x => println(x) }) }
    val DF = huemulLib.spark.createDataFrame(rowRDD, DataSchema)
    
    //Assign DataFrame to LocalDataFrame
    setDataFrame(DF, Alias)
    
    //Unpersisnt unused data
    rowRDD.unpersist(false)
    
    //Show sample data and structure
    if (huemulLib.DebugMode) { 
      DataDF.printSchema()
      DataDF.show()
    }
    
  }
  
  /**
   * DQ_NumRowsInterval: Test DQ for N° Rows in DF
   */
  def DQ_NumRowsInterval(ObjectData: Object, NumMin: Long, NumMax: Long): huemul_DataQualityResult = {

    val DQResult = new huemul_DataQualityResult()
    //var Rows = NumRows
    if (NumRows >= NumMin && NumRows <= NumMax ) {
      DQResult.isError = false
    } else if (NumRows < NumMin) {
      DQResult.isError = true
      DQResult.Description = s"Rows($NumRows) < MinDef($NumMin)"
    } else if (NumRows > NumMax) {
      DQResult.isError = true
      DQResult.Description = s"Rows($NumRows) > MaxDef($NumMax)"
    }
    
    if (Control != null) {
      var TableName: String = null
      var BBDDName: String = null
      if (ObjectData.isInstanceOf[huemul_Table]) {
        val Data = ObjectData.asInstanceOf[huemul_Table]
        TableName = Data.TableName
        BBDDName = Data.GetDataBase(Data.DataBase)
      }
      
      val Values = new huemul_DQRecord()
      Values.Table_Name =TableName
      Values.BBDD_Name =BBDDName
      Values.DF_Alias =AliasDF
      Values.ColumnName =null
      Values.DQ_Name ="DQ_NumRowsInterval"
      Values.DQ_Description =s"N° rows between ${NumMin} and ${NumMax}"
      Values.DQ_IsAggregate =true
      Values.DQ_RaiseError =true
      Values.DQ_SQLFormula =""
      Values.DQ_Error_MaxNumRows =0
      Values.DQ_Error_MaxPercent =Decimal.apply(0)
      Values.DQ_ResultDQ =DQResult.Description
      Values.DQ_NumRowsOK =0
      Values.DQ_NumRowsError =0
      Values.DQ_NumRowsTotal =NumRows
      
      this.DQ_Register(Values)    
    }
    
    return DQResult
  }
  
  /****
   * DQ_NumRowsInterval: Test DQ for N° Rows in DF
   */
  def DQ_NumRows(ObjectData: Object, NumRowsExpected: Long): huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    //var Rows = NumRows
    if (NumRows == NumRowsExpected ) {
      DQResult.isError = false
    } else if (NumRows < NumRowsExpected) {
      DQResult.isError = true
      DQResult.Description = s"Rows($NumRows) < NumExpected($NumRowsExpected)"
    } else if (NumRows > NumRowsExpected) {
      DQResult.isError = true
      DQResult.Description = s"Rows($NumRows) > NumExpected($NumRowsExpected)"
    }
    
    if (Control != null) {
      var TableName: String = null
      var BBDDName: String = null
      if (ObjectData.isInstanceOf[huemul_Table]) {
        val Data = ObjectData.asInstanceOf[huemul_Table]
        TableName = Data.TableName
        BBDDName = Data.GetDataBase(Data.DataBase)
      }
      
      val Values = new huemul_DQRecord()
      Values.Table_Name =TableName
      Values.BBDD_Name =BBDDName
      Values.DF_Alias =AliasDF
      Values.ColumnName =null
      Values.DQ_Name ="DQ_NumRows"
      Values.DQ_Description =s"N° rows = ${NumRowsExpected} "
      Values.DQ_IsAggregate =true
      Values.DQ_RaiseError =true
      Values.DQ_SQLFormula =""
      Values.DQ_Error_MaxNumRows = 0
      Values.DQ_Error_MaxPercent =Decimal.apply(0)
      Values.DQ_ResultDQ =DQResult.Description
      Values.DQ_NumRowsOK =0
      Values.DQ_NumRowsError =0
      Values.DQ_NumRowsTotal =NumRows
        
      this.DQ_Register(Values)
      
    }    
    
    return DQResult
  }
  
  /**
   * Compare actual DF with other DF (only columns exist in Actual DF and other DF)
   */
  def DQ_CompareDF(ObjectData: Object, DF_to_Compare: DataFrame, PKFields: String) : huemul_DataQualityResult = {
    if (!huemulLib.HasName(PKFields)) {
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
      if (huemulLib.DebugMode) println(SQL_Compare)
      DQResult.dqDF = huemulLib.spark.sql(SQL_Compare)
      DQResult.dqDF.createOrReplaceTempView("__DF_CompareResult")
      
      if (huemulLib.DebugMode) DQResult.dqDF.show()
      
      
      //Query Resume
      SQL_Resumen = s"${SQL_Resumen} FROM __DF_CompareResult "
      if (huemulLib.DebugMode) println(SQL_Resumen)
      val DF_FinalResult = huemulLib.spark.sql(SQL_Resumen)
      if (huemulLib.DebugMode) DF_FinalResult.show()
      
      
      val  DF_FinalResultFirst = DF_FinalResult.first()
      val totalCount = DF_FinalResultFirst.getAs[Long]("total")
      this.DataSchema.foreach { x =>
        if (DF_to_Compare.schema.filter { y => y.name.toUpperCase() == x.name.toUpperCase() }.length == 1) {
       
          val currentColumn = DF_FinalResultFirst.getAs[Long](s"Total_Equal_${x.name}")
          if (currentColumn != totalCount) {
            DQResult.Description = s"Column ${x.name} have different values: Total rows ${totalCount}, N° OK : ${currentColumn} "
            DQResult.isError = true
            println(DQResult.Description)
            DQResult.dqDF.filter(s"Equal_${x.name} = 0").show()
            NumColumnsError += 1
          }
          
          NumColumns += 1
        }
        
        
      }
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulLib.DebugMode)
      }
    }
    
    
    if (Control != null) {
      var TableName: String = null
      var BBDDName: String = null
      if (ObjectData.isInstanceOf[huemul_Table]) {
        val Data = ObjectData.asInstanceOf[huemul_Table]
        TableName = Data.TableName
        BBDDName = Data.GetDataBase(Data.DataBase)
      }
      
      val Values = new huemul_DQRecord()
      Values.Table_Name =TableName
      Values.BBDD_Name =BBDDName
      Values.DF_Alias =AliasDF
      Values.ColumnName =null
      Values.DQ_Name ="COMPARE"
      Values.DQ_Description ="COMPARE TWO DATAFRAMES"
      Values.DQ_IsAggregate =true
      Values.DQ_RaiseError =true
      Values.DQ_SQLFormula =""
      Values.DQ_Error_MaxNumRows =0
      Values.DQ_Error_MaxPercent =Decimal.apply(0)
      Values.DQ_ResultDQ =DQResult.Description
      Values.DQ_NumRowsOK =NumColumns - NumColumnsError
      Values.DQ_NumRowsError =NumColumnsError
      Values.DQ_NumRowsTotal =NumColumns

      this.DQ_Register(Values)
      
    }  
    
    
    return DQResult
  }
  
  
  def DQ_StatsAllCols(ObjectData: Object) : huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    DQResult.isError = false
    
    this.DataSchema.foreach { x =>
      val Res = DQ_StatsByCol(ObjectData, x.name)
      
      if (Res.isError) {
        println("ERROR DQ")
        println(Res.Description)
      }
      
      if (DQResult.dqDF != null)
        DQResult.dqDF =  Res.dqDF.union(DQResult.dqDF)
      else 
        DQResult.dqDF =  Res.dqDF
    }
    
    if (huemulLib.DebugMode) DQResult.dqDF.show()
    
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
    println(DataType)
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
                                  ,${if (DataType == DoubleType || DataType == FloatType || DataType == IntegerType
                                      || DataType == LongType  || DataType == DataTypes.ShortType ) s"cast(sum(CASE WHEN $Col = 0 then 1 else 0 end) as long)" else "cast(-1 as long)" }  as count_cero
                            FROM """ + AliasDF   
    if (huemulLib.DebugMode && !huemulLib.HideLibQuery) println(SQL)
    try {
      DQResult.dqDF = huemulLib.spark.sql(SQL)
            
      if (huemulLib.DebugMode) DQResult.dqDF.show()        
      huemulLib.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_StatsByCol_${Col}",huemulLib.DebugMode)
      
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
        DQResult.GetError(e,huemulLib.DebugMode)
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

    if (huemulLib.DebugMode && !huemulLib.HideLibQuery) println(SQL)
    try {
      DQResult.dqDF = huemulLib.spark.sql(SQL)
      
      if (huemulLib.DebugMode) {
        DQResult.dqDF.printSchema()
        DQResult.dqDF.show()
      }
      huemulLib.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_StatsByFunction_${function}",huemulLib.DebugMode)
      
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulLib.DebugMode)
      }
    }
    
       
    return DQResult
  }
  
   /**
   * DQ_DuplicateValues: validate duplicate rows for ColDuplicate
   *  
   */
  def DQ_DuplicateValues(ObjectData: Object, ColDuplicate: String, colMaxMin: String, TempFileName: String = null) : huemul_DataQualityResult = {
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
    
    if (huemulLib.DebugMode && !huemulLib.HideLibQuery) println(SQL)
    try {
      DQResult.dqDF = huemulLib.spark.sql(SQL) 
      if (huemulLib.DebugMode) {
        
        DQResult.dqDF.printSchema()
        DQResult.dqDF.show()
        
        /*OJO: Cambiar el nombre del método*/
      }
      
      var TempFileName_local = TempFileName
      if (TempFileName_local == null)
        TempFileName_local = ColDuplicate
      huemulLib.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_DupliVal_${TempFileName_local}",huemulLib.DebugMode)
      
      //duplicate rows found
      DQDup = DQResult.dqDF.count()
      if (DQDup > 0) {
        DQResult.Description = s"N° Rows Duplicate in $ColDuplicate: " + DQDup.toString()
        DQResult.isError = true
        println(DQResult.Description)
        DQResult.dqDF.show()
      }
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulLib.DebugMode)
      }
    }
    
    if (Control != null) {
      var TableName: String = null
      var BBDDName: String = null
      if (ObjectData.isInstanceOf[huemul_Table]) {
        val Data = ObjectData.asInstanceOf[huemul_Table]
        TableName = Data.TableName
        BBDDName = Data.GetDataBase(Data.DataBase)
      }
      
      val Values = new huemul_DQRecord()
      Values.Table_Name =TableName
      Values.BBDD_Name =BBDDName
      Values.DF_Alias =AliasDF
      Values.ColumnName =ColDuplicate
      Values.DQ_Name ="UNIQUE"
      Values.DQ_Description =s"UNIQUE VALUES FOR FIELD $ColDuplicate"
      Values.DQ_IsAggregate =true
      Values.DQ_RaiseError =true
      Values.DQ_SQLFormula =""
      Values.DQ_Error_MaxNumRows =0
      Values.DQ_Error_MaxPercent =Decimal.apply(0)
      Values.DQ_ResultDQ =DQResult.Description
      Values.DQ_NumRowsOK =0
      Values.DQ_NumRowsError =DQDup
      Values.DQ_NumRowsTotal =NumRows

      this.DQ_Register(Values)
    }  
    
    return DQResult
  }
  
   /**
   * DQ_DuplicateValues: validate duplicate rows for ColDuplicate
   *  
   */
  def DQ_NotNullValues(ObjectData: Object, Col: String) : huemul_DataQualityResult = {
    val DQResult = new huemul_DataQualityResult()
    val talias = AliasDF
    DQResult.isError = false
    var SQL : String = s""" SELECT *
                            FROM $talias
                            WHERE $Col is null
                        """   

    var DQDup: Long = 0
    if (huemulLib.DebugMode && !huemulLib.HideLibQuery) println(SQL)
    try {
      DQResult.dqDF = huemulLib.spark.sql(SQL) 
      if (huemulLib.DebugMode) {
        
        DQResult.dqDF.printSchema()
        DQResult.dqDF.show()
        
      }
      huemulLib.CreateTempTable(DQResult.dqDF,s"${AliasDF}_DQ_NotNullValues_${Col}",huemulLib.DebugMode)
      
      //null rows found
      DQDup = DQResult.dqDF.count()
      if (DQDup > 0) {
        DQResult.Description = s"N° Rows Null in $Col: " + DQDup.toString()
        DQResult.isError = true
        println(DQResult.Description)
        DQResult.dqDF.show()
      }
    } catch {
      case e: Exception => {
        DQResult.GetError(e,huemulLib.DebugMode)
      }
    }
    
    if (Control != null) {
      var TableName: String = null
      var BBDDName: String = null
      if (ObjectData.isInstanceOf[huemul_Table]) {
        val Data = ObjectData.asInstanceOf[huemul_Table]
        TableName = Data.TableName
        BBDDName = Data.GetDataBase(Data.DataBase)
      }
      
      val Values = new huemul_DQRecord()
      Values.Table_Name =TableName
      Values.BBDD_Name =BBDDName
      Values.DF_Alias =AliasDF
      Values.ColumnName =Col
      Values.DQ_Name ="NOT NULL"
      Values.DQ_Description =s"NOT NULL VALUES FOR FIELD $Col"
      Values.DQ_IsAggregate =false
      Values.DQ_RaiseError =true
      Values.DQ_SQLFormula =""
      Values.DQ_Error_MaxNumRows =0
      Values.DQ_Error_MaxPercent =Decimal.apply(0)
      Values.DQ_ResultDQ =DQResult.Description
      Values.DQ_NumRowsOK =0
      Values.DQ_NumRowsError =DQDup
      Values.DQ_NumRowsTotal =0

      this.DQ_Register(Values)
    }  
            
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
        SQLResult += s",CAST(${if (x.getIsAggregated) "" else "SUM"}(CASE WHEN ${x.getSQLFormula} THEN 1 ELSE 0 END) AS LONG) AS ___DQ_${x.getId} \n"        
    }
    
        
    if (SQLResult != "") {
      SQLResult  = s"SELECT count(1) as ___Total \n ${SQLResult} FROM ${AliasDF}"           
    }
         
    return SQLResult
  }
  
  /**
   Run DataQuality defined in Master
   */
  def DF_RunDataQuality(OfficialDataQuality: ArrayBuffer[huemul_DataQuality], ManualRules: ArrayBuffer[huemul_DataQuality], AliasDF: String, dMaster: huemul_Table): huemul_DataQualityResult = {
    
    /*****************************************************************/
    /********** S Q L   F O R M U L A   F O R   D Q    ***************/
    /*****************************************************************/
    
    var NumTotalErrors: Integer = 0
    var txtTotalErrors: String = ""
    var ErrorLog: huemul_DataQualityResult = new huemul_DataQualityResult()
      
    //DataQuality AdHoc
    val SQLDQ = getSQL_DataQualityForRun(OfficialDataQuality, ManualRules, AliasDF)
    if (huemulLib.DebugMode && !huemulLib.HideLibQuery) {
      println("DATA QUALITY ADHOC QUERY")
      println(SQLDQ)
    }
    
    if (SQLDQ != ""){
      //Execute DQ
      val AliasDQ = s"${AliasDF}__DQ_p0"
      ErrorLog.dqDF = huemulLib.DF_ExecuteQuery(AliasDQ
                                        , SQLDQ)
      
      val FirstReg = ErrorLog.dqDF.first()
      val DQTotalRows = FirstReg.getAs[Long](s"___Total")
      import java.util.Calendar;
      //Get DQ Result from DF
      
      getDataQualitySentences(OfficialDataQuality, ManualRules).foreach { x =>
        x.NumRowsOK = FirstReg.getAs[Long](s"___DQ_${x.getId}")
        x.NumRowsTotal = if (x.getIsAggregated) 1 else DQTotalRows
        
        val DQWithError = x.NumRowsTotal - x.NumRowsOK
        val DQWithErrorPerc = Decimal.apply(DQWithError) / Decimal.apply(x.NumRowsTotal)
        
        
        //Validate max N° rows with error vs definition 
        if (x.Error_MaxNumRows != null) {
           
          if (DQWithError > x.Error_MaxNumRows)
            x.ResultDQ = s"Max Rows with error defined: ${x.Error_MaxNumRows}, Real N° rows with error: ${DQWithError}"
        } 
        
        //Validate % rows with error vs definition
        if (x.Error_Percent != null) {
           
          if (DQWithErrorPerc > x.Error_Percent)
            x.ResultDQ = s"% Rows with error defined: ${x.Error_Percent}, Real N° rows with error: ${DQWithErrorPerc}"
        }
        
        if (huemulLib.DebugMode) println(s"N° DQ Name: ${x.getMyName} (___DQ_${x.getId}), N° OK: ${x.NumRowsOK}, N° Total: ${x.NumRowsTotal}, Error: ${DQWithError}, Error %: ${DQWithErrorPerc * Decimal.apply(100)}, ${x.ResultDQ}")
                       
        var dfTableName: String = null
        var dfDataBaseName: String = null
        if (dMaster != null) {
          dfTableName = dMaster.TableName
          dfDataBaseName = dMaster.GetDataBase(dMaster.DataBase)
        }
        
        val Values = new huemul_DQRecord()
        Values.Table_Name =dfTableName
        Values.BBDD_Name =dfDataBaseName
        Values.DF_Alias =AliasDF
        Values.ColumnName =if (x.getFieldName == null) null else x.getFieldName.get_MyName()
        Values.DQ_Name =x.getMyName()
        Values.DQ_Description =s"(Id ${x.getId}) ${x.getDescription}"
        Values.DQ_IsAggregate =x.getIsAggregated
        Values.DQ_RaiseError =x.getRaiseError
        Values.DQ_SQLFormula =x.getSQLFormula
        Values.DQ_Error_MaxNumRows =x.Error_MaxNumRows
        Values.DQ_Error_MaxPercent =x.Error_Percent
        Values.DQ_ResultDQ =x.ResultDQ
        Values.DQ_NumRowsOK =x.NumRowsOK
        Values.DQ_NumRowsError =DQWithError
        Values.DQ_NumRowsTotal =x.NumRowsTotal
  
        this.DQ_Register(Values)
        
        if (x.getRaiseError == true && x.ResultDQ != null) {
          txtTotalErrors += s"DQ Name: ${x.getMyName} with error: ${x.ResultDQ} \n"
          NumTotalErrors += 1
        }
      }
    }
    
    if (NumTotalErrors > 0){
      ErrorLog.isError = true
      ErrorLog.Description = txtTotalErrors
    }
    
    return ErrorLog
  }
  
  def DQ_Register(DQ: huemul_DQRecord) {
    DQ_List.append(DQ)
    
  }
  
}