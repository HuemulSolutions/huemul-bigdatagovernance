package com.huemulsolutions.bigdata.control

import org.apache.spark.sql._
import java.sql.Types
import java.sql.DriverManager
import java.sql.Connection
import scala.collection.mutable.ArrayBuffer
import java.sql.Types
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import com.huemulsolutions.bigdata.common.huemul_BigDataGovernance
import java.util.Date

class huemul_JDBCResult extends Serializable {
  var ResultSet: Array[Row] = null
  var ErrorDescription: String = ""
  var IsError: Boolean = false
  var fieldsStruct = new Array[StructField](0);
  
  /**
   * open variable for keep any value
   */
  var OpenVar: String = null
  
  /**
   * open variable for keep any value
   */
  var OpenVar2: String = null
  
  def GetValue(columnName: String, row: Row): Any = {
    val rows = fieldsStruct.filter { x => x.name == columnName }
    var Values: Any = null
    
    if (rows.length == 1) {
      val firstRow = rows.array(0)
      
      
      if (firstRow.dataType == DataTypes.BooleanType)
        Values = row.getAs[Boolean](columnName)
      else if (firstRow.dataType == DataTypes.ShortType)
        Values = row.getAs[Short](columnName)
      else if (firstRow.dataType == DataTypes.LongType)
        Values = row.getAs[Long](columnName)
      else if (firstRow.dataType == DataTypes.BinaryType)
        Values = row.getAs[BinaryType](columnName)
      else if (firstRow.dataType == DataTypes.StringType)
        Values = row.getAs[String](columnName)
      else if (firstRow.dataType == DataTypes.NullType)
        Values = row.getAs[NullType](columnName)
      else if (firstRow.dataType == DecimalType || firstRow.dataType.typeName.toLowerCase().contains("decimal"))
        Values = row.getAs[BigDecimal](columnName)
      else if (firstRow.dataType == DataTypes.IntegerType)
        Values = row.getAs[Integer](columnName)
      else if (firstRow.dataType == DataTypes.FloatType)
        Values = row.getAs[Float](columnName)
      else if (firstRow.dataType == DataTypes.DoubleType)
        Values = row.getAs[Double](columnName)
      else if (firstRow.dataType == DataTypes.DateType)
        Values = row.getAs[Date](columnName)
      else if (firstRow.dataType == DataTypes.TimestampType)
        Values = row.getAs[String](columnName)
      else
        Values = row.getAs[String](columnName)
  
    } else {
        Values = null
        sys.error("error: column name not found")
    }
    
    return Values
  }
}

class huemul_JDBCProperties(huemulBigDataGob: huemul_BigDataGovernance,  connectionString: String, driver: String, DebugMode: Boolean) extends Serializable {
  val Driver = driver
  val ConnectionString = connectionString
  var connection: Connection = null
  var statement: java.sql.Statement  = null
  
  def StartConnection() {
    if (driver != null && driver != "")
      Class.forName(driver)
    this.connection = DriverManager.getConnection(ConnectionString)
    this.statement = this.connection.createStatement()
  }
  
  /**
   * ExecuteJDBC_WithResult: Returns SQL Result
   * @param SQL: String = Query
   * @param toLowerCase: Boolean defaul true: convert all result to lowercase, otherwise returns as engine define
   */
  def ExecuteJDBC_WithResult(SQL: String, toLowerCase: Boolean = true): huemul_JDBCResult = {   
    var Result: huemul_JDBCResult = new huemul_JDBCResult()
    
    var i = 0
    if (connection == null) sys.error("error JDBC connection failed")
    while (i<=2 && connection.isClosed()) {
      huemulBigDataGob.logMessageWarn("CONTROL connection closed, trying to establish new connection")
      StartConnection()
      i+=1
    }
    
    if (i == 3) {
      sys.error("error while trying to establish new connection")
    }
      
        
      try {
        
        val Resultado = statement.executeQuery(SQL)
        //var fieldsStruct = new Array[StructField](0);
 
        var i:Integer = 1
        val Metadata = Resultado.getMetaData
        val NumColumns = Metadata.getColumnCount
        
        while (i <= NumColumns) {
          val DataTypeInt = Metadata.getColumnType(i)
  
        val DataType: DataType =      if (DataTypeInt == -7) DataTypes.BooleanType
                                 else if (DataTypeInt == -6) DataTypes.ShortType
                                 else if (DataTypeInt == -5) DataTypes.LongType
                                 else if (DataTypeInt == -4) DataTypes.BinaryType
                                 else if (DataTypeInt == -3) DataTypes.BinaryType
                                 else if (DataTypeInt == -2) DataTypes.BinaryType
                                 else if (DataTypeInt == -1) DataTypes.StringType
                                 else if (DataTypeInt == 0) DataTypes.NullType
                                 else if (DataTypeInt == 1) DataTypes.StringType
                                 else if (DataTypeInt == 2) DataTypes.LongType
                                 else if (DataTypeInt == 3) DecimalType(Metadata.getPrecision(i), Metadata.getScale(i))
                                 else if (DataTypeInt == 4) DataTypes.IntegerType
                                 else if (DataTypeInt == 5) DataTypes.ShortType
                                 else if (DataTypeInt == 6) DataTypes.FloatType
                                 else if (DataTypeInt == 7) DataTypes.DoubleType
                                 else if (DataTypeInt == 8) DataTypes.DoubleType
                                 else if (DataTypeInt == 12) DataTypes.StringType
                                 else if (DataTypeInt == 91) DataTypes.DateType
                                 else if (DataTypeInt == 92) DataTypes.TimestampType
                                 else if (DataTypeInt == 93) DataTypes.TimestampType
                                 else if (DataTypeInt == 1111) DataTypes.StringType
                                 else DataTypes.StringType
                                 
        if (toLowerCase)
          Result.fieldsStruct = Result.fieldsStruct:+ ( StructField(Metadata.getColumnName(i).toLowerCase() , DataType  , false , null))
        else 
          Result.fieldsStruct = Result.fieldsStruct:+ ( StructField(Metadata.getColumnName(i) , DataType  , false , null))
       i+=1
      }
      
      var ListaDatos: Array[Row] = new Array[Row](0)
      
      
      //Ciclo para cargar el dataframe
 
      
      while (Resultado.next()) {
        val Fila = new Array[Any](NumColumns)
        var i = 1
        
        //Ciclo para cada columna
        while (i <= NumColumns) {
          val DataTypeInt = Metadata.getColumnType(i)
          
          Fila(i-1) = if (DataTypeInt == -7) Resultado.getBoolean(i)
               else if (DataTypeInt == -6) Resultado.getShort(i)
               else if (DataTypeInt == -5) Resultado.getLong(i)
               else if (DataTypeInt == -4) Resultado.getBlob((i))
               else if (DataTypeInt == -3) Resultado.getBlob((i))
               else if (DataTypeInt == -2) Resultado.getBlob((i))
               else if (DataTypeInt == -1) Resultado.getString(i)
               else if (DataTypeInt == 0) Resultado.getString(i)
               else if (DataTypeInt == 1) Resultado.getString(i)
               else if (DataTypeInt == 2) Resultado.getLong(i)
               else if (DataTypeInt == 3) Resultado.getBigDecimal(i)
               else if (DataTypeInt == 4) Resultado.getInt(i)
               else if (DataTypeInt == 5) Resultado.getShort(i)
               else if (DataTypeInt == 6) Resultado.getFloat(i)
               else if (DataTypeInt == 7) Resultado.getDouble(i)
               else if (DataTypeInt == 8) Resultado.getDouble(i)
               else if (DataTypeInt == 12) Resultado.getString(i)
               else if (DataTypeInt == 91) Resultado.getDate(i)
               else if (DataTypeInt == 92) Resultado.getTimestamp(i)
               else if (DataTypeInt == 93) Resultado.getTimestamp(i)
               else if (DataTypeInt == 1111) Resultado.getString(i)
               else DataTypes.StringType
          i+= 1
        }
        
        val b = new GenericRowWithSchema(Fila,StructType.apply(Result.fieldsStruct)) 
        
        ListaDatos = ListaDatos:+ (b)
        
      }
              
      
      Result.ResultSet = ListaDatos
      //Crea arreglo
  
    //  connection.close()
     
    } catch {
      case e: Exception  =>  
        var l_trace = s"${e} (${SQL})"
        if (l_trace.length() > 3999)
          l_trace = l_trace.substring(3999)
        huemulBigDataGob.RegisterError(9999,e.getMessage(),l_trace, "", "ExecuteJDBC_WithResult", getClass().getSimpleName(), 0, "HuemulJDBC")
            
        if (DebugMode) huemulBigDataGob.logMessageError(SQL)
        if (DebugMode) huemulBigDataGob.logMessageError(s"JDBC Error: $e")
        if (DebugMode) huemulBigDataGob.logMessageError(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => huemulBigDataGob.logMessageDebug(x) }}")
        Result.ErrorDescription = s"JDBC Error: ${e}"
        Result.IsError = true
    }
      
    return Result
  }
  
  def ExecuteJDBC_NoResulSet(SQL: String, CallErrorRegister: Boolean = true): huemul_JDBCResult = {   
    var Result: huemul_JDBCResult = new huemul_JDBCResult()
    
    //val driver = "org.postgresql.Driver"
    //val url = ""
   
    var i = 0
    if (connection == null) sys.error("error JDBC connection failed")
    while (i<=2 && connection.isClosed()) {
      huemulBigDataGob.logMessageWarn("CONTROL connection closed, trying to establish new connection")
      StartConnection()
      i+=1
    }
    
    if (i == 3) {
      sys.error("error while trying to establish new connection")
    }
  
    try {
      //val statement = connection.createStatement()
      val Resultado = statement.execute(SQL)
    
      //connection.close()
     
    } catch {
      case e: Exception  =>  
        if (CallErrorRegister){
          var l_trace = s"${e} (${SQL})"
          if (l_trace.length() > 3999)
            l_trace = l_trace.substring(3999)
          huemulBigDataGob.RegisterError(9999
              ,e.getMessage()
              ,l_trace
              , ""
              , "ExecuteJDBC_NoResulSet"
              , getClass().getSimpleName()
              , 0
              , "HuemulJDBC")
        }
        
        if (DebugMode) huemulBigDataGob.logMessageError(SQL)
        if (DebugMode) huemulBigDataGob.logMessageError(s"JDBC Error: $e")
        if (DebugMode) huemulBigDataGob.logMessageError(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => huemulBigDataGob.logMessageError(x) }}")
        Result.ErrorDescription = s"JDBC Error: ${e}"
        Result.IsError = true
    }
    
    return Result
  }
  
  
  
}