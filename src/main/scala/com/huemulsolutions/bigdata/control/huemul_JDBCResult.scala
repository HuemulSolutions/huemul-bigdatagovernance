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

class huemul_JDBCResult extends Serializable {
  var ResultSet: Array[Row] = null
  var ErrorDescription: String = ""
  var IsError: Boolean = false
  
  /**
   * open variable for keep any value
   */
  var OpenVar: String = null
  
  /**
   * open variable for keep any value
   */
  var OpenVar2: String = null
}

class huemul_JDBCProperties(huemulBigDataGob: huemul_BigDataGovernance,  connectionString: String, driver: String, DebugMode: Boolean) extends Serializable {
  val Driver = driver
  val ConnectionString = connectionString
  var connection: Connection = null
  
  def StartConnection() {
    if (driver != null && driver != "")
      Class.forName(driver)
    this.connection = DriverManager.getConnection(ConnectionString)
    
  }
  
  def ExecuteJDBC_WithResult(SQL: String): huemul_JDBCResult = {   
    var Result: huemul_JDBCResult = new huemul_JDBCResult()
    
    var i = 0
    while (i<=2 && connection.isClosed()) {
      println("postgres connection closed, trying to establish new connection")
      StartConnection()
      i+=1
    }
    
    if (i == 3) {
      sys.error("error while trying to establish new connection")
    }
      
        
      try {
        val statement = connection.createStatement()
        val Resultado = statement.executeQuery(SQL)
        var fieldsStruct = new Array[StructField](0);
 
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
                                 else if (DataTypeInt == 2) DecimalType(Metadata.getPrecision(i), Metadata.getScale(i)) //DataTypes.DoubleType
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
                                 
        
        fieldsStruct = fieldsStruct:+ ( StructField(Metadata.getColumnName(i) , DataType  , false , null))
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
               else if (DataTypeInt == 2) Resultado.getBigDecimal(i)
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
        
        val b = new GenericRowWithSchema(Fila,StructType.apply(fieldsStruct)) 
        
        ListaDatos = ListaDatos:+ (b)
        
      }
              
      
      Result.ResultSet = ListaDatos
      //Crea arreglo
  
    //  connection.close()
     
    } catch {
      case e: Exception  =>  
        huemulBigDataGob.RegisterError(9999,e.getMessage(),s"${e}", "", "ExecuteJDBC_WithResult", getClass().getSimpleName(), 0, "HuemulJDBC")
            
        if (DebugMode) println(SQL)
        if (DebugMode) println(s"JDBC Error: $e")
        if (DebugMode) println(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => println(x) }}")
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
    while (i<=2 && connection.isClosed()) {
      println("postgres connection closed, trying to establish new connection")
      StartConnection()
      i+=1
    }
    
    if (i == 3) {
      sys.error("error while trying to establish new connection")
    }
  
    try {
      val statement = connection.createStatement()
      val Resultado = statement.execute(SQL)
    
      //connection.close()
     
    } catch {
      case e: Exception  =>  
        if (CallErrorRegister)
          huemulBigDataGob.RegisterError(9999,e.getMessage(),s"${e}", "", "ExecuteJDBC_NoResulSet", getClass().getSimpleName(), 0, "HuemulJDBC")
        
        if (DebugMode) println(SQL)
        if (DebugMode) println(s"JDBC Error: $e")
        if (DebugMode) println(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => println(x) }}")
        Result.ErrorDescription = s"JDBC Error: ${e}"
        Result.IsError = true
    }
    
    return Result
  }
  
  
  
}