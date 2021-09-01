package com.huemulsolutions.bigdata.control

import java.sql.{Connection, DriverManager}

import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, StructField, StructType}

class HuemulJDBCProperties(huemulBigDataGob: HuemulBigDataGovernance, connectionString: String, driver: String, DebugMode: Boolean) extends Serializable {
  val Driver: String = driver
  val ConnectionString: String = connectionString
  var connection: Connection = _
  var statement: java.sql.Statement  = _

  def StartConnection() {
    if (driver != null && driver != "")
      Class.forName(driver)

    this.connection = if (getUserName == null) DriverManager.getConnection(ConnectionString) else DriverManager.getConnection(ConnectionString, getUserName, passwordConnection )
    this.statement = this.connection.createStatement()
  }

  private var userName: String = _
  /**
   * set userName to connect DB
   * @param userName userName
   * @return huemul_JDBCProperties
   */
  def setUserName(userName: String): HuemulJDBCProperties = {
    this.userName = userName
    this
  }

  /**
   * get userName to connect to DB
   * @return
   */
  def getUserName: String = userName

  private var passwordConnection: String = _
  /**
   * set password to db connection
   * @param password password
   * @return
   */
  def setPassword(password: String): HuemulJDBCProperties = {
    passwordConnection = password
    this
  }

  /**
   * ExecuteJDBC_WithResult: Returns SQL Result
   * @param SQL: String = Query
   * @param toLowerCase: Boolean defaul true: convert all result to lowercase, otherwise returns as engine define
   */
  def ExecuteJDBC_WithResult(SQL: String, toLowerCase: Boolean = true): HuemulJDBCResult = {
    val Result: HuemulJDBCResult = new HuemulJDBCResult()

    //var i = 0
    if (connection == null) sys.error("error JDBC connection failed")
    while (connection.isClosed) {
      huemulBigDataGob.logMessageWarn("JDBC connection closed, trying to establish new connection")
      Thread.sleep(5000)
      StartConnection()
      //i+=1
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
          Result.fieldsStruct = Result.fieldsStruct:+  StructField(Metadata.getColumnName(i).toLowerCase() , DataType  , nullable = false , null)
        else
          Result.fieldsStruct = Result.fieldsStruct:+  StructField(Metadata.getColumnName(i) , DataType  , nullable = false , null)
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
          else if (DataTypeInt == -4) Resultado.getBlob(i)
          else if (DataTypeInt == -3) Resultado.getBlob(i)
          else if (DataTypeInt == -2) Resultado.getBlob(i)
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

        ListaDatos = ListaDatos:+ b

      }


      Result.ResultSet = ListaDatos
      //Crea arreglo

      //  connection.close()

    } catch {
      case e: Exception  =>
        var l_trace = s"$e ($SQL)"
        if (l_trace.length() > 3999)
          l_trace = l_trace.substring(3999)
        huemulBigDataGob.RegisterError(9999,e.getMessage,l_trace, "", "ExecuteJDBC_WithResult", getClass.getSimpleName, 0, "HuemulJDBC")

        if (DebugMode) huemulBigDataGob.logMessageError(SQL)
        if (DebugMode) huemulBigDataGob.logMessageError(s"JDBC Error: $e")
        if (DebugMode) huemulBigDataGob.logMessageError(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => huemulBigDataGob.logMessageDebug(x) }}")
        Result.ErrorDescription = s"JDBC Error: $e"
        Result.IsError = true
    }

     Result
  }

  def ExecuteJDBC_NoResulSet(SQL: String, CallErrorRegister: Boolean = true): HuemulJDBCResult = {
    val Result: HuemulJDBCResult = new HuemulJDBCResult()

    //val driver = "org.postgresql.Driver"
    //val url = ""

    //var i = 0
    if (connection == null) sys.error("error JDBC connection failed")
    while (connection.isClosed) {
      huemulBigDataGob.logMessageWarn("JDBC connection closed, trying to establish new connection")
      Thread.sleep(5000)
      StartConnection()
      //i+=1
    }

    try {
      //val statement = connection.createStatement()
      val Resultado = statement.execute(SQL)

      //connection.close()

    } catch {
      case e: Exception  =>
        if (CallErrorRegister){
          var l_trace = s"$e ($SQL)"
          if (l_trace.length() > 3999)
            l_trace = l_trace.substring(3999)
          huemulBigDataGob.RegisterError(9999
            ,e.getMessage
            ,l_trace
            , ""
            , "ExecuteJDBC_NoResulSet"
            , getClass.getSimpleName
            , 0
            , "HuemulJDBC")
        }

        if (DebugMode) huemulBigDataGob.logMessageError(SQL)
        if (DebugMode) huemulBigDataGob.logMessageError(s"JDBC Error: $e")
        if (DebugMode) huemulBigDataGob.logMessageError(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => huemulBigDataGob.logMessageError(x) }}")
        Result.ErrorDescription = s"JDBC Error: $e"
        Result.IsError = true
    }

    Result
  }



}
