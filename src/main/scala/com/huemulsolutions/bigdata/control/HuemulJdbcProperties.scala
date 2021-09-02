package com.huemulsolutions.bigdata.control

import java.sql.{Connection, DriverManager}

import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, StructField, StructType}

class HuemulJdbcProperties(huemulBigDataGob: HuemulBigDataGovernance, connectionString: String, driver: String, debugMode: Boolean) extends Serializable {
  //val driver: String = driver
  //val connectionString: String = connectionString
  var connection: Connection = _
  var statement: java.sql.Statement  = _

  def startConnection() {
    if (driver != null && driver != "")
      Class.forName(driver)

    this.connection = if (getUserName == null) DriverManager.getConnection(connectionString) else DriverManager.getConnection(connectionString, getUserName, passwordConnection )
    this.statement = this.connection.createStatement()
  }

  private var userName: String = _
  /**
   * set userName to connect DB
   * @param userName userName
   * @return huemul_JDBCProperties
   */
  def setUserName(userName: String): HuemulJdbcProperties = {
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
  def setPassword(password: String): HuemulJdbcProperties = {
    passwordConnection = password
    this
  }

  /**
   * ExecuteJDBC_WithResult: Returns SQL Result
   * @param sql: String = Query
   * @param toLowerCase: Boolean defaul true: convert all result to lowercase, otherwise returns as engine define
   */
  def executeJdbcWithResult(sql: String, toLowerCase: Boolean = true): HuemulJdbcResult = {
    val result: HuemulJdbcResult = new HuemulJdbcResult()

    //var i = 0
    if (connection == null) sys.error("error JDBC connection failed")
    while (connection.isClosed) {
      huemulBigDataGob.logMessageWarn("JDBC connection closed, trying to establish new connection")
      Thread.sleep(5000)
      startConnection()
      //i+=1
    }


    try {

      val resultado = statement.executeQuery(sql)
      //var fieldsStruct = new Array[StructField](0);

      var i:Integer = 1
      val metadata = resultado.getMetaData
      val numColumns = metadata.getColumnCount

      while (i <= numColumns) {
        val dataTypeInt = metadata.getColumnType(i)

        val dataType: DataType =      if (dataTypeInt == -7) DataTypes.BooleanType
        else if (dataTypeInt == -6) DataTypes.ShortType
        else if (dataTypeInt == -5) DataTypes.LongType
        else if (dataTypeInt == -4) DataTypes.BinaryType
        else if (dataTypeInt == -3) DataTypes.BinaryType
        else if (dataTypeInt == -2) DataTypes.BinaryType
        else if (dataTypeInt == -1) DataTypes.StringType
        else if (dataTypeInt == 0) DataTypes.NullType
        else if (dataTypeInt == 1) DataTypes.StringType
        else if (dataTypeInt == 2) DataTypes.LongType
        else if (dataTypeInt == 3) DecimalType(metadata.getPrecision(i), metadata.getScale(i))
        else if (dataTypeInt == 4) DataTypes.IntegerType
        else if (dataTypeInt == 5) DataTypes.ShortType
        else if (dataTypeInt == 6) DataTypes.FloatType
        else if (dataTypeInt == 7) DataTypes.DoubleType
        else if (dataTypeInt == 8) DataTypes.DoubleType
        else if (dataTypeInt == 12) DataTypes.StringType
        else if (dataTypeInt == 91) DataTypes.DateType
        else if (dataTypeInt == 92) DataTypes.TimestampType
        else if (dataTypeInt == 93) DataTypes.TimestampType
        else if (dataTypeInt == 1111) DataTypes.StringType
        else DataTypes.StringType

        if (toLowerCase)
          result.fieldsStruct = result.fieldsStruct:+  StructField(metadata.getColumnName(i).toLowerCase() , dataType  , nullable = false , null)
        else
          result.fieldsStruct = result.fieldsStruct:+  StructField(metadata.getColumnName(i) , dataType  , nullable = false , null)
        i+=1
      }

      var listaDatos: Array[Row] = new Array[Row](0)


      //Ciclo para cargar el dataframe


      while (resultado.next()) {
        val Fila = new Array[Any](numColumns)
        var i = 1

        //Ciclo para cada columna
        while (i <= numColumns) {
          val dataTypeInt = metadata.getColumnType(i)

          Fila(i-1) = if (dataTypeInt == -7) resultado.getBoolean(i)
          else if (dataTypeInt == -6) resultado.getShort(i)
          else if (dataTypeInt == -5) resultado.getLong(i)
          else if (dataTypeInt == -4) resultado.getBlob(i)
          else if (dataTypeInt == -3) resultado.getBlob(i)
          else if (dataTypeInt == -2) resultado.getBlob(i)
          else if (dataTypeInt == -1) resultado.getString(i)
          else if (dataTypeInt == 0) resultado.getString(i)
          else if (dataTypeInt == 1) resultado.getString(i)
          else if (dataTypeInt == 2) resultado.getLong(i)
          else if (dataTypeInt == 3) resultado.getBigDecimal(i)
          else if (dataTypeInt == 4) resultado.getInt(i)
          else if (dataTypeInt == 5) resultado.getShort(i)
          else if (dataTypeInt == 6) resultado.getFloat(i)
          else if (dataTypeInt == 7) resultado.getDouble(i)
          else if (dataTypeInt == 8) resultado.getDouble(i)
          else if (dataTypeInt == 12) resultado.getString(i)
          else if (dataTypeInt == 91) resultado.getDate(i)
          else if (dataTypeInt == 92) resultado.getTimestamp(i)
          else if (dataTypeInt == 93) resultado.getTimestamp(i)
          else if (dataTypeInt == 1111) resultado.getString(i)
          else DataTypes.StringType
          i+= 1
        }

        val b = new GenericRowWithSchema(Fila,StructType.apply(result.fieldsStruct))

        listaDatos = listaDatos:+ b

      }


      result.resultSet = listaDatos
      //Crea arreglo

      //  connection.close()

    } catch {
      case e: Exception  =>
        var l_trace = s"$e ($sql)"
        if (l_trace.length() > 3999)
          l_trace = l_trace.substring(3999)
        huemulBigDataGob.RegisterError(9999,e.getMessage,l_trace, "", "ExecuteJDBC_WithResult", getClass.getSimpleName, 0, "HuemulJDBC")

        if (debugMode) huemulBigDataGob.logMessageError(sql)
        if (debugMode) huemulBigDataGob.logMessageError(s"JDBC Error: $e")
        if (debugMode) huemulBigDataGob.logMessageError(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => huemulBigDataGob.logMessageDebug(x) }}")
        result.errorDescription = s"JDBC Error: $e"
        result.isError = true
    }

     result
  }

  def executeJdbcNoResultSet(SQL: String, CallErrorRegister: Boolean = true): HuemulJdbcResult = {
    val Result: HuemulJdbcResult = new HuemulJdbcResult()

    //val driver = "org.postgresql.Driver"
    //val url = ""

    //var i = 0
    if (connection == null) sys.error("error JDBC connection failed")
    while (connection.isClosed) {
      huemulBigDataGob.logMessageWarn("JDBC connection closed, trying to establish new connection")
      Thread.sleep(5000)
      startConnection()
      //i+=1
    }

    try {
      //val statement = connection.createStatement()
      //val Resultado =
        statement.execute(SQL)

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

        if (debugMode) huemulBigDataGob.logMessageError(SQL)
        if (debugMode) huemulBigDataGob.logMessageError(s"JDBC Error: $e")
        if (debugMode) huemulBigDataGob.logMessageError(s"JDBC Error TRACE: ${e.getStackTrace.foreach { x => huemulBigDataGob.logMessageError(x) }}")
        Result.errorDescription = s"JDBC Error: $e"
        Result.isError = true
    }

    Result
  }



}
