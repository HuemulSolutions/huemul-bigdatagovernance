package com.huemulsolutions.bigdata.control

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Date

class HuemulJDBCResult extends Serializable {
  var ResultSet: Array[Row] = _
  var ErrorDescription: String = ""
  var IsError: Boolean = false
  var fieldsStruct = new Array[StructField](0);
  
  /**
   * open variable for keep any value
   */
  var OpenVar: String = _
  
  /**
   * open variable for keep any value
   */
  var OpenVar2: String = _
  
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
    
    Values
  }
}
