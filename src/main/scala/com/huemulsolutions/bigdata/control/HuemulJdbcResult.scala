package com.huemulsolutions.bigdata.control

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.util.Date

class HuemulJdbcResult extends Serializable {
  var resultSet: Array[Row] = _
  var errorDescription: String = ""
  var isError: Boolean = false
  var fieldsStruct = new Array[StructField](0);
  
  /**
   * open variable for keep any value
   */
  var openVar: String = _
  
  /**
   * open variable for keep any value
   */
  var openVar2: String = _
  
  def getValue(columnName: String, row: Row): Any = {
    val rows = fieldsStruct.filter { x => x.name == columnName }
    var values: Any = null
    
    if (rows.length == 1) {
      val firstRow = rows.array(0)
      
      
      if (firstRow.dataType == DataTypes.BooleanType)
        values = row.getAs[Boolean](columnName)
      else if (firstRow.dataType == DataTypes.ShortType)
        values = row.getAs[Short](columnName)
      else if (firstRow.dataType == DataTypes.LongType)
        values = row.getAs[Long](columnName)
      else if (firstRow.dataType == DataTypes.BinaryType)
        values = row.getAs[BinaryType](columnName)
      else if (firstRow.dataType == DataTypes.StringType)
        values = row.getAs[String](columnName)
      else if (firstRow.dataType == DataTypes.NullType)
        values = row.getAs[NullType](columnName)
      else if (firstRow.dataType == DecimalType || firstRow.dataType.typeName.toLowerCase().contains("decimal"))
        values = row.getAs[BigDecimal](columnName)
      else if (firstRow.dataType == DataTypes.IntegerType)
        values = row.getAs[Integer](columnName)
      else if (firstRow.dataType == DataTypes.FloatType)
        values = row.getAs[Float](columnName)
      else if (firstRow.dataType == DataTypes.DoubleType)
        values = row.getAs[Double](columnName)
      else if (firstRow.dataType == DataTypes.DateType)
        values = row.getAs[Date](columnName)
      else if (firstRow.dataType == DataTypes.TimestampType)
        values = row.getAs[String](columnName)
      else
        values = row.getAs[String](columnName)
  
    } else {
        values = null
        sys.error("error: column name not found")
    }
    
    values
  }
}
