package samples

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.huemul_Table
import com.huemulsolutions.bigdata.tables.huemul_Columns
import com.huemulsolutions.bigdata.tables.huemulType_StorageType
import com.huemulsolutions.bigdata.tables.huemulType_Tables
import com.huemulsolutions.bigdata.tables.huemulType_SecurityLevel
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.Decimal

class tbl_demo_test(huemulLib: huemul_Library, Control: huemul_Control) extends huemul_Table(huemulLib, Control) with Serializable {
  this.setAutoCast(true)
  this.setBusiness_ResponsibleName("Nombre 1")
  this.setDataBase(huemulLib.GlobalSettings.DIM_DataBase)
  this.setDescription("descripcion")
  this.setDQ_MaxNewRecords_Num(10)
  this.setDQ_MaxNewRecords_Perc(Decimal.apply(0.30))
  this.setGlobalPaths(huemulLib.GlobalSettings.DIM_BigFiles_Path)
  this.setIT_ResponsibleName("IT Responsible")
  this.setLocalPath("demo/")
  this.setPartitionField("periodo_id")
  this.setStorageType(huemulType_StorageType.ORC)
  this.setTableType(huemulType_Tables.Reference)
  this.WhoCanRun_executeFull_addAccess("classname","package") 
  this.WhoCanRun_executeOnlyInsert_addAccess("classname","package")
  this.WhoCanRun_executeOnlyUpdate_addAccess("classname","package")
  
  
  val codigo_id: huemul_Columns = new huemul_Columns(StringType, true, "descripción del campo")
  codigo_id.IsPK = true
  codigo_id.IsUnique = true
  codigo_id.DQ_MaxDateTimeValue = ""
  codigo_id.DQ_MinDateTimeValue = ""
  codigo_id.DQ_MaxDecimalValue = Decimal.apply(10)
  codigo_id.DQ_MinDecimalValue = Decimal.apply(10)
  codigo_id.DQ_MaxLen = 10
  codigo_id.DQ_MinLen = 9
  codigo_id.Nullable = true
  codigo_id.DefaultValue = "'nada'"
  
  codigo_id.SecurityLevel = huemulType_SecurityLevel.Public
  codigo_id.EncryptedType = "sin encriptar"
  
  codigo_id.MDM_EnableOldValue = false
  codigo_id.MDM_EnableDTLog= false
  codigo_id.MDM_EnableProcessLog= true
  
  
  
  this.ApplyTableDefinition()
}