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

class tbl_demo_test(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {
  this.setAutoCast(true)
  this.setBusiness_ResponsibleName("Nombre 1")
  this.setDataBase(huemulBigDataGov.GlobalSettings.DIM_DataBase)
  this.setDescription("descripcion")
  this.setDQ_MaxNewRecords_Num(10)
  this.setDQ_MaxNewRecords_Perc(Decimal.apply(0.30))
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.DIM_BigFiles_Path)
  this.setIT_ResponsibleName("IT Responsible")
  this.setLocalPath("demo/")
  this.setFrequency(huemulType_Frequency.MONTHLY)
  //this.setPartitionField("periodo_id")
  this.setStorageType(huemulType_StorageType.ORC)
  this.setTableType(huemulType_Tables.Reference)
  this.WhoCanRun_executeFull_addAccess("classname","package") 
  this.WhoCanRun_executeOnlyInsert_addAccess("classname","package")
  this.WhoCanRun_executeOnlyUpdate_addAccess("classname","package")
  
  
  val codigo_id: huemul_Columns = new huemul_Columns(StringType, true, "descripción del campo")
  codigo_id.setIsPK (true)
  codigo_id.setPartitionColumn(2,false, true)
  codigo_id.setIsUnique ( true)
  codigo_id.setDQ_MaxDateTimeValue ( "")
  codigo_id.setDQ_MinDateTimeValue ( "")
  codigo_id.setDQ_MaxDecimalValue ( Decimal.apply(10))
  codigo_id.setDQ_MinDecimalValue ( Decimal.apply(10))
  codigo_id.setDQ_MaxLen ( 10)
  codigo_id.setDQ_MinLen ( 9)
  codigo_id.setNullable ( true)
  codigo_id.setDefaultValue ( "'nada'")
  
  codigo_id.setSecurityLevel ( huemulType_SecurityLevel.Public)
  codigo_id.setEncryptedType ( "sin encriptar")
  
  codigo_id.setMDM_EnableOldValue ( false)
  codigo_id.setMDM_EnableDTLog( false)
  codigo_id.setMDM_EnableProcessLog( false)
  
  
  
  this.ApplyTableDefinition()
}