package samples

import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.huemulsolutions.bigdata.tables.HuemulTable
import com.huemulsolutions.bigdata.tables.HuemulColumns
import com.huemulsolutions.bigdata.tables.HuemulTypeStorageType
import com.huemulsolutions.bigdata.tables.HuemulTypeTables
import com.huemulsolutions.bigdata.tables.HuemulTypeSecurityLevel
import org.apache.spark.sql.types.DataTypes._
//import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.Decimal

class tbl_demo_test(huemulBigDataGov: HuemulBigDataGovernance, Control: HuemulControl) extends HuemulTable(huemulBigDataGov, Control) with Serializable {
  this.setAutoCast(true)
  this.setBusiness_ResponsibleName("Nombre 1")
  this.setDataBase(huemulBigDataGov.GlobalSettings.DIM_DataBase)
  this.setDescription("descripcion")
  this.setDQ_MaxNewRecords_Num(10)
  this.setDQ_MaxNewRecords_Perc(Decimal.apply(0.30))
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.DIM_BigFiles_Path)
  this.setIT_ResponsibleName("IT Responsible")
  this.setLocalPath("demo/")
  this.setFrequency(HuemulTypeFrequency.MONTHLY)
  //this.setPartitionField("periodo_id")
  this.setStorageType(HuemulTypeStorageType.ORC)
  this.setTableType(HuemulTypeTables.Reference)
  this.WhoCanRun_executeFull_addAccess("classname","package") 
  this.WhoCanRun_executeOnlyInsert_addAccess("classname","package")
  this.WhoCanRun_executeOnlyUpdate_addAccess("classname","package")

  this.setNameForMDM_hash("myTempHash")
  
  val codigo_id: HuemulColumns = new HuemulColumns(StringType, true, "descripción del campo")
  codigo_id.setIsPK ()
  codigo_id.setPartitionColumn(2,dropBeforeInsert = false)
  codigo_id.setIsUnique ( )
  codigo_id.setDQ_MaxDateTimeValue ( "")
  codigo_id.setDQ_MinDateTimeValue ( "")
  codigo_id.setDQ_MaxDecimalValue ( Decimal.apply(10))
  codigo_id.setDQ_MinDecimalValue ( Decimal.apply(10))
  codigo_id.setDQ_MaxLen ( 10)
  codigo_id.setDQ_MinLen ( 9)
  codigo_id.setNullable ()
  codigo_id.setDefaultValues ( "'nada'")
  
  codigo_id.securityLevel ( HuemulTypeSecurityLevel.Public)
  codigo_id.encryptedType ( "sin encriptar")
  
  //codigo_id.setMDM_EnableOldValue ( false)
  //codigo_id.setMDM_EnableDTLog( false)
  //codigo_id.setMDM_EnableProcessLog( false)
  
  
  
  this.ApplyTableDefinition()
}