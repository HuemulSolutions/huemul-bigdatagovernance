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
  this.setBusinessResponsibleName("Nombre 1")
  this.setDataBase(huemulBigDataGov.globalSettings.dimDataBase)
  this.setDescription("descripcion")
  this.setDQMaxNewRecordsNum(10)
  this.setDqMaxNewRecordsPerc(Decimal.apply(0.30))
  this.setGlobalPaths(huemulBigDataGov.globalSettings.dimBigFilesPath)
  this.setItResponsibleName("IT Responsible")
  this.setLocalPath("demo/")
  this.setFrequency(HuemulTypeFrequency.MONTHLY)
  //this.setPartitionField("periodo_id")
  this.setStorageType(HuemulTypeStorageType.ORC)
  this.setTableType(HuemulTypeTables.Reference)
  this.whoCanRunExecuteFullAddAccess("classname","package")
  this.whoCanRunExecuteOnlyInsertAddAccess("classname","package")
  this.whoCanRunExecuteOnlyUpdateAddAccess("classname","package")

  this.setNameForMDM_hash("myTempHash")
  
  val codigo_id: HuemulColumns = new HuemulColumns(StringType, true, "descripción del campo")
  codigo_id.setIsPK ()
  codigo_id.setPartitionColumn(2,dropBeforeInsert = false)
  codigo_id.setIsUnique ( )
  codigo_id.setDqMaxDateTimeValue ( "")
  codigo_id.setDqMinDateTimeValue ( "")
  codigo_id.setDqMaxDecimalValue ( Decimal.apply(10))
  codigo_id.setDqMinDecimalValue ( Decimal.apply(10))
  codigo_id.setDqMaxLen ( 10)
  codigo_id.setDqMinLen ( 9)
  codigo_id.setNullable ()
  codigo_id.setDefaultValues ( "'nada'")
  
  codigo_id.securityLevel ( HuemulTypeSecurityLevel.Public)
  codigo_id.encryptedType ( "sin encriptar")
  
  //codigo_id.setMDM_EnableOldValue ( false)
  //codigo_id.setMDM_EnableDTLog( false)
  //codigo_id.setMDM_EnableProcessLog( false)
  
  
  
  this.applyTableDefinition()
}