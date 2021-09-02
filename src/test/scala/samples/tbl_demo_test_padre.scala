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
import com.huemulsolutions.bigdata.tables.HuemulTableRelationship

class tbl_demo_test_padre(huemulBigDataGov: HuemulBigDataGovernance, Control: HuemulControl) extends HuemulTable(huemulBigDataGov, Control) with Serializable {
  this.setAutoCast(true)
  this.setBusinessResponsibleName("Nombre 1")
  this.setDataBase(huemulBigDataGov.globalSettings.dimDataBase)
  this.setDescription("descripcion")
  this.setFrequency(HuemulTypeFrequency.MONTHLY)
  this.setDQMaxNewRecordsNum(10)
  this.setDqMaxNewRecordsPerc(Decimal.apply(0.20))
  this.setGlobalPaths(huemulBigDataGov.globalSettings.dimBigFilesPath)
  this.setItResponsibleName("IT Responsible")
  this.setLocalPath("demo/")
  //this.setPartitionField("periodo_id")
  this.setStorageType(HuemulTypeStorageType.ORC)
  this.setTableType(HuemulTypeTables.Reference)
  this.whoCanRunExecuteFullAddAccess("classname","package")
  this.whoCanRunExecuteOnlyInsertAddAccess("classname","package")
  this.whoCanRunExecuteOnlyUpdateAddAccess("classname","package")
  
  
  val miClave_id: HuemulColumns = new HuemulColumns(StringType, true, "descripción del campo")
  miClave_id.setIsPK()
  miClave_id.setIsUnique()
  miClave_id.setDqMaxDateTimeValue ("")
  miClave_id.setDqMinDateTimeValue ("")
  miClave_id.setDqMaxDecimalValue ( Decimal.apply(10))
  miClave_id.setDqMinDecimalValue ( Decimal.apply(10))
  miClave_id.setDqMaxLen ( 10)
  miClave_id.setDqMinLen ( 9)
  miClave_id.setNullable ()
  miClave_id.setDefaultValues ( "'nada'")

  //se quita set
  miClave_id.securityLevel ( HuemulTypeSecurityLevel.Public)
  miClave_id.encryptedType( "nada")
  
  //miClave_id.setMDM_EnableOldValue ( )
  //miClave_id.setMDM_EnableDTLog( )
  //miClave_id.setMDM_EnableProcessLog()
  
  val codigo_id_aca: HuemulColumns = new HuemulColumns(StringType, true, "descripción del campo fk")
  
  val instancia_tbl_demo_test = new tbl_demo_test(huemulBigDataGov, Control)
  val FK_Rel = new HuemulTableRelationship(instancia_tbl_demo_test, false)
  FK_Rel.addRelationship(instancia_tbl_demo_test.codigo_id, this.codigo_id_aca)
  
  
  
  this.applyTableDefinition()
}