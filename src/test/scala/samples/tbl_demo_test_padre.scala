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
import com.huemulsolutions.bigdata.tables.huemul_Table_Relationship
import javax.naming.ldap.Control

class tbl_demo_test_padre(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends huemul_Table(huemulBigDataGov, Control) with Serializable {
  this.setAutoCast(true)
  this.setBusiness_ResponsibleName("Nombre 1")
  this.setDataBase(huemulBigDataGov.GlobalSettings.DIM_DataBase)
  this.setDescription("descripcion")
  this.setDQ_MaxNewRecords_Num(10)
  this.setDQ_MaxNewRecords_Perc(Decimal.apply(0.20))
  this.setGlobalPaths(huemulBigDataGov.GlobalSettings.DIM_BigFiles_Path)
  this.setIT_ResponsibleName("IT Responsible")
  this.setLocalPath("demo/")
  //this.setPartitionField("periodo_id")
  this.setStorageType(huemulType_StorageType.ORC)
  this.setTableType(huemulType_Tables.Reference)
  this.WhoCanRun_executeFull_addAccess("classname","package") 
  this.WhoCanRun_executeOnlyInsert_addAccess("classname","package")
  this.WhoCanRun_executeOnlyUpdate_addAccess("classname","package")
  
  
  val miClave_id: huemul_Columns = new huemul_Columns(StringType, true, "descripción del campo")
  miClave_id.setIsPK(true)
  miClave_id.setIsUnique(true)
  miClave_id.setDQ_MaxDateTimeValue ("")
  miClave_id.setDQ_MinDateTimeValue ("")
  miClave_id.setDQ_MaxDecimalValue ( Decimal.apply(10))
  miClave_id.setDQ_MinDecimalValue ( Decimal.apply(10))
  miClave_id.setDQ_MaxLen ( 10)
  miClave_id.setDQ_MinLen ( 9)
  miClave_id.setNullable ( true)
  miClave_id.setDefaultValue ( "'nada'")
  
  miClave_id.setSecurityLevel ( huemulType_SecurityLevel.Public)
  miClave_id.setEncryptedType ( "nada")
  
  miClave_id.setMDM_EnableOldValue ( false)
  miClave_id.setMDM_EnableDTLog( false)
  miClave_id.setMDM_EnableProcessLog( true)
  
  val codigo_id_aca: huemul_Columns = new huemul_Columns(StringType, true, "descripción del campo fk")
  
  val instancia_tbl_demo_test = new tbl_demo_test(huemulBigDataGov, Control)
  val FK_Rel = new huemul_Table_Relationship(instancia_tbl_demo_test, false)
  FK_Rel.AddRelationship(instancia_tbl_demo_test.codigo_id, this.codigo_id_aca)
  
  
  
  this.ApplyTableDefinition()
}