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

class tbl_demo_test_padre(huemulLib: huemul_Library, Control: huemul_Control) extends huemul_Table(huemulLib, Control) with Serializable {
  this.setAutoCast(true)
  this.setBusiness_ResponsibleName("Nombre 1")
  this.setDataBase(huemulLib.GlobalSettings.DIM_DataBase)
  this.setDescription("descripcion")
  this.setDQ_MaxNewRecords_Num(10)
  this.setDQ_MaxNewRecords_Perc(Decimal.apply(0.20))
  this.setGlobalPaths(huemulLib.GlobalSettings.DIM_BigFiles_Path)
  this.setIT_ResponsibleName("IT Responsible")
  this.setLocalPath("demo/")
  this.setPartitionField("periodo_id")
  this.setStorageType(huemulType_StorageType.ORC)
  this.setTableType(huemulType_Tables.Reference)
  this.WhoCanRun_executeFull_addAccess("classname","package") 
  this.WhoCanRun_executeOnlyInsert_addAccess("classname","package")
  this.WhoCanRun_executeOnlyUpdate_addAccess("classname","package")
  
  
  val miClave_id: huemul_Columns = new huemul_Columns(StringType, true, "descripción del campo")
  miClave_id.IsPK = true
  miClave_id.IsUnique = true
  miClave_id.DQ_MaxDateTimeValue = ""
  miClave_id.DQ_MinDateTimeValue = ""
  miClave_id.DQ_MaxDecimalValue = Decimal.apply(10)
  miClave_id.DQ_MinDecimalValue = Decimal.apply(10)
  miClave_id.DQ_MaxLen = 10
  miClave_id.DQ_MinLen = 9
  miClave_id.Nullable = true
  miClave_id.DefaultValue = "'nada'"
  
  miClave_id.SecurityLevel = huemulType_SecurityLevel.Public
  miClave_id.EncryptedType = "nada"
  
  miClave_id.MDM_EnableOldValue = false
  miClave_id.MDM_EnableDTLog= false
  miClave_id.MDM_EnableProcessLog= true
  
  val codigo_id_aca: huemul_Columns = new huemul_Columns(StringType, true, "descripción del campo fk")
  
  val instancia_tbl_demo_test = new tbl_demo_test(huemulLib, Control)
  val FK_Rel = new huemul_Table_Relationship(instancia_tbl_demo_test, false)
  FK_Rel.AddRelationship(instancia_tbl_demo_test.codigo_id, this.codigo_id_aca)
  
  
  
  this.ApplyTableDefinition()
}