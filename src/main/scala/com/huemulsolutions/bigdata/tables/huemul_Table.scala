package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util._
import org.apache.spark.sql.Row
import java.lang.Long
import scala.collection.mutable._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.permission.FsPermission
import huemulType_Tables._
import huemulType_StorageType._
import com.huemulsolutions.bigdata._
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.common._
import com.huemulsolutions.bigdata.control._
import com.sun.xml.internal.ws.api.pipe.NextAction


class huemul_Table(huemulLib: huemul_Library, Control: huemul_Control) extends Serializable {
  if (Control == null) 
    sys.error("Control is null in huemul_DataFrame")
  
  /*  ********************************************************************************
   *****   T A B L E   P R O P E R T I E S    **************************************** 
   ******************************************************************************** */
  /**
   Table Name
   */
  val TableName : String= this.getClass.getSimpleName.replace("$", "") // ""
  println("**********************************************************")
  println(s"NOMBRE TABLA: ${TableName}")
  println("**********************************************************")
  
  var DataBase: ArrayBuffer[huemul_KeyValuePath] = null
    
  /**
   Type of Table (Reference; Master; Transaction
   */
  var TableType : huemulType_Tables = huemulType_Tables.Transaction
  /**
   Type of Persistent storage (parquet, csv, json)
   */
  var StorageType : huemulType_StorageType = null
  /**
    Table description
   */
  var Description   : String= ""
  /**
    Responsible contact in IT
   */
  var IT_ResponsibleName   : String= ""
  /**
    Responsible contact in Business (Name & Area)
   */
  var Business_ResponsibleName   : String= ""

  /**
   DataQuality: max N° records, null does'nt apply  DQ , 0 value doesn't accept new records (raiseError if new record found)
   */
  var DQ_MaxNewRecords_Num: Long = null
  /**
   DataQuality: Max % new records vs old records, null does'n apply DQ, 0% doesn't accept new records (raiseError if new record found), 100% accept double old records)
   */
  var DQ_MaxNewRecords_Perc: Integer = null
  
  private var autoCast: Boolean = true
  def setAutoCast(value: Boolean) {autoCast = value}
  
  private var ApplyDistinct: Boolean = true
  def setApplyDistinct(value: Boolean) {ApplyDistinct = value}
  
  
  private var Table_id: String = ""
  
  /**
    Directory's data is managed by Hive (automatically) or manually.
   */
  //var ManagedByHive   : Boolean= false
  /**
    Fields used to partition table in parquet
   */
  var PartitionField   : String= null
  /**
    Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  var LocalPath   : String= ""
  
  var GlobalPaths: ArrayBuffer[huemul_KeyValuePath] = null
  
  var WhoCanRun_executeFull: huemul_Authorization = new huemul_Authorization()
  var WhoCanRun_executeOnlyInsert: huemul_Authorization = new huemul_Authorization()
  var WhoCanRun_executeOnlyUpdate: huemul_Authorization = new huemul_Authorization()
  
  private var CreateInHive: Boolean = true
  private var CreateTableScript: String = ""
  
  private var _PartitionCreateTable: String = ""
  

  /*  ********************************************************************************
   *****   F I E L D   P R O P E R T I E S    **************************************** 
   ******************************************************************************** */
  val MDM_hash = new huemul_Columns (StringType, true, "Valor hash de los datos de la tabla", false)
  val MDM_fhNew = new huemul_Columns (TimestampType, true, "Fecha/hora cuando se insertaron los datos nuevos", false)
  val MDM_ProcessNew = new huemul_Columns (StringType, false, "Nombre del proceso que insertó los datos", false)
  val MDM_fhChange = new huemul_Columns (TimestampType, false, "fecha / hora de último cambio de valor en los campos de negocio", false)
  val MDM_ProcessChange = new huemul_Columns (StringType, false, "Nombre del proceso que cambió los datos", false)
  val MDM_StatusReg = new huemul_Columns (IntegerType, true, "indica si el registro fue insertado en forma automática por otro proceso (1), o fue insertado por el proceso formal (2), si está eliminado (-1)", false)
      
  var AdditionalRowsForDistint: String = ""
  
  
  /***
   * from Global path (small files, large files, DataBase, etc)
   */
  def GlobalPath()  : String= {
    return GetPath(GlobalPaths)
  }
  
  def GlobalPath(ManualEnvironment: String)  : String= {
    return GetPath(GlobalPaths, ManualEnvironment)
  }
  
  
    
  def GetFullNameWithPath() : String = {
    return GlobalPath + LocalPath + TableName
  }
  
  def GetFullNameWithPath2(ManualEnvironment: String) : String = {
    return GlobalPath(ManualEnvironment) + LocalPath + TableName
  }
    
  def GetFullPath() : String = {
    return GlobalPath + LocalPath 
  }
  
  
  /**
   * Return DataBaseName.TableName
   */
  def GetTable(): String = {
    return s"${GetDataBase(DataBase)}.${TableName}"
  }
  
  /**
   * Return DataBaseName.TableName
   */
  def GetTable(ManualEnvironment: String): String = {
    return s"${GetDataBase(this.DataBase, ManualEnvironment)}.${TableName}"
  }
  
  
  def GetDataBase(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    return huemulLib.GlobalSettings.GetDataBase(huemulLib, Division)  
  }
  
  def GetDataBase(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    return huemulLib.GlobalSettings.GetDataBase(huemulLib, Division, ManualEnvironment)  
  }
  
  def GetPath(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    return huemulLib.GlobalSettings.GetPath(huemulLib, Division)  
  }
  
  def GetPath(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    return huemulLib.GlobalSettings.GetPath(huemulLib, Division, ManualEnvironment)  
  }
  
  
  
  /**
   get execution's info about rows and DF
   */
  var DataFramehuemul: huemul_DataFrame = new huemul_DataFrame(huemulLib, Control)  

   
  var Error_isError: Boolean = false
  var Error_Text: String = ""
  var Error_Code: Integer = null
  
  
  def ApplyTableDefinition(): Boolean = {
    if (this.PartitionField == null)
      PartitionField = ""
      
    if (this.GlobalPaths == null)
      RaiseError(s"huemul_Table Error: GlobalPaths must be defined",1000)
      
    if (this.LocalPath == null)
      RaiseError(s"huemul_Table Error: LocalPath must be defined",1001)
      
    if (this.StorageType == null)
      RaiseError(s"huemul_Table Error: StorageType must be defined",1002)
      
    getALLDeclaredFields().filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] || x.get(this).isInstanceOf[huemul_DataQuality] || x.get(this).isInstanceOf[huemul_Table_Relationship]  
    } foreach { x =>
      x.setAccessible(true)
          
      //Nombre de campos
      if (x.get(this).isInstanceOf[huemul_Columns]) {
        val DataField = x.get(this).asInstanceOf[huemul_Columns]
        DataField.Set_MyName(x.getName)
        
        if (DataField.DQ_MaxLen != null && DataField.DQ_MaxLen < 0)
          RaiseError(s"Error column ${x.getName}: DQ_MaxLen must be positive",1003)
        else if (DataField.DQ_MaxLen != null && DataField.DQ_MinLen != null && DataField.DQ_MaxLen < DataField.DQ_MinLen)
          RaiseError(s"Error column ${x.getName}: DQ_MinLen(${DataField.DQ_MinLen}) must be less than DQ_MaxLen(${DataField.DQ_MaxLen})",1028)
        else if (DataField.DQ_MaxDecimalValue != null && DataField.DQ_MinDecimalValue != null && DataField.DQ_MaxDecimalValue < DataField.DQ_MinDecimalValue)
          RaiseError(s"Error column ${x.getName}: DQ_MinDecimalValue(${DataField.DQ_MinDecimalValue}) must be less than DQ_MaxDecimalValue(${DataField.DQ_MaxDecimalValue})",1029)
        else if (DataField.DQ_MaxDateTimeValue != null && DataField.DQ_MinDateTimeValue != null && DataField.DQ_MaxDateTimeValue < DataField.DQ_MinDateTimeValue)
          RaiseError(s"Error column ${x.getName}: DQ_MinDateTimeValue(${DataField.DQ_MinDateTimeValue}) must be less than DQ_MaxDateTimeValue(${DataField.DQ_MaxDateTimeValue})",1030)
        else if (DataField.DefaultValue != null && DataField.DataType == StringType && DataField.DefaultValue.toUpperCase() != "NULL" && !DataField.DefaultValue.contains("'"))
          RaiseError(s"Error column ${x.getName}: DefaultValue  must be like this: 'something', not something wihtout ')",1031)
          
        
        
      }
      
      //Nombre de DQ      
      if (x.get(this).isInstanceOf[huemul_DataQuality]) {
        val DQField = x.get(this).asInstanceOf[huemul_DataQuality]
        DQField.setMyName(x.getName)       
      }
      
      //Nombre de FK
      if (x.get(this).isInstanceOf[huemul_Table_Relationship]) {
        val FKField = x.get(this).asInstanceOf[huemul_Table_Relationship]
        FKField.MyName = x.getName
        
        //TODO: Validate FK Setting
        
      }
    }
    
    //Register TableName and fields
    Control.RegisterMASTER_CREATE_Basic(this)    
              
    return true
  }
  
  
  private def getALLDeclaredFields(OnlyUserDefined: Boolean = false) : Array[java.lang.reflect.Field] = {
    val pClass = getClass()  
    
    val a = pClass.getDeclaredFields()
    var c = a
    if (!OnlyUserDefined){
      var b = pClass.getSuperclass().getDeclaredFields()
      
      if (this.TableType == huemulType_Tables.Transaction) 
        b = b.filter { x => x.getName != "MDM_ProcessChange" && x.getName != "MDM_fhChange" && x.getName != "MDM_StatusReg"  }       
      
      c = a.union(b)  
    }

    
    return c
  }
  
  private var _NumRows_New: Long = null
  private var _NumRows_Update: Long = null
  private var _NumRows_Delete: Long = null
  private var _NumRows_Total: Long = null
  
  def NumRows_New(): Long = {
    return _NumRows_New
  }
  
  def NumRows_Update(): Long = {
    return _NumRows_Update
  }
  
  def NumRows_Delete(): Long = {
    return _NumRows_Delete
  }
  
  def NumRows_Total(): Long = {
    return _NumRows_Total
  }
   /*  ********************************************************************************
   *****   F I E L D   M E T H O D S    **************************************** 
   ******************************************************************************** */
  private def GetSchema(): StructType = {        
    val fieldsStruct = new ArrayBuffer[StructField]();
    
    var i:Integer = 0
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      //create StructType
      fieldsStruct.append( StructField(x.getName, Field.DataType , nullable = Field.Nullable , null))

      if (Field.MDM_EnableOldValue) {
        fieldsStruct.append( StructField(x.getName + "_old", Field.DataType , nullable = true , null))
      }
      if (Field.MDM_EnableDTLog) {
        fieldsStruct.append( StructField(x.getName + "_fhChange", TimestampType , nullable = true , null))
      }
      if (Field.MDM_EnableProcessLog) {
        fieldsStruct.append( StructField(x.getName + "_ProcessLog", StringType , nullable = true , null))
      }
      
    }
    if (huemulLib.DebugMode) println(s"N° Total: ${fieldsStruct.length}")
    return StructType.apply(fieldsStruct)
  }
  
  def GetDataQuality(): ArrayBuffer[huemul_DataQuality] = {
    val GetDeclaredfields = getALLDeclaredFields()
    val Result = new ArrayBuffer[huemul_DataQuality]()
     
    if (GetDeclaredfields != null ) {
        GetDeclaredfields.filter { x => x.setAccessible(true)
                             x.get(this).isInstanceOf[huemul_DataQuality] }
        .foreach { x =>
          //Get DQ
          var DQRule = x.get(this).asInstanceOf[huemul_DataQuality]
          Result.append(DQRule)
        }
      }
    
    return Result
  }
  
  def GetForeingKey(): ArrayBuffer[huemul_Table_Relationship] = {
    val GetDeclaredfields = getALLDeclaredFields()
    val Result = new ArrayBuffer[huemul_Table_Relationship]()
     
    if (GetDeclaredfields != null ) {
        GetDeclaredfields.filter { x => x.setAccessible(true)
                             x.get(this).isInstanceOf[huemul_Table_Relationship] }
        .foreach { x =>
          //Get DQ
          var FKRule = x.get(this).asInstanceOf[huemul_Table_Relationship]
          Result.append(FKRule)
        }
      }
    
    return Result
  }
  
  /**
   Create schema from DataDef Definition
   */
  def GetColumns(): ArrayBuffer[huemul_Columns] = {    
    var Result: ArrayBuffer[huemul_Columns] = new ArrayBuffer[huemul_Columns]()
    val fieldList = getALLDeclaredFields()
    val NumFields = fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }.length
    
    var ColumnsCreateTable : String = ""
    var coma: String = ""
    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      //Field.Set_MyName(x.getName)
      Result.append(Field)
                      
      
      if (Field.MDM_EnableOldValue) {
        val MDM_EnableOldValue = new huemul_Columns(Field.DataType, false, s"Old value for ${x.getName}")
        MDM_EnableOldValue.Set_MyName(s"${x.getName}_Old")
        Result.append(MDM_EnableOldValue)        
      } 
      if (Field.MDM_EnableDTLog){
        val MDM_EnableDTLog = new huemul_Columns(TimestampType, false, s"Last change DT for ${x.getName}")
        MDM_EnableDTLog.Set_MyName(s"${x.getName}_fhChange")
        Result.append(MDM_EnableDTLog)                
      } 
      if (Field.MDM_EnableProcessLog) {
        val MDM_EnableProcessLog = new huemul_Columns(StringType, false, s"System Name change for ${x.getName}")
        MDM_EnableProcessLog.Set_MyName(s"${x.getName}_ProcessLog")
        Result.append(MDM_EnableProcessLog) 
      }
              
    }
    
    return Result
  }
  
  
  /**
   Create schema from DataDef Definition
   */
  private def GetColumns_CreateTable(ForHive: Boolean = false): String = {
    val fieldList = getALLDeclaredFields()
    val NumFields = fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }.length
    
    var ColumnsCreateTable : String = ""
    var coma: String = ""
    fieldList.filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      var DataTypeLocal = Field.DataType.sql
      
      /*
      if (ForHive && DataTypeLocal.toUpperCase() == "DATE"){
        DataTypeLocal = TimestampType.sql
      }
      * 
      */
          
      //create StructType
      if (PartitionField != null && PartitionField.toUpperCase() != x.getName.toUpperCase()) {
        ColumnsCreateTable += s"$coma${x.getName} ${DataTypeLocal} \n"
        coma = ","
      }
      
      if (Field.MDM_EnableOldValue)
        ColumnsCreateTable += s"$coma${x.getName}_old ${DataTypeLocal} \n"  
      if (Field.MDM_EnableDTLog) 
        ColumnsCreateTable += s"$coma${x.getName}_fhChange ${TimestampType.sql} \n"  
      if (Field.MDM_EnableProcessLog) 
        ColumnsCreateTable += s"$coma${x.getName}_ProcessLog ${StringType.sql} \n"      
    }
    
    return ColumnsCreateTable
  }
  
  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE PRIMARY KEY
   */
  private def SQL_PrimaryKey_FinalTable(): String = {
    
    
    var StringSQL: String = ""
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] &&
                                      x.get(this).asInstanceOf[huemul_Columns].IsPK }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      if (!huemulLib.HasName(Field.get_MappedName()))
        sys.error(s"field ${x.getName} doesn't have an assigned name in 'name' attribute")
      StringSQL += coma + x.getName
      coma = ","
    }
    
       
    return StringSQL 
  }
  
  /**
  CREATE SQL SCRIPT FOR TRANSACTIONAL DATA
   */
  private def SQL_Step0_TXHash(OfficialAlias: String, processName: String): String = {
       
    var StringSQL: String = ""
    var StringSQl_PK: String = ""
    
    var StringSQL_hash: String = "sha2(concat( "
    var coma_hash: String = ""
    
    
    var coma: String = ""    
    _PartitionCreateTable = ""
    var coma_partition: String = ""
    
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val NewColumnCast = ApplyAutoCast(s"New.${Field.get_MappedName()}",Field.DataType.sql)
      
      if (PartitionField.toUpperCase() == x.getName().toUpperCase() ) {
        _PartitionCreateTable += s"${coma_partition}${PartitionField} ${Field.DataType.sql}"
        coma_partition = ","
      }
      
      //New value (from field or compute column )
      if (huemulLib.HasName(Field.get_MappedName()) ){
        StringSQL += s"${coma}${NewColumnCast} as ${x.getName} \n"
        coma = ","
      } else {
        if (x.getName == "MDM_fhNew") {
          StringSQL += s"${coma}CAST(now() AS ${Field.DataType.sql} ) as ${x.getName} \n"
          coma = ","
        } else if (x.getName == "MDM_ProcessNew") {
          StringSQL += s"${coma}CAST('${processName}' AS ${Field.DataType.sql} ) as ${x.getName} \n"
          coma = ","
        }
      }
          
       
      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""${coma_hash}${if (Field.Nullable) s"coalesce(${Field.get_MappedName()},'null')" else Field.get_MappedName()}"""
        coma_hash = ","
      }
      
      
    }
    
    StringSQL_hash += "),256) "
    
    
    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL = s"""SELECT ${StringSQL}
                    ,${StringSQL_hash} as MDM_hash
                    FROM $OfficialAlias new
                          """
       
    return StringSQL 
  }
  
  /**
  CREATE SQL SCRIPT DISTINCT WITH NEW DATAFRAME
   */
  private def SQL_Step0_Distinct(NewAlias: String): String = {
    
    var StringSQL: String = ""
    var Distintos: ArrayBuffer[String] = new ArrayBuffer[String]()
    var coma: String = ""
    getALLDeclaredFields(true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns]
                                        }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val MappedField: String = Field.get_MappedName()
      if (huemulLib.HasName(MappedField)) {        
        if (Distintos.filter { x => x == MappedField }.length == 0){
          Distintos.append(MappedField)
          StringSQL += s"${coma}${MappedField}  \n"
          coma = "," 
        }
      }
    }
      
    //Aditional field from new table
    if (huemulLib.HasName(AdditionalRowsForDistint))
      StringSQL += s",${AdditionalRowsForDistint}"

    //set field New or Update action
    StringSQL = s"""SELECT DISTINCT ${StringSQL}
                     FROM $NewAlias"""
       
    return StringSQL 
  }
  
  
  private def ApplyAutoCast(ColumnName: String, DataType: String): String = {
    return if (autoCast) s"CAST($ColumnName AS $DataType)" else ColumnName
  }
  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step1_FullJoin(OfficialAlias: String, NewAlias: String, isUpdate: Boolean, isDelete: Boolean): String = {
    //Get fields in old table
    val OfficialColumns = huemulLib.spark.catalog.listColumns(OfficialAlias).collect()
       
    var StringSQL_FullJoin: String = ""
    var StringSQl_PK: String = ""
    var StringSQL_ActionType: String = ""
    
    var coma: String = ""
    var sand: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val NewColumnCast = ApplyAutoCast(s"New.${Field.get_MappedName()}",Field.DataType.sql)
      //string for key on
      if (Field.IsPK){
        StringSQL_FullJoin += s"${coma}CAST(coalesce(Old.${x.getName}, New.${Field.get_MappedName()}) as ${Field.DataType.sql}) AS ${x.getName} \n"
        StringSQl_PK += s" $sand Old.${x.getName} = ${NewColumnCast}  " 
        sand = " and "

        if (StringSQL_ActionType == "")
          StringSQL_ActionType = s" case when Old.${x.getName} is null then 'NEW' when ${NewColumnCast} is null then ${if (isDelete) "'DELETE'" else "'EQUAL'"} else ${if (isUpdate) "'UPDATE'" else "'EQUAL'"} END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = !(OfficialColumns.filter { y => y.name == x.getName}.length == 0)
        
        //String for new DataFrame fulljoin
        if (columnExist)
          StringSQL_FullJoin += s"${coma}cast(old.${x.getName} as ${Field.DataType.sql} ) as old_${x.getName} \n"
        else
          StringSQL_FullJoin += s"${coma}cast(null as ${Field.DataType.sql} ) as old_${x.getName} \n"
        
        //New value (from field or compute column )
        if (huemulLib.HasName(Field.get_MappedName()))
          StringSQL_FullJoin += s",${NewColumnCast} as new_${x.getName} \n"
        if (huemulLib.HasName(Field.get_SQLForInsert()))
          StringSQL_FullJoin += s",CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} ) as new_insert_${x.getName} \n"
        if (huemulLib.HasName(Field.get_SQLForUpdate()))
          StringSQL_FullJoin += s",CAST(${Field.get_SQLForUpdate()} as ${Field.DataType.sql} ) as new_update_${x.getName} \n"
          
        if (Field.MDM_EnableOldValue || Field.MDM_EnableDTLog || Field.MDM_EnableProcessLog) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulLib.HasName(Field.get_MappedName())) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulLib.HasName(Field.get_SQLForUpdate())) Field.get_SQLForUpdate() else "new.".concat(Field.get_MappedName()),Field.DataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(x.getName) else "null"
            StringSQL_FullJoin += s",CAST(CASE WHEN ${NewFieldTXT} = ${OldFieldTXT} or (${NewFieldTXT} is null and ${OldFieldTXT} is null) THEN 0 ELSE 1 END as Integer ) as __Change_${x.getName}  \n"
          }
          else {
            StringSQL_FullJoin += s",CAST(0 as Integer ) as __Change_${x.getName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae, 
            //la solución es poner el campo "tiene cambio" en falso
          }
              
        }
      
        if (Field.MDM_EnableOldValue){
          if (OfficialColumns.filter { y => y.name == s"${x.getName}_old"}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${x.getName}_old AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
        }
        if (Field.MDM_EnableDTLog){
          if (OfficialColumns.filter { y => y.name == s"${x.getName}_fhChange"}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS TimeStamp) as old_${x.getName}_fhChange \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${x.getName}_fhChange AS TimeStamp) as old_${x.getName}_fhChange \n"
        }
        if (Field.MDM_EnableProcessLog){
          if (OfficialColumns.filter { y => y.name == s"${x.getName}_ProcessLog"}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS STRING) as old_${x.getName}_ProcessLog \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${x.getName}_ProcessLog AS STRING) as old_${x.getName}_ProcessLog \n"
        }   
        
        
          
      }
       
      coma = ","
    }
    
    
    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL_FullJoin = s"""SELECT ${StringSQL_FullJoin}
                                   ,${StringSQL_ActionType}
                             FROM $OfficialAlias Old
                                FULL JOIN ${NewAlias} New \n 
                                   on $StringSQl_PK  
                          """
       
    return StringSQL_FullJoin 
  }
   
  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step2_UpdateAndInsert(NewAlias: String, ProcessName: String, isInsert: Boolean): String = {
    
    var StringSQL: String = "SELECT "
    
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      //string for key on
      if (Field.IsPK){
        StringSQL += s" ${coma}${x.getName} as ${x.getName} \n"
        
      } else {
        if (   x.getName == "MDM_fhNew" || x.getName == "MDM_ProcessNew" || x.getName == "MDM_fhChange"
            || x.getName == "MDM_ProcessChange" || x.getName == "MDM_StatusReg" 
            || x.getName == "MDM_hash")
          StringSQL += s"${coma}old_${x.getName}  \n"
        else {          
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'NEW'    THEN ${if (this.huemulLib.HasName(Field.get_SQLForInsert())) s"new_insert_${x.getName}" //si tiene valor en SQL insert, lo usa
                                                                               else if (this.huemulLib.HasName(Field.get_MappedName())) s"new_${x.getName}"     //si no, si tiene nombre de campo en DataFrame nuevo, lo usa 
                                                                               else ApplyAutoCast(Field.DefaultValue,Field.DataType.sql)                        //si no tiene campo asignado, pone valor por default
                                                                           }
                                        WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulLib.HasName(Field.get_SQLForUpdate())) s"new_update_${x.getName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.get_ReplaceValueOnUpdate() && this.huemulLib.HasName(Field.get_MappedName())) s"new_${x.getName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${x.getName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${x.getName} END  as ${x.getName} \n"""
                
           if (Field.MDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN old_${x.getName} ELSE old_${x.getName}_old END as ${x.getName}_old \n"""
           if (Field.MDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN now() ELSE old_${x.getName}_fhChange END AS TimeStamp) as ${x.getName}_fhChange \n"""
           if (Field.MDM_EnableProcessLog)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN '${ProcessName}' WHEN ___ActionType__ = 'NEW' THEN '${ProcessName}' ELSE old_${x.getName}_ProcessLog END as ${x.getName}_ProcessLog \n"""             
        }
      }
                         
      coma = ","
    }
                
    
    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" ,___ActionType__
                      FROM $NewAlias New 
                      ${if (isInsert) "" else "WHERE ___ActionType__ <> 'NEW' "}
                  """
    
           
    return StringSQL 
  }
  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step3_Hash_p1(NewAlias: String): String = {
    
    var StringSQL: String = "SELECT "
    var StringSQL_hash: String = "sha2(concat( "
  
    
    var coma: String = ""
    var coma_hash: String = ""

    //Obtiene todos los campos
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns]
                                    }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
           
      if (   x.getName == "MDM_fhNew" || x.getName == "MDM_ProcessNew" || x.getName == "MDM_fhChange"
            || x.getName == "MDM_ProcessChange" || x.getName == "MDM_StatusReg" 
            || x.getName == "MDM_hash")
        StringSQL += s"${coma}old_${x.getName}  \n"
      else 
        StringSQL += s" ${coma}${x.getName}  \n"
        
      if (Field.MDM_EnableOldValue)
        StringSQL += s""",${x.getName}_old \n"""
      if (Field.MDM_EnableDTLog)
        StringSQL += s""",${x.getName}_fhChange \n"""
      if (Field.MDM_EnableProcessLog)
        StringSQL += s""",${x.getName}_ProcessLog \n"""
       
      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""${coma_hash}${if (Field.Nullable) s"coalesce(${x.getName},'null')" else x.getName}"""
        coma_hash = ","
      }
        
      coma = ","
    }
    
    //Hash fields
    if (coma_hash == "")
      sys.error(s"must define one field as UsedForCheckSum")
    
    StringSQL_hash += "),256) "
                
    //Include HashFields to SQL
    StringSQL += s""",${StringSQL_hash} as MDM_hash \n"""
    
    if (this.TableType == huemulType_Tables.Reference || this.TableType == huemulType_Tables.Master) {
      StringSQL += s""",case when old_MDM_hash = ${StringSQL_hash} THEN 1 ELSE 0 END AS SameHashKey  \n ,___ActionType__ \n"""
    }
    
    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" FROM $NewAlias New 
                  """
               
    return StringSQL 
  }
  
  
  private def SQL_Step4_Final(NewAlias: String, ProcessName: String): String = {
    var StringSQL: String = "SELECT "
    
    var coma: String = ""

    //Obtiene todos los campos
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns]
                                    }
    .foreach { x =>     
      //Get field      
      var Field = x.get(this).asInstanceOf[huemul_Columns]
           
      if (x.getName == "MDM_fhNew")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN now() ELSE old_${x.getName} END as ${x.getName} \n"
      else if (x.getName == "MDM_ProcessNew")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN '$ProcessName' ELSE old_${x.getName} END as ${x.getName} \n"
      else if (x.getName == "MDM_fhChange")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN now() ELSE old_${x.getName} END as ${x.getName} \n"
      else if (x.getName == "MDM_ProcessChange")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' AND SameHashKey = 0 THEN '$ProcessName' ELSE old_${x.getName} END as ${x.getName} \n"
      else if (x.getName == "MDM_StatusReg")
        StringSQL += s" ${coma}CASE WHEN ___ActionType__ = 'NEW' THEN CAST(2 as Int) WHEN ___ActionType__ = 'DELETE' THEN CAST(-1 AS Int)  ELSE CAST(old_${x.getName} AS Int) END as ${x.getName} \n"
      else if (x.getName == "MDM_hash")
        StringSQL += s"${coma}MDM_hash \n"
      else 
        StringSQL += s" ${coma}${x.getName}  \n"
        
      if (Field.MDM_EnableOldValue)
        StringSQL += s""",${x.getName}_old \n"""
      if (Field.MDM_EnableDTLog)
        StringSQL += s""",${x.getName}_fhChange \n"""
      if (Field.MDM_EnableProcessLog)
        StringSQL += s""",${x.getName}_ProcessLog \n"""
       
       
      coma = ","
    }
    
    StringSQL += s", ___ActionType__ \n FROM $NewAlias New\n" 
       
    return StringSQL
  }
  
  
  
  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE PRIMARY KEY
   */
  private def SQL_Unique_FinalTable(): ArrayBuffer[String] = {
    
    var StringSQL: ArrayBuffer[String] = new ArrayBuffer[String]()
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                         x.get(this).isInstanceOf[huemul_Columns] &&
                                         x.get(this).asInstanceOf[huemul_Columns].IsUnique && huemulLib.HasName(x.get(this).asInstanceOf[huemul_Columns].get_MappedName)}
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      StringSQL.append(x.getName)
    }
       
    return StringSQL 
  }
  
  /**
  CREATE SQL SCRIPT FIELDS FOR VALIDATE NOT NULL ATTRIBUTES
   */
  private def SQL_NotNull_FinalTable(): ArrayBuffer[String] = {
    var StringSQL: ArrayBuffer[String] = new ArrayBuffer[String]()
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                          x.get(this).isInstanceOf[huemul_Columns] &&
                                         !x.get(this).asInstanceOf[huemul_Columns].Nullable && huemulLib.HasName(x.get(this).asInstanceOf[huemul_Columns].get_MappedName)}
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      StringSQL.append(x.getName)
    }
       
    return StringSQL 
  }
  
  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required 
   */
  private def MissingRequiredFields(): ArrayBuffer[String] = {
    
    var StringSQL: ArrayBuffer[String] = new ArrayBuffer[String]()
    getALLDeclaredFields(true).filter { x => x.setAccessible(true)                                         
                                         x.get(this).isInstanceOf[huemul_Columns] &&
                                         x.get(this).asInstanceOf[huemul_Columns].Required  
                                          }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      //println(s"${Field.get_MyName()} Field.get_MappedName: ${Field.get_MappedName}, Field.get_SQLForUpdate(): ${Field.get_SQLForUpdate()}, Field.get_SQLForInsert(): ${Field.get_SQLForInsert()}")
      
      var isOK: Boolean = false
      if (huemulLib.HasName(Field.get_MappedName))
        isOK = true
      else if (!huemulLib.HasName(Field.get_MappedName) && 
              (huemulLib.HasName(Field.get_SQLForUpdate()) && huemulLib.HasName(Field.get_SQLForInsert())))
        isOK = true
      else
        isOK = false
        
      if (!isOK)
        StringSQL.append(x.getName)
    }
       
    return StringSQL 
  }
  
  
  
  
  
  /*  ********************************************************************************
   *****   T A B L E   M E T H O D S    **************************************** 
   ******************************************************************************** */
  
  private def DF_CreateTableScript(): String = {
                                  
    //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
    CreateTableScript = s"""
                                 CREATE EXTERNAL TABLE IF NOT EXISTS ${GetTable()} (${GetColumns_CreateTable(true) })
                                 ${if (PartitionField.length() > 0) s"PARTITIONED BY (${_PartitionCreateTable})" else "" }
                                 STORED AS ${StorageType.toString()}                                  
                                 LOCATION '${GetFullNameWithPath()}'"""
                                 
    if (huemulLib.DebugMode)
      println(s"Create Table sentence: ${CreateTableScript} ")
      
    return CreateTableScript    
  }
  
  def GetFieldName[T0](ClassInstance: Object, Field: Object): String = {
    var ResultName: String = null
    
    if (ClassInstance == this){
      val abc_1 = ClassInstance.getClass
      abc_1.getDeclaredFields().filter { z => z.setAccessible(true)
          z.get(this).isInstanceOf[T0] }.foreach { zz =>
            if (ResultName ==  null) {
              zz.setAccessible(true)
              val zze = zz.get(this).asInstanceOf[T0]
              if (zze == Field) {
                ResultName = zz.getName
              }
            }
          }  
    } else {
      if (ClassInstance.isInstanceOf[huemul_Table]) {
        ResultName = ClassInstance.asInstanceOf[huemul_Table].GetFieldName[T0](ClassInstance, Field)
      }
    }
    
    
    return ResultName
  }
  
  private def DF_ForeingKeyMasterAuto(): huemul_DataQualityResult = {
    var Result: huemul_DataQualityResult = new huemul_DataQualityResult()
    val ArrayFK = this.GetForeingKey()
    val DataBaseName = this.GetDataBase(this.DataBase)
    //For Each Foreing Key Declared
    ArrayFK.foreach { x =>
      
      //Step1: Create distinct FROM NEW DF
      var SQLFields: String = ""
      var SQLLeftJoin: String = ""
      var sqlAnd: String = ""
      var sqlComa: String = ""
      var FirstRowPK: String = ""
      var FirstRowFK: String = ""
      x.Relationship.foreach { y =>
        //Get fields name
        FirstRowPK = s"PK.${y.PK.get_MyName()}"
        FirstRowFK = y.FK.get_MyName()
        SQLFields += s"${sqlComa}${y.FK.get_MyName()}"
        SQLLeftJoin += s"${sqlAnd}PK.${y.PK.get_MyName()} = FK.${y.FK.get_MyName()}"
        sqlAnd = " and "
        sqlComa = ","
      }
      
      //val FKRuleName: String = GetFieldName[DAPI_MASTER_FK](this, x)
      val AliasDistinct: String = s"___${x.MyName}_FKRuleDist__"
      val DF_Distinct = huemulLib.DF_ExecuteQuery(AliasDistinct, s"SELECT DISTINCT ${SQLFields} FROM ${this.DataFramehuemul.Alias} ${if (x.AllowNull) s"${FirstRowFK} is not null " else "" }")
           
      //Step2: left join with TABLE MASTER DATA
      val AliasLeft: String = s"___${x.MyName}_FKRuleLeft__"
      val InstanceTable = x._Class_TableName.asInstanceOf[huemul_Table]
      val SQLLeft: String = s"""SELECT FK.* 
                                 FROM ${AliasDistinct} FK 
                                   LEFT JOIN ${InstanceTable.GetTable()} PK
                                     ON ${SQLLeftJoin} 
                                 WHERE ${FirstRowPK} IS NULL
                              """
      val DF_Left = huemulLib.DF_ExecuteQuery(AliasDistinct, SQLLeft)
      
      //Step3: Return DQ Validation
      val TotalLeft = DF_Left.count()
      if (TotalLeft > 0) {
        Result.isError = true
        Result.Description = s"huemul_Table Error: Foreing Key DQ Error, ${TotalLeft} records not found"
        Result.Error_Code = 1024
        Result.dqDF = DF_Left
        Result.profilingResult.count_all_Col = TotalLeft
        DF_Left.show()
      }
      
      
      val NumTotalDistinct = DF_Distinct.count()
      
      val Values = new huemul_DQRecord()
      Values.Table_Name =TableName
      Values.BBDD_Name =DataBaseName
      Values.DF_Alias = DataFramehuemul.Alias
      Values.ColumnName =null
      Values.DQ_Name =s"FK - ${SQLFields}"
      Values.DQ_Description =s"FK Validation: PK Table: ${InstanceTable.GetTable()} "
      Values.DQ_IsAggregate =false
      Values.DQ_RaiseError =true
      Values.DQ_SQLFormula =SQLLeft
      Values.DQ_ErrorCode = Result.Error_Code
      Values.DQ_Error_MaxNumRows =0
      Values.DQ_Error_MaxPercent =null
      Values.DQ_ResultDQ =Result.Description
      Values.DQ_NumRowsOK =NumTotalDistinct - TotalLeft
      Values.DQ_NumRowsError =TotalLeft
      Values.DQ_NumRowsTotal =NumTotalDistinct

      this.DataFramehuemul.DQ_Register(Values) 
      
      DF_Distinct.unpersist()
    }
    
    return Result
  }
  
  private def DF_DataQualityMasterAuto(): huemul_DataQualityResult = {
    //TODO: incorporar detalle de registros (muestra de 3 casos) que no cumplen cada condición
    var Result: huemul_DataQualityResult = new huemul_DataQualityResult()
    val ArrayDQ: ArrayBuffer[huemul_DataQuality] = new ArrayBuffer[huemul_DataQuality]()
    
    //All required fields have been set
    val SQL_Missing = MissingRequiredFields()
    if (SQL_Missing.length > 0) {
      Result.isError = true
      Result.Description += "huemul_Table Error: requiered fields missing"
      Result.Error_Code = 1016
      SQL_Missing.foreach { x => Result.Description +=  s",$x " }
    }
    
    //*********************************
    //Primary Key Validation
    //*********************************
    val SQL_PK: String = SQL_PrimaryKey_FinalTable()
    if (SQL_PK == "" || SQL_PK == null) {
      Result.isError = true
      Result.Description += "huemul_Table Error: PK not defined"
      Result.Error_Code = 1017
    }    
    
    if (huemulLib.DebugMode) println("DF_SAVE DQ: VALIDATE PRIMARY KEY")
    val DQ_PK = DataFramehuemul.DQ_DuplicateValues(this, SQL_PK, null, "PK")
    if (DQ_PK.isError) {
      Result.isError = true
      Result.Description += s"huemul_Table Error: PK: ${DQ_PK.Description} " 
      Result.Error_Code = 1018
    }
    
    
    //*********************************
    //Aplicar DQ según definición de campos en DataDefDQ: Unique Values   
    //*********************************
    SQL_Unique_FinalTable().foreach { x => 
      if (huemulLib.DebugMode) println(s"DF_SAVE DQ: VALIDATE UNIQUE FOR FIELD $x")
      val DQ_Unique = DataFramehuemul.DQ_DuplicateValues(this, x, null) 
      if (DQ_Unique.isError) {
        Result.isError = true
        Result.Description += s"huemul_Table Error: error Unique for field $x: ${DQ_Unique.Description} "
        Result.Error_Code = 1019
      }            
    }
    
    //Aplicar DQ según definición de campos en DataDefDQ: Acepta nulos (nullable)
    SQL_NotNull_FinalTable().foreach { x => 
      if (huemulLib.DebugMode) println(s"DF_SAVE DQ: VALIDATE NOT NULL FOR FIELD $x")
      
        val NotNullDQ : huemul_DataQuality = new huemul_DataQuality(null, false,true,s"huemul_Table Error: Not Null for field $x ",1023, s"$x IS NOT NULL")
        NotNullDQ.Error_MaxNumRows = 0
        ArrayDQ.append(NotNullDQ)
    }

  
    //VAlidación DQ máximo y mínimo largo de texto
    GetColumns().filter { x => x.DQ_MinLen != null || x.DQ_MaxLen != null  }.foreach { x => 
        var SQLFormula : String = ""

        var tand = ""
        if (x.DQ_MinLen != null){
          SQLFormula += s"length(${x.get_MyName()}) >= ${x.DQ_MinLen}"
          tand = " and "
        }
        
        if (x.DQ_MaxLen != null)
          SQLFormula += s" $tand length(${x.get_MyName()}) <= ${x.DQ_MaxLen}"
                  
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxLen : huemul_DataQuality = new huemul_DataQuality(x, false, true,s"huemul_Table Error: MinMax length Column ${x.get_MyName()}",1020,SQLFormula )
        MinMaxLen.Error_MaxNumRows = 0
        ArrayDQ.append(MinMaxLen)
    }
    
    //VAlidación DQ máximo y mínimo de números
    GetColumns().filter { x => x.DQ_MinDecimalValue != null || x.DQ_MaxDecimalValue != null  }.foreach { x => 
        
        var SQLFormula : String = ""
        var tand = ""
        if (x.DQ_MinDecimalValue != null){
          SQLFormula += s"${x.get_MyName()} >= ${x.DQ_MinDecimalValue}"
          tand = " and "
        }
        
        if (x.DQ_MaxDecimalValue != null)
          SQLFormula += s" $tand ${x.get_MyName()} <= ${x.DQ_MaxDecimalValue}"
        
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxNumber : huemul_DataQuality = new huemul_DataQuality(x, false,true,s"huemul_Table Error: MinMax Number Column ${x.get_MyName()}",1021, SQLFormula)
        MinMaxNumber.Error_MaxNumRows = 0        
        ArrayDQ.append(MinMaxNumber)
    }
    
    //VAlidación DQ máximo y mínimo de fechas
    GetColumns().filter { x => x.DQ_MinDateTimeValue != null || x.DQ_MaxDateTimeValue != null  }.foreach { x => 
        
        var SQLFormula : String = ""
        var tand = ""
        if (x.DQ_MinDateTimeValue != null){
          SQLFormula += s"${x.get_MyName()} >= '${x.DQ_MinDateTimeValue}'"
          tand = " and "
        }
        
        if (x.DQ_MaxDateTimeValue != null)
          SQLFormula += s" $tand ${x.get_MyName()} <= '${x.DQ_MaxDateTimeValue}'"
          
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxDT : huemul_DataQuality = new huemul_DataQuality(x, false,true,s"huemul_Table Error: MinMax DateTime Column ${x.get_MyName()} ",1022, SQLFormula)
        MinMaxDT.Error_MaxNumRows = 0
        ArrayDQ.append(MinMaxDT)
    }
    
    val ResultDQ = this.DataFramehuemul.DF_RunDataQuality(this.GetDataQuality(), ArrayDQ, this.DataFramehuemul.Alias, this)
    if (ResultDQ.isError){
      Result.isError = true
      Result.Description += ResultDQ.Description
      Result.Error_Code = ResultDQ.Error_Code
    }
    
    return Result
  }
  
  
 
  /**
   Create final DataFrame with full join New DF with old DF
   */
  private def DF_MDM_Dohuemul(LocalControl: huemul_Control, AliasNewData: String, isInsert: Boolean, isUpdate: Boolean, isDelete: Boolean) {
    if (TableType == huemulType_Tables.Transaction) {
      LocalControl.NewStep("Transaction: Validating fields ")
      //STEP 1: validate name setting
      getALLDeclaredFields(true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
      .foreach { x =>     
        //Get field
        var Field = x.get(this).asInstanceOf[huemul_Columns]
        
        //New value (from field or compute column )
        if (!huemulLib.HasName(Field.get_MappedName()) ){
          RaiseError(s"${x.getName()} MUST have an assigned local name",1004)
        }                 
      }
      
      //STEP 2: Create Hash
      LocalControl.NewStep("Transaction: Create Hash Field")
      val SQLFinalTable = SQL_Step0_TXHash(this.DataFramehuemul.Alias, huemulLib.ProcessNameCall)
      if (huemulLib.DebugMode && !huemulLib.HideLibQuery)
        println(SQLFinalTable)
      //STEP 2: Execute final table 
      DataFramehuemul.DF_from_SQL(AliasNewData , SQLFinalTable)
      
      LocalControl.NewStep("Transaction: Get Statistics info")
      this._NumRows_Total = this.DataFramehuemul.getNumRows
      this._NumRows_New = this.DataFramehuemul.getNumRows
      this._NumRows_Update  = 0
      this._NumRows_Delete = 0
      
      if (huemulLib.DebugMode) this.DataFramehuemul.DataFrame.show()
    } 
    else if (TableType == huemulType_Tables.Reference || TableType == huemulType_Tables.Master)
    {
      /*
       * isInsert: se aplica en SQL_Step2_UpdateAndInsert, si no permite insertar, filtra esos registros y no los inserta
       * isUpdate: se aplica en SQL_Step1_FullJoin: si no permite update, cambia el tipo ___ActionType__ de UPDATE a EQUAL
       */
      
      val OnlyInsert: Boolean = isInsert && !isUpdate
      
      //All required fields have been set
      val SQL_Missing = MissingRequiredFields()
      if (SQL_Missing.length > 0) {
        var ColumnsMissing: String = ""
        SQL_Missing.foreach { x => ColumnsMissing +=  s",$x " }
        this.RaiseError(s"huemul_Table Error: requiered fields missing ${ColumnsMissing}", 1016)
         
      }
    
      //**************************************************//
      //STEP 0: Apply distinct to New DataFrame
      //**************************************************//
      var NextAlias = this.DataFramehuemul.Alias
      
      if (ApplyDistinct) {
        LocalControl.NewStep("Ref & Master: Select distinct")
        val SQLDistinct_DF = huemulLib.DF_ExecuteQuery("__Distinct"
                                                , SQL_Step0_Distinct(this.DataFramehuemul.Alias)
                                               )
        NextAlias = "__Distinct"
      }
      
      
      //**************************************************//
      //STEP 0.1: CREATE TEMP TABLE IF MASTER TABLE DOES NOT EXIST
      //**************************************************//
      LocalControl.NewStep("Ref & Master: Select Old Table")                                             
      val TempAlias: String = s"__${this.TableName}_old"
      val fs = FileSystem.get(huemulLib.spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new org.apache.hadoop.fs.Path(this.GetFullNameWithPath()))){
        //Exist, copy for use
        
        //Open actual file
        val DFTempCopy = huemulLib.spark.read.format(this.StorageType.toString()).load(this.GetFullNameWithPath())
        val tempPath = huemulLib.GlobalSettings.GetDebugTempPath(huemulLib, huemulLib.ProcessNameCall, TempAlias)
        if (huemulLib.DebugMode) println(s"copy to temp dir: $tempPath ")
        DFTempCopy.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
        DFTempCopy.unpersist()
       
        //Open temp file
        if (huemulLib.DebugMode) println(s"open temp old df: $tempPath ")
        val DFTempOpen = huemulLib.spark.read.parquet(tempPath)        
        DFTempOpen.createOrReplaceTempView(TempAlias)        
      } else {
          println(s"create empty dataframe because ${this.GetTable()} does not exist")
          val Schema = GetSchema()
          val SchemaForEmpty = StructType(Schema.map { x => StructField(x.name, x.dataType, x.nullable) })
          val EmptyRDD = huemulLib.spark.sparkContext.emptyRDD[Row]
          val EmptyDF = huemulLib.spark.createDataFrame(EmptyRDD, SchemaForEmpty)
          EmptyDF.createOrReplaceTempView(TempAlias)
          if (huemulLib.DebugMode) EmptyDF.show()
      }
                                             
        
      //**************************************************//
      //STEP 1: Create Full Join with all fields
      //**************************************************//
      LocalControl.NewStep("Ref & Master: Full Join")
      val SQLFullJoin_DF = huemulLib.DF_ExecuteQuery("__FullJoin"
                                              , SQL_Step1_FullJoin(TempAlias, NextAlias, isUpdate, isDelete)
                                             )
                        
      //STEP 2: Create Tabla with Update and Insert result
      LocalControl.NewStep("Ref & Master: Update & Insert Logic")
      val SQLHash_p1_DF = huemulLib.DF_ExecuteQuery("__Hash_p1"
                                          , SQL_Step2_UpdateAndInsert("__FullJoin", huemulLib.ProcessNameCall, isInsert)
                                         )
      
      //STEP 3: Create Hash
      LocalControl.NewStep("Ref & Master: Hash Code")                                         
      val SQLHash_p2_DF = huemulLib.DF_ExecuteQuery("__Hash_p2"
                                          , SQL_Step3_Hash_p1("__Hash_p1")
                                         )   
               
                                        
      LocalControl.NewStep("Ref & Master: Statistics")
      //DQ for Reference and Master Data
      val DQ_ReferenceData: DataFrame = huemulLib.spark.sql(
                        s"""SELECT CAST(SUM(CASE WHEN ___ActionType__ = 'NEW' then 1 else 0 end) as Long) as __New
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'UPDATE' and SameHashKey = 0 then 1 else 0 end) as Long) as __Update
                                  ,CAST(SUM(CASE WHEN ___ActionType__ = 'DELETE' then 1 else 0 end) as Long) as __Delete
                                  ,CAST(count(1) AS Long) as __Total
                            FROM __Hash_p2 temp 
                         """)
                         
      if (huemulLib.DebugMode) DQ_ReferenceData.show()
      
      val FirstRow = DQ_ReferenceData.first()
      this._NumRows_Total = FirstRow.getAs("__Total")
      this._NumRows_New = FirstRow.getAs("__New")
      this._NumRows_Update  = FirstRow.getAs("__Update")
      this._NumRows_Delete = FirstRow.getAs("__Delete")
      
      LocalControl.NewStep("Ref & Master: Validating Insert & Update")
      var DQ_Error: String = ""
      var Error_Number: Integer = null
      if (this._NumRows_Total == this._NumRows_New)
        DQ_Error = "" //Doesn't have error, first run
      else if (this.DQ_MaxNewRecords_Num != null && this.DQ_MaxNewRecords_Num > 0 && this._NumRows_New > this.DQ_MaxNewRecords_Num){
        DQ_Error = s"huemul_Table Error: DQ MDM Error: N° New Rows (${this._NumRows_New}) exceeds max defined (${this.DQ_MaxNewRecords_Num}) "
        Error_Number = 1005
      }
      else if (this.DQ_MaxNewRecords_Perc != null && this.DQ_MaxNewRecords_Perc > 0 && (this._NumRows_New / this._NumRows_Total) > this.DQ_MaxNewRecords_Perc) {
        DQ_Error = s"huemul_Table Error: DQ MDM Error: % New Rows (${(this._NumRows_New / this._NumRows_Total)}) exceeds % max defined (${this.DQ_MaxNewRecords_Perc}) "
        Error_Number = 1006
      }

      if (DQ_Error != "")
        RaiseError(DQ_Error, Error_Number)
        
      DQ_ReferenceData.unpersist()
      
                                         
      LocalControl.NewStep("Ref & Master: Final Table")
      val SQLFinalTable = SQL_Step4_Final("__Hash_p2", huemulLib.ProcessNameCall)
     
      //STEP 2: Execute final table 
      DataFramehuemul.DF_from_SQL(AliasNewData , SQLFinalTable)
      if (huemulLib.DebugMode) this.DataFramehuemul.DataFrame.show()
      
      //Unpersist first DF
      SQLHash_p2_DF.unpersist()
      SQLHash_p1_DF.unpersist()
      SQLFullJoin_DF.unpersist()
      //SQLDistinct_DF.unpersist()
      
    } else
      RaiseError(s"huemul_Table Error: ${TableType} found, Master o Reference required ", 1007)
  }
  
  private def GetClassAndPackage(): huemul_AuthorizationPair = {
    val Invoker = new Exception().getStackTrace()(2)
    val InvokerName: String = Invoker.getClassName().replace("$", "")
    
    val ArrayResult = InvokerName.split('.')
    
    val ClassNameInvoker = ArrayResult(ArrayResult.length-1)
    val PackageNameInvoker: String = InvokerName.replace(".".concat(ClassNameInvoker), "")
    
    return new huemul_AuthorizationPair(ClassNameInvoker,PackageNameInvoker)
  }
  
  def executeFull(NewAlias: String): Boolean = {
    var Result: Boolean = false
    val whoExecute = GetClassAndPackage()  
    if (this.WhoCanRun_executeFull.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.ExecuteSave(NewAlias, true, true, true)      
    else {
      RaiseError(s"huemul_Table Error: Don't have access to executeFull in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}", 1008)
      
    }
    
    return Result
  }
  
  def executeOnlyInsert(NewAlias: String): Boolean = {
    var Result: Boolean = false
    if (this.TableType == huemulType_Tables.Transaction)
      RaiseError("huemul_Table Error: DoOnlyInserthuemul is not available for Transaction Tables",1009)

    val whoExecute = GetClassAndPackage()  
    if (this.WhoCanRun_executeOnlyInsert.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.ExecuteSave(NewAlias, true, false, false) 
    else {
      RaiseError(s"huemul_Table Error: Don't have access to executeOnlyInsert in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}", 1010)
    }
         
    return Result
  }
  
  def executeOnlyUpdate(NewAlias: String): Boolean = {   
    var Result: Boolean = false
    if (this.TableType == huemulType_Tables.Transaction)
      RaiseError("huemul_Table Error: DoOnlyUpdatehuemul is not available for Transaction Tables", 1011)
      
    val whoExecute = GetClassAndPackage()  
    if (this.WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.ExecuteSave(NewAlias, false, true, false)  
    else {
      RaiseError(s"huemul_Table Error: Don't have access to executeOnlyUpdate in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}",1012)
    }
    
    return Result
        
  }
  
  private def CompareSchema(Columns: ArrayBuffer[huemul_Columns], Schema: StructType): String = {
    var Errores: String = ""
    Columns.foreach { x => 
      if (huemulLib.HasName(x.get_MappedName())) {
        val dataType = Schema.filter { y => y.name == x.get_MappedName() }(0).dataType
        if (dataType != x.DataType) {
          Errores = Errores.concat(s"Error Column ${x.get_MappedName()}, Requiered: ${x.DataType}, actual: ${dataType}  \n")
        }
      }
    }
    
    return Errores
  }
  
  /**
   Save Full data: <br>
   Master & Reference: update and Insert
   Transactional: Delete and Insert new data
   */
  private def ExecuteSave(AliasNewData: String, IsInsert: Boolean, IsUpdate: Boolean, IsDelete: Boolean): Boolean = {
   
    var LocalControl = new huemul_Control(huemulLib, Control ,false )
    LocalControl.AddParamInfo("AliasNewData", AliasNewData)
    LocalControl.AddParamInfo("IsInsert", IsInsert.toString())
    LocalControl.AddParamInfo("IsUpdate", IsUpdate.toString())
    
    var result : Boolean = true
    var ErrorCode: Integer = null
    
    try {
      val OnlyInsert: Boolean = IsInsert && !IsUpdate
      
      //Compare schemas
      if (!autoCast) {
        LocalControl.NewStep("Compare Schema")
        val ResultCompareSchema = CompareSchema(this.GetColumns(), this.DataFramehuemul.DataFrame.schema) 
        if (ResultCompareSchema != "") {
          result = false
          ErrorCode = 1013
          RaiseError(s"huemul_Table Error: User Error: incorrect DataType: \n${ResultCompareSchema}", ErrorCode)
        }
      }
    
      //do work
      DF_MDM_Dohuemul(LocalControl, AliasNewData,IsInsert, IsUpdate, IsDelete)
  
      LocalControl.NewStep("Register Master Information ")
      Control.RegisterMASTER_CREATE_Use(this)
      
      
      //DataQuality by Columns
      LocalControl.NewStep("Start DataQuality")
      val DQResult = DF_DataQualityMasterAuto()
      //Foreing Keys by Columns
      LocalControl.NewStep("Start ForeingKey ")
      val FKResult = DF_ForeingKeyMasterAuto()
      
      LocalControl.NewStep("Validating errors ")
      var localErrorCode: Integer = null
      if (DQResult.isError || FKResult.isError) {
        result = false
        var ErrorDetail: String = ""
        if (DQResult.isError) {
          ErrorDetail = s"DataQuality Error: \n${DQResult.Description}"
          localErrorCode = DQResult.Error_Code
        }
        
        if (FKResult.isError) {
          ErrorDetail += s"\nForeing Key Validation Error: \n${FKResult.Description}"
          localErrorCode = DQResult.Error_Code
        }
              
        
        RaiseError(ErrorDetail, localErrorCode)
      }
      
      //Compare schemas final table
      LocalControl.NewStep("Compare Schema Final DF")
      val ResultCompareSchemaFinal = CompareSchema(this.GetColumns(), this.DataFramehuemul.DataFrame.schema) 
      if (ResultCompareSchemaFinal != "") {
        ErrorCode = 1014
        RaiseError(s"huemul_Table Error: User Error: incorrect DataType: \n${ResultCompareSchemaFinal}",ErrorCode)
      }
      
      //Create table persistent
      if (huemulLib.DebugMode){
        println(s"Saving ${GetTable()} Table with params: ") 
        println(s"${PartitionField} field for partitioning table")
        println(s"${GetFullNameWithPath()} path")
      }
      
      LocalControl.NewStep("Start Save ")                
      if (SavePersistent(LocalControl, DataFramehuemul.DataFrame, OnlyInsert))
        LocalControl.FinishProcessOK
      else {
        result = false
        LocalControl.FinishProcessError()
      }
      
    } catch {
      case e: Exception => 
        result = false
        if (ErrorCode == null)
          ErrorCode = 1027
        LocalControl.Control_Error.GetError(e, getClass().getSimpleName, ErrorCode)
        LocalControl.FinishProcessError()
    }
    
    return result
  }
  
  def CopyToDest(PartitionValue: String, DestEnvironment: String) {
    if (huemulLib.HasName(PartitionField)) {
       val ProdFullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath()}/${PartitionField.toLowerCase()}=${PartitionValue}")
       val ManualFullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath2(DestEnvironment)}/${PartitionField.toLowerCase()}=${PartitionValue}")
       
       val fs = FileSystem.get(huemulLib.spark.sparkContext.hadoopConfiguration)
       org.apache.hadoop.fs.FileUtil.copy(fs, ProdFullPath, fs, ManualFullPath, false, true, huemulLib.spark.sparkContext.hadoopConfiguration)
       
       val DestTableName: String = GetTable(DestEnvironment)
       println(s"MSCK REPAIR TABLE ${DestTableName}")
       huemulLib.spark.sql(s"MSCK REPAIR TABLE ${DestTableName}")
              
    } else {
       val ProdFullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath()}/${PartitionField.toLowerCase()}")
       val ManualFullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath2(DestEnvironment)}/${PartitionField.toLowerCase()}")
       
       val fs = FileSystem.get(huemulLib.spark.sparkContext.hadoopConfiguration)
       org.apache.hadoop.fs.FileUtil.copy(fs, ProdFullPath, fs, ManualFullPath, false, true, huemulLib.spark.sparkContext.hadoopConfiguration)
    }
    
  }
  
  private def SavePersistent(LocalControl: huemul_Control, DF: DataFrame, OnlyInsert: Boolean): Boolean = {
    var DF_Final = DF
    var Result: Boolean = true
    
    if (this.TableType == huemulType_Tables.Reference || this.TableType == huemulType_Tables.Master) {
      LocalControl.NewStep("Save: Drop ActionType column")
   
      if (OnlyInsert)
        DF_Final = DF_Final.where("___ActionType__ = 'NEW'") 
     
      DF_Final = DF_Final.drop("___ActionType__")
    }
      
    val sqlDrop01 = s"drop table if exists ${GetTable()}"
    if (CreateInHive ) {
      LocalControl.NewStep("Save: Drop Hive table Def")
      if (huemulLib.DebugMode && !huemulLib.HideLibQuery) println(sqlDrop01)
      try {
        huemulLib.spark.sql(sqlDrop01)
      } catch {
        case t: Throwable => println(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }
     
    }
    
    try {
      if (PartitionField == null || PartitionField == ""){
        if (OnlyInsert) {
          LocalControl.NewStep("Save: Append Master & Ref Data")
          DF_Final.write.mode(SaveMode.Append).format(this.StorageType.toString()).save(GetFullNameWithPath())
        }
        else {
          LocalControl.NewStep("Save: Overwrite Master & Ref Data")
          DF_Final.write.mode(SaveMode.Overwrite).format(this.StorageType.toString()).save(GetFullNameWithPath())
        }
        
        //val fs = FileSystem.get(huemulLib.spark.sparkContext.hadoopConfiguration)       
        //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath()), new FsPermission("770"))
      }
      else{
        //Get Partition_Id Values
        LocalControl.NewStep("Save: Validating N° partitions")
        val DFDistinct = DF_Final.select(PartitionField).distinct().withColumn(PartitionField, DF_Final.col(PartitionField).cast(StringType))
        if (DFDistinct.count() != 1){
          RaiseError(s"huemul_Table Error: N° values in partition wrong!, expected: 1, real: ${DFDistinct.count()}",1015)
        } else {
          val PartitionValue = DFDistinct.first().getAs[String](PartitionField)
          val FullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath()}/${PartitionField.toLowerCase()}=${PartitionValue}")
          
          LocalControl.NewStep("Save: Drop old partition")
          val fs = FileSystem.get(huemulLib.spark.sparkContext.hadoopConfiguration)       
          fs.delete(FullPath, true)
          LocalControl.NewStep("Save: OverWrite partition with new data")
          if (huemulLib.DebugMode) println(s"saving path: ${FullPath} ")        
          DF_Final.write.mode(SaveMode.Append).format(this.StorageType.toString()).partitionBy(PartitionField).save(GetFullNameWithPath())
                
          //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath()), new FsPermission("770"))
  
        }                       
      }
    } catch {
      case e: Exception => 
        
        this.Error_isError = true
        this.Error_Text = s"huemul_Table Error: write in disk failed. ${e.getMessage}"
        this.Error_Code = 1026
        Result = false
        LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
    }
    
    if (Result) {
      try {
        //create table
        if (CreateInHive ) {
          LocalControl.NewStep("Save: Create Table in Hive Metadata")
          DF_CreateTableScript() 
         
          huemulLib.spark.sql(CreateTableScript)
        }
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        if (CreateInHive && (PartitionField != null && PartitionField != "")) {
          LocalControl.NewStep("Save: Repair Hive Metadata")
          if (huemulLib.DebugMode) println(s"MSCK REPAIR TABLE ${GetTable()}")
          huemulLib.spark.sql(s"MSCK REPAIR TABLE ${GetTable()}")
        }
      } catch {
        case e: Exception => 
          
          this.Error_isError = true
          this.Error_Text = s"huemul_Table Error: create external table failed. ${e.getMessage}"
          this.Error_Code = 1025
          Result = false
          LocalControl.Control_Error.GetError(e, getClass.getSimpleName, this.Error_Code)
      }
    }
      
    return Result
    
  }
  
  
  def DF_from_SQL(Alias: String, sql: String, SaveInTemp: Boolean = true) {
    this.DataFramehuemul.DF_from_SQL(Alias, sql, SaveInTemp) 
  }
  
  
  def DF_from_DF(Alias: String, DF: DataFrame, SaveInTemp: Boolean = true) {
    this.DataFramehuemul.setDataFrame(DF, Alias, SaveInTemp)  
  }
  
  
  
  /**
   * Raise Error
   */
  def RaiseError(txt: String, code: Integer) {
    Error_Text = txt
    Error_isError = true
    Error_Code = code
    
    if (huemulLib.DebugMode) println(txt)
    Control.RaiseError(txt)
    //sys.error(txt)
  }  
  
  

}
