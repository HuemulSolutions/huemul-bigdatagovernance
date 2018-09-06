package com.huemulsolutions.bigdata.tables

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
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
import com.huemulsolutions.bigdata.dataquality.huemulType_DQQueryLevel._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification._
import com.huemulsolutions.bigdata.dataquality.huemul_DQRecord
//import com.sun.imageio.plugins.jpeg.DQTMarkerSegment


class huemul_Table(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends Serializable {
  if (Control == null) 
    sys.error("Control is null in huemul_DataFrame")
  
  /*  ********************************************************************************
   *****   T A B L E   P R O P E R T I E S    **************************************** 
   ******************************************************************************** */
  /**
   Table Name
   */
  val TableName : String= this.getClass.getSimpleName.replace("$", "") // ""
  println(s"HuemulControlLog: [${huemulBigDataGov.huemul_getDateForLog()}]    table instance: ${TableName}")
  
  def setDataBase(value: ArrayBuffer[huemul_KeyValuePath]) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of DataBase, definition is close", 1033)
    else
      _DataBase = value
  }
  private var _DataBase: ArrayBuffer[huemul_KeyValuePath] = null
    
  /**
   Type of Table (Reference; Master; Transaction
   */
  def setTableType(value: huemulType_Tables) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of TableType, definition is close", 1033)
    else
      _TableType = value
  }
  def getTableType: huemulType_Tables = {return _TableType}
  private var _TableType : huemulType_Tables = huemulType_Tables.Transaction
  
  /**
   Type of Persistent storage (parquet, csv, json)
   */
  def setStorageType(value: huemulType_StorageType) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of StorageType, definition is close", 1033)
    else
      _StorageType = value
  }
  def getStorageType: huemulType_StorageType = {return _StorageType}
  private var _StorageType : huemulType_StorageType = null
  
  /**
    Table description
   */
  def setDescription(value: String) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of Description, definition is close", 1033)
    else
      _Description = value
  }
  def getDescription: String = {return _Description}
  private var _Description   : String= ""
  
  /**
    Responsible contact in IT
   */
  def setIT_ResponsibleName(value: String) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of IT_ResponsibleName, definition is close", 1033)
    else
      _IT_ResponsibleName = value
  }
  def getIT_ResponsibleName: String = {return _IT_ResponsibleName}
  private var _IT_ResponsibleName   : String= ""
  
  /**
    Responsible contact in Business (Name & Area)
   */
  def setBusiness_ResponsibleName(value: String) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of Business_ResponsibleName, definition is close", 1033)
    else
      _Business_ResponsibleName = value
  }
  def getBusiness_ResponsibleName: String = {return _Business_ResponsibleName}
  private var _Business_ResponsibleName   : String= ""
  
  /**
    Fields used to partition table 
   */
  def setPartitionField(value: String) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of PartitionField, definition is close", 1033)
    else
      _PartitionField = value
  }
  def getPartitionField: String = {return _PartitionField}
  private var _PartitionField   : String= null
  
  /**
    Local name (example "SBIF\\{{YYYY}}{{MM}}\\"
   */
  def setLocalPath(value: String) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of LocalPath, definition is close", 1033)
    else
      _LocalPath = value
  }
  def getLocalPath: String = {return _LocalPath}
  private var _LocalPath   : String= ""
  
  def setGlobalPaths(value: ArrayBuffer[huemul_KeyValuePath]) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of GlobalPaths, definition is close", 1033)
    else
      _GlobalPaths = value
  }
  def getGlobalPaths: ArrayBuffer[huemul_KeyValuePath] = {return _GlobalPaths}
  private var _GlobalPaths: ArrayBuffer[huemul_KeyValuePath] = null
  
  def WhoCanRun_executeFull_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of WhoCanRun_executeFull, definition is close", 1033)
    else
      _WhoCanRun_executeFull.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeFull: huemul_Authorization = {return _WhoCanRun_executeFull}
  private var _WhoCanRun_executeFull: huemul_Authorization = new huemul_Authorization()
  
  def WhoCanRun_executeOnlyInsert_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of WhoCanRun_executeOnlyInsert, definition is close", 1033)
    else
      _WhoCanRun_executeOnlyInsert.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeOnlyInsert: huemul_Authorization = {return _WhoCanRun_executeOnlyInsert}
  private var _WhoCanRun_executeOnlyInsert: huemul_Authorization = new huemul_Authorization()
  
  def WhoCanRun_executeOnlyUpdate_addAccess(ClassName: String, PackageName: String) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of WhoCanRun_executeOnlyUpdate, definition is close", 1033)
    else
      _WhoCanRun_executeOnlyUpdate.AddAccess(ClassName, PackageName)
  }
  def getWhoCanRun_executeOnlyUpdate: huemul_Authorization = {return _WhoCanRun_executeOnlyUpdate}
  private var _WhoCanRun_executeOnlyUpdate: huemul_Authorization = new huemul_Authorization()
  
  /**
   * Automatically map query names
   */
  def setMappingAuto() {
    getALLDeclaredFields(true).foreach { x =>  
      x.setAccessible(true)
          
      //Nombre de campos
      if (x.get(this).isInstanceOf[huemul_Columns]) {
        val DataField = x.get(this).asInstanceOf[huemul_Columns]
        DataField.SetMapping(DataField.get_MyName())
      }
    }
  }
    

  /**
   DataQuality: max N° records, null does'nt apply  DQ , 0 value doesn't accept new records (raiseError if new record found)
   If table is empty, DQ doesn't apply
   */
  def setDQ_MaxNewRecords_Num(value: Long) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of DQ_MaxNewRecords_Num, definition is close", 1033)
    else
      _DQ_MaxNewRecords_Num = value
  }
  def getDQ_MaxNewRecords_Num: Long = {return _DQ_MaxNewRecords_Num}
  private var _DQ_MaxNewRecords_Num: Long = null
  /**
   DataQuality: Max % new records vs old records, null does'n apply DQ, 0% doesn't accept new records (raiseError if new record found), 100%-> 0.1 accept double old records)
   */
  def setDQ_MaxNewRecords_Perc(value: Decimal) {
    if (DefinitionIsClose)
      this.RaiseError("You can't change value of DQ_MaxNewRecords_Perc, definition is close", 1033)
    else
      _DQ_MaxNewRecords_Perc = value
  }
  def getDQ_MaxNewRecords_Perc: Decimal = {return _DQ_MaxNewRecords_Perc}
  private var _DQ_MaxNewRecords_Perc: Decimal = null
  
  
  
  
  /****** METODOS DEL LADO DEL "USUARIO" **************************/
  
  private var autoCast: Boolean = true
  def setAutoCast(value: Boolean) {autoCast = value}
  
  private var ApplyDistinct: Boolean = true
  def setApplyDistinct(value: Boolean) {ApplyDistinct = value}
  
  
  private var Table_id: String = ""
  
  
  
  private var CreateInHive: Boolean = true
  private var CreateTableScript: String = ""
  
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
  private var DefinitionIsClose: Boolean = false
  
  /***
   * from Global path (small files, large files, DataBase, etc)
   */
  def GlobalPath()  : String= {
    return GetPath(_GlobalPaths)
  }
  
  def GlobalPath(ManualEnvironment: String)  : String= {
    return GetPath(_GlobalPaths, ManualEnvironment)
  }
  
  
    
  def GetFullNameWithPath() : String = {
    return GlobalPath + _LocalPath + TableName
  }
  
  def GetFullNameWithPath2(ManualEnvironment: String) : String = {
    return GlobalPath(ManualEnvironment) + _LocalPath + TableName
  }
    
  def GetFullPath() : String = {
    return GlobalPath + _LocalPath 
  }
  
  
  /**
   * Return DataBaseName.TableName
   */
  def GetTable(): String = {
    return s"${GetDataBase(_DataBase)}.${TableName}"
  }
  
  def GetCurrentDataBase(): String = {
    return s"${GetDataBase(_DataBase)}"
  }
  
  /**
   * Return DataBaseName.TableName
   */
  def GetTable(ManualEnvironment: String): String = {
    return s"${GetDataBase(this._DataBase, ManualEnvironment)}.${TableName}"
  }
  
  
  def GetDataBase(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    return huemulBigDataGov.GlobalSettings.GetDataBase(huemulBigDataGov, Division)  
  }
  
  def GetDataBase(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    return huemulBigDataGov.GlobalSettings.GetDataBase(huemulBigDataGov, Division, ManualEnvironment)  
  }
  
  def GetPath(Division: ArrayBuffer[huemul_KeyValuePath]): String = {
    return huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov, Division)  
  }
  
  def GetPath(Division: ArrayBuffer[huemul_KeyValuePath], ManualEnvironment: String): String = {
    return huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov, Division, ManualEnvironment)  
  }
  
  
  
  /**
   get execution's info about rows and DF
   */
  var DataFramehuemul: huemul_DataFrame = new huemul_DataFrame(huemulBigDataGov, Control)  

   
  var Error_isError: Boolean = false
  var Error_Text: String = ""
  var Error_Code: Integer = null
  //var HasColumns: Boolean = false
  var HasPK: Boolean = false
  
  def ApplyTableDefinition(): Boolean = {
    if (huemulBigDataGov.DebugMode) println(s"HuemulControlLog: [${huemulBigDataGov.huemul_getDateForLog()}] starting ApplyTableDefinition")
    if (this._PartitionField == null)
      _PartitionField = ""
      
    if (this._GlobalPaths == null)
      RaiseError(s"huemul_Table Error: GlobalPaths must be defined",1000)
      
    if (this._LocalPath == null)
      RaiseError(s"huemul_Table Error: LocalPath must be defined",1001)
      
    if (this._StorageType == null)
      RaiseError(s"huemul_Table Error: StorageType must be defined",1002)
      
    if (this._TableType == null)
      RaiseError(s"huemul_Table Error: TableType must be defined",1034)
    else if (this._TableType == huemulType_Tables.Transaction && _PartitionField == "")
      RaiseError(s"huemul_Table Error: PartitionField should be defined if TableType is Transactional",1035)
    else if (this._TableType != huemulType_Tables.Transaction && _PartitionField != "")
      RaiseError(s"huemul_Table Error: PartitionField shouldn't be defined if TableType is ${this._TableType}",1036)
      
    if (this._DataBase == null)
      RaiseError(s"huemul_Table Error: DataBase must be defined",1037)
    
      
    
      
    getALLDeclaredFields().filter { x => x.setAccessible(true) 
                x.get(this).isInstanceOf[huemul_Columns] || x.get(this).isInstanceOf[huemul_DataQuality] || x.get(this).isInstanceOf[huemul_Table_Relationship]  
    } foreach { x =>
      x.setAccessible(true)
          
      //Nombre de campos
      if (x.get(this).isInstanceOf[huemul_Columns]) {
        val DataField = x.get(this).asInstanceOf[huemul_Columns]
        DataField.Set_MyName(x.getName)
        
        if (DataField.getDQ_MaxLen != null && DataField.getDQ_MaxLen < 0)
          RaiseError(s"Error column ${x.getName}: DQ_MaxLen must be positive",1003)
        else if (DataField.getDQ_MaxLen != null && DataField.getDQ_MinLen != null && DataField.getDQ_MaxLen < DataField.getDQ_MinLen)
          RaiseError(s"Error column ${x.getName}: DQ_MinLen(${DataField.getDQ_MinLen}) must be less than DQ_MaxLen(${DataField.getDQ_MaxLen})",1028)
        else if (DataField.getDQ_MaxDecimalValue != null && DataField.getDQ_MinDecimalValue != null && DataField.getDQ_MaxDecimalValue < DataField.getDQ_MinDecimalValue)
          RaiseError(s"Error column ${x.getName}: DQ_MinDecimalValue(${DataField.getDQ_MinDecimalValue}) must be less than DQ_MaxDecimalValue(${DataField.getDQ_MaxDecimalValue})",1029)
        else if (DataField.getDQ_MaxDateTimeValue != null && DataField.getDQ_MinDateTimeValue != null && DataField.getDQ_MaxDateTimeValue < DataField.getDQ_MinDateTimeValue)
          RaiseError(s"Error column ${x.getName}: DQ_MinDateTimeValue(${DataField.getDQ_MinDateTimeValue}) must be less than DQ_MaxDateTimeValue(${DataField.getDQ_MaxDateTimeValue})",1030)
        else if (DataField.getDefaultValue != null && DataField.DataType == StringType && DataField.getDefaultValue.toUpperCase() != "NULL" && !DataField.getDefaultValue.contains("'"))
          RaiseError(s"Error column ${x.getName}: DefaultValue  must be like this: 'something', not something wihtout ')",1031)
          
        if (DataField.getIsPK) {
          DataField.setNullable(false)
          HasPK = true
          
          if (DataField.getMDM_EnableDTLog || DataField.getMDM_EnableOldValue || DataField.getMDM_EnableProcessLog) {
            RaiseError(s"Error column ${x.getName}:, is PK, can't enabled MDM_EnableDTLog, MDM_EnableOldValue or MDM_EnableProcessLog",1040)
          }
        }
        
        DataField.SetDefinitionIsClose()
        
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
    if (huemulBigDataGov.DebugMode) println(s"HuemulControlLog: [${huemulBigDataGov.huemul_getDateForLog()}] register metadata")
    //Register TableName and fields
    Control.RegisterMASTER_CREATE_Basic(this)
    if (huemulBigDataGov.DebugMode) println(s"HuemulControlLog: [${huemulBigDataGov.huemul_getDateForLog()}] end ApplyTableDefinition")
    if (!HasPK) this.RaiseError("huemul_Table Error: PK not defined", 1017)
    DefinitionIsClose = true
    return true
  }
  
  
  private def getALLDeclaredFields(OnlyUserDefined: Boolean = false) : Array[java.lang.reflect.Field] = {
    val pClass = getClass()  
    
    val a = pClass.getDeclaredFields()
    var c = a
    if (!OnlyUserDefined){
      var b = pClass.getSuperclass().getDeclaredFields()
      
      if (this._TableType == huemulType_Tables.Transaction) 
        b = b.filter { x => x.getName != "MDM_ProcessChange" && x.getName != "MDM_fhChange" && x.getName != "MDM_StatusReg"  }       
      
      c = a.union(b)  
    }

    
    return c
  }
  
  private var _NumRows_New: Long = null
  private var _NumRows_Update: Long = null
  private var _NumRows_Updatable: Long = null
  private var _NumRows_Delete: Long = null
  private var _NumRows_Total: Long = null
  private var _NumRows_NoChange: Long = null
  
  def NumRows_New(): Long = {
    return _NumRows_New
  }
  
  def NumRows_Update(): Long = {
    return _NumRows_Update
  }
  
  def NumRows_Updatable(): Long = {
    return _NumRows_Updatable
  }
  
  def NumRows_Delete(): Long = {
    return _NumRows_Delete
  }
  
  def NumRows_Total(): Long = {
    return _NumRows_Total
  }
  
  def NumRows_NoChange(): Long = {
    return _NumRows_NoChange
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
      fieldsStruct.append( StructField(x.getName, Field.DataType , nullable = Field.getNullable , null))

      if (Field.getMDM_EnableOldValue) {
        fieldsStruct.append( StructField(x.getName + "_old", Field.DataType , nullable = true , null))
      }
      if (Field.getMDM_EnableDTLog) {
        fieldsStruct.append( StructField(x.getName + "_fhChange", TimestampType , nullable = true , null))
      }
      if (Field.getMDM_EnableProcessLog) {
        fieldsStruct.append( StructField(x.getName + "_ProcessLog", StringType , nullable = true , null))
      }
      
    }
    if (huemulBigDataGov.DebugMode) println(s"N° Total: ${fieldsStruct.length}")
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
                      
      
      if (Field.getMDM_EnableOldValue) {
        val MDM_EnableOldValue = new huemul_Columns(Field.DataType, false, s"Old value for ${x.getName}")
        MDM_EnableOldValue.Set_MyName(s"${x.getName}_Old")
        Result.append(MDM_EnableOldValue)        
      } 
      if (Field.getMDM_EnableDTLog){
        val MDM_EnableDTLog = new huemul_Columns(TimestampType, false, s"Last change DT for ${x.getName}")
        MDM_EnableDTLog.Set_MyName(s"${x.getName}_fhChange")
        Result.append(MDM_EnableDTLog)                
      } 
      if (Field.getMDM_EnableProcessLog) {
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
      if (_PartitionField != null && _PartitionField.toUpperCase() != x.getName.toUpperCase()) {
        ColumnsCreateTable += s"$coma${x.getName} ${DataTypeLocal} \n"
        coma = ","
      }
      
      if (Field.getMDM_EnableOldValue)
        ColumnsCreateTable += s"$coma${x.getName}_old ${DataTypeLocal} \n"  
      if (Field.getMDM_EnableDTLog) 
        ColumnsCreateTable += s"$coma${x.getName}_fhChange ${TimestampType.sql} \n"  
      if (Field.getMDM_EnableProcessLog) 
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
                                      x.get(this).asInstanceOf[huemul_Columns].getIsPK }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      //All PK columns shouldn't be null
      Field.setNullable(false)
      
      if (!huemulBigDataGov.HasName(Field.get_MappedName()))
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
    
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      val NewColumnCast = ApplyAutoCast(if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"${Field.get_SQLForInsert()} " else s"New.${Field.get_MappedName()}"
                                        ,Field.DataType.sql)
        
     
      //New value (from field or compute column )
      if (huemulBigDataGov.HasName(Field.get_MappedName()) ){
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
        StringSQL_hash += s"""${coma_hash}${if (Field.getNullable) s"coalesce(${NewColumnCast},'null')" else NewColumnCast}"""
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
      if (huemulBigDataGov.HasName(MappedField)) {        
        if (Distintos.filter { x => x == MappedField }.length == 0){
          Distintos.append(MappedField)
          StringSQL += s"${coma}${MappedField}  \n"
          coma = "," 
        }
      }
    }
      
    //Aditional field from new table
    if (huemulBigDataGov.HasName(AdditionalRowsForDistint))
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
  private def SQL_Step2_LeftJoin(OfficialAlias: String, NewAlias: String): String = {
    //Get fields in old table
    val OfficialColumns = huemulBigDataGov.spark.catalog.listColumns(OfficialAlias).collect()
       
    var StringSQL_LeftJoin: String = ""
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
      if (Field.getIsPK){
        val NewPKSentence = if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} )" else NewColumnCast
        StringSQL_LeftJoin += s"${coma}Old.${x.getName} AS ${x.getName} \n"
        StringSQl_PK += s" $sand Old.${x.getName} = ${NewPKSentence}  " 
        sand = " and "

        if (StringSQL_ActionType == "")
          StringSQL_ActionType = s" case when Old.${x.getName} is null then 'NEW' when ${NewColumnCast} is null then 'EQUAL' else 'UPDATE' END as ___ActionType__  "
      } else {
        //¿column exist in DataFrame?
        val columnExist = !(OfficialColumns.filter { y => y.name == x.getName}.length == 0)
        
        //String for new DataFrame fulljoin
        if (columnExist){
          StringSQL_LeftJoin += s"${coma}cast(old.${x.getName} as ${Field.DataType.sql} ) as old_${x.getName} \n"
          //StringSQL_LeftJoin += s"${coma}old.${x.getName} as old_${x.getName} \n"
        }
        else
          StringSQL_LeftJoin += s"${coma}cast(null as ${Field.DataType.sql} ) as old_${x.getName} \n"
        
        //New value (from field or compute column )
        if (huemulBigDataGov.HasName(Field.get_MappedName()))
          StringSQL_LeftJoin += s",${NewColumnCast} as new_${x.getName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForInsert()))
          StringSQL_LeftJoin += s",CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} ) as new_insert_${x.getName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForUpdate()))
          StringSQL_LeftJoin += s",CAST(${Field.get_SQLForUpdate()} as ${Field.DataType.sql} ) as new_update_${x.getName} \n"
          
        if (Field.getMDM_EnableOldValue || Field.getMDM_EnableDTLog || Field.getMDM_EnableProcessLog) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.HasName(Field.get_MappedName())) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) Field.get_SQLForUpdate() else "new.".concat(Field.get_MappedName()),Field.DataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(x.getName) else "null"
            StringSQL_LeftJoin += s",CAST(CASE WHEN ${NewFieldTXT} = ${OldFieldTXT} or (${NewFieldTXT} is null and ${OldFieldTXT} is null) THEN 0 ELSE 1 END as Integer ) as __Change_${x.getName}  \n"
          }
          else {
            StringSQL_LeftJoin += s",CAST(0 as Integer ) as __Change_${x.getName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae, 
            //la solución es poner el campo "tiene cambio" en falso
          }
              
        }
      
        if (Field.getMDM_EnableOldValue){
          if (OfficialColumns.filter { y => y.name == s"${x.getName}_old"}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${x.getName}_old AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
        }
        if (Field.getMDM_EnableDTLog){
          if (OfficialColumns.filter { y => y.name == s"${x.getName}_fhChange"}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS TimeStamp) as old_${x.getName}_fhChange \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${x.getName}_fhChange AS TimeStamp) as old_${x.getName}_fhChange \n"
        }
        if (Field.getMDM_EnableProcessLog){
          if (OfficialColumns.filter { y => y.name == s"${x.getName}_ProcessLog"}.length == 0) //no existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(null AS STRING) as old_${x.getName}_ProcessLog \n"
          else //existe columna en dataframe
            StringSQL_LeftJoin += s",CAST(old.${x.getName}_ProcessLog AS STRING) as old_${x.getName}_ProcessLog \n"
        }   
          
      }
       
      coma = ","
    }
    
    
    //Step 1: full join of both DataSet, old for actual recordset, new for new DataFrame
    StringSQL_LeftJoin = s"""SELECT ${StringSQL_LeftJoin}
                                   ,${StringSQL_ActionType}
                             FROM $OfficialAlias Old
                                LEFT JOIN ${NewAlias} New \n 
                                   on $StringSQl_PK  
                          """
       
    return StringSQL_LeftJoin 
  }
  
  
  
  /**
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA
   */
  private def SQL_Step1_FullJoin(OfficialAlias: String, NewAlias: String, isUpdate: Boolean, isDelete: Boolean): String = {
    //Get fields in old table
    val OfficialColumns = huemulBigDataGov.spark.catalog.listColumns(OfficialAlias).collect()
       
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
      if (Field.getIsPK){
        val NewPKSentence = if (huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} )" else NewColumnCast
        StringSQL_FullJoin += s"${coma}CAST(coalesce(Old.${x.getName}, ${NewPKSentence}) as ${Field.DataType.sql}) AS ${x.getName} \n"
        StringSQl_PK += s" $sand Old.${x.getName} = ${NewPKSentence}  " 
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
        if (huemulBigDataGov.HasName(Field.get_MappedName()))
          StringSQL_FullJoin += s",${NewColumnCast} as new_${x.getName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForInsert()))
          StringSQL_FullJoin += s",CAST(${Field.get_SQLForInsert()} as ${Field.DataType.sql} ) as new_insert_${x.getName} \n"
        if (huemulBigDataGov.HasName(Field.get_SQLForUpdate()))
          StringSQL_FullJoin += s",CAST(${Field.get_SQLForUpdate()} as ${Field.DataType.sql} ) as new_update_${x.getName} \n"
          
        if (Field.getMDM_EnableOldValue || Field.getMDM_EnableDTLog || Field.getMDM_EnableProcessLog) {
          //Change field, take update field if exist, otherwise use get_name()
          if (huemulBigDataGov.HasName(Field.get_MappedName())) {
            val NewFieldTXT = ApplyAutoCast(if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) Field.get_SQLForUpdate() else "new.".concat(Field.get_MappedName()),Field.DataType.sql)
            val OldFieldTXT = if (columnExist) "old.".concat(x.getName) else "null"
            StringSQL_FullJoin += s",CAST(CASE WHEN ${NewFieldTXT} = ${OldFieldTXT} or (${NewFieldTXT} is null and ${OldFieldTXT} is null) THEN 0 ELSE 1 END as Integer ) as __Change_${x.getName}  \n"
          }
          else {
            StringSQL_FullJoin += s",CAST(0 as Integer ) as __Change_${x.getName}  \n"
            //Cambio aplicado el 8 de agosto, al no tener campo del df mapeado se cae, 
            //la solución es poner el campo "tiene cambio" en falso
          }
              
        }
      
        if (Field.getMDM_EnableOldValue){
          if (OfficialColumns.filter { y => y.name == s"${x.getName}_old"}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${x.getName}_old AS ${Field.DataType.sql}) as old_${x.getName}_old \n"
        }
        if (Field.getMDM_EnableDTLog){
          if (OfficialColumns.filter { y => y.name == s"${x.getName}_fhChange"}.length == 0) //no existe columna en dataframe
            StringSQL_FullJoin += s",CAST(null AS TimeStamp) as old_${x.getName}_fhChange \n"
          else //existe columna en dataframe
            StringSQL_FullJoin += s",CAST(old.${x.getName}_fhChange AS TimeStamp) as old_${x.getName}_fhChange \n"
        }
        if (Field.getMDM_EnableProcessLog){
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
  CREATE SQL SCRIPT LEFT JOIN 
   */
  private def SQL_Step4_Update(NewAlias: String, ProcessName: String): String = {
    
    var StringSQL: String = "SELECT "
    
    var coma: String = ""
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      //string for key on
      if (Field.getIsPK){
        StringSQL += s" ${coma}${x.getName} as ${x.getName} \n"
        
      } else {
        if (   x.getName == "MDM_fhNew" || x.getName == "MDM_ProcessNew" || x.getName == "MDM_fhChange"
            || x.getName == "MDM_ProcessChange" || x.getName == "MDM_StatusReg" 
            || x.getName == "MDM_hash")
          StringSQL += s"${coma}old_${x.getName}  \n"
        else {          
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) s"new_update_${x.getName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.get_ReplaceValueOnUpdate() && this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${x.getName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${x.getName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${x.getName} END  as ${x.getName} \n"""
                
           if (Field.getMDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN old_${x.getName} ELSE old_${x.getName}_old END as ${x.getName}_old \n"""
           if (Field.getMDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN now() ELSE old_${x.getName}_fhChange END AS TimeStamp) as ${x.getName}_fhChange \n"""
           if (Field.getMDM_EnableProcessLog)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN '${ProcessName}' WHEN ___ActionType__ = 'NEW' THEN '${ProcessName}' ELSE old_${x.getName}_ProcessLog END as ${x.getName}_ProcessLog \n"""             
        }
      }
                         
      coma = ","
    }
                
    
    //with previous dataset, make statistics about N° rows new vs old dataset, alerts.
    StringSQL += s""" ,___ActionType__
                      FROM $NewAlias New 
                  """
    return StringSQL 
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
      if (Field.getIsPK){
        StringSQL += s" ${coma}${x.getName} as ${x.getName} \n"
        
      } else {
        if (   x.getName == "MDM_fhNew" || x.getName == "MDM_ProcessNew" || x.getName == "MDM_fhChange"
            || x.getName == "MDM_ProcessChange" || x.getName == "MDM_StatusReg" 
            || x.getName == "MDM_hash")
          StringSQL += s"${coma}old_${x.getName}  \n"
        else {          
          StringSQL += s""" ${coma}CASE WHEN ___ActionType__ = 'NEW'    THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForInsert())) s"new_insert_${x.getName}" //si tiene valor en SQL insert, lo usa
                                                                               else if (this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${x.getName}"     //si no, si tiene nombre de campo en DataFrame nuevo, lo usa 
                                                                               else ApplyAutoCast(Field.getDefaultValue,Field.DataType.sql)                        //si no tiene campo asignado, pone valor por default
                                                                           }
                                        WHEN ___ActionType__ = 'UPDATE' THEN ${if (this.huemulBigDataGov.HasName(Field.get_SQLForUpdate())) s"new_update_${x.getName}"  //si tiene valor en SQL update, lo usa
                                                                               else if (Field.get_ReplaceValueOnUpdate() && this.huemulBigDataGov.HasName(Field.get_MappedName())) s"new_${x.getName}"  //Si hay que reemplaar el valor antiguo con uno nuevo, y está seteado un campo en del dataframe, lo usa
                                                                               else s"old_${x.getName}"   //de lo contrario deja el valor antiguo
                                                                           }
                                        ELSE old_${x.getName} END  as ${x.getName} \n"""
                
           if (Field.getMDM_EnableOldValue)
             StringSQL += s""",CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN old_${x.getName} ELSE old_${x.getName}_old END as ${x.getName}_old \n"""
           if (Field.getMDM_EnableDTLog)
             StringSQL += s""",CAST(CASE WHEN ___ActionType__ = 'UPDATE' AND __Change_${x.getName} = 1 THEN now() ELSE old_${x.getName}_fhChange END AS TimeStamp) as ${x.getName}_fhChange \n"""
           if (Field.getMDM_EnableProcessLog)
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
  CREATE SQL SCRIPT FULL JOIN MASTER DATA OR REFERENCE DATA and for Selective update
   */
  private def SQL_Step3_Hash_p1(NewAlias: String, isSelectiveUpdate: Boolean): String = {
    
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
        
      if (Field.getMDM_EnableOldValue)
        StringSQL += s""",${x.getName}_old \n"""
      if (Field.getMDM_EnableDTLog)
        StringSQL += s""",${x.getName}_fhChange \n"""
      if (Field.getMDM_EnableProcessLog)
        StringSQL += s""",${x.getName}_ProcessLog \n"""
       
      if (Field.UsedForCheckSum) {
        StringSQL_hash += s"""${coma_hash}${if (Field.getNullable) s"coalesce(${x.getName},'null')" else x.getName}"""
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
    
    if (this._TableType == huemulType_Tables.Reference || this._TableType == huemulType_Tables.Master || isSelectiveUpdate) {
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
        
      if (Field.getMDM_EnableOldValue)
        StringSQL += s""",${x.getName}_old \n"""
      if (Field.getMDM_EnableDTLog)
        StringSQL += s""",${x.getName}_fhChange \n"""
      if (Field.getMDM_EnableProcessLog)
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
                                         x.get(this).asInstanceOf[huemul_Columns].getIsUnique && huemulBigDataGov.HasName(x.get(this).asInstanceOf[huemul_Columns].get_MappedName)}
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
  private def SQL_NotNull_FinalTable(): ArrayBuffer[huemul_Columns] = {
    var StringSQL: ArrayBuffer[huemul_Columns] = new ArrayBuffer[huemul_Columns]()
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                          x.get(this).isInstanceOf[huemul_Columns] &&
                                         !x.get(this).asInstanceOf[huemul_Columns].getNullable && huemulBigDataGov.HasName(x.get(this).asInstanceOf[huemul_Columns].get_MappedName)}
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      StringSQL.append(Field)
    }
       
    return StringSQL 
  }
  
  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required 
   */
  private def MissingRequiredFields(IsSelectiveUpdate: Boolean): ArrayBuffer[String] = {
    var StringSQL: ArrayBuffer[String] = new ArrayBuffer[String]()
    if (IsSelectiveUpdate) return StringSQL
    
    
    getALLDeclaredFields(true).filter { x => x.setAccessible(true)                                         
                                         x.get(this).isInstanceOf[huemul_Columns] &&
                                         x.get(this).asInstanceOf[huemul_Columns].Required  
                                          }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      //println(s"${Field.get_MyName()} Field.get_MappedName: ${Field.get_MappedName}, Field.get_SQLForUpdate(): ${Field.get_SQLForUpdate()}, Field.get_SQLForInsert(): ${Field.get_SQLForInsert()}")
      
      var isOK: Boolean = false
      if (huemulBigDataGov.HasName(Field.get_MappedName))
        isOK = true
      else if (!huemulBigDataGov.HasName(Field.get_MappedName) && 
              (huemulBigDataGov.HasName(Field.get_SQLForUpdate()) && huemulBigDataGov.HasName(Field.get_SQLForInsert())))
        isOK = true
      else
        isOK = false
        
      if (!isOK)
        StringSQL.append(x.getName)
    }
       
    return StringSQL 
  }
  
  /**
  GET ALL REQUIRED ATTRIBUTES
  Return all fields missing that have been required 
   */
  private def MissingRequiredFields_SelectiveUpdate(): String = {
    var PKNotMapped: String = ""
    var OneColumnMapped: Boolean = false
    //STEP 1: validate name setting
    getALLDeclaredFields(true).filter { x => x.setAccessible(true)
                                    x.get(this).isInstanceOf[huemul_Columns]}
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      if (Field.getIsPK) {
        
        var isOK: Boolean = false
        if (huemulBigDataGov.HasName(Field.get_MappedName))
          isOK = true
        else if (!huemulBigDataGov.HasName(Field.get_MappedName) && 
                (huemulBigDataGov.HasName(Field.get_SQLForUpdate()) && huemulBigDataGov.HasName(Field.get_SQLForInsert())))
          isOK = true
        else
          isOK = false
          
        if (!isOK) {
          PKNotMapped = s"${if (PKNotMapped == "") "" else ", "}${Field.get_MyName()} "
        }
        
      } else 
        OneColumnMapped = true
                
    }
    
    if (!OneColumnMapped)
        RaiseError(s"huemul_Table Error: Only PK mapped, no columns mapped for update",1042)
    
    return PKNotMapped
  }
  
  
  /*  ********************************************************************************
   *****   T A B L E   M E T H O D S    **************************************** 
   ******************************************************************************** */
  
  private def DF_CreateTableScript(): String = {
              
    var coma_partition = ""
    var PartitionForCreateTable = ""
    //Get SQL DataType for Partition Columns
    getALLDeclaredFields().filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
    .foreach { x =>     
      //Get field
      var Field = x.get(this).asInstanceOf[huemul_Columns]
      
      if (_PartitionField.toUpperCase() == x.getName().toUpperCase() ) {
          PartitionForCreateTable += s"${coma_partition}${_PartitionField} ${Field.DataType.sql}"
          coma_partition = ","
      }
    }
    
    //get from: https://docs.databricks.com/user-guide/tables.html (see Create Partitioned Table section)
    CreateTableScript = s"""
                                 CREATE EXTERNAL TABLE IF NOT EXISTS ${GetTable()} (${GetColumns_CreateTable(true) })
                                 ${if (_PartitionField.length() > 0) s"PARTITIONED BY (${PartitionForCreateTable})" else "" }
                                 STORED AS ${_StorageType.toString()}                                  
                                 LOCATION '${GetFullNameWithPath()}'"""
                                 
    if (huemulBigDataGov.DebugMode)
      println(s"Create Table sentence: ${CreateTableScript} ")
      
    return CreateTableScript    
  }
  
  /* //este metodo retorna el nombre de un objeto
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
  */
  
  private def DF_ForeingKeyMasterAuto(): huemul_DataQualityResult = {
    var Result: huemul_DataQualityResult = new huemul_DataQualityResult()
    val ArrayFK = this.GetForeingKey()
    val DataBaseName = this.GetDataBase(this._DataBase)
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
      val AliasDistinct_B: String = s"___${x.MyName}_FKRuleDistB__"
      val DF_Distinct = huemulBigDataGov.DF_ExecuteQuery(AliasDistinct_B, s"SELECT DISTINCT ${SQLFields} FROM ${this.DataFramehuemul.Alias} ${if (x.AllowNull) s" WHERE ${FirstRowFK} is not null " else "" }")
           
      //Step2: left join with TABLE MASTER DATA
      val AliasLeft: String = s"___${x.MyName}_FKRuleLeft__"
      val InstanceTable = x._Class_TableName.asInstanceOf[huemul_Table]
      val SQLLeft: String = s"""SELECT FK.* 
                                 FROM ${AliasDistinct_B} FK 
                                   LEFT JOIN ${InstanceTable.GetTable()} PK
                                     ON ${SQLLeftJoin} 
                                 WHERE ${FirstRowPK} IS NULL
                              """
                                 
      val AliasDistinct: String = s"___${x.MyName}_FKRuleDist__"
      val DF_Left = huemulBigDataGov.DF_ExecuteQuery(AliasDistinct, SQLLeft)
      
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
      Values.DQ_QueryLevel = huemulType_DQQueryLevel.Row // IsAggregate =false
      Values.DQ_Notification = huemulType_DQNotification.ERROR// RaiseError =true
      Values.DQ_SQLFormula =SQLLeft
      Values.DQ_ErrorCode = Result.Error_Code
      Values.DQ_toleranceError_Rows =0
      Values.DQ_toleranceError_Percent =null
      Values.DQ_ResultDQ =Result.Description
      Values.DQ_NumRowsOK =NumTotalDistinct - TotalLeft
      Values.DQ_NumRowsError =TotalLeft
      Values.DQ_NumRowsTotal =NumTotalDistinct
      Values.DQ_IsError = Result.isError

      this.DataFramehuemul.DQ_Register(Values) 
      
      DF_Distinct.unpersist()
    }
    
    return Result
  }
  
  private def DF_DataQualityMasterAuto(IsSelectiveUpdate: Boolean): huemul_DataQualityResult = {
    //TODO: incorporar detalle de registros (muestra de 3 casos) que no cumplen cada condición
    var Result: huemul_DataQualityResult = new huemul_DataQualityResult()
    val ArrayDQ: ArrayBuffer[huemul_DataQuality] = new ArrayBuffer[huemul_DataQuality]()
    
    //All required fields have been set
    val SQL_Missing = MissingRequiredFields(IsSelectiveUpdate)
    if (SQL_Missing.length > 0) {
      Result.isError = true
      Result.Description += "\nhuemul_Table Error: requiered columns missing "
      Result.Error_Code = 1016
      SQL_Missing.foreach { x => Result.Description +=  s",$x " }
    }
    
    //*********************************
    //Primary Key Validation
    //*********************************
    val SQL_PK: String = SQL_PrimaryKey_FinalTable()
    if (SQL_PK == "" || SQL_PK == null) {
      Result.isError = true
      Result.Description += "\nhuemul_Table Error: PK not defined"
      Result.Error_Code = 1017
    }    
    
    if (huemulBigDataGov.DebugMode) println("DF_SAVE DQ: VALIDATE PRIMARY KEY")
    val DQ_PK = DataFramehuemul.DQ_DuplicateValues(this, SQL_PK, null, "PK")
    if (DQ_PK.isError) {
      Result.isError = true
      Result.Description += s"\nhuemul_Table Error: PK: ${DQ_PK.Description} " 
      Result.Error_Code = 1018
    }
    
    
    //*********************************
    //Aplicar DQ según definición de campos en DataDefDQ: Unique Values   
    //*********************************
    SQL_Unique_FinalTable().foreach { x => 
      if (huemulBigDataGov.DebugMode) println(s"DF_SAVE DQ: VALIDATE UNIQUE FOR FIELD $x")
      val DQ_Unique = DataFramehuemul.DQ_DuplicateValues(this, x, null) 
      if (DQ_Unique.isError) {
        Result.isError = true
        Result.Description += s"\nhuemul_Table Error: error Unique for field $x: ${DQ_Unique.Description} "
        Result.Error_Code = 1019
      }            
    }
    
    //Aplicar DQ según definición de campos en DataDefDQ: Acepta nulos (nullable)
    SQL_NotNull_FinalTable().foreach { x => 
      if (huemulBigDataGov.DebugMode) println(s"DF_SAVE DQ: VALIDATE NOT NULL FOR FIELD ${x.get_MyName()}")
      
        val NotNullDQ : huemul_DataQuality = new huemul_DataQuality(x, s"huemul_Table Error: Not Null for field ${x.get_MyName()} ", s"${x.get_MyName()} IS NOT NULL",1023)
        NotNullDQ.setTolerance(0, null)
        ArrayDQ.append(NotNullDQ)
    }
    
    //VAlidación DQ_RegExp
    GetColumns().filter { x => x.getDQ_RegExp != null}.foreach { x => 
        var SQLFormula : String = s"""${x.get_MyName()} rlike "${x.getDQ_RegExp}" """

        var tand = ""
                  
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val RegExp : huemul_DataQuality = new huemul_DataQuality(x, s"huemul_Table Error: RegExp Column ${x.get_MyName()}",SQLFormula, 1041 )
        
        RegExp.setTolerance(0, null)
        ArrayDQ.append(RegExp)
    }
    
   
    //VAlidación DQ máximo y mínimo largo de texto
    GetColumns().filter { x => x.getDQ_MinLen != null || x.getDQ_MaxLen != null  }.foreach { x => 
        var SQLFormula : String = ""

        var tand = ""
        if (x.getDQ_MinLen != null){
          SQLFormula += s"length(${x.get_MyName()}) >= ${x.getDQ_MinLen}"
          tand = " and "
        }
        
        if (x.getDQ_MaxLen != null)
          SQLFormula += s" $tand length(${x.get_MyName()}) <= ${x.getDQ_MaxLen}"
                  
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxLen : huemul_DataQuality = new huemul_DataQuality(x, s"huemul_Table Error: MinMax length Column ${x.get_MyName()}",SQLFormula, 1020 )
        MinMaxLen.setTolerance(0, null)
        ArrayDQ.append(MinMaxLen)
    }
    
    //VAlidación DQ máximo y mínimo de números
    GetColumns().filter { x => x.getDQ_MinDecimalValue != null || x.getDQ_MaxDecimalValue != null  }.foreach { x => 
        
        var SQLFormula : String = ""
        var tand = ""
        if (x.getDQ_MinDecimalValue != null){
          SQLFormula += s"${x.get_MyName()} >= ${x.getDQ_MinDecimalValue}"
          tand = " and "
        }
        
        if (x.getDQ_MaxDecimalValue != null)
          SQLFormula += s" $tand ${x.get_MyName()} <= ${x.getDQ_MaxDecimalValue}"
        
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxNumber : huemul_DataQuality = new huemul_DataQuality(x, s"huemul_Table Error: MinMax Number Column ${x.get_MyName()}", SQLFormula,1021)
        MinMaxNumber.setTolerance(0, null)         
        ArrayDQ.append(MinMaxNumber)
    }
    
    //VAlidación DQ máximo y mínimo de fechas
    GetColumns().filter { x => x.getDQ_MinDateTimeValue != null || x.getDQ_MaxDateTimeValue != null  }.foreach { x => 
        
        var SQLFormula : String = ""
        var tand = ""
        if (x.getDQ_MinDateTimeValue != null){
          SQLFormula += s"${x.get_MyName()} >= '${x.getDQ_MinDateTimeValue}'"
          tand = " and "
        }
        
        if (x.getDQ_MaxDateTimeValue != null)
          SQLFormula += s" $tand ${x.get_MyName()} <= '${x.getDQ_MaxDateTimeValue}'"
          
        SQLFormula = s" (${SQLFormula}) or (${x.get_MyName()} is null) "
        val MinMaxDT : huemul_DataQuality = new huemul_DataQuality(x, s"huemul_Table Error: MinMax DateTime Column ${x.get_MyName()} ", SQLFormula, 1022)
        MinMaxDT.setTolerance(0, null)
        ArrayDQ.append(MinMaxDT)
    }
    
    val ResultDQ = this.DataFramehuemul.DF_RunDataQuality(this.GetDataQuality(), ArrayDQ, this.DataFramehuemul.Alias, this)
    if (ResultDQ.isError){
      Result.isError = true
      Result.Description += s"\n${ResultDQ.Description}"
      Result.Error_Code = ResultDQ.Error_Code
    }
    
    return Result
  }
  
  
 
  /**
   Create final DataFrame with full join New DF with old DF
   */
  private def DF_MDM_Dohuemul(LocalControl: huemul_Control, AliasNewData: String, isInsert: Boolean, isUpdate: Boolean, isDelete: Boolean, isSelectiveUpdate: Boolean, PartitionValueForSelectiveUpdate: String = null) {
    if (isSelectiveUpdate) {
      //Update some rows with some columns
      //Cant update PK fields
      
      //***********************************************************************/
      //STEP 1:    V A L I D A T E   M A P P E D   C O L U M N  S   *********/
      //***********************************************************************/
      
      LocalControl.NewStep("Selective Update: Validating fields ")
      
      val PKNotMapped = this.MissingRequiredFields_SelectiveUpdate()
      
      if (PKNotMapped != "")
        RaiseError(s"huemul_Table Error: PK not defined: ${PKNotMapped}",1017)
      
      //Get N° rows from user dataframe
      val NumRowsUserData: Long = this.DataFramehuemul.DataFrame.count()
      var NumRowsOldDataFrame: Long = 0
      //**************************************************//
      //STEP 2:   CREATE BACKUP
      //**************************************************//
      LocalControl.NewStep("Selective Update: Select Old Table")                                             
      val TempAlias: String = s"__${this.TableName}_old"
      val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      
      if (!huemulBigDataGov.HasName(PartitionValueForSelectiveUpdate) && _TableType == huemulType_Tables.Transaction)
        RaiseError(s"huemul_Table Error: Partition Value not defined", 1044)
        
      
      val FullPathString = if (_TableType == huemulType_Tables.Transaction) 
                      s"${GetFullNameWithPath()}/${_PartitionField.toLowerCase()}=${PartitionValueForSelectiveUpdate}"
                    else
                      this.GetFullNameWithPath()
      
      val FullPath = new org.apache.hadoop.fs.Path(FullPathString)
      
      if (fs.exists(FullPath)){
        //Exist, copy for use
        
        //Open actual file
        val DFTempCopy = huemulBigDataGov.spark.read.format(this._StorageType.toString()).load(FullPathString)
        val tempPath = huemulBigDataGov.GlobalSettings.GetDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, TempAlias)
        if (huemulBigDataGov.DebugMode) println(s"copy to temp dir: $tempPath ")
        DFTempCopy.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
        DFTempCopy.unpersist()
       
        //Open temp file
        if (huemulBigDataGov.DebugMode) println(s"open temp old df: $tempPath ")
        val DFTempOpen = if (_TableType == huemulType_Tables.Transaction) 
                            huemulBigDataGov.spark.read.parquet(tempPath).withColumn(_PartitionField.toLowerCase(), lit(PartitionValueForSelectiveUpdate))
                         else huemulBigDataGov.spark.read.parquet(tempPath)
        NumRowsOldDataFrame = DFTempOpen.count()
        DFTempOpen.createOrReplaceTempView(TempAlias)        
      } else 
        RaiseError(s"huemul_Table Error: Table ${FullPathString} doesn't exists",1043)
        
      
      //**************************************************//
      //STEP 3: Create left Join updating columns
      //**************************************************//
      LocalControl.NewStep("Selective Update: Left Join")
      val SQLLeftJoin_DF = huemulBigDataGov.DF_ExecuteQuery("__LeftJoin"
                                              , SQL_Step2_LeftJoin(TempAlias, this.DataFramehuemul.Alias)
                                             )
                                             
                                             
      //**************************************************//
      //STEP 4: Create Tabla with Update and Insert result
      //**************************************************//

      LocalControl.NewStep("Selective Update: Update Logic")
      val SQLHash_p1_DF = huemulBigDataGov.DF_ExecuteQuery("__Hash_p1"
                                          , SQL_Step4_Update("__LeftJoin", huemulBigDataGov.ProcessNameCall)
                                         )
                                         
      //**************************************************//
      //STEP 5: Create Hash
      //**************************************************//
      LocalControl.NewStep("Selective Update: Hash Code")                                         
      val SQLHash_p2_DF = huemulBigDataGov.DF_ExecuteQuery("__Hash_p2"
                                          , SQL_Step3_Hash_p1("__Hash_p1", isSelectiveUpdate)
                                         )
                                         
      this.UpdateStatistics(LocalControl, "Selective Update")
      
      //N° Rows Updated == NumRowsUserData
      if (NumRowsUserData != this._NumRows_Updatable) {
        RaiseError(s"huemul_Table Error: it was expected to update ${NumRowsUserData} rows, only ${_NumRows_Updatable} was found", 1045)
      } else if (this._NumRows_Total != NumRowsOldDataFrame ) {
        RaiseError(s"huemul_Table Error: Different number of rows in dataframes, original: ${NumRowsOldDataFrame}, new: ${this._NumRows_Total}, check your dataframe, maybe have duplicate keys", 1046)
      }
      
      LocalControl.NewStep("Selective Update: Final Table")
      val SQLFinalTable = SQL_Step4_Final("__Hash_p2", huemulBigDataGov.ProcessNameCall)

     
      //STEP 2: Execute final table 
      DataFramehuemul.DF_from_SQL(AliasNewData , SQLFinalTable)
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
        
      //Unpersist first DF
      SQLHash_p2_DF.unpersist()
      SQLHash_p1_DF.unpersist()
      SQLLeftJoin_DF.unpersist()
    }
    else if (_TableType == huemulType_Tables.Transaction) {
      LocalControl.NewStep("Transaction: Validating fields ")
      //STEP 1: validate name setting
      getALLDeclaredFields(true).filter { x => x.setAccessible(true)
                                      x.get(this).isInstanceOf[huemul_Columns] }
      .foreach { x =>     
        //Get field
        var Field = x.get(this).asInstanceOf[huemul_Columns]
        
        //New value (from field or compute column )
        if (!huemulBigDataGov.HasName(Field.get_MappedName()) ){
          RaiseError(s"${x.getName()} MUST have an assigned local name",1004)
        }                 
      }
      
      //STEP 2: Create Hash
      LocalControl.NewStep("Transaction: Create Hash Field")
      val SQLFinalTable = SQL_Step0_TXHash(this.DataFramehuemul.Alias, huemulBigDataGov.ProcessNameCall)
      if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery)
        println(SQLFinalTable)
      //STEP 2: Execute final table 
      DataFramehuemul.DF_from_SQL(AliasNewData , SQLFinalTable)
      
      LocalControl.NewStep("Transaction: Get Statistics info")
      this._NumRows_Total = this.DataFramehuemul.getNumRows
      this._NumRows_New = this.DataFramehuemul.getNumRows
      this._NumRows_Update  = 0
      this._NumRows_Updatable = 0
      this._NumRows_Delete = 0
      this._NumRows_NoChange = 0
      
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
    } 
    else if (_TableType == huemulType_Tables.Reference || _TableType == huemulType_Tables.Master)
    {
      /*
       * isInsert: se aplica en SQL_Step2_UpdateAndInsert, si no permite insertar, filtra esos registros y no los inserta
       * isUpdate: se aplica en SQL_Step1_FullJoin: si no permite update, cambia el tipo ___ActionType__ de UPDATE a EQUAL
       */
      
      val OnlyInsert: Boolean = isInsert && !isUpdate
      
      //All required fields have been set
      val SQL_Missing = MissingRequiredFields(isSelectiveUpdate)
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
        val SQLDistinct_DF = huemulBigDataGov.DF_ExecuteQuery("__Distinct"
                                                , SQL_Step0_Distinct(this.DataFramehuemul.Alias)
                                               )
        NextAlias = "__Distinct"
      }
      
      
      //**************************************************//
      //STEP 0.1: CREATE TEMP TABLE IF MASTER TABLE DOES NOT EXIST
      //**************************************************//
      LocalControl.NewStep("Ref & Master: Select Old Table")                                             
      val TempAlias: String = s"__${this.TableName}_old"
      val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new org.apache.hadoop.fs.Path(this.GetFullNameWithPath()))){
        //Exist, copy for use
        
        //Open actual file
        val DFTempCopy = huemulBigDataGov.spark.read.format(this._StorageType.toString()).load(this.GetFullNameWithPath())
        val tempPath = huemulBigDataGov.GlobalSettings.GetDebugTempPath(huemulBigDataGov, huemulBigDataGov.ProcessNameCall, TempAlias)
        if (huemulBigDataGov.DebugMode) println(s"copy to temp dir: $tempPath ")
        DFTempCopy.write.mode(SaveMode.Overwrite).format("parquet").save(tempPath)
        DFTempCopy.unpersist()
       
        //Open temp file
        if (huemulBigDataGov.DebugMode) println(s"open temp old df: $tempPath ")
        val DFTempOpen = huemulBigDataGov.spark.read.parquet(tempPath)        
        DFTempOpen.createOrReplaceTempView(TempAlias)        
      } else {
          println(s"create empty dataframe because ${this.GetTable()} does not exist")
          val Schema = GetSchema()
          val SchemaForEmpty = StructType(Schema.map { x => StructField(x.name, x.dataType, x.nullable) })
          val EmptyRDD = huemulBigDataGov.spark.sparkContext.emptyRDD[Row]
          val EmptyDF = huemulBigDataGov.spark.createDataFrame(EmptyRDD, SchemaForEmpty)
          EmptyDF.createOrReplaceTempView(TempAlias)
          if (huemulBigDataGov.DebugMode) EmptyDF.show()
      }
                                             
        
      //**************************************************//
      //STEP 1: Create Full Join with all fields
      //**************************************************//
      LocalControl.NewStep("Ref & Master: Full Join")
      val SQLFullJoin_DF = huemulBigDataGov.DF_ExecuteQuery("__FullJoin"
                                              , SQL_Step1_FullJoin(TempAlias, NextAlias, isUpdate, isDelete)
                                             )
      var toMemory: Boolean = false
      //if (SQLFullJoin_DF.count() <= 1000000)
      //  toMemory = true
                        
      //STEP 2: Create Tabla with Update and Insert result
      LocalControl.NewStep("Ref & Master: Update & Insert Logic")
      val SQLHash_p1_DF = huemulBigDataGov.DF_ExecuteQuery("__Hash_p1"
                                          , SQL_Step2_UpdateAndInsert("__FullJoin", huemulBigDataGov.ProcessNameCall, isInsert)
                                         )
      if (toMemory)
        SQLHash_p1_DF.cache()
      
      //STEP 3: Create Hash
      LocalControl.NewStep("Ref & Master: Hash Code")                                         
      val SQLHash_p2_DF = huemulBigDataGov.DF_ExecuteQuery("__Hash_p2"
                                          , SQL_Step3_Hash_p1("__Hash_p1", false)
                                         )
                                         
      if (toMemory)
        SQLHash_p2_DF.cache()
               
                                        
      this.UpdateStatistics(LocalControl, "Ref & Master")
      
                                         
      LocalControl.NewStep("Ref & Master: Final Table")
      val SQLFinalTable = SQL_Step4_Final("__Hash_p2", huemulBigDataGov.ProcessNameCall)

     
      //STEP 2: Execute final table 
      DataFramehuemul.DF_from_SQL(AliasNewData , SQLFinalTable)
      if (huemulBigDataGov.DebugMode) this.DataFramehuemul.DataFrame.show()
      
      //Unpersist first DF
      SQLHash_p2_DF.unpersist()
      SQLHash_p1_DF.unpersist()
      SQLFullJoin_DF.unpersist()
      
      
    } else
      RaiseError(s"huemul_Table Error: ${_TableType} found, Master o Reference required ", 1007)
  }
  
  private def UpdateStatistics(LocalControl: huemul_Control, TypeOfCall: String) {
    LocalControl.NewStep(s"${TypeOfCall}: Statistics")
    //DQ for Reference and Master Data
    val DQ_ReferenceData: DataFrame = huemulBigDataGov.spark.sql(
                      s"""SELECT CAST(SUM(CASE WHEN ___ActionType__ = 'NEW' then 1 else 0 end) as Long) as __New
                                ,CAST(SUM(CASE WHEN ___ActionType__ = 'UPDATE' and SameHashKey = 0 then 1 else 0 end) as Long) as __Update
                                ,CAST(SUM(CASE WHEN ___ActionType__ = 'UPDATE' then 1 else 0 end) as Long) as __Updatable
                                ,CAST(SUM(CASE WHEN ___ActionType__ = 'DELETE' then 1 else 0 end) as Long) as __Delete
                                ,CAST(SUM(CASE WHEN ___ActionType__ = 'EQUAL' then 1 else 0 end) as Long) as __NoChange
                                ,CAST(count(1) AS Long) as __Total
                          FROM __Hash_p2 temp 
                       """)
    
                       if (huemulBigDataGov.DebugMode) DQ_ReferenceData.show()
    
    val FirstRow = DQ_ReferenceData.first()
    this._NumRows_Total = FirstRow.getAs("__Total")
    this._NumRows_New = FirstRow.getAs("__New")
    this._NumRows_Update  = FirstRow.getAs("__Update")
    this._NumRows_Updatable  = FirstRow.getAs("__Updatable")
    this._NumRows_Delete = FirstRow.getAs("__Delete")
    this._NumRows_NoChange= FirstRow.getAs("__NoChange")
    
    
    LocalControl.NewStep(s"${TypeOfCall}: Validating Insert & Update")
    var DQ_Error: String = ""
    var Error_Number: Integer = null
    if (this._NumRows_Total == this._NumRows_New)
      DQ_Error = "" //Doesn't have error, first run
    else if (this._DQ_MaxNewRecords_Num != null && this._DQ_MaxNewRecords_Num > 0 && this._NumRows_New > this._DQ_MaxNewRecords_Num){
      DQ_Error = s"huemul_Table Error: DQ MDM Error: N° New Rows (${this._NumRows_New}) exceeds max defined (${this._DQ_MaxNewRecords_Num}) "
      Error_Number = 1005
    }
    else if (this._DQ_MaxNewRecords_Perc != null && this._DQ_MaxNewRecords_Perc > Decimal.apply(0) && (Decimal.apply(this._NumRows_New) / Decimal.apply(this._NumRows_Total)) > this._DQ_MaxNewRecords_Perc) {
      DQ_Error = s"huemul_Table Error: DQ MDM Error: % New Rows (${(this._NumRows_New / this._NumRows_Total)}) exceeds % max defined (${this._DQ_MaxNewRecords_Perc}) "
      Error_Number = 1006
    }

    if (DQ_Error != "")
      RaiseError(DQ_Error, Error_Number)
      
    DQ_ReferenceData.unpersist()
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
    if (this._WhoCanRun_executeFull.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.ExecuteSave(NewAlias, true, true, true, false, null)      
    else {
      RaiseError(s"huemul_Table Error: Don't have access to executeFull in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}", 1008)
      
    }
    
    return Result
  }
  
  def executeOnlyInsert(NewAlias: String): Boolean = {
    var Result: Boolean = false
    if (this._TableType == huemulType_Tables.Transaction)
      RaiseError("huemul_Table Error: DoOnlyInserthuemul is not available for Transaction Tables",1009)

    val whoExecute = GetClassAndPackage()  
    if (this._WhoCanRun_executeOnlyInsert.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.ExecuteSave(NewAlias, true, false, false, false, null) 
    else {
      RaiseError(s"huemul_Table Error: Don't have access to executeOnlyInsert in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}", 1010)
    }
         
    return Result
  }
  
  def executeOnlyUpdate(NewAlias: String): Boolean = {   
    var Result: Boolean = false
    if (this._TableType == huemulType_Tables.Transaction)
      RaiseError("huemul_Table Error: DoOnlyUpdatehuemul is not available for Transaction Tables", 1011)
      
    val whoExecute = GetClassAndPackage()  
    if (this._WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.ExecuteSave(NewAlias, false, true, false, false, null)  
    else {
      RaiseError(s"huemul_Table Error: Don't have access to executeOnlyUpdate in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}",1012)
    }
    
    return Result
        
  }
  
  def executeSelectiveUpdate(NewAlias: String, PartitionValueForSelectiveUpdate: String): Boolean = {   
    var Result: Boolean = false
      
    val whoExecute = GetClassAndPackage()  
    if (this._WhoCanRun_executeOnlyUpdate.HasAccess(whoExecute.getLocalClassName(), whoExecute.getLocalPackageName()))
      Result = this.ExecuteSave(NewAlias, false, false, false, true, PartitionValueForSelectiveUpdate)  
    else {
      RaiseError(s"huemul_Table Error: Don't have access to executeSelectiveUpdate in ${this.getClass.getSimpleName().replace("$", "")}  : Class: ${whoExecute.getLocalClassName()}, Package: ${whoExecute.getLocalPackageName()}",1012)
    }
    
    return Result
        
  }
  
  private def CompareSchema(Columns: ArrayBuffer[huemul_Columns], Schema: StructType): String = {
    var Errores: String = ""
    Columns.foreach { x => 
      if (huemulBigDataGov.HasName(x.get_MappedName())) {
        //val ColumnNames = if (UseAliasColumnName) x.get_MappedName() else x.get_MyName()
        val ColumnNames = x.get_MyName()
        val ColumnInSchema = Schema.filter { y => y.name == ColumnNames }
        if (ColumnInSchema == null || ColumnInSchema.length == 0)
          RaiseError(s"huemul_Table Error: column missing in Schema ${ColumnNames}", 1038)
        if (ColumnInSchema.length != 1)
          RaiseError(s"huemul_Table Error: multiples columns found in Schema with name ${ColumnNames}", 1039)
        
          val dataType = ColumnInSchema(0).dataType
        if (dataType != x.DataType) {
          Errores = Errores.concat(s"Error Column ${ColumnNames}, Requiered: ${x.DataType}, actual: ${dataType}  \n")
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
  private def ExecuteSave(AliasNewData: String, IsInsert: Boolean, IsUpdate: Boolean, IsDelete: Boolean, IsSelectiveUpdate: Boolean, PartitionValueForSelectiveUpdate: String): Boolean = {
   
    var LocalControl = new huemul_Control(huemulBigDataGov, Control ,false )
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
      DF_MDM_Dohuemul(LocalControl, AliasNewData,IsInsert, IsUpdate, IsDelete, IsSelectiveUpdate, PartitionValueForSelectiveUpdate)
  
      LocalControl.NewStep("Register Master Information ")
      Control.RegisterMASTER_CREATE_Use(this)
      
      
      //DataQuality by Columns
      LocalControl.NewStep("Start DataQuality")
      val DQResult = DF_DataQualityMasterAuto(IsSelectiveUpdate)
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
      if (huemulBigDataGov.DebugMode){
        println(s"Saving ${GetTable()} Table with params: ") 
        println(s"${_PartitionField} field for partitioning table")
        println(s"${GetFullNameWithPath()} path")
      }
      
      LocalControl.NewStep("Start Save ")                
      if (SavePersistent(LocalControl, DataFramehuemul.DataFrame, OnlyInsert, IsSelectiveUpdate))
        LocalControl.FinishProcessOK
      else {
        result = false
        LocalControl.FinishProcessError()
      }
      
    } catch {
      case e: Exception => 
        result = false
        if (ErrorCode == null) {
          if (this.Error_Code != null && this.Error_Code != 0)
            ErrorCode  = this.Error_Code
          else if (LocalControl.Control_Error.ControlError_ErrorCode == null)
            ErrorCode = 1027
          else
            ErrorCode = LocalControl.Control_Error.ControlError_ErrorCode
        }
        
        LocalControl.Control_Error.GetError(e, getClass().getSimpleName, ErrorCode)
        LocalControl.FinishProcessError()
    }
    
    return result
  }
  
  def CopyToDest(PartitionValue: String, DestEnvironment: String) {
    if (huemulBigDataGov.HasName(_PartitionField)) {
       val ProdFullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath()}/${_PartitionField.toLowerCase()}=${PartitionValue}")
       val ManualFullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath2(DestEnvironment)}/${_PartitionField.toLowerCase()}=${PartitionValue}")
       
       val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       org.apache.hadoop.fs.FileUtil.copy(fs, ProdFullPath, fs, ManualFullPath, false, true, huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       
       val DestTableName: String = GetTable(DestEnvironment)
       println(s"MSCK REPAIR TABLE ${DestTableName}")
       huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${DestTableName}")
              
    } else {
       val ProdFullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath()}/${_PartitionField.toLowerCase()}")
       val ManualFullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath2(DestEnvironment)}/${_PartitionField.toLowerCase()}")
       
       val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
       org.apache.hadoop.fs.FileUtil.copy(fs, ProdFullPath, fs, ManualFullPath, false, true, huemulBigDataGov.spark.sparkContext.hadoopConfiguration)
    }
    
  }
  
  private def SavePersistent(LocalControl: huemul_Control, DF: DataFrame, OnlyInsert: Boolean, IsSelectiveUpdate: Boolean): Boolean = {
    var DF_Final = DF
    var Result: Boolean = true
    
    
    if (this._TableType == huemulType_Tables.Reference || this._TableType == huemulType_Tables.Master || IsSelectiveUpdate) {
      LocalControl.NewStep("Save: Drop ActionType column")
   
      if (OnlyInsert && !IsSelectiveUpdate)
        DF_Final = DF_Final.where("___ActionType__ = 'NEW'") 
     
      DF_Final = DF_Final.drop("___ActionType__")
    }
      
    val sqlDrop01 = s"drop table if exists ${GetTable()}"
    if (CreateInHive ) {
      LocalControl.NewStep("Save: Drop Hive table Def")
      if (huemulBigDataGov.DebugMode && !huemulBigDataGov.HideLibQuery) println(sqlDrop01)
      try {
        val TablesListFromHive = huemulBigDataGov.spark.catalog.listTables(GetCurrentDataBase()).collect()
        if (TablesListFromHive.filter { x => x.name.toUpperCase() == TableName.toUpperCase()  }.length > 0) 
          huemulBigDataGov.spark.sql(sqlDrop01)
          
      } catch {
        case t: Throwable => println(s"Error drop hive table: ${t.getMessage}") //t.printStackTrace()
      }
     
    }
    
    try {
      if (_PartitionField == null || _PartitionField == ""){
        if (OnlyInsert) {
          LocalControl.NewStep("Save: Append Master & Ref Data")
          DF_Final.write.mode(SaveMode.Append).format(this._StorageType.toString()).save(GetFullNameWithPath())
        }
        else {
          LocalControl.NewStep("Save: Overwrite Master & Ref Data")
          DF_Final.write.mode(SaveMode.Overwrite).format(this._StorageType.toString()).save(GetFullNameWithPath())
        }
        
        //val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)       
        //fs.setPermission(new org.apache.hadoop.fs.Path(GetFullNameWithPath()), new FsPermission("770"))
      }
      else{
        //Get Partition_Id Values
        LocalControl.NewStep("Save: Validating N° partitions")
        val DFDistinct = DF_Final.select(_PartitionField).distinct().withColumn(_PartitionField, DF_Final.col(_PartitionField).cast(StringType))
        if (DFDistinct.count() != 1){
          RaiseError(s"huemul_Table Error: N° values in partition wrong!, expected: 1, real: ${DFDistinct.count()}",1015)
        } else {
          val PartitionValue = DFDistinct.first().getAs[String](_PartitionField)
          val FullPath = new org.apache.hadoop.fs.Path(s"${GetFullNameWithPath()}/${_PartitionField.toLowerCase()}=${PartitionValue}")
          
          LocalControl.NewStep("Save: Drop old partition")
          val fs = FileSystem.get(huemulBigDataGov.spark.sparkContext.hadoopConfiguration)       
          fs.delete(FullPath, true)
          LocalControl.NewStep("Save: OverWrite partition with new data")
          if (huemulBigDataGov.DebugMode) println(s"saving path: ${FullPath} ")        
          DF_Final.write.mode(SaveMode.Append).format(this._StorageType.toString()).partitionBy(_PartitionField).save(GetFullNameWithPath())
                
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
         
          huemulBigDataGov.spark.sql(CreateTableScript)
        }
    
        //Hive read partitioning metadata, see https://docs.databricks.com/user-guide/tables.html
        if (CreateInHive && (_PartitionField != null && _PartitionField != "")) {
          LocalControl.NewStep("Save: Repair Hive Metadata")
          if (huemulBigDataGov.DebugMode) println(s"MSCK REPAIR TABLE ${GetTable()}")
          huemulBigDataGov.spark.sql(s"MSCK REPAIR TABLE ${GetTable()}")
        }
        
        if (huemulBigDataGov.ImpalaEnabled) {
          LocalControl.NewStep("Save: refresh Impala Metadata")
          huemulBigDataGov.impala_connection.ExecuteJDBC_NoResulSet(s"refresh table ${GetTable()}")
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
    Control.Control_Error.ControlError_ErrorCode = code
    if (huemulBigDataGov.DebugMode) println(txt)
    Control.RaiseError(txt)
    //sys.error(txt)
  }  
  
  

}
