package com.huemulsolutions.bigdata.tables

import com.huemulsolutions.bigdata.control.HuemulControl
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.huemulsolutions.bigdata.common.HuemulBigDataGovernance


import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.client.Delete
//import org.apache.hadoop.hbase.HTableDescriptors // HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
//import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog


class HuemulTableConnector(huemulBigDataGov: HuemulBigDataGovernance, Control: HuemulControl) extends Serializable {
  
  def tableDeleteHBase(HBase_Namespace: String
                     , HBase_tableName: String): Unit = {

    //Crea tabla
    
    val hbaseConf = HBaseConfiguration.create()
   // val hbaseContext = new HBaseContext(huemulBigDataGov.spark.sparkContext, hbaseConf)
    
    
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    
    val tableNameString: String = s"$HBase_Namespace:$HBase_tableName"
    val tableName: org.apache.hadoop.hbase.TableName = org.apache.hadoop.hbase.TableName.valueOf(tableNameString)
    
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
      
    admin.close()
    connection.close()
    
    
  }
  
  def tableExistsHBase(HBase_Namespace: String
                     , HBase_tableName: String): Boolean = {
    var result: Boolean = false
    //Crea tabla
    Control.newStep(s"HBase: Create hBaseConfiguration and HBaseContext")
    val hbaseConf = HBaseConfiguration.create()
    //val hbaseContext = new HBaseContext(huemulBigDataGov.spark.sparkContext, hbaseConf)
    
    Control.newStep("HBase: Create connection")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    
    val tableNameString: String = s"$HBase_Namespace:$HBase_tableName"
    val tableName: org.apache.hadoop.hbase.TableName = org.apache.hadoop.hbase.TableName.valueOf(tableNameString)
    
    Control.newStep(s"HBase: Namespaces validation...")
    result = admin.tableExists(tableName)
    
    admin.close()
    connection.close()
    
    result
  }
  
  /*
  def getDFFromHBase(Alias: String, catalog: String): DataFrame = {
    val DF = huemulBigDataGov.spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog)).format("org.apache.hadoop.hbase.spark").load()
    DF.createOrReplaceTempView(Alias)
    return DF
  }
  * 
  */
  
  def saveToHBase(DF_to_save: DataFrame
                , HBase_Namespace: String
                , HBase_tableName: String
                , numPartitions: Int
                , isOnlyInsert: Boolean
                , DF_ColumnPKName: String
                ): Boolean = {
     saveToHBase(DF_to_save
                                    ,HBase_Namespace
                                    ,HBase_tableName
                                    ,numPartitions
                                    ,isOnlyInsert
                                    ,DF_ColumnPKName
                                    ,null)
  }
    
  
  def saveToHBase(DF_to_save: DataFrame
                , HBase_Namespace: String
                , HBase_tableName: String
                , numPartitions: Int
                , isOnlyInsert: Boolean
                , DF_ColumnPKName: String
                , huemulDeclaredFieldsForHBase : Array[(java.lang.reflect.Field, String, String, DataType)] //Optional
                ): Boolean = {
    val result: Boolean = true
    
    val numPartition: String = if (numPartitions > 5) numPartitions.toString else "5"
    Control.newStep(s"HBase: num partitions = $numPartition")

    //get Schema
    val __schema = DF_to_save.schema
    
    //GET index to PK
    var indexPK = -1
    var isFound: Boolean = false
    __schema.foreach { x => 
      if (!isFound) {
        indexPK+=1
        if (x.name.toLowerCase() == DF_ColumnPKName.toLowerCase())
          isFound = true   
      }
    }
    
    //create array with family and column info
    val __valCols1 = __schema.filter { x => x.name.toLowerCase() != DF_ColumnPKName.toLowerCase()  } .map { x => {
      var fam: String = "default"
      var nom: String = x.name
      var dataType: DataType = __schema.fields( __schema.fieldIndex(x.name)).dataType
      val pos = __schema.fieldIndex(x.name)

      //Only if huemulDeclaredFields has value
      if (huemulDeclaredFieldsForHBase != null) {
        val fam_fil = huemulDeclaredFieldsForHBase.filter { y => y._1.getName.toUpperCase() == x.name.toUpperCase() }
        
        if (fam_fil.length == 1) {
          val __reg = fam_fil(0) 
          fam = __reg._2
          nom = __reg._3
          dataType = __reg._4
        }
      }
      
      (nom, fam, dataType, pos )
    }}

    //reorder to optimize HBase insert
    @transient lazy val __valCols2 = __valCols1.sortBy( f => f._2.concat(f._1))
    //get num cols
    @transient lazy  val __numCols2: Int = __valCols2.length

    Control.newStep(s"HBase: Map to HBase format ")
    
    //map to HBase format (keyValue, family, colname, value)
    import huemulBigDataGov.spark.implicits._ 
    val __pdd_2 = DF_to_save.flatMap(row => {
      val rowKey = row(indexPK).toString //Bytes.toBytes(x._1)
      
      for (ii <- 0 until __numCols2) yield {
          val colName = __valCols2(ii)._1
          val famName = __valCols2(ii)._2
          val colDataType = __valCols2(ii)._3
          var colValue: Array[Byte] = null
          val y = __valCols2(ii)._4

          if (row(y) == null)
            colValue = null
          else if (colDataType == DataTypes.BooleanType)
            colValue = Bytes.toBytes(row.getBoolean(y)) 
          else if (colDataType == DataTypes.ShortType)
            colValue = Bytes.toBytes(row.getShort(y))
          else if (colDataType == DataTypes.LongType)
            colValue = Bytes.toBytes(row.getLong(y))
          //else if (colDataType == DataTypes.BinaryType)
          //  colValue = Bytes.toBytes(row.getBinary(y))
          else if (colDataType == DataTypes.StringType)
            colValue = Bytes.toBytes(row(y).toString)
          //else if (colDataType == DataTypes.NullType)
          //  colValue = Bytes.toBytes(row.getAs[NullType](columnName)
          else if (colDataType == DecimalType || colDataType.typeName.toLowerCase().contains("decimal"))
            colValue = Bytes.toBytes(row(y).toString)
            //colValue = Bytes.toBytes(row.getDecimal(y))
          else if (colDataType == DataTypes.IntegerType)
            colValue = Bytes.toBytes(row.getInt(y))
          else if (colDataType == DataTypes.FloatType)
            colValue = Bytes.toBytes(row.getFloat(y))
          else if (colDataType == DataTypes.DoubleType)
            colValue = Bytes.toBytes(row.getDouble(y))
          else if (colDataType == DataTypes.DateType)
            colValue = Bytes.toBytes(row(y).toString)
          else if (colDataType == DataTypes.TimestampType)
            colValue = Bytes.toBytes(row(y).toString)
          else
            colValue = Bytes.toBytes(row(y).toString)
                
          (rowKey, (famName, colName, colValue))
        }
      }
    ).rdd
    
    __pdd_2.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)
    //val numRowsTot = __pdd_2.count()
        
     //Table Assign
    Control.newStep(s"HBase: Set staging Folder and Family:Table Name")
    
    val tableNameString: String = s"$HBase_Namespace:$HBase_tableName"
    val tableName: org.apache.hadoop.hbase.TableName = org.apache.hadoop.hbase.TableName.valueOf(tableNameString)
    
    //Starting HBase
    Control.newStep(s"HBase: Create hBaseConfiguration and HBaseContext")
    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(huemulBigDataGov.spark.sparkContext, hbaseConf)
    
    //create table
    Control.newStep("HBase: Create connection")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin
    
    Control.newStep(s"HBase: Namespaces validation...")
    val _listNamespace = admin.listNamespaceDescriptors()
    
    //Create namespace if it doesn't exist
    //_listNamespace.foreach { x => println(x.getName) }
    if (!_listNamespace.exists { x => x.getName == HBase_Namespace }) {
      admin.createNamespace(org.apache.hadoop.hbase.NamespaceDescriptor.create(HBase_Namespace).build())
    }
    
    Control.newStep("HBase: TableExists validation...")
    if (!admin.tableExists(tableName)) {
      /* desde hbase 2.0
      val __newTable = TableDescriptorBuilder.newBuilder(tableName)
                  .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("default".getBytes).build())
                  .build()
                  * 
                  */
     
      Control.newStep(s"HBase: Table doesn't exists, creating table... ")
      val __newTable = new org.apache.hadoop.hbase.HTableDescriptor(tableName)
      
      //Add families
      __valCols2.map(x=>x._2).distinct.foreach { x =>
        __newTable.addFamily(new HColumnDescriptor(x))  
      }
      
      admin.createTable(__newTable)
    } else {
      Control.newStep(s"HBase: Table exists, get families ")
      val __oldTable = admin.getTableDescriptor(tableName)
      val _getFamilies = __oldTable.getFamilies.toArray()
      var _newFamilies = __valCols2.map(x=>x._2).distinct
      
      /*
       * CHECK FAMILIES
       */
      //get current families
      _getFamilies.foreach { x =>
            val _reg = x.asInstanceOf[org.apache.hadoop.hbase.HColumnDescriptor].getNameAsString
            //println(_reg)
              _newFamilies = _newFamilies.filter { y => y != _reg }
            }
      
      //Add new families
      if (_newFamilies.nonEmpty) {
        Control.newStep(s"HBase: creating new families ")
        _newFamilies.foreach { x =>
          //println(s"nuevas: ${x}")
        __oldTable.addFamily(new HColumnDescriptor(x))  
        }
        
        admin.modifyTable(tableName, __oldTable)
        
        //sys.error("fin obligatorio")
      }
                    
    }
    
    //elimina los registros que tengan algún valor en null
    //si es OnlyInsert no existen los registros anteriormente, por tanto no hay registros nulos que eliminar.
    if (!isOnlyInsert) {
      Control.newStep(s"HBase: set null when previous values were not null")
      val __tdd_null = __pdd_2.filter(x=> x._2._3 == null).map(x=>x._1).distinct().map(x=> Bytes.toBytes(x))
      Control.newStep("HBase: Delete nulls")
      hbaseContext.bulkDelete[Array[Byte]](__tdd_null
              ,tableName
              ,putRecord => new Delete( putRecord)
      		     
              ,4)
    }
        
    Control.newStep(s"HBase: exclude null values ")
    val __tdd_notnull = __pdd_2.filter(x=> x._2._3 != null)
    
    val stagingFolder = s"/tmp/user/${Control.getStepId}"
    huemulBigDataGov.logMessageDebug(s"staging folder: $stagingFolder")
  
    Control.newStep(s"HBase: insert and update values ")
    if (__tdd_notnull.count() > 0) {
      __tdd_notnull.hbaseBulkLoad(hbaseContext
                            , tableName
                            , t =>  {
                              val rowKey = Bytes.toBytes(t._1)
                              val family: Array[Byte] = Bytes.toBytes(t._2._1)
                              val qualifier = Bytes.toBytes(t._2._2)
                              val value = t._2._3
                              
                              val keyFamilyQualifier = new KeyFamilyQualifier(rowKey,family, qualifier)
                              Seq((keyFamilyQualifier, value)).iterator
                              
                            }
                            , stagingFolder)
      
      Control.newStep(s"HBase: execute HBase job ")
      val load = new LoadIncrementalHFiles(hbaseConf)
      load.run(Array(stagingFolder, tableNameString))
    }
      
    admin.close()
    connection.close()
    
    /*
    DF_to_save.write.mode(localSaveMode).options(Map(HBaseTableCatalog.tableCatalog -> getHBaseCatalog()
                                                   , HBaseTableCatalog.newTable -> numPartition)
                                                ).format(huemulBigDataGov.GlobalSettings.getHBase_formatTable()).save()
                                                * 
                                                */
  
    result
  }
  
  
}