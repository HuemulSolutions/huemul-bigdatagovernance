package com.huemulsolutions.bigdata.tables

import com.huemulsolutions.bigdata.control.huemul_Control
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.huemulsolutions.bigdata.common.huemul_BigDataGovernance


import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.client.{Connection,ConnectionFactory,HBaseAdmin,HTable,Put,Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Admin
//import org.apache.hadoop.hbase.HTableDescriptors // HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hive.jdbc.HiveConnection
//import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog


class huemul_TableConnector(huemulBigDataGov: huemul_BigDataGovernance, Control: huemul_Control) extends Serializable {
  
  def tableDeleteHBase(HBase_Namespace: String
                     , HBase_tableName: String) = {

    //Crea tabla
    
    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(huemulBigDataGov.spark.sparkContext, hbaseConf)
    
    
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin()
    
    val tableNameString: String = s"${HBase_Namespace}:${HBase_tableName}"
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
    Control.NewStep(s"HBase: Create hBaseConfiguration and HBaseContext")
    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(huemulBigDataGov.spark.sparkContext, hbaseConf)
    
    Control.NewStep("HBase: Create connection")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin()
    
    val tableNameString: String = s"${HBase_Namespace}:${HBase_tableName}"
    val tableName: org.apache.hadoop.hbase.TableName = org.apache.hadoop.hbase.TableName.valueOf(tableNameString)
    
    Control.NewStep(s"HBase: Namespaces validation...")
    result = admin.tableExists(tableName)
    
    admin.close()
    connection.close()
    
    return result
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
    return saveToHBase(DF_to_save
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
    var result: Boolean = true
    
    var numPartition: String = if (numPartitions > 5) numPartitions.toString() else "5"
    Control.NewStep(s"HBase: num partitions = ${numPartition} ")
    
    //array with column names    
    val __cols = DF_to_save.columns.sortBy { x => (if (x==DF_ColumnPKName) "0" else "1").concat(x) } 
    val __colSortedDF = DF_to_save.select(__cols.map( x => col(x) ): _*)
    val __schema = __colSortedDF.schema
    //exclude PK from columns to save (PK is a row key)
    val __valCols = __cols.filterNot(x => x.equals(DF_ColumnPKName)).map { x => {
      var fam: String = "default"
      var nom: String = x
      var dataType: DataType = __schema.fields( __schema.fieldIndex(x)).dataType
      
      //Only if huemulDeclaredFields has value
      if (huemulDeclaredFieldsForHBase != null) {
        val fam_fil = huemulDeclaredFieldsForHBase.filter { y => y._1.getName.toUpperCase() == x.toUpperCase() }
        
        if (fam_fil.length == 1) {
          val __reg = fam_fil(0) 
          fam = __reg._2
          nom = __reg._3
          dataType = __reg._4
        }
      }
      
      (nom, fam, dataType )
    }}
    
    //get num cols
    val __numCols: Int = __valCols.length

    Control.NewStep(s"HBase: Map to HBase format ")
    //map to HBase format (keyValue, family, colname, value)
    
    import huemulBigDataGov.spark.implicits._ 
    val __pdd_2 = __colSortedDF.flatMap(row => {
      val rowKey = row(0).toString() //Bytes.toBytes(x._1)
      
      for (i <- 0 until __numCols) yield {
          val colName = __valCols(i)._1.toString()
          val famName = __valCols(i)._2.toString()
          val colDataType = __valCols(i)._3
          var colValue: Array[Byte] = null

          if (row(i+1) == null)
            colValue = null
          else if (colDataType == DataTypes.BooleanType)
            colValue = Bytes.toBytes(row.getBoolean(i+1)) 
          else if (colDataType == DataTypes.ShortType)
            colValue = Bytes.toBytes(row.getShort(i+1))
          else if (colDataType == DataTypes.LongType)
            colValue = Bytes.toBytes(row.getLong(i+1))
          //else if (colDataType == DataTypes.BinaryType)
          //  colValue = Bytes.toBytes(row.getBinary(i+1))
          else if (colDataType == DataTypes.StringType)
            colValue = Bytes.toBytes(row.getString(i+1))
          //else if (colDataType == DataTypes.NullType)
          //  colValue = Bytes.toBytes(row.getAs[NullType](columnName)
          else if (colDataType == DecimalType || colDataType.typeName.toLowerCase().contains("decimal"))
            colValue = Bytes.toBytes(row(i+1).toString())
            //colValue = Bytes.toBytes(row.getDecimal(i+1))
          else if (colDataType == DataTypes.IntegerType)
            colValue = Bytes.toBytes(row.getInt(i+1))
          else if (colDataType == DataTypes.FloatType)
            colValue = Bytes.toBytes(row.getFloat(i+1))
          else if (colDataType == DataTypes.DoubleType)
            colValue = Bytes.toBytes(row.getDouble(i+1))
          else if (colDataType == DataTypes.DateType)
            colValue = Bytes.toBytes(row(i+1).toString())
          else if (colDataType == DataTypes.TimestampType)
            colValue = Bytes.toBytes(row(i+1).toString())
          else
            colValue = Bytes.toBytes(row(i+1).toString())
                
          (rowKey, (famName, colName, colValue))
        }
      }
    ).rdd
    
    
     //Table Assign
    Control.NewStep(s"HBase: Set staging Folder and Family:Table Name")
    
    val tableNameString: String = s"${HBase_Namespace}:${HBase_tableName}"
    val tableName: org.apache.hadoop.hbase.TableName = org.apache.hadoop.hbase.TableName.valueOf(tableNameString)
    
    //Starting HBase
    Control.NewStep(s"HBase: Create hBaseConfiguration and HBaseContext")
    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(huemulBigDataGov.spark.sparkContext, hbaseConf)
    
    //Crea tabla
    Control.NewStep("HBase: Create connection")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin()
    
    Control.NewStep(s"HBase: Namespaces validation...")
    val _listNamespace = admin.listNamespaceDescriptors()
    
    //Create namespace if it doesn't exist
    //_listNamespace.foreach { x => println(x.getName) }
    if (_listNamespace.filter { x => x.getName == HBase_Namespace }.length == 0) {
      admin.createNamespace(org.apache.hadoop.hbase.NamespaceDescriptor.create(HBase_Namespace).build())
    }
    
    Control.NewStep("HBase: TableExists validation...")
    if (!admin.tableExists(tableName)) {
      /* desde hbase 2.0
      val __newTable = TableDescriptorBuilder.newBuilder(tableName)
                  .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("default".getBytes).build())
                  .build()
                  * 
                  */
     
      Control.NewStep(s"HBase: Table doesn't exists, creating table... ")
      val __newTable = new org.apache.hadoop.hbase.HTableDescriptor(tableName)
      
      //Add families
      val a = __valCols.map(x=>x._2).distinct.foreach { x => 
        __newTable.addFamily(new HColumnDescriptor(x))  
      }
      
      admin.createTable(__newTable)
    } else {
      Control.NewStep(s"HBase: Table exists, get families ")
      val __oldTable = admin.getTableDescriptor(tableName)
      val _getFamilies = __oldTable.getFamilies.toArray()
      var _newFamilies = __valCols.map(x=>x._2).distinct
      
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
      if (_newFamilies.length > 0) {
        Control.NewStep(s"HBase: creating new families ")
        val a = _newFamilies.foreach { x => 
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
      Control.NewStep(s"HBase: set null when previous values were not null")
      val __tdd_null = __pdd_2.filter(x=> x._2._3 == null).map(x=>x._1).distinct().map(x=> Bytes.toBytes(x))
      Control.NewStep("HBase: Delete nulls")
      hbaseContext.bulkDelete[Array[Byte]](__tdd_null
              ,tableName
              ,putRecord => new Delete( putRecord)
      		     
              ,4)
    }
        
    Control.NewStep(s"HBase: exclude null values ")
    val __tdd_notnull = __pdd_2.filter(x=> x._2._3 != null)
   // println(s"N° total: ${__pdd_2.count()}")
   // println(s"N° filtardos: ${__tdd_notnull.count()}")
    
    val stagingFolder = s"/tmp/user/${Control.getStepId}"
    huemulBigDataGov.logMessageDebug(s"staging folder: ${stagingFolder}")
  
    Control.NewStep(s"HBase: insert and update values ")
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
      
      Control.NewStep(s"HBase: execute HBase job ")
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
  
    return result
  }
}