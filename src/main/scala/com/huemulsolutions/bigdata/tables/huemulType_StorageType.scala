package com.huemulsolutions.bigdata.tables

//any change to this values, please add XX_createExternalTableUsingSpark and XX_createExternalTableUsingHive in GlobalSettings

object huemulType_StorageType extends Enumeration {
  type huemulType_StorageType = Value
  val PARQUET, ORC, AVRO, HBASE = Value
}
