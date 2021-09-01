package com.huemulsolutions.bigdata.tables

//any change to this values, please add XX_createExternalTableUsingSpark and XX_createExternalTableUsingHive in GlobalSettings

object HuemulTypeStorageType extends Enumeration {
  type HuemulTypeStorageType = Value
  val PARQUET, ORC, AVRO, HBASE, DELTA = Value
}
