package com.huemulsolutions.bigdata.datalake

object HuemulTypeFileType extends Enumeration {
  type HuemulTypeFileType = Value
  val TEXT_FILE, PDF_FILE, AVRO_FILE, PARQUET_FILE, ORC_FILE, DELTA_FILE = Value
  
}
