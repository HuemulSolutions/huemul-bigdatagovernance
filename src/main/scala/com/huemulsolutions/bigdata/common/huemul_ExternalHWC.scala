package com.huemulsolutions.bigdata.common

import com.hortonworks.hwc.HiveWarehouseSession
import org.apache.spark.sql._
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl

/**
 * connect using Hortonworks Warehouse connector
 * used by huemul_ExternalDB.Using_HWC 
 */
class huemul_ExternalHWC(huemulBigDataGov: huemul_BigDataGovernance) extends Serializable {
  @transient private var _HWC_Hive: HiveWarehouseSessionImpl = null
  def getHWC_Hive: HiveWarehouseSessionImpl = {
    if (_HWC_Hive != null)
      return _HWC_Hive
      
    _HWC_Hive = HiveWarehouseSession.session(huemulBigDataGov.spark).build()
    

    return _HWC_Hive
  }
  
  def execute_NoResulSet(sql: String): Boolean = {
    val _hive = getHWC_Hive
    if (_hive == null)
      sys.error("can't connect with HIVE, HiveWarehouseSession.session doesnt works")
      
    return _hive.executeUpdate(sql)
  }
  
  def close {
    val _hive = getHWC_Hive
    if (_hive != null)
      _hive.session().close()
  }
}