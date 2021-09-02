package com.huemulsolutions.bigdata.common

import com.hortonworks.hwc.HiveWarehouseSession
//import org.apache.spark.sql._
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl

/**
 * connect using Hortonworks Warehouse connector
 * used by huemul_ExternalDB.Using_HWC 
 */
class HuemulExternalHWC(huemulBigDataGov: HuemulBigDataGovernance) extends Serializable {
  @transient private var _hwcHive: HiveWarehouseSessionImpl = _
  def getHwcHive: HiveWarehouseSessionImpl = {
    if (_hwcHive != null)
      return _hwcHive

    _hwcHive = HiveWarehouseSession.session(huemulBigDataGov.spark).build()


    _hwcHive
  }

  def executeNoResultSet(sql: String): Boolean = {
    val _hive = getHwcHive
    if (_hive == null)
      sys.error("can't connect with HIVE, HiveWarehouseSession.session doesnt works")

    _hive.executeUpdate(sql)
  }

  def close() {
    val _hive = getHwcHive
    if (_hive != null)
      _hive.session().close()
  }
}