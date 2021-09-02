package com.huemulsolutions.bigdata.common

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.control.HuemulJdbcProperties

class HuemulExternalDbType() extends Serializable {
  private var _active: Boolean = false
  def getActive: Boolean =  _active
  def setActive(value: Boolean): HuemulExternalDbType = {
    _active = value
    this
  }

  private var _activeForHBASE: Boolean = false
  def getActiveForHBase: Boolean =  _activeForHBASE
  def setActiveForHBASE(value: Boolean): HuemulExternalDbType = {
    _activeForHBASE = value
    this
  }

  private var _jdbcDriver: String = _
  def getJdbcDriver: String =  _jdbcDriver
  def setJdbcDriver(value: String): HuemulExternalDbType = {
    _jdbcDriver = value
    this
  }

  private var _connectionStrings: ArrayBuffer[HuemulKeyValuePath] = _
  def getConnectionStrings: ArrayBuffer[HuemulKeyValuePath] =  _connectionStrings
  def setConnectionStrings(value: ArrayBuffer[HuemulKeyValuePath]): HuemulExternalDbType = {
    _connectionStrings = value
    this
  }

  @transient private var jdbcConnection: HuemulJdbcProperties = _
  def getJdbcConnection(huemulBigDataGov: HuemulBigDataGovernance): HuemulJdbcProperties = {
    if (jdbcConnection != null)
      return jdbcConnection

    if (!getActive && !getActiveForHBase)
      return null

    if (getConnectionStrings == null)
      return null

    if (!huemulBigDataGov.globalSettings.validPath(getConnectionStrings, huemulBigDataGov.environment)) {
      huemulBigDataGov.logMessageWarn(s"JDBC Connection not defined (driver: $getJdbcDriver)")
      return null
    }

    val _connString: String = huemulBigDataGov.globalSettings.getPath(huemulBigDataGov, getConnectionStrings)
    jdbcConnection = new HuemulJdbcProperties(huemulBigDataGov, _connString,getJdbcDriver, huemulBigDataGov.debugMode)
    jdbcConnection.startConnection()
    jdbcConnection
  }
  //(this, _HIVE_connString,null, DebugMode) //Connection = null
 
}
