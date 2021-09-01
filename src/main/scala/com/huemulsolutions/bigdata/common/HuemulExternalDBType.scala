package com.huemulsolutions.bigdata.common

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.control.HuemulJDBCProperties

class HuemulExternalDBType() extends Serializable {
  private var _active: Boolean = false
  def getActive: Boolean =  _active
  def setActive(value: Boolean): HuemulExternalDBType = {
    _active = value
    this
  }

  private var _activeForHBASE: Boolean = false
  def getActiveForHBASE: Boolean =  _activeForHBASE
  def setActiveForHBASE(value: Boolean): HuemulExternalDBType = {
    _activeForHBASE = value
    this
  }

  private var _JDBCdriver: String = _
  def getJDBCDriver: String =  _JDBCdriver
  def setJDBCDriver(value: String): HuemulExternalDBType = {
    _JDBCdriver = value
    this
  }

  private var _connectionStrings: ArrayBuffer[HuemulKeyValuePath] = _
  def getConnectionStrings: ArrayBuffer[HuemulKeyValuePath] =  _connectionStrings
  def setConnectionStrings(value: ArrayBuffer[HuemulKeyValuePath]): HuemulExternalDBType = {
    _connectionStrings = value
    this
  }
  
  @transient private var JDBC_connection: HuemulJDBCProperties = _
  def getJDBC_connection(huemulBigDataGov: HuemulBigDataGovernance): HuemulJDBCProperties = {
    if (JDBC_connection != null)
      return JDBC_connection
      
    if (!getActive && !getActiveForHBASE)
      return null
      
    if (getConnectionStrings == null)
      return null

    if (!huemulBigDataGov.GlobalSettings.validPath(getConnectionStrings, huemulBigDataGov.Environment)) {
      huemulBigDataGov.logMessageWarn(s"JDBC Connection not defined (driver: $getJDBCDriver)")
      return null
    }
    
    val _connString: String = huemulBigDataGov.GlobalSettings.getPath(huemulBigDataGov, getConnectionStrings)
    JDBC_connection = new HuemulJDBCProperties(huemulBigDataGov, _connString,getJDBCDriver, huemulBigDataGov.DebugMode)
    JDBC_connection.StartConnection()
    JDBC_connection
  }
  //(this, _HIVE_connString,null, DebugMode) //Connection = null
 
}
