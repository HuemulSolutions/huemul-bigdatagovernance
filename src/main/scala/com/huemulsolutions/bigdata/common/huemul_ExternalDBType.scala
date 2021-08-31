package com.huemulsolutions.bigdata.common

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.control.huemul_JDBCProperties

class huemul_ExternalDBType() extends Serializable {
  private var _active: Boolean = false
  def getActive: Boolean =  _active
  def setActive(value: Boolean): huemul_ExternalDBType = {
    _active = value
    this
  }
  
  private var _activeForHBASE: Boolean = false
  def getActiveForHBASE: Boolean =  _activeForHBASE
  def setActiveForHBASE(value: Boolean): huemul_ExternalDBType = {
    _activeForHBASE = value
    this
  }
  
  private var _JDBCdriver: String = _
  def getJDBCDriver: String =  _JDBCdriver
  def setJDBCDriver(value: String): huemul_ExternalDBType = {
    _JDBCdriver = value
    this
  }
  
  private var _connectionStrings: ArrayBuffer[huemul_KeyValuePath] = _
  def getConnectionStrings: ArrayBuffer[huemul_KeyValuePath] =  _connectionStrings
  def setConnectionStrings(value: ArrayBuffer[huemul_KeyValuePath]): huemul_ExternalDBType = {
    _connectionStrings = value
    this
  }
  
  @transient private var JDBC_connection: huemul_JDBCProperties = _
  def getJDBC_connection(huemulBigDataGov: huemul_BigDataGovernance): huemul_JDBCProperties = {
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
    JDBC_connection = new huemul_JDBCProperties(huemulBigDataGov, _connString,getJDBCDriver, huemulBigDataGov.DebugMode)
    JDBC_connection.StartConnection()
    JDBC_connection
  }
  //(this, _HIVE_connString,null, DebugMode) //Connection = null
 
}
