package com.huemulsolutions.bigdata.common

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.control.huemul_JDBCProperties

class huemul_ExternalDBType() extends Serializable {
  private var _active: Boolean = false
  def getActive(): Boolean = {return _active}
  def setActive(value: Boolean): huemul_ExternalDBType = {
    _active = value
    this
  }
  
  private var _activeForHBASE: Boolean = false
  def getActiveForHBASE(): Boolean = {return _activeForHBASE}
  def setActiveForHBASE(value: Boolean): huemul_ExternalDBType = {
    _activeForHBASE = value
    this
  }
  
  private var _JDBCdriver: String = null
  def getJDBCdriver(): String = {return _JDBCdriver}
  def setJDBCdriver(value: String): huemul_ExternalDBType = {
    _JDBCdriver = value
    this
  }
  
  private var _connectionStrings: ArrayBuffer[huemul_KeyValuePath] = null
  def getConnectionStrings(): ArrayBuffer[huemul_KeyValuePath] = {return _connectionStrings}
  def setConnectionStrings(value: ArrayBuffer[huemul_KeyValuePath]): huemul_ExternalDBType = {
    _connectionStrings = value
    this
  }
  
  @transient private var JDBC_connection: huemul_JDBCProperties = null
  def getJDBC_connection(huemulBigDataGov: huemul_BigDataGovernance): huemul_JDBCProperties = {
    if (JDBC_connection != null)
      return JDBC_connection
      
    if (getActive() == false && getActiveForHBASE() == false)
      return null
      
    if (getConnectionStrings == null)
      return null

    if (!huemulBigDataGov.GlobalSettings.ValidPath(getConnectionStrings, huemulBigDataGov.Environment)) {
      huemulBigDataGov.logMessageWarn(s"JDBC Connection not defined (driver: ${getJDBCdriver()})")
      return null
    }
    
    val _connString: String = huemulBigDataGov.GlobalSettings.GetPath(huemulBigDataGov, getConnectionStrings)
    JDBC_connection = new huemul_JDBCProperties(huemulBigDataGov, _connString,getJDBCdriver, huemulBigDataGov.DebugMode)
    JDBC_connection.StartConnection()
    return JDBC_connection
  }
  //(this, _HIVE_connString,null, DebugMode) //Connection = null
 
}
