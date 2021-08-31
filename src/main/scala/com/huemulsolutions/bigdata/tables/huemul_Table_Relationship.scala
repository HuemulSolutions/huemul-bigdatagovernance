package com.huemulsolutions.bigdata.tables

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification.huemulType_DQNotification


class huemul_Table_Relationship(Class_TableName: Object, allowNull: Boolean) {
  val Relationship: ArrayBuffer[huemul_Table_RelationshipColumns] = new ArrayBuffer[huemul_Table_RelationshipColumns]()
  var MyName: String = _
  val AllowNull: Boolean = allowNull
  def AddRelationship (PK: huemul_Columns, FK: huemul_Columns) {
      Relationship.append(new huemul_Table_RelationshipColumns(PK, FK) )
  }
  val _Class_TableName: Object = Class_TableName
 
  private var _Notification: com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification.huemulType_DQNotification = huemulType_DQNotification.ERROR
  def setNotification(value: huemulType_DQNotification ): huemul_Table_Relationship = {
    _Notification = value
    this
  } 
  def getNotification: huemulType_DQNotification =  _Notification
  
  //From 2.1 --> apply broadcast
  private var _BroadcastJoin: Boolean = false
  def broadcastJoin(value: Boolean ): huemul_Table_Relationship = {
    _BroadcastJoin = value
    this
  } 
  def getBroadcastJoin: Boolean =  _BroadcastJoin
  
  private var _externalCode: String = "HUEMUL_DQ_001"
  def getExternalCode: String = {
    //return
    if (_externalCode==null)
      "HUEMUL_DQ_001"
    else
      _externalCode
  }
  def setExternalCode(value: String): huemul_Table_Relationship = {
    _externalCode = value  
    this
  }
  
}