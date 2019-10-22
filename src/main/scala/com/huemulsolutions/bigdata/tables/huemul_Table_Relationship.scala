package com.huemulsolutions.bigdata.tables

import scala.collection.mutable.ArrayBuffer
import com.huemulsolutions.bigdata.common.huemul_BigDataGovernance
import com.huemulsolutions.bigdata.dataquality._
import com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification.huemulType_DQNotification

class huemul_Table_RelationshipColumns (pk: huemul_Columns, fk: huemul_Columns) {
  var PK: huemul_Columns = pk
  var FK: huemul_Columns = fk
}

class huemul_Table_Relationship(Class_TableName: Object, allowNull: Boolean) {
  val Relationship: ArrayBuffer[huemul_Table_RelationshipColumns] = new ArrayBuffer[huemul_Table_RelationshipColumns]()
  var MyName: String = null
  val AllowNull = allowNull
  def AddRelationship (PK: huemul_Columns, FK: huemul_Columns) {
      Relationship.append(new huemul_Table_RelationshipColumns(PK, FK) )
  }
  val _Class_TableName: Object = Class_TableName
 
  private var _Notification: com.huemulsolutions.bigdata.dataquality.huemulType_DQNotification.huemulType_DQNotification = huemulType_DQNotification.ERROR
  def setNotification(value: huemulType_DQNotification ): huemul_Table_Relationship = {
    _Notification = value
    this
  } 
  def getNotification(): huemulType_DQNotification = {return _Notification}
  
  //From 2.1 --> apply broadcast
  private var _BroadcastJoin: Boolean = false
  def broadcastJoin(value: Boolean ): huemul_Table_Relationship = {
    _BroadcastJoin = value
    this
  } 
  def getBroadcastJoin(): Boolean = {return _BroadcastJoin}
  
}