package com.huemulsolutions.bigdata.control

class huemul_TestPlan( testPlan_Id: String
                      ,testPlanGroup_Id: String
                      ,testPlan_name: String
                      ,testPlan_description: String
                      ,testPlan_resultExpected: String
                      ,testPlan_resultReal: String
                      ,testPlan_IsOK: Boolean) extends Serializable {
  def getTestPlan_Id(): String = testPlan_Id
  def gettestPlanGroup_Id(): String =  testPlanGroup_Id
  def gettestPlan_name(): String =  testPlan_name
  def gettestPlan_description(): String =  testPlan_description
  def gettestPlan_resultExpected(): String =  testPlan_resultExpected
  def gettestPlan_resultReal(): String =  testPlan_resultReal
  def gettestPlan_IsOK(): Boolean =  testPlan_IsOK
}