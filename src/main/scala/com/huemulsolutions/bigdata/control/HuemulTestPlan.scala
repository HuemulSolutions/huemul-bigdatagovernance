package com.huemulsolutions.bigdata.control

class HuemulTestPlan(testPlan_Id: String
                     , testPlanGroup_Id: String
                     , testPlan_name: String
                     , testPlan_description: String
                     , testPlan_resultExpected: String
                     , testPlan_resultReal: String
                     , testPlan_IsOK: Boolean) extends Serializable {
  def getTestPlan_Id: String = testPlan_Id
  def getTestPlanGroup_Id: String =  testPlanGroup_Id
  def getTestPlanName: String =  testPlan_name
  def getTestPlanDescription: String =  testPlan_description
  def getTestPlanResultExpected: String =  testPlan_resultExpected
  def getTestPlanResultReal: String =  testPlan_resultReal
  def getTestPlanIsOK: Boolean =  testPlan_IsOK
}