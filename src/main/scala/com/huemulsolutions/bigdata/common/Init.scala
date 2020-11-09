package com.huemulsolutions.bigdata.common

import com.huemulsolutions.bigdata.control.huemul_Control
import com.huemulsolutions.bigdata.control.huemulType_Frequency

object Init {
  def main(args: Array[String]): Unit = {
    val Global: huemul_GlobalPath = new huemul_GlobalPath()
    Global.GlobalEnvironments = "production,experimental"
 
    val huemulBigDataGov  = new huemul_BigDataGovernance(s"BigData API", args, Global)
    
    new huemul_Control(huemulBigDataGov,null, huemulType_Frequency.ANY_MOMENT, false, false)
    //Control.Init_CreateTables()

    huemulBigDataGov.close()
  }
}

object testReadUrlMonitoring {
  def main(args: Array[String]): Unit = {
    val Global: huemul_GlobalPath = new huemul_GlobalPath()
    Global.GlobalEnvironments = "production, experimental"
    Global.CONTROL_Setting.append(new huemul_KeyValuePath("production",s"file.txt"))
    Global.IMPALA_Setting.append(new huemul_KeyValuePath("production",s"file.txt"))
    Global.TEMPORAL_Path.append(new huemul_KeyValuePath("production",s"/usr/production/temp/"))
    Global.DQError_Path.append(new huemul_KeyValuePath("production",s"/usr/production/temp/"))
    Global.DQError_DataBase.append(new huemul_KeyValuePath("production",s"dqerror_database"))
    Global.setValidationLight()

    val huemulBigDataGov  = new huemul_BigDataGovernance(s"BigData API test URL Monitoring", args, Global)

    val control_test = new huemul_Control(huemulBigDataGov,null, huemulType_Frequency.ANY_MOMENT, false, false)

    control_test.NewStep(s"start test")
    //get url from spark
    val URLMonitor = s"${huemulBigDataGov.IdPortMonitoring}/api/v1/applications/"
    control_test.NewStep(s"url monitoring: $URLMonitor")
    //Get Id App from Spark URL Monitoring
    try {
      var i = 1
      while (i <= 5) {
        val (idAppFromAPI, status) = huemulBigDataGov.getIdFromExecution(URLMonitor)
        control_test.NewStep(s"wait for 10 seconds, cycle $i/5, read from port: $idAppFromAPI, $status ")
        Thread.sleep(10000)

        i += 1
      }
    } catch {
      case e: Exception =>
        huemulBigDataGov.logMessageError(e)
    }


    huemulBigDataGov.close()
    //Control.Init_CreateTables()
  }
}