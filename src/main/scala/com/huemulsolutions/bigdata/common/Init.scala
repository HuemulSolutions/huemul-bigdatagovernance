package com.huemulsolutions.bigdata.common

import com.huemulsolutions.bigdata.control.HuemulControl
import com.huemulsolutions.bigdata.control.HuemulTypeFrequency

object Init {
  def main(args: Array[String]): Unit = {
    val global: HuemulGlobalPath = new HuemulGlobalPath()
    global.globalEnvironments = "production,experimental"
 
    val huemulBigDataGov  = new HuemulBigDataGovernance(s"BigData API", args, global)
    
    new HuemulControl(huemulBigDataGov,null, HuemulTypeFrequency.ANY_MOMENT, false, false)
    //Control.Init_CreateTables()

    huemulBigDataGov.close()
  }
}

object testReadUrlMonitoring {
  def main(args: Array[String]): Unit = {
    val global: HuemulGlobalPath = new HuemulGlobalPath()
    global.globalEnvironments = "production, experimental"
    global.controlSetting.append(new HuemulKeyValuePath("production",s"file.txt"))
    global.impalaSetting.append(new HuemulKeyValuePath("production",s"file.txt"))
    global.temporalPath.append(new HuemulKeyValuePath("production",s"/usr/production/temp/"))
    global.dqErrorPath.append(new HuemulKeyValuePath("production",s"/usr/production/temp/"))
    global.dqErrorDataBase.append(new HuemulKeyValuePath("production",s"dqerror_database"))
    global.setValidationLight()

    val huemulBigDataGov  = new HuemulBigDataGovernance(s"BigData API test URL Monitoring", args, global)

    val controlTest = new HuemulControl(huemulBigDataGov,null, HuemulTypeFrequency.ANY_MOMENT, false, false)

    controlTest.newStep(s"start test")
    //get url from spark
    val urlMonitor = s"${huemulBigDataGov.IdPortMonitoring}/api/v1/applications/"
    controlTest.newStep(s"url monitoring: $urlMonitor")
    //Get Id App from Spark URL Monitoring
    try {
      var i = 1
      while (i <= 5) {
        val (idAppFromAPI, status) = huemulBigDataGov.getIdFromExecution(urlMonitor)
        controlTest.newStep(s"wait for 10 seconds, cycle $i/5, read from port: $idAppFromAPI, $status ")
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