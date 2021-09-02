package samples


import com.huemulsolutions.bigdata.common._

object globalSettings {
   val Global: HuemulGlobalPath  = new HuemulGlobalPath()
   Global.globalEnvironments = "production, experimental"
   
   Global.controlSetting.append(new HuemulKeyValuePath("production","jdbc:postgresql://{{000.000.000.000}}:5432/{{database_name}}?user={{user_name}}&password={{password}}&currentSchema=public"))
   Global.controlSetting.append(new HuemulKeyValuePath("experimental","jdbc:postgresql://{{000.000.000.000}}:5432/{{database_name}}?user={{user_name}}&password={{password}}&currentSchema=public"))
   
   Global.impalaSetting.append(new HuemulKeyValuePath("production","jdbc:postgresql://{{000.000.000.000}}:5432/{{database_name}}?user={{user_name}}&password={{password}}&currentSchema=public"))
   Global.impalaSetting.append(new HuemulKeyValuePath("experimental","jdbc:postgresql://{{000.000.000.000}}:5432/{{database_name}}?user={{user_name}}&password={{password}}&currentSchema=public"))
   
   //TEMPORAL SETTING
   Global.temporalPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/temp/"))
   Global.temporalPath.append(new HuemulKeyValuePath("experimental","hdfs://hdfs:///user/data/experimental/temp/"))
     
   //RAW SETTING
   Global.rawSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/raw/"))
   Global.rawSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/raw/"))
   
   Global.rawBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/raw/"))
   Global.rawBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/raw/"))
   
   
   
   //MASTER SETTING
   Global.masterDataBase.append(new HuemulKeyValuePath("production","production_master"))
   Global.masterDataBase.append(new HuemulKeyValuePath("experimental","experimental_master"))

   Global.masterSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/master/"))
   Global.masterSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/master/"))
   
   Global.masterBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/master/"))
   Global.masterBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/master/"))

   //DIM SETTING
   Global.dimDataBase.append(new HuemulKeyValuePath("production","production_dim"))
   Global.dimDataBase.append(new HuemulKeyValuePath("experimental","experimental_dim"))

   Global.dimSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/dim/"))
   Global.dimSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/dim/"))
   
   Global.dimBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/dim/"))
   Global.dimBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/dim/"))

   //ANALYTICS SETTING
   Global.analyticsDataBase.append(new HuemulKeyValuePath("production","production_analytics"))
   Global.analyticsDataBase.append(new HuemulKeyValuePath("experimental","experimental_analytics"))
   
   Global.analyticsSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/analytics/"))
   Global.analyticsSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/analytics/"))
   
   Global.analyticsBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/analytics/"))
   Global.analyticsBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/analytics/"))

   //REPORTING SETTING
   Global.reportingDataBase.append(new HuemulKeyValuePath("production","production_reporting"))
   Global.reportingDataBase.append(new HuemulKeyValuePath("experimental","experimental_reporting"))

   Global.reportingSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/reporting/"))
   Global.reportingSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/reporting/"))
   
   Global.reportingBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/reporting/"))
   Global.reportingBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/reporting/"))

   //SANDBOX SETTING
   Global.sandboxDataBase.append(new HuemulKeyValuePath("production","production_sandbox"))
   Global.sandboxDataBase.append(new HuemulKeyValuePath("experimental","experimental_sandbox"))
   
   Global.sandboxSmallFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/sandbox/"))
   Global.sandboxSmallFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/sandbox/"))
   
   Global.sandboxBigFilesPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/sandbox/"))
   Global.sandboxBigFilesPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/sandbox/"))
   
   //DQ_ERROR SETTING
   Global.dqSaveErrorDetails = true
   Global.dqErrorDataBase.append(new HuemulKeyValuePath("production","production_DQError"))
   Global.dqErrorDataBase.append(new HuemulKeyValuePath("experimental","experimental_DQError"))
   
   Global.dqErrorPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/dqerror/"))
   Global.dqErrorPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/dqerror/"))

   //OLD VALUE TRACE
   Global.mdmSaveOldValueTrace = true
   Global.mdmOldValueTraceDataBase.append(new HuemulKeyValuePath("production","production_mdm_oldvalue"))
   Global.mdmOldValueTraceDataBase.append(new HuemulKeyValuePath("experimental","experimental_mdm_oldvalue"))
   
   Global.mdmOldValueTracePath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/mdm_oldvalue/"))
   Global.mdmOldValueTracePath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/mdm_oldvalue/"))

   //BACKUP
   Global.mdmBackupPath.append(new HuemulKeyValuePath("production","hdfs:///user/data/production/backup/"))
   Global.mdmBackupPath.append(new HuemulKeyValuePath("experimental","hdfs:///user/data/experimental/backup/"))

}

