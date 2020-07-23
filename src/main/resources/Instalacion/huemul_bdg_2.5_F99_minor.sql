-- Stores the parameters of the Spark environment
CREATE TABLE control_processexecenv(
	 processexec_id             varchar(100) NOT NULL
	,processexecenvcategory_name    VARCHAR(50) NOT NULL
	,processexecenv_name         VARCHAR(500) NOT NULL
	,processexecenv_value        VARCHAR(4000) NULL
	,mdm_fhcreate               VARCHAR(30) NULL
	,mdm_processname            VARCHAR(200) NULL
	,PRIMARY KEY (processexec_id, processexecenvcategory_name, processexecenv_name)
);

COMMENT ON TABLE control_processexecenv IS 'Stores the parameters of the Spark environment';
COMMENT ON COLUMN control_processexecenv.processexec_id IS 'Identification of the spark application execution id';
COMMENT ON COLUMN control_processexecenv.processexecenvcategory_name IS 'Parameter category type (RunTime, SparkProperties, SystemProperties)';
COMMENT ON COLUMN control_processexecenv.processexecenv_name IS 'Parameter name (e.g. spark.executor.cores)';
COMMENT ON COLUMN control_processexecenv.processexecenv_value IS 'Parameter value';
COMMENT ON COLUMN control_processexecenv.mdm_fhcreate IS 'DateTime of record creation';
COMMENT ON COLUMN control_processexecenv.mdm_processname IS 'Process Invocation Name (class name)';
