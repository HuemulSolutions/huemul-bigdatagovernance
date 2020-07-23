
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
 
 
 UPDATE control_config
 SET version_mayor	= 2
    ,version_minor	= 6
    ,version_patch  = 1
where config_id = 1;


 