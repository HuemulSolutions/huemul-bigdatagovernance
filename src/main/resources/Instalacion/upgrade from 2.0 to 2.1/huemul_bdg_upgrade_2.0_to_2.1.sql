 ALTER TABLE control_tablesuse add tableuse_numrowsexcluded			  int;
 
 CREATE TABLE control_config (   config_id		int
				 				,version_mayor	int
				                ,version_minor	int
				                ,version_patch	int
								,config_dtlog	string
								,primary key (config_id));
								
								
ALTER TABLE control_processexec ADD processexec_huemulversion varchar(20);
ALTER TABLE control_processexec ADD processexec_controlversion varchar(20);