
 ALTER TABLE control_columns add column_datatypedeploy			  varchar(50);
 
 UPDATE control_columns SET column_datatypedeploy = column_datatype WHERE column_datatypedeploy IS NULL;
 
 
 UPDATE control_config
 SET version_mayor	= 2
    ,version_minor	= 5
    ,version_patch  = 0
where config_id = 1;


 