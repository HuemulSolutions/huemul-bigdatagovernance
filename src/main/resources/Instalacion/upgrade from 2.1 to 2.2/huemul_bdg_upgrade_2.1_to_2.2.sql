
 ALTER TABLE control_tablesuse add tableuse_backupstatus			  int;
 /* status null --> old version
    status 0 --> without backup
    status 1 --> with backup
    status 2 --> backup deleted
 */
 
 CREATE INDEX IDX_control_tablesuse_I01 ON control_tablesuse (tableuse_backupstatus);
 
 UPDATE control_config
 SET version_mayor	= 2
    ,version_minor	= 2
    ,version_patch  = 0
where config_id = 1;


 