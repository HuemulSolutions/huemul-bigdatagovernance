
 ALTER TABLE control_tablesuse add tableuse_backupstatus			  int;
 /* status null --> old version
    status 0 --> without backup
    status 1 --> with backup
    status 2 --> backup deleted
 */
 
 