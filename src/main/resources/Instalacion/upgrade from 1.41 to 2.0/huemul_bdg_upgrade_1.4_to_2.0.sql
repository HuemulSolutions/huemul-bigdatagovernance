alter table control_rawfilesdet rename column rawfilesdet_data_colseparatortype     to rawfilesdet_data_colseptype;
alter table control_rawfilesdet rename column rawfilesdet_data_colseparator         to rawfilesdet_data_colsep;
alter table control_rawfilesdet rename column rawfilesdet_data_headercolumnsstring  to rawfilesdet_data_headcolstring;
alter table control_rawfilesdet rename column rawfilesdet_log_colseparatortype      to rawfilesdet_log_colseptype;
alter table control_rawfilesdet rename column rawfilesdet_log_colseparator          to rawfilesdet_log_colsep;
alter table control_rawfilesdet rename column rawfilesdet_log_headercolumnsstring   to rawfilesdet_log_headcolstring;
alter table control_rawfilesdet rename column rawfilesdet_log_numrowsfieldname      to rawfilesdet_log_numrowsfield;

alter table control_rawfilesdet alter column rawfilesdet_data_headcolstring type varchar(4000);
alter table control_rawfilesdet alter column rawfilesdet_log_headcolstring type varchar(4000);

alter table control_rawfilesuse alter column rawfiles_headerline type varchar(4000);
alter table control_tables alter column table_sqlcreate     type varchar(4000);
alter table control_dq alter column dq_sqlformula       type varchar(4000);
alter table control_dq alter column dq_resultdq         type varchar(4000);
alter table control_error alter column error_message       type varchar(4000);
alter table control_error alter column error_trace         type varchar(4000);
alter table control_processexecparams alter column processexecparams_value type varchar(4000);

alter table control_dq add dq_iswarning			  int;
alter table control_dq add dq_duration_hour		  int;
alter table control_dq add dq_duration_minute	  int;
alter table control_dq add dq_duration_second	  int;


alter table control_tables add table_autoincupdate     int;
alter table control_columns add column_enableoldvaluetrace int;

alter table control_columns add column_businessglossary	 varchar(100);

alter table control_tables add table_dq_isused int;
alter table control_tables add table_fullname_dq varchar(1200);
alter table control_tables add table_ovt_isused int;
alter table control_tables add table_fullname_ovt varchar(1200);

alter table control_tables add table_backup		 	  int;

alter table control_dq add dq_externalcode         varchar(200);

create index idx_control_testplan_i01 on control_testplan (testplangroup_id, testplan_name);
create index idx_control_tablesrel_i01 on control_tablesrel (table_idpk, table_idfk, tablefk_namerelationship);
                               
create table control_query 		(query_id			     varchar(50)
								,processexecstep_id      varchar(50)
                                ,processexec_id          varchar(50)      
                                ,rawfiles_id             varchar(50)
                                ,rawfilesdet_id		     varchar(50)
                                ,table_id                varchar(50)
                                ,query_alias			 varchar(200)
                                ,query_sql_from		     varchar(4000)
                                ,query_sql_where		 varchar(4000)
                                ,query_numerrors	     int
                                ,query_autoinc			 int
                                ,query_israw			 int
                                ,query_isfinaltable	     int
                                ,query_isquery			 int
                                ,query_isreferenced		 int
                                ,query_numrows_real		 int
                                ,query_numrows_expected  int
                                ,query_duration_hour	 int
                                ,query_duration_min		 int
                                ,query_duration_sec		 int			 
                                ,error_id                varchar(50)
                                ,mdm_fhcreate            varchar(30)
                                ,mdm_processname         varchar(200)    
                                ,primary key (query_id)
                                );
                                
create table control_querycolumn 	  (querycol_id					varchar(50)
									  ,query_id						varchar(50)
									  ,rawfilesdet_id               varchar(50)
									  ,column_id                  	varchar(50)
									  ,querycol_pos					int
									  ,querycol_name				varchar(200)
									  ,querycol_sql					varchar(4000)
									  ,querycol_posstart			int
									  ,querycol_posend				int
									  ,querycol_line				int
									  ,mdm_fhcreate            varchar(30)
                                	  ,mdm_processname         varchar(200)    
									  ,primary key (querycol_id)
                                );
                                
create index idx_control_querycolumn_i01 on control_querycolumn (query_id, querycol_name);                                
                                
create table control_querycolumnori		(querycolori_id					varchar(50)       
										,querycol_id					varchar(50)
										,table_idori                	varchar(50)
										,column_idori					varchar(50)
										,rawfilesdet_idori             	varchar(50)
										,rawfilesdetfields_idori		varchar(50)
										,query_idori					varchar(50)
										,querycol_idori					varchar(50)
										,querycolori_dbname				varchar(200)
										,querycolori_tabname			varchar(200)
										,querycolori_tabalias		 	varchar(200)
										,querycolori_colname			varchar(200)	
										,querycolori_isselect			int
										,querycolori_iswhere			int
										,querycolori_ishaving			int
										,querycolori_isorder			int
										,mdm_fhcreate            varchar(30)
                                		,mdm_processname         varchar(200)    
										,primary key (querycolori_id)
                                );	
                                
                                
                                
 ALTER TABLE control_tablesuse RENAME TO control_tablesuse_old;
 
 create table  control_tablesuse (			  tablesuse_id            varchar(50)  
                                             ,table_id                varchar(50)
                                             ,process_id              varchar(200)
                                             ,processexec_id          varchar(50)
                                             ,processexecstep_id      varchar(50)
                                             ,tableuse_year        int
                                             ,tableuse_month       int
                                             ,tableuse_day         int
                                             ,tableuse_hour        int
                                             ,tableuse_minute       int
                                             ,tableuse_second      int
                                             ,tableuse_params      varchar(1000)
                                             ,tableuse_read           int
                                             ,tableuse_write          int
                                             ,tableuse_numrowsnew     int
                                             ,tableuse_numrowsupdate  int
											 ,tableuse_numrowsupdatable  int
											 ,tableuse_numrowsnochange  int
                                             ,tableuse_numrowsmarkdelete  int
                                             ,tableuse_numrowstotal   int
											 ,tableuse_partitionvalue varchar(200)
											 ,tableuse_pathbackup	  varchar(1000)
                                             ,mdm_fhcreate            varchar(30)
                                             ,mdm_processname         varchar(200)
                                             ,primary key (tablesuse_id) 
                                            );
 
 insert into control_tablesuse (  tablesuse_id
                                 ,table_id
                                 ,process_id
                                 ,processexec_id
                                 ,processexecstep_id
                                 ,tableuse_year
                                 ,tableuse_month
                                 ,tableuse_day
                                 ,tableuse_hour
                                 ,tableuse_minute
                                 ,tableuse_second
                                 ,tableuse_params
                                 ,tableuse_read
                                 ,tableuse_write
                                 ,tableuse_numrowsnew
                                 ,tableuse_numrowsupdate
								 ,tableuse_numrowsupdatable 
								 ,tableuse_numrowsnochange  
                                 ,tableuse_numrowsmarkdelete
                                 ,tableuse_numrowstotal 
								 ,tableuse_partitionvalue
								 ,tableuse_pathbackup
                                 ,mdm_fhcreate
                                 ,mdm_processname)
 select  row_number() over(order by table_id) as tablesuse_id
         ,table_id
         ,process_id
         ,processexec_id
         ,processexecstep_id
         ,tableuse_year
         ,tableuse_month
         ,tableuse_day
         ,tableuse_hour
         ,tableuse_minute
         ,tableuse_second
         ,tableuse_params
         ,tableuse_read
         ,tableuse_write
         ,tableuse_numrowsnew
         ,tableuse_numrowsupdate
		 ,tableuse_numrowsupdatable 
		 ,tableuse_numrowsnochange  
         ,tableuse_numrowsmarkdelete
         ,tableuse_numrowstotal 
		 ,tableuse_partitionvalue
		 ,null as tableuse_pathbackup
         ,mdm_fhcreate
         ,mdm_processname
FROM control_tablesuse_old;

drop table control_tablesuse_old;
 
 