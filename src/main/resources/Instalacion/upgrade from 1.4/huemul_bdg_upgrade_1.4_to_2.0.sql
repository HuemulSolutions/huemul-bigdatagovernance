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


alter table control_tables add table_autoincupdate     int;