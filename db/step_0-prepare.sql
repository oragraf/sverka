define script_step='0'
define script_name='prepare'
define script_full_name='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
define script_desc='Скрипт осуществляет удаление ограничений целостности в БД, создание промежуточных объектов, необходимых для работы. Запускается один раз.'

@@utl_head.sql

prompt Создание списка таблиц для размножения данных

begin
   ddl_pkg.drop_table ('it$$step');
   ddl_pkg.drop_table ('it$$enlarge_log');
   ddl_pkg.drop_table ('it$$bank_code');
   ddl_pkg.drop_table ('it$$dup_tables');

   ddl_pkg.create_table (p_table_name => 'it$$step', p_ddl => 'CREATE TABLE it$$step
(
   step_no       NUMBER,
   completed     timestamp,
   script_name   varchar2(1000 char),
   description   VARCHAR2 (2000 CHAR)
)
TABLESPACE &&TS.');
   ddl_pkg.create_table (p_table_name => 'it$$bank_code', p_ddl => 'create table it$$bank_code
(
   bank_code      varchar2 (2 char)
  ,is_process     number(1) default 0
  ,last_ok_step   number
  ,ini_size       number default 0
  ,final_size     number default 0
  ,ini_row_count  number default 0
  ,final_row_count number default 0  
  ,is_source      number(1)
)
tablespace &&ts.');
   ddl_pkg.create_table (p_table_name => 'it$$dup_tables', p_ddl => 'create table it$$dup_tables
(
   orig_tbl_name   varchar2 (30 char)
  ,tmp_tbl_name    varchar2 (30 char)
  ,cn              varchar2 (2000 char)
  ,date_col        varchar2 (30 char)
  ,ini_size        number default 0
  ,final_size      number default 0
  ,rebuild_order   number
  ,constr_order    number
  ,min_id          number
  ,max_id          number
  ,id_diff         number
  ,ini_row_count   number
  ,final_row_count number
  ,min_trans_date  timestamp
  ,max_trans_date  timestamp
  ,date_diff       interval day(9) to second(9) 
)
tablespace &&ts.');
   ddl_pkg.create_table (
      p_table_name   => 'it$$enlarge_log'
     ,p_ddl          => 'create table it$$enlarge_log
(
   timemark        number   default to_number (to_char (systimestamp, ''yyyymmddhh24missff9''), ''fm99999999999999999999999999999999999'')
  ,step_no         number
  ,bank_code       varchar2 (2 char)
  ,orig_tbl_name   varchar2 (30 char)  
  ,ddl_dml         clob
  ,error_message   clob
)
tablespace &&ts.');
   ddl_pkg.alter_table ('alter table it$$step add constraint it$$step_pk primary key (step_no) using index tablespace &&ts.');
   ddl_pkg.alter_table ('alter table it$$bank_code add constraint it$$bank_code_pk primary key (bank_code) using index tablespace &&ts.');
   ddl_pkg.alter_table ('alter table it$$dup_tables add constraint it$$dup_tables_pk primary key (orig_tbl_name) using index tablespace &&ts.');
   --ddl_pkg.alter_table ('alter table it$$enlarge_log add constraint it$$enlarge_log_pk primary key (timemark) using index tablespace &&ts.');
   ddl_pkg.alter_table ('alter table it$$bank_code add constraint it$$bank_code_fk foreign key (last_ok_step) references it$$step(step_no)');
   ddl_pkg.alter_table ('alter table it$$enlarge_log add constraint it$$enlarge_log_fk1 foreign key (bank_code) references it$$bank_code(bank_code)');
   ddl_pkg.alter_table ('alter table it$$enlarge_log add constraint it$$enlarge_log_fk2 foreign key (orig_tbl_name) references it$$dup_tables(orig_tbl_name)');
   ddl_pkg.alter_table ('alter table it$$enlarge_log add constraint it$$enlarge_log_fk3 foreign key (step_no) references it$$step(step_no)');
   ddl_pkg.comment_tab ('it$$step', 'Служебная таблица для целей распучивания БД. Перечень шагов.');
   ddl_pkg.comment_tab (
      'it$$bank_code'
     ,'Служебная таблица для целей распучивания БД. Перечень партиций, они же банковские коды.');
   ddl_pkg.alter_table ('alter table it$$bank_code add ( constraint it$$bank_code_c01 check (is_source is null or is_source=1) enable validate)');
   ddl_pkg.alter_table ('alter table it$$bank_code add ( unique (is_source) using index tablespace &&ts. )');
end;
/

merge into it$$step t
     using (select 0 step_no, 'Подготовительный этап для всех ТБ. Выполняется один раз.' description
                  ,'&&script_full_name.' script_name
              from dual
            union all
            select 1 step_no, 'Этап размножения данных по одному ТБ.' description, 'step_1-enlarge' script_name from dual
            union all
            select 2 step_no, 'Завершительный этап для всех ТБ. Выполняется один раз.' description, 'step_2-final' script_name
              from dual) s
        on (s.step_no = t.step_no)
when matched
then
   update set t.description = s.description, t.script_name = s.script_name
when not matched
then
   insert     (step_no, description, script_name)
       values (s.step_no, s.description, s.script_name);

declare
   l_date_diff   interval day (9) to second (9);
begin
   l_date_diff := numtodsinterval (DBMS_RANDOM.value (1, power (2, 10)), 'second');

   merge into it$$dup_tables t
        using (select tn orig_tbl_name
                     ,regexp_replace (tn || case when instr (tn, '_') > 0 then '' else '_' end
                                     ,'\_'
                                     ,'$'
                                     ,1
                                     ,1)
                         tmp_tbl_name
                     ,cn
                     ,l
                     ,date_col
                     ,l_date_diff date_diff
                 from (select 0 l, 'JOURNAL_INFO' tn, ',JOURNAL_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 1 l, 'JRN_SESSION_INFO' tn, ',SESSION_ID,JOURNAL_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 2 l, 'JRN_OPER_REQ_RESP' tn, ',REQUEST_ID,SESSION_ID,PAYMENT_PARENT_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 3 l, 'JRN_OPER_EVENT' tn, ',EVENT_ID,REQUEST_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 4 l, 'JRN_DTL_EVENT' tn, ',EVENT_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 4 l, 'JRN_DTL_EVENT_PROCESSING' tn, ',EVENT_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 4 l, 'JRN_EVENT_ERROR' tn, ',ERROR_ID,EVENT_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 4 l, 'JRN_NOTE_INFO' tn, ',NOTE_ID,EVENT_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 3 l, 'JRN_OPER_INFO' tn, ',REQUEST_ID,' cn, 'REPORT_DATE' date_col from dual
                       union all
                       select 1 l, 'DISPUTED_REQUEST' tn, ',ID,INCIDENT_ID,' cn, 'TRANS_DATE' date_col from dual
                       union all
                       select 1 l, 'INC_DOC_PRINT' tn, ',ID,INCIDENT_ID,' cn, 'TRANS_DATE' date_col from dual
                       union all
                       select 1 l, 'INC_OPER_INFO' tn, ',INCIDENT_ID,INC_ID,' cn, 'TRANS_DATE' date_col from dual
                       union all
                       select 2 l, 'INC_OPER_DTL_INFO' tn, ',DTL_INFO_ID,INCIDENT_ID,' cn, 'INC_TRANS_DATE' date_col from dual
                       union all
                       select 3 l, 'INC_NOTE_INFO' tn, ',NOTE_ID,DTL_INFO_ID,' cn, 'TRANS_DATE' date_col from dual
                       union all
                       select 3 l, 'INC_OPER_CLOB' tn, ',DTL_INFO_ID,' cn, 'TRANS_DATE' date_col from dual
                       union all
                       select 3 l, 'INC_PAYMENT' tn, ',ID,INCIDENT_ID,RBC_ID,SP_ID,DEBATE_ID,' cn, 'TRANS_DATE' date_col from dual
                       union all
                       select 3 l, 'INC_SYS_STATUS' tn, ',DTL_INFO_ID,' cn, 'TRANS_DATE' date_col from dual
                       union all
                       select 2 l, 'INC_SYSTEM_STATUS' tn, ',SYS_STATUS_ID,INCIDENT_ID,' cn, 'TRANS_DATE' date_col from dual
                       union all
                       select 0 l, 'INCIDENT' tn, ',INCIDENT_ID,ACTIVE_OPER_INFO_ID,' cn, 'TRANS_DATE' date_col from dual)) s
           on (s.orig_tbl_name = t.orig_tbl_name)
   when not matched
   then
      insert     (orig_tbl_name
                 ,tmp_tbl_name
                 ,cn
                 ,constr_order
                 ,date_col
                 ,date_diff)
          values (s.orig_tbl_name
                 ,s.tmp_tbl_name
                 ,s.cn
                 ,s.l
                 ,s.date_col
                 ,s.date_diff);
end;
/

merge into it$$bank_code t
     using (select b.bank_code, case when bank_code = &&def_bank. or row_number () over (order by null) <= &&ratio. - 1 then 1 else 0 end is_process
                  ,decode (bank_code, &&def_bank., 1) is_source
              from dic_bank b
             where active = 1 and b.bank_code != 92) s
        on (s.bank_code = t.bank_code)
when not matched
then
   insert     (bank_code
              ,last_ok_step
              ,ini_size
              ,final_size
              ,is_process
              ,is_source)
       values (s.bank_code
              ,null
              ,0
              ,0
              ,s.is_process
              ,s.is_source);

prompt Проставим исходный суммарный размер сегментов в разрезе банков

merge into it$$bank_code t
     using (  select substr (sp.partition_name, 3, 2) bank_code, sum (nvl (s.bytes, 0)) bytes
                from user_tab_subpartitions sp
                     inner join it$$dup_tables t on t.orig_tbl_name = sp.table_name
                     left join user_segments s on sp.table_name = s.segment_name
            group by substr (sp.partition_name, 3, 2)) s
        on (s.bank_code = t.bank_code)
when matched
then
   update set ini_size = s.bytes
           where t.is_process = 1;

prompt Проставим исходный суммарный размер сегментов в разрезе таблиц

merge into it$$dup_tables t
     using (  select sp.table_name, sum (nvl (s.bytes, 0)) bytes
                from user_tab_subpartitions sp
                     inner join it$$dup_tables t on t.orig_tbl_name = sp.table_name
                     left join user_segments s on sp.table_name = s.segment_name
            group by sp.table_name) s
        on (t.orig_tbl_name = s.table_name)
when matched
then
   update set ini_size = s.bytes;

commit;


@@it$$utl.sql

exec it$$utl.checkoff_cons(&&script_step.);

exec it$$utl.merge_part(&&script_step.);

exec it$$utl.enlarge_source (p_step=> &&script_step., p_ratio => &&source_ratio., p_tablespace=> '&&TS.');

@@utl_foot.sql

exit