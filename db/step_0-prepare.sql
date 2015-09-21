rem TS - табличное пространство, в котором будут создаваться таблицы для миграции
--define ts='&1'
define ts=sbrftbs
----------------------------------------------
define script_step='0'
define script_name='prepare'
define script_full_name='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
define script_desc='Скрипт осуществляет удаление ограничений целостности в БД, создание промежуточных объектов, необходимых для работы. Запускается один раз.'

@@utl_head.sql
@@it$$check.sql

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
  ,last_ok_step   number
  ,ini_size       number default 0
  ,final_size     number default 0
)
tablespace &&ts.');
   ddl_pkg.create_table (p_table_name => 'it$$dup_tables', p_ddl => 'create table it$$dup_tables
(
   orig_tbl_name   varchar2 (30 char)
  ,tmp_tbl_name    varchar2 (30 char)
  ,cn              varchar2 (2000 char)
  ,ini_size        number default 0
  ,final_size      number default 0
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
  ,ddl_dml         varchar2 (2000 char)
  ,error_message   varchar2 (2000 char)
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
end;
/

merge into it$$step t
     using (select 0 step_no, 'Подготовительный этап для всех ТБ. Выполняется один раз.' description
                  ,'&&script_full_name.' script_name
              from dual
            union all
            select 1 step_no, 'Этап размножения данных по одному ТБ.' description, 'step_1-enlarge' script_name from dual
            union all
            select 3 step_no, 'Завершительный этап для всех ТБ. Выполняется один раз.' description
                  ,'step_2-final' script_name
              from dual) s
        on (s.step_no = t.step_no)
when matched
then
   update set t.description = s.description, t.script_name = s.script_name
when not matched
then
   insert     (step_no, description, script_name)
       values (s.step_no, s.description, s.script_name);

merge into it$$dup_tables t
     using (select tn orig_tbl_name
                  ,regexp_replace (tn || case when instr (tn, '_') > 0 then '' else '_' end
                                  ,'\_'
                                  ,'$'
                                  ,1
                                  ,1)
                      tmp_tbl_name
                  ,cn
              from (select 'JRN_SESSION_INFO' tn, ',SESSION_ID,JOURNAL_ID,' cn from dual
                    union all
                    select 'JRN_OPER_REQ_RESP' tn, ',REQUEST_ID,SESSION_ID,PAYMENT_PARENT_ID,' cn from dual
                    union all
                    select 'JRN_OPER_EVENT' tn, ',EVENT_ID,REQUEST_ID,' cn from dual
                    union all
                    select 'JRN_DTL_EVENT' tn, ',EVENT_ID,' cn from dual
                    union all
                    select 'JRN_DTL_EVENT_PROCESSING' tn, ',EVENT_ID,' cn from dual
                    union all
                    select 'JRN_EVENT_ERROR' tn, ',ERROR_ID,EVENT_ID,' cn from dual
                    union all
                    select 'JRN_NOTE_INFO' tn, ',NOTE_ID,EVENT_ID,' cn from dual
                    union all
                    select 'JRN_OPER_INFO' tn, ',REQUEST_ID,' cn from dual
                    union all
                    select 'JOURNAL_INFO' tn, ',JOURNAL_ID,' cn from dual
                    union all
                    select 'DISPUTED_REQUEST' tn, ',ID,INCIDENT_ID,' cn from dual
                    union all
                    select 'INC_DOC_PRINT' tn, ',ID,INCIDENT_ID,' cn from dual
                    union all
                    select 'INC_OPER_INFO' tn, ',INCIDENT_ID,INC_ID,' cn from dual
                    union all
                    select 'INC_OPER_DTL_INFO' tn, ',DTL_INFO_ID,INCIDENT_ID,' cn from dual
                    union all
                    select 'INC_NOTE_INFO' tn, ',NOTE_ID,DTL_INFO_ID,' cn from dual
                    union all
                    select 'INC_OPER_CLOB' tn, ',DTL_INFO_ID,' cn from dual
                    union all
                    select 'INC_PAYMENT' tn, ',ID,INCIDENT_ID,RBC_ID,SP_ID,DEBATE_ID,' cn from dual
                    union all
                    select 'INC_SYS_STATUS' tn, ',DTL_INFO_ID,' cn from dual
                    union all
                    select 'INC_SYSTEM_STATUS' tn, ',SYS_STATUS_ID,INCIDENT_ID,' cn from dual
                    union all
                    select 'INCIDENT' tn, ',INCIDENT_ID,ACTIVE_OPER_INFO_ID,' cn from dual)) s
        on (s.orig_tbl_name = t.orig_tbl_name)
when not matched
then
   insert     (orig_tbl_name, tmp_tbl_name, cn)
       values (s.orig_tbl_name, s.tmp_tbl_name, s.cn);

merge into it$$bank_code t
     using (select b.bank_code
              from dic_bank b
             where active = 1) s
        on (s.bank_code = t.bank_code)
when not matched
then
   insert     (bank_code
              ,last_ok_step
              ,ini_size
              ,final_size)
       values (s.bank_code
              ,null
              ,0
              ,0);

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
   update set ini_size = s.bytes;

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

declare
   exn#resource_busy   number := -54;
   exc#resource_busy   exception;
   pragma exception_init (exc#resource_busy, -54);
   exn#no_table        number := -942;
   exc#no_table        exception;
   pragma exception_init (exc#no_table, -942);
   c_err               varchar2 (2000 char);

   procedure wl (p_bank_code    it$$enlarge_log.bank_code%type
                ,p_tbl          it$$enlarge_log.orig_tbl_name%type
                ,p_ddl          it$$enlarge_log.ddl_dml%type
                ,p_err          it$$enlarge_log.error_message%type)
   as
      pragma autonomous_transaction;
   begin
      insert into it$$enlarge_log (step_no
                                  ,bank_code
                                  ,orig_tbl_name
                                  ,ddl_dml
                                  ,error_message)
           values (&&script_step.
                  ,p_bank_code
                  ,p_tbl
                  ,p_ddl
                  ,p_err);

      commit;
   end;
begin
   DBMS_OUTPUT.put_line ('Создание $-таблиц для сгенерированных искусственных данных');

   for t in (select tt.orig_tbl_name, tt.tmp_tbl_name, 'TRUNCATE TABLE ' || tt.tmp_tbl_name || ' REUSE STORAGE' trunc_ddl
                   ,'CREATE TABLE ' || tt.tmp_tbl_name || ' TABLESPACE &&TS. AS SELECT * FROM ' || tt.orig_tbl_name || ' WHERE 1=0' create_ddl
               from it$$dup_tables tt)
   loop
      begin
         execute immediate t.trunc_ddl;

         wl (null
            ,t.orig_tbl_name
            ,t.trunc_ddl
            ,null);
      exception
         when exc#resource_busy
         then
            c_err :=
                  'Таблица '
               || t.tmp_tbl_name
               || ' заблокирована и не может быть очищена. Устраните блокировку и запустите скрипт заново.';
            DBMS_OUTPUT.put_line (c_err);

            wl (null
               ,t.orig_tbl_name
               ,t.trunc_ddl
               ,c_err);
         when exc#no_table
         then
            execute immediate t.create_ddl;

            wl (null
               ,t.orig_tbl_name
               ,t.create_ddl
               ,null);
      end;
   end loop;


   DBMS_OUTPUT.put_line ('Отключение ограничений целостности на таблицах');

   declare
      should_repeat    boolean := false;
      try_count        number := 0;
      exn#dep_exists   number := -2297;
      exc#dep_exists   exception;
      pragma exception_init (exc#dep_exists, -2297);
   begin
      while (should_repeat = true or try_count < 10)
      loop
         should_repeat := false;

         for c
            in (select 'ALTER TABLE ' || c.table_name || ' MODIFY CONSTRAINT ' || c.constraint_name || ' DISABLE' alter_ddl, t.orig_tbl_name
                  from user_constraints c, it$$dup_tables t
                 where     status = 'ENABLED'
                       and (c.constraint_type in ('P', 'R') or (c.constraint_type = 'C' and c.generated = 'USER NAME'))
                       and c.table_name = t.orig_tbl_name)
         loop
            begin
               ddl_pkg.alter_table (c.alter_ddl);
               wl (null
                  ,c.orig_tbl_name
                  ,c.alter_ddl
                  ,null);
            exception
               when exc#dep_exists
               then
                  should_repeat := true;
            end;
         end loop;

         try_count := try_count + 1;
      end loop;
   end;

   DBMS_OUTPUT.enable (10000000);

   begin
      DBMS_OUTPUT.put_line (
         'Осуществляем слияние всех подсекций по секциям всех тербанков в одну секцию по умолчанию.');

      for b in (select * from it$$bank_code)
      loop
         for s in (  select *
                       from (select t.orig_tbl_name, tp.partition_name, tsp.subpartition_name, tsp.subpartition_position
                                   ,'ALTER TABLE ' || t.orig_tbl_name || ' MERGE SUBPARTITIONS ' || lag (tsp.subpartition_name) over (partition by t.orig_tbl_name order by tp.partition_position, tsp.subpartition_position) || ',' || tsp.subpartition_name || ' INTO SUBPARTITION ' || tsp.subpartition_name csql
                               from it$$dup_tables t
                                    inner join user_tab_partitions tp on tp.table_name = t.orig_tbl_name
                                    inner join user_tab_subpartitions tsp on tsp.partition_name = tp.partition_name and tsp.table_name = tp.table_name
                              where tp.partition_name = 'TB' || b.bank_code) q
                      where subpartition_position > 1
                   order by orig_tbl_name, subpartition_position)
         loop
            DBMS_OUTPUT.put_line (s.csql || ';');

            execute immediate s.csql;

            wl (b.bank_code
               ,s.orig_tbl_name
               ,s.csql
               ,null);
         end loop;
      end loop;
   end;
end;
/

prompt Проставим признак завершения этапа &&script_step. для банков

update it$$bank_code t
   set t.last_ok_step = &&script_step.;

prompt Проставим признак завершения этапа &&script_step. для этапа

update it$$step
   set completed = systimestamp
 where step_no = to_number (&&script_step.);

@@utl_foot.sql
undefine ts

exit