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
  ,is_source      number(1)
)
tablespace &&ts.');
   ddl_pkg.create_table (p_table_name => 'it$$dup_tables', p_ddl => 'create table it$$dup_tables
(
   orig_tbl_name   varchar2 (30 char)
  ,tmp_tbl_name    varchar2 (30 char)
  ,cn              varchar2 (2000 char)
  ,ini_size        number default 0
  ,final_size      number default 0
  ,rebuild_order   number
  ,constr_order    number
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
              from (select 0 l, 'JOURNAL_INFO' tn, ',JOURNAL_ID,' cn from dual
                    union all
                    select 1 l, 'JRN_SESSION_INFO' tn, ',SESSION_ID,JOURNAL_ID,' cn from dual
                    union all
                    select 2 l, 'JRN_OPER_REQ_RESP' tn, ',REQUEST_ID,SESSION_ID,PAYMENT_PARENT_ID,' cn from dual
                    union all
                    select 3 l, 'JRN_OPER_EVENT' tn, ',EVENT_ID,REQUEST_ID,' cn from dual
                    union all
                    select 4 l, 'JRN_DTL_EVENT' tn, ',EVENT_ID,' cn from dual
                    union all
                    select 4 l, 'JRN_DTL_EVENT_PROCESSING' tn, ',EVENT_ID,' cn from dual
                    union all
                    select 4 l, 'JRN_EVENT_ERROR' tn, ',ERROR_ID,EVENT_ID,' cn from dual
                    union all
                    select 4 l, 'JRN_NOTE_INFO' tn, ',NOTE_ID,EVENT_ID,' cn from dual
                    union all
                    select 3 l, 'JRN_OPER_INFO' tn, ',REQUEST_ID,' cn from dual
                    union all
                    select 1 l, 'DISPUTED_REQUEST' tn, ',ID,INCIDENT_ID,' cn from dual
                    union all
                    select 1 l, 'INC_DOC_PRINT' tn, ',ID,INCIDENT_ID,' cn from dual
                    union all
                    select 1 l, 'INC_OPER_INFO' tn, ',INCIDENT_ID,INC_ID,' cn from dual
                    union all
                    select 2 l, 'INC_OPER_DTL_INFO' tn, ',DTL_INFO_ID,INCIDENT_ID,' cn from dual
                    union all
                    select 3 l, 'INC_NOTE_INFO' tn, ',NOTE_ID,DTL_INFO_ID,' cn from dual
                    union all
                    select 3 l, 'INC_OPER_CLOB' tn, ',DTL_INFO_ID,' cn from dual
                    union all
                    select 3 l, 'INC_PAYMENT' tn, ',ID,INCIDENT_ID,RBC_ID,SP_ID,DEBATE_ID,' cn from dual
                    union all
                    select 3 l, 'INC_SYS_STATUS' tn, ',DTL_INFO_ID,' cn from dual
                    union all
                    select 2 l, 'INC_SYSTEM_STATUS' tn, ',SYS_STATUS_ID,INCIDENT_ID,' cn from dual
                    union all
                    select 0 l, 'INCIDENT' tn, ',INCIDENT_ID,ACTIVE_OPER_INFO_ID,' cn from dual)) s
        on (s.orig_tbl_name = t.orig_tbl_name)
when not matched
then
   insert     (orig_tbl_name
              ,tmp_tbl_name
              ,cn
              ,constr_order)
       values (s.orig_tbl_name
              ,s.tmp_tbl_name
              ,s.cn
              ,s.l);

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

create or replace package it$$utl
as
   exn#resource_busy   number := -54;
   exc#resource_busy   exception;
   pragma exception_init (exc#resource_busy, -54);
   exn#no_table        number := -942;
   exc#no_table        exception;
   pragma exception_init (exc#no_table, -942);

   procedure pl (s in varchar2);

   procedure wl (p_step         it$$step.step_no%type
                ,p_bank_code    it$$enlarge_log.bank_code%type
                ,p_tbl          it$$enlarge_log.orig_tbl_name%type
                ,p_ddl          it$$enlarge_log.ddl_dml%type
                ,p_err          it$$enlarge_log.error_message%type);

   procedure trn (p_step it$$step.step_no%type);

   procedure checkoff_cons (p_step it$$step.step_no%type);

   procedure merge_part (p_step it$$step.step_no%type);

   procedure sync;

   procedure exch_part (p_bank_code it$$bank_code.bank_code%type);

   function get_max
      return number;

   procedure rebuild_indexes;
end;
/

create or replace package body it$$utl
as
   procedure pl (s in varchar2)
   as
   begin
      DBMS_OUTPUT.put_line (s);
   end;

   procedure wl (p_step         it$$step.step_no%type
                ,p_bank_code    it$$enlarge_log.bank_code%type
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
           values (p_step
                  ,p_bank_code
                  ,p_tbl
                  ,p_ddl
                  ,p_err);

      commit;
   end;

   procedure trn (p_step it$$step.step_no%type)
   as
      c_err   varchar2 (2000 char);
   begin
      DBMS_OUTPUT.enable (10000000);
      DBMS_OUTPUT.put_line ('Создание $-таблиц для сгенерированных искусственных данных');

      for t in (select tt.orig_tbl_name, tt.tmp_tbl_name, 'TRUNCATE TABLE ' || tt.tmp_tbl_name || ' REUSE STORAGE' trunc_ddl
                      ,'CREATE TABLE ' || tt.tmp_tbl_name || ' TABLESPACE &&TS. AS SELECT * FROM ' || tt.orig_tbl_name || ' WHERE 1=0' create_ddl
                  from it$$dup_tables tt)
      loop
         begin
            execute immediate t.trunc_ddl;

            wl (p_step
               ,null
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

               wl (p_step
                  ,null
                  ,t.orig_tbl_name
                  ,t.trunc_ddl
                  ,c_err);
            when exc#no_table
            then
               execute immediate t.create_ddl;

               wl (p_step
                  ,null
                  ,t.orig_tbl_name
                  ,t.create_ddl
                  ,null);
         end;
      end loop;
   end;

   procedure checkoff_cons (p_step it$$step.step_no%type)
   as
      should_repeat    boolean := false;
      try_count        number := 0;
      exn#dep_exists   number := -2297;
      exc#dep_exists   exception;
      pragma exception_init (exc#dep_exists, -2297);
   begin
      DBMS_OUTPUT.enable (10000000);
      DBMS_OUTPUT.put_line ('Отключение ограничений целостности на таблицах');

      while (should_repeat = true or try_count < 10)
      loop
         should_repeat := false;

         for c in (select 'ALTER TABLE ' || c.table_name || ' MODIFY CONSTRAINT ' || c.constraint_name || ' DISABLE' alter_ddl, t.orig_tbl_name
                     from user_constraints c, it$$dup_tables t
                    where status = 'ENABLED' and (c.constraint_type in ('P', 'R') or (c.constraint_type = 'C' and c.generated = 'USER NAME')) and c.table_name = t.orig_tbl_name)
         loop
            begin
               ddl_pkg.alter_table (c.alter_ddl);
               wl (p_step
                  ,null
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



   procedure merge_part (p_step it$$step.step_no%type)
   as
   begin
      DBMS_OUTPUT.enable (10000000);
      DBMS_OUTPUT.put_line (
         'Осуществляем слияние всех подсекций по секциям всех тербанков в одну секцию по умолчанию.');

      for b in (select *
                  from it$$bank_code
                 where is_process = 1)
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

            wl (p_step
               ,b.bank_code
               ,s.orig_tbl_name
               ,s.csql
               ,null);
         end loop;
      end loop;
   end;

   function is_job_exists
      return boolean
   as
      i   number;
   begin
      select count (*)
        into i
        from dual
       where exists
                (select j.job_name, j.state
                   from user_scheduler_jobs j
                  where j.job_name like 'IT$$%' and j.state = 'RUNNING');

      return i > 0;
   end;

   procedure sync                                                                                --(p_step it$$enlarge_log.step_no%type, p_bank_code it$$enlarge_log.bank_code%type)
   as
   begin
      -- Подождем, пока стартанут джобы
      DBMS_LOCK.sleep (120);                                                                                                                                       -- спим 60 секунд

      while (is_job_exists)
      loop
         DBMS_LOCK.sleep (60);                                                                                                                                     -- спим 60 секунд
      end loop;
   end sync;

   procedure exch_part (p_bank_code it$$bank_code.bank_code%type)
   as
      exn#check_failed   number := -14281;
      exc#check_failed   exception;
      pragma exception_init (exc#check_failed, -14281);
   begin
      for r in (  select t.orig_tbl_name, tp.partition_name, tsp.subpartition_name, tsp.subpartition_position
                        ,'ALTER TABLE ' || t.orig_tbl_name || ' EXCHANGE SUBPARTITION ' || tsp.subpartition_name || ' WITH TABLE ' || t.tmp_tbl_name csql
                    from it$$dup_tables t
                         inner join user_tab_partitions tp on tp.table_name = t.orig_tbl_name
                         inner join user_tab_subpartitions tsp on tsp.partition_name = tp.partition_name and tsp.table_name = tp.table_name
                   where tp.partition_name = 'TB' || to_char (p_bank_code)                                                                  --and t.orig_tbl_name != 'INC_OPER_CLOB'
                order by orig_tbl_name)
      loop
         DBMS_OUTPUT.put_line (r.csql || ';');

         begin
            execute immediate r.csql;
         exception
            when exc#check_failed
            then
               null;
         end;
      end loop;
   end;

   function get_max
      return number
   as
      sel         varchar2 (1000);
      csql        varchar2 (1000);
      fr          varchar2 (1000);
      imax        number;
      prev_imax   number := 0;
   begin
      for t in (select *
                  from it$$dup_tables j)
      loop
         sel := 'select max(';
         fr := ' from ' || t.orig_tbl_name;

         for c
            in (  select tc.table_name, tc.column_id rn, row_number () over (partition by tc.table_name order by tc.column_id desc) drn, tc.column_name, cc.constraint_name
                        ,cc.position
                    from user_tab_columns tc
                         left join (user_cons_columns cc inner join user_constraints c on cc.constraint_name = c.constraint_name and c.constraint_type = 'P')
                            on cc.table_name = tc.table_name and cc.column_name = tc.column_name
                   where tc.table_name = t.orig_tbl_name
                order by tc.table_name, tc.column_id)
         loop
            if c.position is not null
            then
               sel := sel || c.column_name || ')';
               exit;
            end if;
         end loop;

         csql := sel || fr;

         execute immediate csql into imax;

         pl (csql || ';---->' || to_char (imax));

         if imax > prev_imax
         then
            prev_imax := imax;
         end if;
      end loop;

      return prev_imax;
   end;

   procedure rebuild_indexes
   as
      call_old_version   boolean := false;
   begin
      --   DBMS_OUTPUT.enable (10000000);

      for t in (select * from it$$dup_tables)
      loop
         ddl_pkg.drop_table (t.tmp_tbl_name);
         pkg_manage_partitions.rebuild_indexes (p_table_name => t.orig_tbl_name, p_tabspace => null, call_old_version => call_old_version);
      end loop;
   end;

   procedure enlarge (p_step number)
   as
      prev_imax       number := 0;

      pk              varchar2 (4000);
      tot_pk          varchar2 (4000);
      col_list        varchar2 (4000);
      val_list        varchar2 (4000);
      cdml            varchar2 (4000);
      src_bank_code   varchar2 (2 char);
   begin
      DBMS_OUTPUT.enable (10000000);

      begin
         select to_char (bank_code)
           into src_bank_code
           from it$$bank_code bc
          where bc.is_process = 1 and bc.last_ok_step < p_step and is_source = 1;
      exception
         when others
         then
            raise_application_error (-20000, 'Невозможно определить код банка источника!');
      end;

      for b in (select *
                  from it$$bank_code bc
                 where bc.is_process = 1 and bc.last_ok_step < p_step and is_source is null)
      loop
         prev_imax := it$$utl.get_max;
         it$$utl.trn (p_step);
         prev_imax := trunc (prev_imax, -7) + power (10, 7);
         it$$utl.pl ('Самое максимальное значение ' || to_char (prev_imax));

         /* напилим заданий по копированию таблиц */
         for t in (select *
                     from it$$dup_tables j)
         loop
            cdml := 'INSERT /*+ APPEND */ INTO ' || t.tmp_tbl_name || ' (';
            col_list := '';
            val_list := '';

            for c in (  select tc.table_name, tc.column_id rn, row_number () over (partition by tc.table_name order by tc.column_id desc) drn, tc.column_name
                              ,case when t.cn like '%,' || tc.column_name || ',%' then '+' || to_char (prev_imax) else '' end diff
                          from user_tab_columns tc
                         where tc.table_name = t.orig_tbl_name
                      order by tc.table_name, tc.column_id)
            loop
               col_list := col_list || case when c.rn != 1 then ',' end || '"' || c.column_name || '"';
               val_list :=
                     val_list
                  || case when c.rn != 1 then ',' end
                  || case when c.column_name = 'BANK_CODE' then '''' || to_char (b.bank_code) || '''' else '"' || c.column_name || '"' || c.diff end;
            end loop;

            cdml := cdml || col_list || ') SELECT ' || val_list || ' FROM ' || t.orig_tbl_name || ' WHERE BANK_CODE=''' || src_bank_code || ''';' || 'COMMIT;';

            declare
               v#job_name                        varchar2 (65 char);
               exn#job_doesnot_exists   constant binary_integer := -27475;
               exc#job_doesnot_exists            exception;
               pragma exception_init (exc#job_doesnot_exists, -27475);
            begin
               v#job_name := 'IT$$' || t.orig_tbl_name;

               begin
                  DBMS_SCHEDULER.drop_job (job_name => v#job_name, force => true);
               exception
                  when exc#job_doesnot_exists
                  then
                     null;
               end;

               DBMS_SCHEDULER.create_job (
                  job_name     => v#job_name
                 ,start_date   => systimestamp
                 ,end_date     => trunc (systimestamp, 'DD') + interval '1' day + interval '7' hour
                 ,job_class    => 'DEFAULT_JOB_CLASS'
                 ,job_type     => 'PLSQL_BLOCK'
                 ,job_action   => cdml
                 ,comments     =>    'ЗАО Ай-Теко, 2015'
                                  || chr (10)
                                  || 'Самоудаляемое задание для наполнения промежуточных таблиц для задач нагрузочного тестирования.'
                 ,auto_drop    => true
                 ,enabled      => false);
               DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'RESTARTABLE', value => false);
               DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'JOB_PRIORITY', value => 3);
               DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'MAX_RUNS', value => 1);
               DBMS_SCHEDULER.enable (name => v#job_name);
            end;
         end loop;

         -- Точка синхронизации
         it$$utl.sync;
         it$$utl.exch_part (p_bank_code => b.bank_code);

         -- Дошли до сюда - профит! ТБ обработан!
         update it$$bank_code t
            set t.last_ok_step = p_step
          where is_process = 1 and (last_ok_step < p_step) and t.bank_code = b.bank_code;

         commit;
      end loop;
   end;
end;
/

--exec it$$utl.trn(&&script_step.);

exec it$$utl.checkoff_cons(&&script_step.);

exec it$$utl.merge_part(&&script_step.);

@@utl_foot.sql

exit