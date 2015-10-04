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
                ,p_ddl          varchar2
                ,p_err          varchar2);

   procedure wl (p_step         it$$step.step_no%type
                ,p_bank_code    it$$enlarge_log.bank_code%type
                ,p_tbl          it$$enlarge_log.orig_tbl_name%type
                ,p_ddl          clob
                ,p_err          clob);

   procedure trn (p_step it$$step.step_no%type, p_tablespace varchar2);

   procedure checkoff_cons (p_step it$$step.step_no%type);

   procedure merge_part (p_step it$$step.step_no%type);

   procedure sync;

   procedure exch_part (p_bank_code it$$bank_code.bank_code%type);

   function get_max
      return number;

   procedure enlarge (p_step number, p_tablespace varchar2);

   procedure enlarge_source (p_step number, p_ratio number, p_tablespace varchar2);

   procedure rebuild_indexes;
end;
/

create or replace package body it$$utl
as
   procedure wl (p_step         it$$step.step_no%type
                ,p_bank_code    it$$enlarge_log.bank_code%type
                ,p_tbl          it$$enlarge_log.orig_tbl_name%type
                ,p_ddl          clob
                ,p_err          clob)
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

   procedure wl (p_step         it$$step.step_no%type
                ,p_bank_code    it$$enlarge_log.bank_code%type
                ,p_tbl          it$$enlarge_log.orig_tbl_name%type
                ,p_ddl          varchar2
                ,p_err          varchar2)
   as
   begin
      wl (p_step
         ,p_bank_code
         ,p_tbl
         ,to_clob (p_ddl)
         ,to_clob (p_err));
   end;

   procedure pl (s in varchar2)
   as
   begin
      ddl_pkg.info (s);
   end;

   procedure trn (p_step it$$step.step_no%type, p_tablespace varchar2)
   as
      c_err   varchar2 (2000 char);
   begin
      DBMS_OUTPUT.enable (10000000);
      DBMS_OUTPUT.put_line ('Создание $-таблиц для сгенерированных искусственных данных');

      for t in (select tt.orig_tbl_name, tt.tmp_tbl_name, 'TRUNCATE TABLE ' || tt.tmp_tbl_name || ' REUSE STORAGE' trunc_ddl
                      ,'CREATE TABLE ' || tt.tmp_tbl_name || ' TABLESPACE ' || p_tablespace || ' AS SELECT * FROM ' || tt.orig_tbl_name || ' WHERE 1=0' create_ddl
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
                   where tp.partition_name = 'TB' || to_char (p_bank_code)
                order by orig_tbl_name)
      loop
         DBMS_OUTPUT.put_line (r.csql || ';');

         begin
            ddl_pkg.alter_table (r.csql);
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

   procedure gen_job (p_job_suffix varchar2, p_job_action varchar2)
   as
      v#job_name                        varchar2 (65 char);
      exn#job_doesnot_exists   constant binary_integer := -27475;
      exc#job_doesnot_exists            exception;
      pragma exception_init (exc#job_doesnot_exists, -27475);
   begin
      v#job_name := 'IT$$' || p_job_suffix;

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
        ,job_action   => p_job_action
        ,comments     =>    'ЗАО Ай-Теко, 2015'
                         || chr (10)
                         || 'Самоудаляемое задание для наполнения промежуточных таблиц для задач нагрузочного тестирования.'
        ,auto_drop    => true
        ,enabled      => false);
      DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'RESTARTABLE', value => false);
      DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'JOB_PRIORITY', value => 3);
      DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'MAX_RUNS', value => 1);
      DBMS_SCHEDULER.enable (name => v#job_name);
      ddl_pkg.info (
            'Самоудаляемое задание '
         || v#job_name
         || ' для наполнения промежуточных таблиц для задач нагрузочного тестирования.');
   end;

   procedure enlarge (p_step number, p_tablespace varchar2)
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
         it$$utl.trn (p_step, p_tablespace);
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
            gen_job (p_job_suffix => t.orig_tbl_name, p_job_action => cdml);
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

   /**
    p_step   - номер шага
    p_ratio  - коэффициент размножения в банке-источнике. От 2 до 10..20
   */
   procedure enlarge_source (p_step number, p_ratio number, p_tablespace varchar2)
   as
      prev_imax       number := 0;
      col_list        varchar2 (4000);
      val_list        varchar2 (4000);
      sel_list        varchar2 (4000);
      cdml            clob;
      into_table      varchar2 (100);
      src_bank_code   varchar2 (2 char);
      proc_name       varchar2 (61 char);
   begin
      DBMS_OUTPUT.enable (10000000);

      if p_ratio is null or p_ratio < 2
      then
         return;
      end if;

      begin
         select to_char (bank_code)
           into src_bank_code
           from it$$bank_code bc
          where bc.is_process = 1 and is_source = 1;
      exception
         when others
         then
            raise_application_error (-20000, 'Невозможно определить код банка источника!');
      end;

      prev_imax := it$$utl.get_max;
      it$$utl.trn (p_step, p_tablespace);
      prev_imax := trunc (prev_imax, -7) + power (10, 7);
      it$$utl.pl ('Самое максимальное значение ' || to_char (prev_imax));

      /* напилим заданий по копированию таблиц */
      for t in (select *
                  from it$$dup_tables j)
      loop
         declare
            i   number;
         begin
            execute immediate 'select count(*) from ' || t.orig_tbl_name || ' where bank_code=:a' into i using src_bank_code;

            update it$$dup_tables dt
               set ini_99_count = i
             where dt.orig_tbl_name = t.orig_tbl_name;
         end;

         DBMS_LOB.createtemporary (lob_loc => cdml, cache => true);

         into_table := ' INTO ' || t.tmp_tbl_name;
         col_list := '';
         val_list := '';
         sel_list := '';

         -- коэффициент
         for c in (  select tc.table_name, tc.column_id rn, row_number () over (partition by tc.table_name order by tc.column_id desc) drn, tc.column_name
                           ,case when t.cn like '%,' || tc.column_name || ',%' then '+<RATIO_STEP>*' || to_char (prev_imax) when tc.column_name = t.date_col then '+<RATIO_STEP>*to_dsinterval(''' || to_char (t.date_diff) || ''')' else '' end diff
                       from user_tab_columns tc
                      where tc.table_name = t.orig_tbl_name
                   order by tc.table_name, tc.column_id)
         loop
            col_list := col_list || case when c.rn != 1 then ',' end || '"' || c.column_name || '"';
            sel_list := sel_list || case when c.rn != 1 then ',' end || '"' || c.column_name || '"';
            val_list := val_list || case when c.rn != 1 then ',' end || '"' || c.column_name || '"' || c.diff;
         end loop;

         col_list := into_table || ' (' || col_list || ') values (';
         val_list := val_list || ') ';
         sel_list := ' SELECT ' || sel_list || ' FROM ' || t.orig_tbl_name || ' WHERE BANK_CODE=''' || src_bank_code || ''';' || 'COMMIT;';
         proc_name := sys_context ('userenv', 'current_schema') || '.itp$' || t.orig_tbl_name;

         DBMS_LOB.append (dest_lob => cdml, src_lob => to_clob ('create or replace procedure ' || proc_name || ' as begin insert all '));

         for i in 0 .. p_ratio
         loop
            DBMS_LOB.append (dest_lob => cdml, src_lob => to_clob (col_list || replace (val_list, '<RATIO_STEP>', to_char (i))));
         end loop;

         DBMS_LOB.append (dest_lob => cdml, src_lob => to_clob (sel_list || ' end;'));

         execute immediate cdml;

         ddl_pkg.info ('Создана процедура ' || proc_name);
         wl (p_step        => 1
            ,p_bank_code   => src_bank_code
            ,p_tbl         => t.orig_tbl_name
            ,p_ddl         => cdml
            ,p_err         => null);
         DBMS_LOB.freetemporary (lob_loc => cdml);
         gen_job (p_job_suffix => t.orig_tbl_name, p_job_action => 'begin ' || proc_name || '; execute immediate ''drop procedure ' || proc_name || '''; end;');
      end loop;

      -- Точка синхронизации
      it$$utl.sync;
      it$$utl.exch_part (p_bank_code => src_bank_code);
      commit;
   end;
end;
/