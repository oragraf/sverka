define part='&1'
----------------------------------------------
define script_step='1'
define script_name='enlarge'
define script_full_name='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
define script_desc='Скрипт выполняет основную работу по увеличению объема партиций.'

@@utl_head.sql
--------------

declare
   sel         varchar2 (1000);
   csql        varchar2 (1000);
   fr          varchar2 (1000);
   imax        number;
   prev_imax   number := 0;

   pk          varchar2 (4000);
   tot_pk      varchar2 (4000);
   col_list    varchar2 (4000);
   val_list    varchar2 (4000);
   cdml        varchar2 (4000);

   procedure pl (s in varchar2)
   as
   begin
      DBMS_OUTPUT.put_line (s);
   end;
begin
   DBMS_OUTPUT.enable (1000000);

   for t in (select *
               from it$$dup_tables j)
   loop
      sel := 'select max(';
      fr := ' from ' || t.orig_tbl_name;

      for c
         in (  select tc.table_name, tc.column_id rn, row_number () over (partition by tc.table_name order by tc.column_id desc) drn, tc.column_name
                     ,cc.constraint_name, cc.position
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

   prev_imax := trunc (prev_imax, -7) + power (10, 7);
   pl ('Самое максимальное значение ' || to_char (prev_imax));

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
            || case when c.column_name = 'BANK_CODE' then '''&&PART.''' else '"' || c.column_name || '"' || c.diff end;
      end loop;

      cdml := cdml || col_list || ') SELECT ' || val_list || ' FROM ' || t.orig_tbl_name || ' WHERE BANK_CODE=''99'';' || 'COMMIT;';

      declare
         v#job_name                        varchar2 (65 char);
         exn#job_doesnot_exists   constant binary_integer := -27475;
         exc#job_doesnot_exists            exception;
         pragma exception_init (exc#job_doesnot_exists, -27475);
      begin
         v#job_name := 'J$' || t.orig_tbl_name;

         begin
            DBMS_SCHEDULER.drop_job (job_name => v#job_name, force => true);
         exception
            when exc#job_doesnot_exists
            then
               null;
         end;

         DBMS_SCHEDULER.create_job (
            job_name     => v#job_name
           ,start_date   => systimestamp + interval '3' minute
           ,end_date     => trunc (systimestamp, 'DD') + interval '1' day + interval '7' hour
           ,job_class    => 'DEFAULT_JOB_CLASS'
           ,job_type     => 'PLSQL_BLOCK'
           ,job_action   => cdml
           ,comments     =>    'ЗАО Ай-Теко, 2013'
                            || chr (10)
                            || 'Самоудаляемое задание для наполнения промежуточных таблиц для задач нагрузочного тестирования.'
           ,auto_drop    => false
           ,enabled      => true);
         DBMS_SCHEDULER.disable (name => v#job_name);
         DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'RESTARTABLE', value => false);
         DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'JOB_PRIORITY', value => 3);
         DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'MAX_RUNS', value => 1);
      end;
   end loop;
end;
/

declare
   exn#check_failed   number := -14281;
   exc#check_failed   exception;
   pragma exception_init (exc#check_failed, -14281);
begin
   for r in (  select t.orig_tbl_name, tp.partition_name, tsp.subpartition_name, tsp.subpartition_position
                     ,'ALTER TABLE ' || t.orig_tbl_name || ' EXCHANGE SUBPARTITION ' || tsp.subpartition_name || ' WITH TABLE ' || t.tmp_tbl_name csql
                 from it$$dup_tables t
                      inner join user_tab_partitions tp on tp.table_name = t.orig_tbl_name
                      inner join user_tab_subpartitions tsp on tsp.partition_name = tp.partition_name and tsp.table_name = tp.table_name
                where tp.partition_name = 'TB&&part.' and t.orig_tbl_name != 'INC_OPER_CLOB'
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
/


--------------
@@utl_foot.sql

exit