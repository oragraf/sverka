define script_step='1'
define script_name='enlarge'
define script_full_name='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
define script_desc='Скрипт выполняет основную работу по увеличению объема партиций.'

@@utl_head.sql
--------------


create or replace procedure andrg
as
   --declare
   prev_imax       number := 0;

   pk              varchar2 (4000);
   tot_pk          varchar2 (4000);
   col_list        varchar2 (4000);
   val_list        varchar2 (4000);
   cdml            varchar2 (4000);
   src_bank_code   varchar2 (2 char);
begin
   DBMS_OUTPUT.enable (1000000);

   begin
      select to_char (bank_code)
        into src_bank_code
        from it$$bank_code bc
       where bc.is_process = 1 and bc.last_ok_step < 1                                                                                                            /*&&script_step.*/
                                                      and is_source = 1;
   exception
      when others
      then
         raise_application_error (-20000, 'Невозможно определить код банка источника!');
   end;

   for b in (select *
               from it$$bank_code bc
              where bc.is_process = 1 and bc.last_ok_step < 1                                                                                                     /*&&script_step.*/
                                                             and is_source is null)
   loop
      prev_imax := it$$utl.get_max;
      it$$utl.trn (1                                                                                                                                              /*&&script_step.*/
                    );
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
              ,start_date   => systimestamp + interval '3' minute
              ,end_date     => trunc (systimestamp, 'DD') + interval '1' day + interval '7' hour
              ,job_class    => 'DEFAULT_JOB_CLASS'
              ,job_type     => 'PLSQL_BLOCK'
              ,job_action   => cdml
              ,comments     =>    'ЗАО Ай-Теко, 2015'
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

      -- Точка синхронизации
      it$$utl.sync;
      it$$utl.exch_part (p_bank_code => b.bank_code);

      -- Дошли до сюда - профит! ТБ обработан!
      update it$$bank_code t
         set t.last_ok_step = 1                                                                                                                                   /*&&script_step.*/
       where is_process = 1 and (last_ok_step < 1                                                                                                                 /*&&script_step.*/
                                                 ) and t.bank_code = b.bank_code;

      commit;
   end loop;
end;
/


--------------
@@utl_foot.sql

exit