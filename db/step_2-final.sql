prompt Перекидывание счетчиков в актуальные значения, соответствующие загруженным данным
----------------------------------------------
define script_step='2'
define script_name='final'
define script_full_name='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
define script_desc='Скрипт перекидывает сиквенсы в актуальные значения. Запускается один раз на все тербанки.'

@@utl_head.sql

declare
   cursor it$$dup_tables_c
   is
      select orig_tbl_name, sequence_name, pk_col, row_number () over (partition by sequence_name order by 1) rn
        from (select it$$dup_tables.orig_tbl_name, case when it$$dup_tables.orig_tbl_name = 'INC_OPER_INFO' then 'SEQ_INCIDENT' else s.sequence_name end sequence_name
                    ,cc.column_name as pk_col
                from it$$dup_tables
                     inner join user_constraints c on (it$$dup_tables.orig_tbl_name = c.table_name and c.constraint_type = 'P')
                     inner join user_cons_columns cc on (c.table_name = cc.table_name and c.constraint_name = cc.constraint_name)
                     inner join user_sequences s on (s.sequence_name like '%' || cc.table_name || '%'));

   type seq_need_val_tab_type is table of number
      index by varchar2 (30 char);

   seq_need_val_tab   seq_need_val_tab_type;
   v_max_id           number := 0;
   v_need_val         number := 0;
   v_owner            varchar2 (30 char) := sys_context ('USERENV', 'CURRENT_USER');

   procedure alter_seq (owner in varchar2, seq_name in varchar2, need_val in number)
   is
      p_owner       varchar2 (30) := upper (owner);
      p_name        varchar2 (30) := upper (seq_name);
      p_need_val    number := need_val;
      -- процедура изменяет последовательность таким образом, чтобы при следующем вызове
      -- последовательность возвратит p_need_val
      -- процедура применима только к возрастающим последовательностям (у которых задано MINVALUE)
      -- ВНИМАНИЕ: если p_need_val меньше текущего значения, то MINVALUE изменится
      sql_str       varchar2 (500);
      l_cache       varchar2 (30);
      l_increment   number;
      l_last_num    number;
      l_curr_val    number;
   begin
      -- получить значения cache_size, increment_by, last_number
      select case cache_size when 0 then ' nocache ' else ' cache ' || to_char (cache_size) end, increment_by, last_number
        into l_cache, l_increment, l_last_num
        from user_sequences
       where sequence_name = p_name;

      if p_need_val = l_last_num
      then
         -- последовательность или ни разу не вызывалась,
         -- или смысла что-то трогать нет
         null;
      else
         if p_need_val > l_last_num
         then
            -- отключить кеширование, minvalue не трогать
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' nocache';
         else
            -- отключить кеширование, сдвинуть minvalue
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' nocache' || ' minvalue ' || to_char (p_need_val - l_increment);
         end if;

         execute immediate sql_str;

         -- получить текущее значение последовательности
         sql_str := 'select ' || p_owner || '.' || p_name || '.nextval from dual';

         execute immediate sql_str into l_curr_val;

         -- если после этого следующий вызов последовательности выдаст нужный результат,
         -- то все, иначе
         if (p_need_val - l_increment - l_curr_val) <> 0
         then
            -- increment_by на нужное значение
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' increment by ' || to_char (p_need_val - l_increment - l_curr_val);

            execute immediate sql_str;

            -- установить последовательность в нужное значение
            sql_str := 'select ' || p_owner || '.' || p_name || '.nextval from dual';

            -- после чего l_curr_val будет равен p_need_val - l_increment, т.е. MINVALUE
            execute immediate sql_str into l_curr_val;
         end if;

         -- возвращаем cache_size и increment_by
         sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' increment by ' || to_char (l_increment)                              --|| ' minvalue ' || to_char(l_curr_val)
                                                                                                              || l_cache;

         execute immediate sql_str;
      end if;
   -- следующий вызов последовательности даст p_need_val

   end alter_seq;
begin
   DBMS_OUTPUT.enable (10000000);

   for r in it$$dup_tables_c
   loop
      declare
         csql   varchar2 (1000);
      begin
         csql := 'select MAX(' || r.pk_col || ') from ' || r.orig_tbl_name;

         execute immediate csql into v_max_id;

         ddl_pkg.info (csql || ';');
         v_need_val := v_max_id + 1;

         seq_need_val_tab (r.sequence_name) := v_need_val;

         if r.rn = 1
         then
            seq_need_val_tab (r.sequence_name) := v_need_val;
            alter_seq (owner => v_owner, seq_name => r.sequence_name, need_val => seq_need_val_tab (r.sequence_name));
         elsif r.rn > 1 and seq_need_val_tab (r.sequence_name) < v_max_id
         then
            seq_need_val_tab (r.sequence_name) := v_need_val;
            alter_seq (owner => v_owner, seq_name => r.sequence_name, need_val => seq_need_val_tab (r.sequence_name));
         elsif r.rn > 1 and seq_need_val_tab (r.sequence_name) > v_max_id
         then
            null;                                                                                                                            -- ничего не делать, все уже перекинуто
         end if;
      exception
         when others
         then
            ddl_pkg.error ('--' || csql || ';');
      end;
   end loop;
end change_seq_val;
/

prompt Удаление $-таблиц для сгенерированных искусственных данных и перестройка индексов
exec it$$utl.rebuild_indexes;

prompt Этап 1. Все констрайнты переводим в enabled

declare
   should_repeat             boolean := false;
   try_count                 number := 0;
   exn#dep_exists            number := -2297;
   exc#dep_exists            exception;
   pragma exception_init (exc#dep_exists, -2297);

   exn#no_key_exists         number := -2270;
   exc#no_key_exists         exception;
   pragma exception_init (exc#no_key_exists, -2270);

   exn#pkey_doesnot_exists   number := -2298;
   exc#pkey_doesnot_exists   exception;
   pragma exception_init (exc#pkey_doesnot_exists, -2298);
begin
   DBMS_OUTPUT.enable (10000000);

   while (try_count < 3)
   loop
      should_repeat := false;

      for c in (  select 'ALTER TABLE ' || tn || listagg (enable_ddl, ' ') within group (order by tn, ord) alter_ddl
                    from (select q.*, row_number () over (partition by tn order by ord) rn
                            from (select c.table_name tn, c.constraint_name, c.status, c.constraint_type, generated, ' ENABLE NOVALIDATE CONSTRAINT ' || constraint_name enable_ddl
                                        ,case constraint_type when 'P' then 1 else 2 end ord, t.*
                                    from user_constraints c inner join it$$dup_tables t on c.table_name = t.orig_tbl_name
                                   where status = 'DISABLED') q)
                group by constr_order, tn)
      loop
         begin
            ddl_pkg.alter_table (c.alter_ddl);
         exception
            when exc#dep_exists or exc#no_key_exists or exc#pkey_doesnot_exists
            then
               should_repeat := true;
         end;
      end loop;

      try_count := try_count + case when should_repeat then 1 else 3 end;                                        -- Если были ошибки, пройдем еще раз. Иначе сбросим счетчик и выход
   end loop;
end;
/

prompt Шаг 2. Проводим валидацию ссылочных констрейнтов. С использованием джобов

declare
   sql_stmnt      varchar2 (4000);
   v_altr_stmnt   varchar2 (2000);
begin
   DBMS_OUTPUT.enable (10000000);

   for tt in (select *
                from it$$dup_tables t)
   loop
      for c in (  select q.table_name, status, consn, constraint_name, constraint_type
                        ,'ALTER TABLE ' || q.table_name || ' MODIFY CONSTRAINT ' || constraint_name || ' VALIDATE ' alter_ddl
                    from (    select c.table_name, lpad (' ', 8 * (level - 1)) || c.constraint_name consn, constraint_name, c.status, c.constraint_type, generated, level lev, validated
                                    ,rownum rn
                                from user_constraints c
                          start with c.r_constraint_name is null
                          connect by prior c.constraint_name = c.r_constraint_name) q
                   where status = 'ENABLED' and validated = 'NOT VALIDATED' and constraint_type != 'C' and q.table_name = tt.orig_tbl_name
                order by rn)
      loop
         v_altr_stmnt := v_altr_stmnt || 'DDL_PKG.ALTER_TABLE (''' || c.alter_ddl || ''');';
      end loop;

      sql_stmnt :=
            'DECLARE  '
         || '   SHOULD_REPEAT             BOOLEAN := FALSE; '
         || '   TRY_COUNT                 NUMBER := 0;      '
         || '   EXN#DEP_EXISTS            NUMBER := -2297;  '
         || '   EXC#DEP_EXISTS            EXCEPTION;        '
         || '   PRAGMA EXCEPTION_INIT (EXC#DEP_EXISTS, -2297);     '
         || '   EXN#NO_KEY_EXISTS         NUMBER := -2270;         '
         || '   EXC#NO_KEY_EXISTS         EXCEPTION;               '
         || '   PRAGMA EXCEPTION_INIT (EXC#NO_KEY_EXISTS, -2270);  '
         || '   EXN#PKEY_DOESNOT_EXISTS   NUMBER := -2298;         '
         || '   EXC#PKEY_DOESNOT_EXISTS   EXCEPTION;               '
         || '   PRAGMA EXCEPTION_INIT (EXC#PKEY_DOESNOT_EXISTS, -2298); '
         || 'BEGIN                                                '
         || '   WHILE (SHOULD_REPEAT = TRUE OR TRY_COUNT < 3)      '
         || '   LOOP                                               '
         || '       SHOULD_REPEAT := FALSE;                        '
         || '         BEGIN                                          '
         || v_altr_stmnt
         || '       EXCEPTION                                      '
         || '            WHEN EXC#DEP_EXISTS OR EXC#NO_KEY_EXISTS OR EXC#PKEY_DOESNOT_EXISTS '
         || '            THEN                                      '
         || '               SHOULD_REPEAT := TRUE;                 '
         || '       END;                                           '
         || '       TRY_COUNT := TRY_COUNT + 1;                    '
         || '   END LOOP;                                           '
         || 'END;                                                  ';

      declare
         v#job_name                        varchar2 (65 char);
         exn#job_doesnot_exists   constant binary_integer := -27475;
         exc#job_doesnot_exists            exception;
         pragma exception_init (exc#job_doesnot_exists, -27475);
      begin
         v#job_name := 'IT$$' || tt.orig_tbl_name;

         begin
            DBMS_SCHEDULER.drop_job (job_name => v#job_name, force => true);
         exception
            when exc#job_doesnot_exists
            then
               null;
         end;

         DBMS_SCHEDULER.create_job (
            job_name     => v#job_name
           ,start_date   => systimestamp + interval '1' minute
           ,end_date     => trunc (systimestamp, 'DD') + interval '1' day + interval '7' hour
           ,job_class    => 'DEFAULT_JOB_CLASS'
           ,job_type     => 'PLSQL_BLOCK'
           ,job_action   => sql_stmnt
           ,comments     =>    'ЗАО Ай-Теко, 2015'
                            || chr (10)
                            || 'Самоудаляемое задание для распараллеливания валидации ограничений целостности'
           ,auto_drop    => true
           ,enabled      => false);
         DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'RESTARTABLE', value => false);
         DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'JOB_PRIORITY', value => 3);
         DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'MAX_RUNS', value => 1);
         DBMS_SCHEDULER.enable (name => v#job_name);
      end;

      v_altr_stmnt := '';
   end loop;

   -- точка синхронизации. Нужно дождаться, пока джобы отработают
   it$$utl.sync;
end;
/

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
   update set final_size = s.bytes
           where t.is_process = 1 and t.last_ok_step < &&script_step.;

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
   update set final_size = s.bytes;

commit;

@@utl_foot.sql

exit