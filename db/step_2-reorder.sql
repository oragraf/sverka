define script_step='2'
define script_name='reorder'
define script_full_name='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
define script_desc='Скрипт выполняет распределение сгенерированных данных по дням в соответствии с заданным количеством'

@@utl_head.sql
@@it$$utl.sql
--------------
prompt Шагов стало больше на один - обновим

merge into it$$step t
     using (select 0 step_no, 'Подготовительный этап для всех ТБ. Выполняется один раз.' description, 'step_0-prepare' script_name
              from dual
            union all
            select 1 step_no, 'Этап размножения данных по одному ТБ.' description, 'step_1-enlarge' script_name from dual
            union all
            select 2 step_no, 'Распределяем данные' description, 'step_2-reorder' script_name from dual
            union all
            select 3 step_no, 'Завершительный этап для всех ТБ. Выполняется один раз.' description, 'step_3-final' script_name
              from dual) s
        on (s.step_no = t.step_no)
when matched
then
   update set t.description = s.description, t.script_name = s.script_name
when not matched
then
   insert     (step_no, description, script_name)
       values (s.step_no, s.description, s.script_name);

commit;
prompt Создадим таблицу с перечнем обрабатываемых партиций

begin
   DBMS_OUTPUT.enable (10000000);
   ddl_pkg.drop_table ('it$$partitions');

   ddl_pkg.create_table (p_table_name => 'it$$partitions', p_ddl => 'CREATE TABLE it$$partitions
(
   table_name      varchar2(30 char),
   partition_name  varchar2(30 char),
   date_col_name   varchar2(30 char),
   is_completed    number(1) default 0
)
TABLESPACE &&TS.');
   ddl_pkg.alter_table ('alter table it$$partitions add constraint it$$partitions_pk primary key (table_name,partition_name) using index tablespace &&ts.');
   ddl_pkg.create_table (p_table_name => 'it$$map', p_ddl => 'create table it$$map
(
  dt_start timestamp,
  dt_end   timestamp,
  np       number
)
TABLESPACE &&TS.');
end;
/

prompt Заполним справочник обрабатываемых партиций

insert into it$$partitions (table_name, partition_name, date_col_name)
   select t.orig_tbl_name, p.partition_name, t.date_col
     from it$$dup_tables t, user_tab_partitions p, it$$bank_code b
    where t.orig_tbl_name = p.table_name and 'TB' || b.bank_code = p.partition_name and b.is_process = 1;

commit;

prompt Разрешим переезд строк в другие подсекции

begin
   DBMS_OUTPUT.enable (10000000);

   for r in (  select 'alter table ' || t.orig_tbl_name || ' enable row movement' alter_ddl
                 from it$$dup_tables t
             order by 1)
   loop
      ddl_pkg.alter_table (r.alter_ddl);
   end loop;
end;
/

declare
   v_dt_start            timestamp;
   v_previous_date       timestamp;
   v_cntday              number := 0;
   p_max_count_rec_day   number := &&inc_subpart_row_count.;
   v_np                  number := 0;
begin
   DBMS_OUTPUT.enable (10000000);

   for rec in (  select trans_date
                   from incident
                  where bank_code = 99
               order by trans_date)
   loop                                                                       ---Открывается курсор  по таблице INCIDENT  отсортированной по TRANS_DATE, данные отбираются для 99 ТБ
      if v_dt_start is null
      then
         v_dt_start := rec.trans_date;                                                                                                            --Начало периода для первой записи

         v_previous_date := rec.trans_date;
      end if;

      v_cntday := v_cntday + 1;                                                                                                                             -- Счетчик записей в дне

      if v_cntday > p_max_count_rec_day and v_previous_date < rec.trans_date
      then                                                                                                   --По дата проверяем, чтобы с одинаковой датой  была одинаковая партиция
         insert into it$$map (dt_start, dt_end, np)
              values (v_dt_start, v_previous_date, v_np);                                                                                                        -- Фиксируем период

         v_dt_start := rec.trans_date;

         v_np := v_np + 1;

         v_cntday := 1;
      end if;

      v_previous_date := rec.trans_date;
   end loop;

   if v_cntday > 1
   then
      insert into it$$map (dt_start, dt_end, np)
           values (v_dt_start, v_previous_date, v_np);                                                                                                           -- Фиксируем период
   end if;

   commit;
end;
/

declare
   i      number;
   cdml   varchar2 (2000);
begin
   for r in (  select *
                 from it$$partitions
                where is_completed = 0
             order by table_name, partition_name)
   loop
      cdml :=
            'begin'
         || (' update ' || r.table_name || ' i set i.' || r.date_col_name || ' = ')
         || '(select to_timestamp (''&&base_day'', ''yyyy-mm-dd'') + numtodsinterval (np, ''day'') + (i.'
         || r.date_col_name
         || ' - trunc (i.'
         || r.date_col_name
         || ', ''DD'')) from it$$map where i.'
         || r.date_col_name
         || ' between dt_start and dt_end and rownum < 2)' -- rownum чтобы не возвращалоьс несколько строк
         || (' where ''TB''||i.bank_code = ''' || r.partition_name || ''';')
         || (' update it$$partitions p set p.is_completed = 1 where p.table_name = ''' || r.table_name || ''' and p.partition_name = ''' || r.partition_name || ''';')
         || ' commit;'
         || 'end;';
      it$$utl.wl (p_step        => &&script_step.
                 ,p_bank_code   => substr (r.partition_name, 3, 2)
                 ,p_tbl         => r.table_name
                 ,p_ddl         => cdml
                 ,p_err         => null);
      -- сгенерируем и запустим джобы
      it$$utl.gen_job (p_job_suffix => substr (r.table_name, 1, 22) || r.partition_name, p_job_action => cdml);
   end loop;

   -- Точка синхронизации
   it$$utl.sync;
end;
/

--------------

@@utl_foot.sql

exit