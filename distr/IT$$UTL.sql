create or replace package it$$utl2
is
   package_version                  constant varchar2 (100) := '0.1';
   package_name                     constant varchar2 (30 char) := 'IT$$UTL2';
   unusable_indexes_exists          constant varchar2 (100 char) := 'Есть инвалидные индексы';
   unusable_indexes_dont_exists     constant varchar2 (100 char) := 'Инвалидных индексов нет';
   process_type_archivate           constant char (1 char) := 'A';
   process_type_partitioning        constant char (1 char) := 'P';
   message_type_error               constant char (1 char) := 'E';
   message_type_info                constant char (1 char) := 'I';
   ddl_parallel_degree              constant number := 16;
   /*
    Исключения при добавлении/удалении/расщеплении/слиянии партиций
    */
   /* Попытка вставки данных в несуществующую партицию */
   exn#ins_into_non_existent_part   constant binary_integer := -14400;
   exc#ins_into_non_existent_part            exception;
   pragma exception_init (exc#ins_into_non_existent_part, -14400);
   /* Попытка добавления уже существующей партиции */
   exn#duplicate_partition_name     constant binary_integer := -14013;
   exc#duplicate_partition_name              exception;
   pragma exception_init (exc#duplicate_partition_name, -14013);
   /* Попытка добавления партиции с таким же ключом но другим именем */
   exn#part_key_already_exists      constant binary_integer := -14312;
   exc#part_key_already_exists               exception;
   pragma exception_init (exc#part_key_already_exists, -14312);
   /* Попытка добавления субпартиции с границей меньшей, чем у другой существующей */
   exn#subpart_key_already_exists   constant binary_integer := -14211;
   exc#subpart_key_already_exists            exception;
   pragma exception_init (exc#subpart_key_already_exists, -14211);
   /* Попытка добавления партиции при существующей DEFAULT */
   exn#part_default_exists          constant binary_integer := -14323;
   exc#part_default_exists                   exception;
   pragma exception_init (exc#part_default_exists, -14323);
   exn#invalid_value                constant binary_integer := -904;
   exc#invalid_value                         exception;
   pragma exception_init (exc#invalid_value, -904);
   exn#name_already_exists          constant binary_integer := -955;
   exc#name_already_exists                   exception;
   pragma exception_init (exc#name_already_exists, -955);
   exn#unique_key_exists            constant binary_integer := -2266;
   exc#unique_key_exists                     exception;
   pragma exception_init (exc#unique_key_exists, -2266);
   exn#enforcing_index              constant binary_integer := -2429;
   exc#enforcing_index                       exception;
   pragma exception_init (exc#enforcing_index, -2429);
   exn#job_doesnot_exists           constant binary_integer := -27475;
   exc#job_doesnot_exists                    exception;
   pragma exception_init (exc#job_doesnot_exists, -27475);
   exn#job_allready_exists          constant binary_integer := -27477;
   exc#job_allready_exists                   exception;
   pragma exception_init (exc#job_allready_exists, -27477);
   /* Часть имени субпартиции по умолчанию */
   default_subpart_name             constant varchar2 (8 char) := '99991231';

   procedure rebuild_indexes (p_table_name in varchar2, p_tabspace in varchar2);

   procedure rebuild_indexes (p_table_name in varchar2, p_tabspace in varchar2, call_old_version in out boolean);


   procedure add_modify_split_partition (p_table_name             in varchar2                                                              -- имя таблицы, в которой проводим работы
                                        ,p_bank_code              in varchar2                                                     -- Код тербанка. Будет включен в название партиции
                                        ,p_part_values_list       in varchar2                                                    --  Список кодов тербанков для добавляемой партиции
                                        ,p_subpart_values_range   in timestamp                                                                    -- Верхняя граница для субпартиции
                                        ,p_tabspace               in varchar2);
end it$$utl2;
/
create or replace package body it$$utl2
is
   /* Процедура генерации-отцепления партиций будет запускаться еженедельно.
      Исходя из этого, количество нарезаемых партиций будет */
   new_partitions_count      binary_integer;
   /* ПАРТИЦИИ С ДАННЫМИ СТАРШЕ ОТ ТЕКУЩЕЙ ДАТЫ БУДУТ УДАЛЕНЫ */
   keep_days_ago             binary_integer;
   keep_part_online          binary_integer;
   max_date_value   constant timestamp := timestamp '9999-12-31 23:59:59.999999';
   min_date_value   constant timestamp := timestamp '0000-01-01 00:00:00.000000';

   cursor get_unusable_indexes (p_table_name in varchar2, p_tabspace in varchar2)
   is
      select 'ALTER INDEX ' || index_name || ' REBUILD ' || case when subpartition_name is not null then 'SUBPARTITION ' || subpartition_name when partition_name is not null then 'PARTITION ' || partition_name end || case when subpartition_name is null and partition_name is null then ' PARALLEL ' || to_char (ddl_parallel_degree) else ' PARALLEL 1' end || nvl2 (p_tabspace, ' TABLESPACE ' || p_tabspace, ' ') rebuild_ddl
            ,case when subpartition_name is null and partition_name is null then 'ALTER INDEX ' || index_name || ' NOPARALLEL' end noparallel_ddl
        from (select ui.status ind_status, coalesce (uip.status, ui.status) part_status, coalesce (uis.status, uip.status, ui.status) subp_status, ui.index_name, ui.table_name
                    ,ui.index_type, uip.partition_name, uis.subpartition_name
                from user_indexes ui
                     left join user_ind_partitions uip on uip.index_name = ui.index_name
                     left join user_ind_subpartitions uis on uis.index_name = uip.index_name and uis.partition_name = uip.partition_name
               where                                                                                                                                         /*generated = 'N' AND*/
                     nvl (p_table_name, table_name) = table_name) q
       where (not (ind_status = 'VALID' and part_status = 'VALID' and subp_status = 'VALID')) and (not subp_status = 'USABLE');

   cursor get_arch_tables
   is
      select table_name, row_number () over (order by refresh_order desc nulls last) - 1 rn
        from (select tt.table_name, refresh_order
                from it#mig_tables tt
               where tt.create_partitions = 'Y'
              union all
              select 'CLAIM' table_name, null from dual
              union all
              select 'CLAIM_HISTORY' table_name, null from dual);

   cursor get_part_tables
   is
      select q.*, rownum - 1 rn
        from (  select *
                  from it#mig_tables tt
                 where tt.create_partitions = 'Y'
              order by refresh_order desc) q;

   g_x_tables                DBMS_UTILITY.name_array;

   type rec#subpart is record
   (
      table_name              varchar2 (65 char)                                                                                           -- имя таблицы, в которой проводим работы
     ,bank_code               varchar2 (2 char)                                                                                   -- Код тербанка. Будет включен в название партиции
     ,part_values_list        varchar2 (100 char)                                                                                --  Список кодов тербанков для добавляемой партиции
     ,subpart_values_range    timestamp                                                                                                           -- Верхняя граница для субпартиции
     ,tabspace                varchar2 (30 char)                                                                                           -- ТАБЛИЧНОЕ ПРОСТРАНСТВО ДЛЯ СУБПАРТИЦИЙ
     ,part_name$              varchar2 (30 char)
     ,subpart_name$           varchar2 (30 char)
     ,default_subpart_name$   varchar2 (30 char)
     ,part_values_list$       varchar2 (100 char)
     ,subpart_values_range$   varchar2 (100 char)
     ,add_part$               varchar2 (2000 char)
     ,add_subpart$            varchar2 (2000 char)
     ,split_subpart$          varchar2 (2000 char)
     ,tabspace_option$        varchar2 (100 char)
     ,part_name_to_split$     varchar2 (30 char)
   );

   cursor get_tables_bank_codes
   is
        select t.table_name, bc.bc
          from it#mig_tables t, it#bank_codes bc
         where t.create_partitions = 'Y'
      order by 1, 2;

   cursor get_segment_info (
      p_segment_name     in varchar2
     ,p_partition_name   in varchar2
     ,p_tabspace         in varchar2)
   is
      select 'ALTER TABLE ' || s.segment_name || ' MOVE ' || replace (s.segment_type, 'TABLE', '') || ' ' || s.partition_name || ' TABLESPACE ' || p_tabspace move_ddl
        from user_segments s
       where     segment_name = p_segment_name
             and coalesce (p_partition_name, partition_name, '~') = coalesce (partition_name, p_partition_name, '~')
             and tablespace_name != p_tabspace;

   type tab_user_segments is table of get_segment_info%rowtype;

   procedure start_log (p_log in out nocopy sys_manage_partition_log%rowtype)
   as
   begin
      p_log.thread_id := upper (p_log.thread_id);

      insert into sys_manage_partition_log (thread_id
                                           ,table_name
                                           ,process_type
                                           ,message_type
                                           ,ddl_error
                                           ,ddl_exp)
           values (p_log.thread_id
                  ,p_log.table_name
                  ,p_log.process_type
                  ,p_log.message_type
                  ,p_log.ddl_error
                  ,p_log.ddl_exp)
        returning thread_id, start_mark, end_mark, ddl_error, ddl_exp
             into p_log.thread_id, p_log.start_mark, p_log.end_mark, p_log.ddl_error, p_log.ddl_exp;
   end;

   function start_log (thread_id      in sys_manage_partition_log.thread_id%type
                      ,table_name     in sys_manage_partition_log.table_name%type
                      ,message_type   in sys_manage_partition_log.message_type%type default message_type_error
                      ,process_type   in sys_manage_partition_log.process_type%type
                      ,ddl_error      in sys_manage_partition_log.ddl_error%type default ''
                      ,ddl_exp        in sys_manage_partition_log.ddl_exp%type)
      return sys_manage_partition_log%rowtype
   as
      log   sys_manage_partition_log%rowtype;
   begin
      log.thread_id := upper (thread_id);
      log.table_name := table_name;
      log.message_type := upper (message_type);
      log.process_type := upper (process_type);
      log.ddl_error := ddl_error;
      log.ddl_exp := ddl_exp;
      start_log (log);
      return log;
   end;

   procedure stop_log_ok (p_log in out nocopy sys_manage_partition_log%rowtype)
   as
   begin
         update sys_manage_partition_log
            set end_mark = systimestamp
               ,ddl_error = p_log.ddl_error
               ,ddl_exp = p_log.ddl_exp
               ,message_type = message_type_info
          where thread_id = p_log.thread_id and start_mark = p_log.start_mark
      returning end_mark, message_type
           into p_log.end_mark, p_log.message_type;
   end;

   procedure stop_log_error (p_log in out nocopy sys_manage_partition_log%rowtype)
   as
   begin
         update sys_manage_partition_log
            set end_mark = systimestamp
               ,ddl_error = p_log.ddl_error
               ,ddl_exp = p_log.ddl_exp
               ,message_type = message_type_error
          where thread_id = p_log.thread_id and start_mark = p_log.start_mark
      returning end_mark, message_type
           into p_log.end_mark, p_log.message_type;
   end;

   function prepare_job_def (p_action in varchar2, p_prefix in varchar2)
      return job_definition
   as
      jd   job_definition;
   begin
      jd :=
         job_definition (job_name              => DBMS_SCHEDULER.generate_job_name (substr (trim (p_prefix), 1, 18))
                        ,job_class             => 'DEFAULT_JOB_CLASS'
                        ,job_type              => 'PLSQL_BLOCK'
                        ,job_style             => 'REGULAR'
                        ,job_action            => p_action
                        ,comments              => substr (p_action, 240)
                        ,auto_drop             => true
                        ,enabled               => false
                        ,job_priority          => 3
                        ,restartable           => false
                        ,max_runs              => 1
                        ,number_of_arguments   => 0);
      return jd;
   end;

   function get_subpart_name (p_bank_code in varchar2, p_subp_range_value in timestamp)
      return varchar2
   as
   begin
      return 'TB' || substr (nvl (p_bank_code, 'XX'), 1, 2) || '_' || to_char (nvl (p_subp_range_value, max_date_value), 'YYYYMMDD');
   end;

   procedure get_subpart_to_split (p in out nocopy rec#subpart)
   as
      v#high_val        timestamp;
      v#prev_high_val   timestamp := min_date_value;
      v#str             varchar2 (32767);
   begin
      for r in (  select tp.table_name, tp.partition_name, tp.partition_position, ts.subpartition_name, ts.subpartition_position, ts.high_value, ts.high_value_length
                    from user_tab_subpartitions ts inner join user_tab_partitions tp on tp.table_name = ts.table_name and tp.partition_name = ts.partition_name
                   where tp.table_name = p.table_name and tp.partition_name = p.part_name$
                order by tp.partition_position, ts.subpartition_position)
      loop
         v#str := substrc (r.high_value, 1, r.high_value_length);

         begin
            execute immediate 'SELECT ' || v#str || ' FROM DUAL' into v#high_val;
         exception
            when exc#invalid_value
            then
               v#high_val := max_date_value;
         end;

         if p.subpart_values_range > v#prev_high_val and p.subpart_values_range < v#high_val
         then
            p.part_name_to_split$ := r.subpartition_name;
            exit;
         else
            v#prev_high_val := v#high_val;
         end if;
      end loop;

      null;                  -- контрольная точка ----------------------------------------------------------------------------------------------------------------------------------
   end;

   /**
     Процедура добавления|модификации/сплита одной партиции с субпартицией
    */
   procedure add_modify_split_partition (p in out nocopy rec#subpart)
   as
      is_completed   boolean := false;
      log            sys_manage_partition_log%rowtype;
   begin
      p.part_name$ := 'TB' || nvl (substr (trim (p.bank_code), 1, 2), 'XX');
      p.part_values_list$ := case when p.part_values_list is null then 'DEFAULT' else '''' || p.part_values_list || '''' end;

      if p.subpart_values_range is null
      then
         p.subpart_name$ := default_subpart_name;
         p.subpart_values_range$ := 'MAXVALUE';
      else
         p.subpart_name$ := to_char (p.subpart_values_range, 'YYYYMMDD');
         p.subpart_values_range$ := 'TO_TIMESTAMP(''' || to_char (p.subpart_values_range, 'YYYYMMDD') || ''',''YYYYMMDD'')';
      end if;

      p.subpart_name$ := p.part_name$ || '_' || p.subpart_name$;
      p.tabspace_option$ := case when p.tabspace is not null then ' TABLESPACE ' || p.tabspace end;
      p.add_part$ :=
            ('ALTER TABLE ' || p.table_name || ' ADD PARTITION ' || p.part_name$)
         || (' VALUES(' || p.part_values_list$ || ')' || p.tabspace_option$ || ' NOLOGGING NOCOMPRESS ')
         || (' (SUBPARTITION ' || p.subpart_name$ || ' VALUES LESS THAN(' || p.subpart_values_range$ || ') ' || p.tabspace_option$ || ')');
      p.add_subpart$ :=
            ('ALTER TABLE ' || p.table_name || ' MODIFY PARTITION ' || p.part_name$)
         || (' ADD SUBPARTITION ' || p.subpart_name$ || ' VALUES LESS THAN(' || p.subpart_values_range$ || ') ' || p.tabspace_option$ || '');
      p.part_name_to_split$ := null;
      get_subpart_to_split (p => p);

      if p.part_name_to_split$ is not null                                              -- явный сплит субпартиции------------------------------------------------------------------
      then
         p.split_subpart$ :=
               ('ALTER TABLE ' || p.table_name)
            || (' SPLIT SUBPARTITION ' || p.part_name_to_split$ || ' AT (' || p.subpart_values_range$)
            || ') INTO ('
            || (' SUBPARTITION ' || p.subpart_name$)
            || (',SUBPARTITION ' || p.part_name_to_split$)
            || ') PARALLEL';

         begin
            /* расщепляем субпартицию и выходим, если все благополучно */
            log :=
               start_log (thread_id      => 'ADD_MODIFY_SPLIT_PARTITION'
                         ,table_name     => p.table_name
                         ,process_type   => process_type_partitioning
                         ,ddl_exp        => to_clob (p.split_subpart$));
            ddl_pkg.alter_table (p.split_subpart$);
            stop_log_ok (log);
            return;
         exception
            when others
            then
               log.ddl_error := sqlerrm;
               ddl_pkg.error (log.ddl_error);
               stop_log_error (log);
               raise;
         end;
      else
         /* пробуем добавлять партицию/субпартицию */
         begin
            /* добавляем партицию */
            log :=
               start_log (thread_id      => 'ADD_MODIFY_SPLIT_PARTITION'
                         ,table_name     => p.table_name
                         ,process_type   => process_type_partitioning
                         ,ddl_exp        => to_clob (p.add_part$));
            ddl_pkg.alter_table (p.add_part$);
            stop_log_ok (log);
            return;
         exception
            when exc#duplicate_partition_name or exc#part_key_already_exists
            then
               begin
                  log.ddl_error := sqlerrm;
                  stop_log_error (log);
                  ddl_pkg.error (log.ddl_error);
                  /* такая партиция уже есть, пробуем ее модифицировать, добавив субпартицию */
                  ddl_pkg.alter_table (p.add_subpart$);
                  return;
               exception
                  when exc#duplicate_partition_name or exc#subpart_key_already_exists
                  then
                     return;
                  when others
                  then
                     ddl_pkg.error (sqlerrm);
                     raise;
               end;
            when others
            then
               ddl_pkg.error (sqlerrm);
               raise;
         end;
      end if;
   end;

   procedure add_modify_split_partition (p_table_name             in varchar2                                                              -- имя таблицы, в которой проводим работы
                                        ,p_bank_code              in varchar2                                                     -- Код тербанка. Будет включен в название партиции
                                        ,p_part_values_list       in varchar2                                                    --  Список кодов тербанков для добавляемой партиции
                                        ,p_subpart_values_range   in timestamp                                                                    -- Верхняя граница для субпартиции
                                        ,p_tabspace               in varchar2)
   as
      p   rec#subpart;
   begin
      p.table_name := p_table_name;
      p.bank_code := p_bank_code;
      p.part_values_list := p_part_values_list;
      p.subpart_values_range := p_subpart_values_range;
      p.tabspace := p_tabspace;
      add_modify_split_partition (p => p);
   end;

   function create_table_like (p_table_name in varchar2, p_tabspace in varchar2)
      return varchar2
   as
      v#ddl               varchar2 (2000 char);
      v#new_table_name    varchar2 (65 char);
      v#old_table_name    varchar2 (65 char);
      v#tabspace_option   varchar2 (100 char);
   begin
      v#new_table_name := 'IT#' || to_char (systimestamp, 'YYYYMMDDHH24MISSFF3');
      v#old_table_name := p_table_name;
      v#tabspace_option := case when p_tabspace is not null then ' TABLESPACE ' || p_tabspace end;
      v#ddl := 'CREATE TABLE ' || v#new_table_name || v#tabspace_option || ' AS SELECT * FROM ' || v#old_table_name || ' WHERE 1=0';

      execute immediate v#ddl;

      return v#new_table_name;
   end;

   /* процедура обменивает партицию с пустой таблицей */
   procedure exchange_subpartition (p_table_name       in varchar2
                                   ,p_subpart_name     in varchar2
                                   ,p_tabspace         in varchar2
                                   ,p_keep_exchanged   in boolean := false)
   as
      v#ddl              varchar2 (2000 char);
      v#exchange_table   varchar2 (65 char);
      v#table_name       varchar2 (65 char);
      v#new_table_name   varchar2 (65 char);
   begin
      v#table_name := p_table_name;

      if p_keep_exchanged
      then
         v#exchange_table := create_table_like (p_table_name, p_tabspace);
         v#ddl := ('ALTER TABLE ' || v#table_name || ' EXCHANGE SUBPARTITION ' || p_subpart_name) || (' WITH TABLE ' || v#exchange_table || ' WITHOUT VALIDATION');
         ddl_pkg.alter_table (v#ddl);
         ddl_pkg.comment_tab (
            v#exchange_table
           , ('Отцепленная партиция ' || v#table_name || '.' || p_subpart_name) || ('. Отцеплено ' || to_char (sysdate, 'yyyy-mm-dd hh24:mi:ss')));

         begin
            v#new_table_name := substr (p_table_name, 1, 30 - length (p_subpart_name) - 1) || '#' || p_subpart_name;
            ddl_pkg.alter_table ('ALTER TABLE ' || v#exchange_table || ' RENAME TO ' || v#new_table_name);
         exception
            when exc#name_already_exists
            then
               ddl_pkg.drop_table (v#exchange_table);
         end;
      end if;

      v#ddl := 'ALTER TABLE ' || v#table_name || ' DROP SUBPARTITION ' || p_subpart_name                                                           /*|| ' UPDATE INDEXES PARALLEL'*/
                                                                                        ;
      ddl_pkg.alter_table (v#ddl);
   end;

   procedure merge_subpartitions (p_table_name in varchar2, p_subp in varchar2, p_next_subp in varchar2)
   as
   begin
      ddl_pkg.alter_table ('ALTER TABLE ' || p_table_name || ' MERGE SUBPARTITIONS ' || p_subp || ',' || p_next_subp || ' INTO SUBPARTITION ' || p_next_subp);
   end;

   procedure exchange_subpartitions (p_table_name       in varchar2
                                    ,p_tabspace         in varchar2
                                    ,p_drop_range       in timestamp := systimestamp - numtodsinterval (366 * 3, 'DAY')
                                    ,p_keep_exchanged   in boolean := false)
   as
      v#high_val             timestamp;
      v#drop_range           timestamp;
      v#ddl                  varchar2 (2000 char);
      v#exchange_table       varchar2 (65 char);
      v#table_name           varchar2 (65 char);
      v#str                  varchar2 (32767);
      is_unique_key_exists   boolean := false;
   begin
      v#drop_range := nvl (p_drop_range, systimestamp - numtodsinterval (keep_days_ago, 'DAY'));

      for r in (  select tp.table_name, tp.partition_name, tp.partition_position, ts.subpartition_name, ts.subpartition_position, ts.high_value, ts.high_value_length
                        ,lead (ts.subpartition_name, 1, null) over (partition by tp.partition_name order by ts.subpartition_position) next_subp
                    from user_tab_subpartitions ts inner join user_tab_partitions tp on tp.table_name = ts.table_name and tp.partition_name = ts.partition_name
                   where tp.table_name = p_table_name
                order by ts.subpartition_position)
      loop
         v#str := substrc (r.high_value, 1, r.high_value_length);

         begin
            execute immediate 'SELECT ' || v#str || ' FROM DUAL' into v#high_val;
         exception
            when exc#invalid_value
            then
               v#high_val := max_date_value;                                                                                                                             -- MAXVALUE
         end;

         if v#high_val < v#drop_range                          -- субпартиция на удаление и эксчендж -------------------------------------------------------------------------------
         then
            begin
               exchange_subpartition (p_table_name       => r.table_name
                                     ,p_subpart_name     => r.subpartition_name
                                     ,p_tabspace         => p_tabspace
                                     ,p_keep_exchanged   => p_keep_exchanged);
            exception
               when exc#unique_key_exists
               then
                  is_unique_key_exists := true;

                  if r.next_subp is not null
                  then
                     merge_subpartitions (p_table_name => r.table_name, p_subp => r.subpartition_name, p_next_subp => r.next_subp);
                  end if;
            end;
         end if;
      end loop;

      if is_unique_key_exists
      then
         raise exc#unique_key_exists;
      end if;
   end;

   function get_dependent_ddl (p_object_type in varchar2, p_object_name in varchar2)
      return clob
   is
      v#handle             number;
      v#transform_handle   number;
      v#ddl                clob;
   begin
      v#handle := DBMS_METADATA.open (p_object_type);
      DBMS_METADATA.set_filter (v#handle, 'NAME', p_object_name);
      v#transform_handle := DBMS_METADATA.add_transform (v#handle, 'DDL');
      DBMS_METADATA.set_transform_param (v#transform_handle, 'SEGMENT_ATTRIBUTES', true);
      v#ddl := DBMS_METADATA.fetch_clob (v#handle);
      DBMS_METADATA.close (v#handle);
      return v#ddl;
   end;

   function get_index_ddl (p_object_name in varchar2)
      return clob
   is
      ddl_h     number;                                                                                                                        -- handle returned by OPEN for tables
      t_h       number;                                                                                                               -- handle returned by ADD_TRANSFORM for tables
      idxddl    clob;                                                                                                                                  -- creation DDL for an object
      pi        sys.ku$_parsed_items;                                                                   -- parse items are returned in this object which is contained in sys.ku$_ddl
      idxddls   sys.ku$_ddls;                                                                         -- metadata is returned in sys.ku$_ddls, a nested table of sys.ku$_ddl objects
      idxname   varchar2 (30);                                                                                                                              -- the parsed index name
   begin
      ddl_h := DBMS_METADATA.open ('INDEX');
      DBMS_METADATA.set_filter (ddl_h, 'NAME', p_object_name);
      DBMS_METADATA.set_filter (ddl_h, 'SYSTEM_GENERATED', false);
      DBMS_METADATA.set_parse_item (ddl_h, 'NAME');
      t_h := DBMS_METADATA.add_transform (ddl_h, 'DDL');
      DBMS_METADATA.set_transform_param (t_h, 'SQLTERMINATOR', false);
      DBMS_METADATA.set_transform_param (t_h, 'SEGMENT_ATTRIBUTES', true);

      loop
         idxddls := DBMS_METADATA.fetch_ddl (ddl_h);
         exit when idxddls is null;

         for i in idxddls.first .. idxddls.last
         loop
            idxddl := idxddls (i).ddltext;
            pi := idxddls (i).parseditems;

            if pi is not null and pi.count > 0
            then
               for j in pi.first .. pi.last
               loop
                  if pi (j).item = 'NAME' and pi (j).value = p_object_name
                  then
                     DBMS_METADATA.close (ddl_h);
                     return idxddl;
                  end if;
               end loop;
            end if;
         end loop;                                                                                                                                                       -- for loop
      end loop;

      DBMS_METADATA.close (ddl_h);
      return empty_clob ();
   end;

   function get_constraint_ddl (p_object_name in varchar2)
      return clob
   is
      ddl_h     number;                                                                                                                        -- handle returned by OPEN for tables
      t_h       number;                                                                                                               -- handle returned by ADD_TRANSFORM for tables
      idxddl    clob;                                                                                                                                  -- creation DDL for an object
      pi        sys.ku$_parsed_items;                                                                   -- parse items are returned in this object which is contained in sys.ku$_ddl
      idxddls   sys.ku$_ddls;                                                                         -- metadata is returned in sys.ku$_ddls, a nested table of sys.ku$_ddl objects
      idxname   varchar2 (30);                                                                                                                              -- the parsed index name
   begin
      ddl_h := DBMS_METADATA.open ('CONSTRAINT');
      DBMS_METADATA.set_filter (ddl_h, 'NAME', p_object_name);
      DBMS_METADATA.set_parse_item (ddl_h, 'NAME');
      t_h := DBMS_METADATA.add_transform (ddl_h, 'DDL');
      DBMS_METADATA.set_transform_param (t_h, 'SQLTERMINATOR', false);
      DBMS_METADATA.set_transform_param (t_h, 'SEGMENT_ATTRIBUTES', true);

      loop
         idxddls := DBMS_METADATA.fetch_ddl (ddl_h);
         exit when idxddls is null;

         for i in idxddls.first .. idxddls.last
         loop
            idxddl := idxddls (i).ddltext;
            pi := idxddls (i).parseditems;

            if pi is not null and pi.count > 0
            then
               for j in pi.first .. pi.last
               loop
                  if pi (j).item = 'NAME' and pi (j).value = p_object_name
                  then
                     DBMS_METADATA.close (ddl_h);
                     return idxddl;
                  end if;
               end loop;
            end if;
         end loop;                                                                                                                                                       -- for loop
      end loop;

      DBMS_METADATA.close (ddl_h);
      return empty_clob ();
   end;

   procedure rebuild_indexes (p_table_name in varchar2, p_tabspace in varchar2)
   as
      jd           job_definition;
      jda          job_definition_array := job_definition_array ();
      job_action   varchar2 (32767);
      r            get_unusable_indexes%rowtype;
   begin
      open get_unusable_indexes (p_table_name => p_table_name, p_tabspace => p_tabspace);

      loop
         fetch get_unusable_indexes into r;

         exit when get_unusable_indexes%notfound;

         execute immediate r.rebuild_ddl;

         if r.noparallel_ddl is not null
         then
            execute immediate r.noparallel_ddl;
         end if;
      end loop;

      close get_unusable_indexes;
   end;

   procedure rebuild_indexes (p_table_name in varchar2, p_tabspace in varchar2, call_old_version in out boolean)
   as
      v#high_val                timestamp;
      v#ddl                     varchar2 (2000 char);
      v#exchange_table          varchar2 (65 char);
      should_rebuild            boolean;
      v#create_index_ddl        clob;
      v#create_constraint_ddl   clob;
   begin
      if call_old_version
      then
         rebuild_indexes (p_table_name => p_table_name, p_tabspace => p_tabspace);
         return;
      end if;

      for i in (select ui.index_name, ui.status, uc.constraint_name, uc.constraint_type
                      ,'ALTER INDEX ' || ui.index_name || ' PARALLEL (DEGREE ' || to_char (ddl_parallel_degree) || ')' parallel_ddl
                      ,'ALTER INDEX ' || ui.index_name || ' NOPARALLEL' noparallel_ddl
                  from user_indexes ui left join user_constraints uc on ui.index_name = uc.index_name
                 where ui.generated = 'N' and ui.table_name = p_table_name)
      loop
         should_rebuild := false;

         for ip
            in (select uip.partition_name, uis.subpartition_name
                  from user_ind_partitions uip left join user_ind_subpartitions uis on uis.index_name = uip.index_name and uis.partition_name = uip.partition_name
                 where     uip.index_name = i.index_name
                       and (not (coalesce (uip.status, i.status) = 'VALID' and coalesce (uis.status, uip.status, i.status) = 'VALID'))
                       and (not coalesce (uis.status, uip.status, i.status) = 'USABLE'))
         loop
            should_rebuild := true;
            exit;
         end loop;

         if i.status = 'UNUSABLE' or should_rebuild
         then
            should_rebuild := true;

            execute immediate i.parallel_ddl; -- set Parallel option. when parsed DDL - it will be there.

            v#create_index_ddl := get_index_ddl (i.index_name);
         else
            v#create_index_ddl := empty_clob ();
         end if;

         if i.constraint_name is not null and should_rebuild
         then
            v#create_constraint_ddl := get_constraint_ddl (i.constraint_name);
         else
            v#create_constraint_ddl := empty_clob ();
         end if;

         declare
            ch   binary_integer;
            rh   binary_integer;
         begin
            ddl_pkg.drop_index (i.index_name);
            ch := DBMS_SQL.open_cursor;
            DBMS_SQL.parse (ch, v#create_index_ddl, DBMS_SQL.native);
            rh := DBMS_SQL.execute (ch);
            DBMS_SQL.close_cursor (ch);
         exception
            when exc#enforcing_index
            then
               call_old_version := true;                                -- Констрейнт мешает удалить/создать индекс. Поэтому вызовем старый метод для ребилда всех индексов/партиций
         end;

         if i.noparallel_ddl is not null and should_rebuild
         then
            execute immediate i.noparallel_ddl;
         end if;
      end loop;

      if call_old_version                                                                       -- если есть необходимость перестроить индексы по-старому, ребилдом, то сделаем это.
      then
         rebuild_indexes (p_table_name => p_table_name, p_tabspace => p_tabspace);
         return;
      end if;
   end;

end it$$utl2;
/
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

   procedure split_part (p_step it$$step.step_no%type, p_tablespace varchar2);
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
         it$$utl2.rebuild_indexes (p_table_name => t.orig_tbl_name, p_tabspace => null, call_old_version => call_old_version);
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

   procedure split_part (p_step it$$step.step_no%type, p_tablespace varchar2)
   as
   begin
      -- сплитим все банки, независимо от распучивания.
      for b in (select *
                  from it$$bank_code bc)
      loop
         for d in (with p
                        as (select cast (min (trans_date) as date) min_d, least (cast (max (trans_date) as date), sysdate) max_d
                              from incident odi
                             where odi.bank_code = b.bank_code)
                       ,p1
                        as (    select min_d + level - 1 d
                                  from p
                            connect by level <= max_d - min_d)
                     select d
                       from p1
                   order by d)
         loop
            for t in (select * from it$$dup_tables)
            loop
               begin
                  it$$utl2.add_modify_split_partition (p_table_name             => t.orig_tbl_name
                                                    ,p_bank_code              => b.bank_code
                                                    ,p_part_values_list       => b.bank_code
                                                    ,p_subpart_values_range   => d.d
                                                    ,p_tabspace               => p_tablespace);
                  wl (p_step
                     ,b.bank_code
                     ,t.orig_tbl_name
                     ,'split subpartition at ' || to_char (d.d, 'yyyy-mm-dd')
                     ,null);
               exception
                  when others
                  then
                     wl (p_step
                        ,b.bank_code
                        ,t.orig_tbl_name
                        ,'split subpartition at ' || to_char (d.d, 'yyyy-mm-dd')
                        ,sqlerrm);
               end;
            end loop;
         end loop;
      end loop;
   end;
end;
/