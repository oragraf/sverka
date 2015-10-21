create or replace package body pkg_mp
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

            execute immediate i.parallel_ddl;                                                                            -- set Parallel option. when parsed DDL - it will be there.

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

   procedure rebuild_indexes
   as
   begin
      for t in (select t.table_name tn, coalesce (t.tablespace_name, pt.def_tablespace_name) ts
                  from user_tables t left join user_part_tables pt on pt.table_name = t.table_name)
      loop
         rebuild_indexes (p_table_name => t.tn, p_tabspace => t.ts);
      end loop;
   end;

   procedure gen_future_subpartition (p_table_name in varchar2, p_part_count in binary_integer := 3, p_tabspace in varchar2)
   as
      v#part_count      binary_integer := greatest (nvl (p_part_count, 1), 1);
      v#subpart_name    varchar2 (30 char);
      v#subpart_range   timestamp;
   begin
      for bc in (select * from it#bank_codes)
      loop
         for d in 1 .. v#part_count
         loop
            v#subpart_range := trunc (systimestamp, 'DD') + numtodsinterval (d, 'DAY');
            add_modify_split_partition (p_table_name             => p_table_name                                                           -- имя таблицы, в которой проводим работы
                                       ,p_bank_code              => bc.bc                                                         -- Код тербанка. Будет включен в название партиции
                                       ,p_part_values_list       => bc.bc                                                        --  Список кодов тербанков для добавляемой партиции
                                       ,p_subpart_values_range   => v#subpart_range                                                               -- Верхняя граница для субпартиции
                                       ,p_tabspace               => p_tabspace);
         end loop;
      end loop;
   end;

   procedure table_part_maintenance (p_table_name in varchar2, p_tabspace in varchar2)
   as
      is_unique_key_exists   boolean := false;
      call_old_version       boolean := true;
   begin
      --INIT_PARAMETERS;
      /* СОЗДАЕМ ПАРТИЦИИ НА БУДУЩЕЕ */
      gen_future_subpartition (p_table_name => p_table_name, p_part_count => new_partitions_count, p_tabspace => p_tabspace);

      begin
         exchange_subpartitions (p_table_name => p_table_name, p_tabspace => p_tabspace);
      exception
         when exc#unique_key_exists
         then
            is_unique_key_exists := true;
      end;

      /* ПЕРЕСТРАИВАЕМ ИНВАЛИДНЫЕ ИНДЕКСЫ */
      rebuild_indexes (p_table_name => p_table_name, p_tabspace => p_tabspace, call_old_version => call_old_version);

      if is_unique_key_exists
      then
         raise exc#unique_key_exists;
      end if;
   end table_part_maintenance;

   procedure table_arch_maintenance (p_table_name in varchar2)
   as
      is_unique_key_exists   boolean := false;
      v#def_tbs              varchar2 (30 char);
      v#high_val             timestamp;
      v#drop_range           timestamp;
      v#ddl                  varchar2 (2000 char);
      v#exchange_table       varchar2 (65 char);
      v#table_name           varchar2 (65 char);
      v#str                  varchar2 (32767);
      log                    sys_manage_partition_log%rowtype;
   begin
      select def_tablespace_name || '_ARCH'
        into v#def_tbs
        from user_part_tables t
       where t.table_name = p_table_name;

      if keep_part_online is null
      then
         --init_parameters;
         null;
      end if;

      v#drop_range := systimestamp - numtodsinterval (keep_part_online, 'DAY');

      for r in (  select tp.table_name, tp.partition_name, tp.partition_position, ts.subpartition_name, ts.subpartition_position, ts.high_value ts_high_value
                        ,ts.high_value_length ts_high_value_length, tp.high_value tp_high_value, tp.high_value_length tp_high_value_length
                        , ('ALTER TABLE ' || tp.table_name || ' MOVE ') || nvl2 (ts.subpartition_name, 'SUBPARTITION ' || ts.subpartition_name, 'PARTITION ' || tp.partition_name) || (' TABLESPACE ' || v#def_tbs || ' COMPRESS UPDATE INDEXES PARALLEL ' || to_char (ddl_parallel_degree)) move_ddl
                        ,nvl2 (ts.subpartition_name, 'S', 'P') section_type
                    from user_tab_subpartitions ts right join user_tab_partitions tp on tp.table_name = ts.table_name and tp.partition_name = ts.partition_name
                   where tp.table_name = p_table_name and nvl (ts.tablespace_name, tp.tablespace_name) != v#def_tbs
                order by ts.subpartition_position)
      loop
         if r.section_type = 'S'
         then
            v#str := substrc (r.ts_high_value, 1, r.ts_high_value_length);
         elsif r.section_type = 'P'
         then
            v#str := substrc (r.tp_high_value, 1, r.tp_high_value_length);
         else
            return;
         end if;

         begin
            execute immediate 'SELECT ' || v#str || ' FROM DUAL' into v#high_val;
         exception
            when exc#invalid_value
            then
               v#high_val := max_date_value;                                                                                                                             -- MAXVALUE
         end;

         if v#high_val < v#drop_range                                    -- субпартиция на АРХИВАЦИЮ -------------------------------------------------------------------------------
         then
            log :=
               start_log (thread_id      => r.table_name || '.' || r.partition_name || '.' || r.subpartition_name
                         ,table_name     => r.table_name
                         ,process_type   => process_type_archivate
                         ,ddl_exp        => to_clob (r.move_ddl));

            begin
               ddl_pkg.alter_table (r.move_ddl);
               stop_log_ok (log);
               commit;
            exception
               when others
               then
                  log.ddl_error := sqlerrm;
                  stop_log_ok (log);
                  commit;
            end;
         end if;
      end loop;
   end table_arch_maintenance;

   procedure tables_part_maintenance (p_tabspace in varchar2, p_keep_exchanged in boolean := false)
   as
      v#owner   constant varchar2 (30 char) := sys_context ('USERENV', 'CURRENT_SCHEMA');
      v#block            varchar2 (2000 char);
   begin
      --init_parameters;

      for t in get_part_tables
      loop
         begin
            v#block :=
                  'BEGIN '
               || (v#owner || '.' || package_name || '.TABLE_PART_MAINTENANCE')
               || '('
               || (' P_TABLE_NAME=>''' || t.table_name || '''')
               || (',P_TABSPACE=>''' || p_tabspace || '''')
               || ');'
               || 'END;';

            execute immediate v#block;
         end;
      end loop moving;
   end;

   procedure tables_arch_maintenance
   as
      v#owner   constant varchar2 (30 char) := sys_context ('USERENV', 'CURRENT_SCHEMA');
      v#block            varchar2 (2000 char);
   begin
      --init_parameters;

      for t in get_arch_tables
      loop
         begin
            v#block := 'BEGIN ' || (v#owner || '.' || package_name || '.TABLE_ARCH_MAINTENANCE') || '(' || (' P_TABLE_NAME=>''' || t.table_name || '''') || ');' || 'END;';

            execute immediate v#block;
         end;
      end loop moving;
   end;

   procedure gen_part_jobs (p_tabspace in varchar2 default '')
   as
      v#owner   constant varchar2 (30 char) := sys_context ('USERENV', 'CURRENT_SCHEMA');
      v#job_name         varchar2 (65 char);
      v#job_prefix       varchar2 (30 char);
      v#start_date       timestamp := systimestamp + interval '3' minute;
      v#end_date         timestamp := trunc (systimestamp, 'DD') + interval '1' day + interval '7' hour;
      v#block_start      varchar2 (2000 char);
      v#block_end        varchar2 (2000 char);
      v#job_priority     binary_integer;
      v#job_comment      varchar2 (200 char);
   begin
      v#block_start := 'BEGIN ';
      v#block_end := ' END;';
      v#job_priority := 3;
      v#job_comment := 'Система автоматизированного секционирования ';
      --init_parameters;

      for t in get_part_tables
      loop
         v#job_prefix := 'MPJ' || to_char (t.rn, 'FM09') || '$';
         v#job_name := v#owner || '.' || v#job_prefix || substr (t.table_name, 1, 30 - length (v#job_prefix));

         begin
            DBMS_SCHEDULER.drop_job (job_name => v#job_name, force => true);
         exception
            when exc#job_doesnot_exists
            then
               null;
         end;

         v#start_date := systimestamp + numtodsinterval (t.rn * 1, 'MINUTE');

         begin
            DBMS_SCHEDULER.create_job (
               job_name     => v#job_name
              ,start_date   => v#start_date
              ,                                            -------------------------------------------------------------------------------------------------------------------------
               end_date     => v#end_date
              ,job_class    => 'DEFAULT_JOB_CLASS'
              ,job_type     => 'PLSQL_BLOCK'
              ,job_action   =>    v#block_start
                               || (v#owner || '.' || package_name || '.TABLE_PART_MAINTENANCE')
                               || '('
                               || (' p_table_name=>''' || t.table_name || '''')
                               || (',p_tabspace=>''' || p_tabspace || '''')
                               || ');'
                               || v#block_end
              ,comments     => v#job_comment || t.table_name
              ,auto_drop    => false
              ,enabled      => true);
            DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'RESTARTABLE', value => false);
            DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'JOB_PRIORITY', value => v#job_priority);
            DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'MAX_RUNS', value => 1);
         exception
            when exc#job_allready_exists
            then
               null;
         end;
      end loop;
   end;

   procedure gen_arch_jobs (p_tabspace in varchar2)
   as
      v#owner   constant varchar2 (30 char) := sys_context ('USERENV', 'CURRENT_SCHEMA');
      v#job_name         varchar2 (65 char);
      v#job_prefix       varchar2 (30 char);
      v#start_date       timestamp := systimestamp + interval '3' minute;
      v#end_date         timestamp := trunc (systimestamp, 'DD') + interval '1' day + interval '7' hour;
      v#block_start      varchar2 (2000 char);
      v#block_end        varchar2 (2000 char);
      v#job_priority     binary_integer;
      v#job_comment      varchar2 (200 char);
   begin
      v#block_start := 'BEGIN ';
      v#block_end := ' END;';
      v#job_priority := 3;
      v#job_comment := 'Система автоматизированного архивирования ';

      for t in get_arch_tables
      loop
         v#job_prefix := 'APJ' || to_char (t.rn, 'FM09') || '$';
         v#job_name := v#owner || '.' || v#job_prefix || substr (t.table_name, 1, 30 - length (v#job_prefix));

         begin
            DBMS_SCHEDULER.drop_job (job_name => v#job_name, force => true);
         exception
            when exc#job_doesnot_exists
            then
               null;
         end;

         v#start_date := systimestamp + numtodsinterval (t.rn * 1, 'MINUTE');

         begin
            DBMS_SCHEDULER.create_job (
               job_name     => v#job_name
              ,start_date   => v#start_date
              ,                                            -------------------------------------------------------------------------------------------------------------------------
               end_date     => v#end_date
              ,job_class    => 'DEFAULT_JOB_CLASS'
              ,job_type     => 'PLSQL_BLOCK'
              ,job_action   =>    v#block_start
                               || (v#owner || '.' || package_name || '.TABLE_ARCH_MAINTENANCE')
                               || '('
                               || (' p_table_name=>''' || t.table_name || '''')
                               || ');'
                               || v#block_end
              ,comments     => v#job_comment || t.table_name
              ,auto_drop    => false);
            DBMS_SCHEDULER.enable (name => v#job_name);
            DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'RESTARTABLE', value => false);
            DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'JOB_PRIORITY', value => v#job_priority);
            DBMS_SCHEDULER.set_attribute (name => v#job_name, attribute => 'MAX_RUNS', value => 1);
         exception
            when exc#job_allready_exists
            then
               null;
         end;
      end loop;
   end gen_arch_jobs;

   procedure gen_part_generator
   as
   begin
      --init_parameters;

      begin
         DBMS_SCHEDULER.create_schedule (
            schedule_name     => clean_ddl_schedule
           ,start_date        => trunc (systimestamp, 'DD') + interval '22' hour
           ,repeat_interval   => 'FREQ=MONTHLY; BYDAY=1SAT; BYHOUR=22; BYMINUTE=0'
           ,end_date          => null
           ,comments          =>    'Система автоматизированного добавления повременных секций в таблицы БД АС Сверка.'
                                 || chr (10)
                                 || 'ЗАО Ай-Теко, 2013, версия '
                                 || package_version);
      exception
         when exc#job_allready_exists
         then
            null;
      end;

      begin
         DBMS_SCHEDULER.drop_job (job_name => part_job_generator, force => false);
      exception
         when exc#job_doesnot_exists
         then
            null;
      end;

      DBMS_SCHEDULER.create_job (
         job_name        => part_job_generator
        ,schedule_name   => clean_ddl_schedule
        ,job_class       => 'DEFAULT_JOB_CLASS'
        ,job_type        => 'PLSQL_BLOCK'
        ,job_action      => package_name || '.GEN_PART_JOBS;'
        ,auto_drop       => false
        ,comments        =>    'Система автоматизированного секционирования БД АС Сверка.'
                            || chr (10)
                            || 'ЗАО Ай-Теко, 2013, версия '
                            || package_version);
      DBMS_SCHEDULER.set_attribute (name => part_job_generator, attribute => 'RESTARTABLE', value => false);
      DBMS_SCHEDULER.set_attribute_null (name => part_job_generator, attribute => 'MAX_FAILURES');
      DBMS_SCHEDULER.set_attribute_null (name => part_job_generator, attribute => 'MAX_RUNS');
      DBMS_SCHEDULER.set_attribute (name => part_job_generator, attribute => 'STOP_ON_WINDOW_CLOSE', value => false);
      DBMS_SCHEDULER.set_attribute (name => part_job_generator, attribute => 'JOB_PRIORITY', value => 3);
      DBMS_SCHEDULER.set_attribute_null (name => part_job_generator, attribute => 'SCHEDULE_LIMIT');
      DBMS_SCHEDULER.set_attribute (name => part_job_generator, attribute => 'LOGGING_LEVEL', value => DBMS_SCHEDULER.logging_full);
      DBMS_SCHEDULER.disable (name => part_job_generator);
   end gen_part_generator;

   procedure gen_arch_generator
   as
   begin
      --init_parameters;

      begin
         DBMS_SCHEDULER.create_schedule (
            schedule_name     => arch_ddl_schedule
           ,start_date        => trunc (systimestamp, 'DD') + interval '22' hour
           ,repeat_interval   => 'FREQ=MONTHLY; BYDAY=2SAT; BYHOUR=22; BYMINUTE=0'
           ,end_date          => null
           ,comments          =>    'Система автоматизированного перемещения устаревших секций в архив.'
                                 || chr (10)
                                 || 'ЗАО Ай-Теко, 2013, версия '
                                 || package_version);
      exception
         when exc#job_allready_exists
         then
            null;
      end;

      begin
         DBMS_SCHEDULER.drop_job (job_name => arch_job_generator, force => false);
      exception
         when exc#job_doesnot_exists
         then
            null;
      end;

      DBMS_SCHEDULER.create_job (
         job_name        => arch_job_generator
        ,schedule_name   => arch_ddl_schedule
        ,job_class       => 'DEFAULT_JOB_CLASS'
        ,job_type        => 'PLSQL_BLOCK'
        ,job_action      => package_name || '.GEN_ARCH_JOBS;'
        ,auto_drop       => false
        ,comments        =>    'Система автоматизированного архивирования БД АС Сверка.'
                            || chr (10)
                            || 'ЗАО Ай-Теко, 2013, версия '
                            || package_version);
      DBMS_SCHEDULER.set_attribute (name => arch_job_generator, attribute => 'RESTARTABLE', value => false);
      DBMS_SCHEDULER.set_attribute_null (name => arch_job_generator, attribute => 'MAX_FAILURES');
      DBMS_SCHEDULER.set_attribute_null (name => arch_job_generator, attribute => 'MAX_RUNS');
      DBMS_SCHEDULER.set_attribute (name => arch_job_generator, attribute => 'STOP_ON_WINDOW_CLOSE', value => false);
      DBMS_SCHEDULER.set_attribute (name => arch_job_generator, attribute => 'JOB_PRIORITY', value => 3);
      DBMS_SCHEDULER.set_attribute_null (name => arch_job_generator, attribute => 'SCHEDULE_LIMIT');
      DBMS_SCHEDULER.set_attribute (name => arch_job_generator, attribute => 'LOGGING_LEVEL', value => DBMS_SCHEDULER.logging_full);
      DBMS_SCHEDULER.enable (name => arch_job_generator);
   end gen_arch_generator;

   /* Функция проверки наличия unusable-индексов */
   function check_unusable_indexes
      return varchar2
   as
      r         get_unusable_indexes%rowtype;
      ret_val   varchar2 (100 char);
   begin
      open get_unusable_indexes (p_table_name => null, p_tabspace => null);

      loop
         fetch get_unusable_indexes into r;

         ret_val := unusable_indexes_dont_exists;
         exit when get_unusable_indexes%notfound;
         ret_val := unusable_indexes_exists;
         exit;
      end loop;

      close get_unusable_indexes;

      return ret_val;
   end;

   /* Процедура добавляет партиции по всем тербанкам. В каждой партиции 1 субпартиция по умолчанию (MAXVALUE)*/
   procedure prc_create_partitions_by_tb (p_first_date date, p_tablespace varchar2)
   is
      r        get_tables_bank_codes%rowtype;
      minval   timestamp;
   begin
      minval := systimestamp - interval '1' year;

      open get_tables_bank_codes;

      loop
         fetch get_tables_bank_codes into r;

         exit when get_tables_bank_codes%notfound;
         add_modify_split_partition (p_table_name             => r.table_name
                                    ,p_bank_code              => r.bc
                                    ,p_part_values_list       => r.bc
                                    ,p_subpart_values_range   => null                               -- NULL - будет добавлена субпартиция по умолчанию------------------------------
                                    ,p_tabspace               => p_tablespace);
         /*  Делаем субпартицию, в которую заносим совсем старые данные старше года ------------*/
         add_modify_split_partition (p_table_name             => r.table_name
                                    ,p_bank_code              => r.bc
                                    ,p_part_values_list       => r.bc
                                    ,p_subpart_values_range   => minval
                                    ,p_tabspace               => p_tablespace);
      end loop;

      close get_tables_bank_codes;
   /* Все необработанные исключения передаем наверх */
   end;

   procedure prc_create_subpart_to_date (p_date_end date, p_tablespace varchar2)
   is
      r        get_tables_bank_codes%rowtype;
      minval   timestamp;
   begin
      open get_tables_bank_codes;

      loop
         fetch get_tables_bank_codes into r;

         exit when get_tables_bank_codes%notfound;

         for d in (    select systimestamp - interval '12' month + numtodsinterval (level, 'DAY') cd
                         from dual
                   connect by systimestamp - interval '12' month + numtodsinterval (level, 'DAY') <= systimestamp + interval '30' day)
         loop
            add_modify_split_partition (p_table_name             => r.table_name
                                       ,p_bank_code              => r.bc
                                       ,p_part_values_list       => r.bc
                                       ,p_subpart_values_range   => d.cd                                                                   -- будет добавлена субпартиция с границей
                                       ,p_tabspace               => p_tablespace);
         end loop;
      end loop;

      close get_tables_bank_codes;
   /* Все необработанные исключения передаем наверх */
   end;

   procedure test_p
   as
   begin
      add_modify_split_partition (p_table_name             => 'INC_DOC_PRINT'
                                 ,p_bank_code              => '90'
                                 ,p_part_values_list       => '90'
                                 ,p_subpart_values_range   => null                                                                       --TO_TIMESTAMP ('2009-01-01', 'yyyy-mm-dd')
                                 ,p_tabspace               => 'USERS');
      null;
   end;
end pkg_mp;
/