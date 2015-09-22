CREATE OR REPLACE PACKAGE it$$ddl
  authid current_user as
  procedure drop_primary_key( p_schema_name in varchar2, p_table_name in varchar2 );
  procedure drop_primary_key( p_table_name in varchar2 );
  procedure drop_index( p_schema_name in varchar2, p_index_name in varchar2 );
  procedure drop_index( p_index_name in varchar2 );
  procedure alter_table( p_ddl in varchar2 );
  procedure drop_table( p_schema_name in varchar2, p_table_name in varchar2 );
  procedure drop_table( p_table_name in varchar2 );
  procedure create_index( p_ddl in varchar2 );
  procedure drop_constraint( p_schema_name         in varchar2
                           , p_table_name          in varchar2
                           , p_constraint_name     in varchar2 );
  procedure drop_constraint( p_table_name in varchar2, p_constraint_name in varchar2 );
  procedure create_table( p_schema_name     in varchar2
                        , p_table_name      in varchar2
                        , p_ddl             in varchar2 );
  procedure create_table( p_table_name in varchar2, p_ddl in varchar2 );
  procedure comment_tab( p_table_name in varchar2, p_comment in varchar2 );
  procedure comment_col( p_column_name in varchar2, p_comment in varchar2 );
  procedure grant_permissions( p_ddl in varchar2 );
  procedure revoke_permissions( p_ddl in varchar2 );
--  procedure info( p_str in varchar2 );
--  procedure error( p_str in varchar2 );
  procedure update_if_exists( p_ddl in varchar2 );
  procedure delete_if_exists( p_ddl in varchar2 );
end;
/

CREATE OR REPLACE PACKAGE BODY it$$ddl as
  exn#already_exists                 constant pls_integer := -1442;
  exc#already_exists                          exception;
  pragma exception_init( exc#already_exists, -1442 );
  exn#invalid_identifier             constant pls_integer := -904;
  exc#invalid_identifier                      exception;
  pragma exception_init( exc#invalid_identifier, -904 );
  exn#constraint_exists              constant pls_integer := -2264;
  exc#constraint_exists                       exception;
  pragma exception_init( exc#constraint_exists, -2264 );
  exn#object_exists                  constant pls_integer := -955;
  exc#object_exists                           exception;
  pragma exception_init( exc#object_exists, -955 );
  exn#column_exists                  constant pls_integer := -1430;
  exc#column_exists                           exception;
  pragma exception_init( exc#column_exists, -1430 );
  exn#pk_exists                      constant pls_integer := -2260;
  exc#pk_exists                               exception;
  pragma exception_init( exc#pk_exists, -2260 );
  exn#fk_exists                      constant pls_integer := -2275;
  exc#fk_exists                               exception;
  pragma exception_init( exc#fk_exists, -2275 );
  exn#ak_exists                      constant pls_integer := -2261;
  exc#ak_exists                               exception;
  pragma exception_init( exc#ak_exists, -2261 );
  exn#table_does_not_exists          constant pls_integer := -942;
  exc#table_does_not_exists                   exception;
  pragma exception_init( exc#table_does_not_exists, -942 );
  exn#priv_not_allowed_for_tab       constant pls_integer := -2224;
  exc#priv_not_allowed_for_tab                exception;
  pragma exception_init( exc#priv_not_allowed_for_tab, -2224 );
  exn#priv_not_allowed_for_proc      constant pls_integer := -2225;
  exc#priv_not_allowed_for_proc               exception;
  pragma exception_init( exc#priv_not_allowed_for_proc, -2225 );
  exn#user_or_role_not_exists        constant pls_integer := -1917;
  exc#user_or_role_not_exists                 exception;
  pragma exception_init( exc#user_or_role_not_exists, -1917 );
  exn#cannot_revoke_non_granted      constant pls_integer := -1927;
  exc#cannot_revoke_non_granted               exception;
  pragma exception_init( exc#cannot_revoke_non_granted, -1927 );
  exn#cannot_change_col_datatype     constant pls_integer := -2267;
  exc#cannot_change_col_datatype              exception;
  pragma exception_init( exc#cannot_change_col_datatype, -2267 );
  exn#already_indexed                constant pls_integer := -1408;
  exc#already_indexed                          exception;
  pragma exception_init( exc#already_indexed, -1408 );
  exn#constraint_rule_violated       constant pls_integer := -2293;
  exc#constraint_rule_violated                exception;
  pragma exception_init( exc#constraint_rule_violated, -2293 );  
  exn#constraint_does_not_exists     constant pls_integer := -2443;
  exc#constraint_does_not_exists              exception;
  pragma exception_init( exc#constraint_does_not_exists, -2443);
  exn#null_values_found              constant pls_integer := -2296;
  exc#null_values_found                       exception;
  pragma exception_init(exc#null_values_found, -2296);  
  exn#cannot_modify_to_null          constant pls_integer := -1451;
  exc#cannot_modify_to_null                   exception;
  pragma exception_init(exc#cannot_modify_to_null, -1451);    
  exn#duplicate_column_exists        constant pls_integer := -957;
  exc#duplicate_column_exists                 exception;
  pragma exception_init(exc#duplicate_column_exists, -957);    
  g_datetime_spool_mask              constant varchar2( 100 CHAR ) := 'YYYY.MM.DD HH24:MI:SS.FF3';
  g_spool_prefix_mask                constant varchar2( 100 CHAR ) := 'P ' || g_datetime_spool_mask || ' ';
  g_max_spool_linesize               constant binary_integer := 255;
  g_spool_linesize                   constant binary_integer := g_max_spool_linesize - length( g_spool_prefix_mask );
  cursor cur#obj(
    p_object_type     in varchar2
  , p_object_name     in varchar2 ) is
    select owner || '.' || object_name fully_qualified_name
         , object_type
         , 'DROP ' || object_type || ' ' || owner || '.' || object_name || case when o.object_type = 'TABLE' then ' CASCADE CONSTRAINTS purge' end drop_statement
      from sys.all_objects o
     where o.owner || '.' || o.object_name = p_object_name and o.object_type = upper( trim( p_object_type ) );
  cursor cur#triggers(p_table_name in varchar2 )
  is
  SELECT T.OWNER, T.TRIGGER_NAME, O.STATUS
  FROM    all_triggers T
       INNER JOIN
          ALL_OBJECTS O
       ON     T.OWNER = O.OWNER
          AND T.TRIGGER_NAME = O.OBJECT_NAME          
          AND t.base_object_type IN ('TABLE', 'VIEW')
   WHERE T.owner || '.' || T.table_name = p_table_name;
  /**
   функция вывода в спул сообщений. Осуществляет подрезку длинных строк так, чтобы не случилось переполнения буфера строки
   для 9.2.0.5 длина строки 255 символов
  */
  procedure pl( p_prefix in varchar2, p_str in varchar2 ) as
    l_piece          varchar2( 255 CHAR );
    l_pos            pls_integer;
    l_len            pls_integer;
    l_cor_len        pls_integer;
    l_checkpoint     varchar2( 100 CHAR );
  begin
    if p_str is null then
      return;
    end if;
    l_checkpoint := p_prefix || ' ' || to_char( systimestamp, g_datetime_spool_mask ) || ' ';
    l_pos        := 1;
    l_len        := length( p_str );
    if l_len > g_spool_linesize then
      while ( l_pos <= l_len ) loop
        l_piece   := substr( p_str, l_pos, g_spool_linesize );
        l_cor_len := instr( l_piece, ' ', -1, 1 );
        if l_cor_len = 0 then
          l_cor_len := g_spool_linesize;
        end if;
        l_piece   := l_checkpoint || substr( p_str, l_pos, l_cor_len );
        l_checkpoint := '';
        dbms_output.put_line( l_piece );
        l_pos     := l_pos + l_cor_len;
      end loop;
    else
      l_piece := l_checkpoint || p_str;
      dbms_output.put_line( l_piece );
    end if;
  end pl;
  /**
  функция вывода информационных(с префиксом I) сообщений в спул
  */
    procedure info (p_bank varchar2, p_tbl varchar2, p_str in varchar2)
    as
       pragma autonomous_transaction;
    begin
       pl (p_prefix => 'I', p_str => p_str);

       insert into it$$enlarge_log (bank_code, orig_tbl_name, ddl_dml)
            values (p_bank, p_tbl, substr (p_str, 1, 2000));
       commit;
    end;

    function info (p_bank varchar2, p_tbl varchar2,p_str in varchar2)
       return number
    as
       pragma autonomous_transaction;
       l_ret number;
    begin
       pl (p_prefix => 'I', p_str => p_str);

       insert into it$$enlarge_log (bank_code, orig_tbl_name, ddl_dml)
            values (p_bank, p_tbl, substr (p_str, 1, 2000)) return timemark into l_ret;

       commit;
       return l_ret;
    end;
  /**
  функция вывода сообщений об ошибках(с префиксом E) в спул
  */
    procedure error (p_timemark      number
                    ,p_bank          varchar2
                    ,p_tbl           varchar2
                    ,p_ddl           varchar2
                    ,p_err           varchar2)
    as
       pragma autonomous_transaction;
    begin
       pl (p_prefix => 'E', p_str => p_err);

       if p_timemark is null
       then
          insert into it$$enlarge_log (bank_code
                                      ,orig_tbl_name
                                      ,ddl_dml
                                      ,error_message)
               values (p_bank
                      ,p_tbl
                      ,substr (p_ddl, 1, 2000)
                      ,substr (p_err, 1, 2000));                                                                                      -- return timemark into l_ret;
       else
          update it$$enlarge_log
             set error_message = substr (p_err, 1, 2000)
           where timemark = p_timemark;
       end if;

       commit;
    end;
  function non( p_object_name in varchar2 )
    return varchar2
    deterministic as
    l_ret           varchar2( 100 char );
    l_is_exists     boolean := false;
  begin
    l_is_exists := ( instr( p_object_name, '"' ) > 0 );
    if l_is_exists then
      l_ret := replace( trim( p_object_name ), '"', '' );
    else
      l_ret := upper( replace( p_object_name, ' ', '' ) );
    end if;
    l_is_exists := not ( instr( l_ret, '.' ) > 0 );
    if l_is_exists then
      l_ret := sys_context( 'USERENV', 'CURRENT_SCHEMA' ) || '.' || L_RET;
    end if;
    return l_ret;
  end;
  function non( p_schema_name in varchar2, p_object_name in varchar2 )
    return varchar2 as
  begin
    return non( p_schema_name || '.' || p_object_name );
  end;
  procedure alter_table(p_bank varchar2, p_tbl varchar2, p_ddl in varchar2 ) as
  begin
--    info( p_ddl || '; ' );
    execute immediate p_ddl;
    info( p_ddl || '; -- Успешно...' );
  exception
    when exc#already_exists or exc#invalid_identifier or exc#constraint_exists or exc#column_exists then
      info( 'Уже существует...' );
    when exc#pk_exists or exc#fk_exists or exc#ak_exists then
      info( 'Уже существует...' );
    when exc#constraint_does_not_exists then
      info( 'Удаляемое ограничение не существует. Пропущено...' );
    when exc#constraint_rule_violated then
      error('Существующие данные в таблице не удовлетворяют создаваемому ограничению! ');
      raise; 
    when  exc#null_values_found  then
      error('При попытке создать ограничение NOT NULL обнаружены строки, не удовлетворяющие этому ограничению.');
      raise;
    when exc#cannot_change_col_datatype then
      error ('Невозможно сменить тип столбца этой командой.');
      raise;         
    when exc#cannot_modify_to_null then
      info ('Невозможно сделать столбец NULLABLE. Столбец уже NULLABLE либо является частью PK/UK. Пропущено...');    
    when exc#duplicate_column_exists then
      info ('Невозможно добавить/переименовать столбец. Столбец уже существует. Пропущено...');                                     
  end;
  procedure create_index( p_ddl in varchar2 ) as
  begin
    info( p_ddl || '; ' );
    execute immediate p_ddl;
    info( 'Успешно...' );
  exception
    when exc#already_exists or exc#invalid_identifier or exc#constraint_exists or exc#object_exists then
      info( 'Уже существует...' );
    when exc#already_indexed then
      error('Данный набор колонок уже проиндексирован. Пропущено...' );      
  end;
  procedure drop_table( p_table_name in varchar2 ) as
  begin
    for r in cur#obj( 'TABLE', non( p_table_name ) ) loop
      execute immediate r.drop_statement;
    end loop;
  end;
  procedure drop_table( p_schema_name in varchar2, p_table_name in varchar2 ) as
  begin
    drop_table( p_schema_name || '.' || p_table_name );
  end;
  procedure drop_index( p_index_name in varchar2 ) as
  begin
    for r in cur#obj( 'INDEX', non( p_index_name ) ) loop
      execute immediate r.drop_statement;
    end loop;
  end;
  procedure drop_index( p_schema_name in varchar2, p_index_name in varchar2 ) as
  begin
    drop_index( p_schema_name || '.' || p_index_name );
  end;
  procedure drop_primary_key( p_table_name in varchar2 ) as
    l_tn     varchar2( 100 char );
  begin
    l_tn := non( p_table_name );
    for r in ( select *
                 from sys.all_constraints c
                where c.owner || '.' || c.table_name = l_tn and c.constraint_type = 'P' ) loop
      execute immediate 'alter table ' || l_tn || ' drop primary key cascade';
    end loop;
  end;
  procedure drop_primary_key( p_schema_name in varchar2, p_table_name in varchar2 ) as
  begin
    drop_primary_key( p_schema_name || '.' || p_table_name );
  end;
  procedure drop_constraint( p_table_name in varchar2, p_constraint_name in varchar2 ) as
    l_tn     varchar2( 100 char );
    l_cn     varchar2( 100 char );
  begin
    l_tn := non( p_table_name );
    l_cn := TRIM(UPPER( p_constraint_name ));
    begin
      for r in ( select *
                   from sys.all_constraints c
                  where c.owner || '.' || c.table_name = l_tn and c.constraint_name = l_cn and c.constraint_type != 'P' ) loop
        execute immediate 'alter table ' || l_tn || ' drop constraint ' || l_cn;
      end loop;
    end;
  end;
  procedure drop_constraint( p_schema_name         in varchar2
                           , p_table_name          in varchar2
                           , p_constraint_name     in varchar2 ) as
  begin
    drop_constraint( p_schema_name || '.' || p_table_name, p_constraint_name );
  end;

  procedure create_table( p_table_name in varchar2, p_ddl in varchar2 ) as
    v_is_exists      boolean := false;
    v_table_name     varchar2( 100 char );
    v_tmp_name       varchar2( 100 char );
    v_schema         varchar2( 30 char );
    v_pos            binary_integer;
    V_ddl            varchar2( 32767 CHAR );
    v_default        varchar2( 32767 CHAR );
  begin
    v_table_name := non( p_table_name );
    for r in cur#obj( 'TABLE', v_table_name ) loop
      v_is_exists := true;
    end loop;
    if v_is_exists then
      v_pos      := instr( v_table_name, '.' );
      v_schema   := substr( v_table_name, 1, v_pos );
      v_tmp_name := 'IT#' || to_char( systimestamp, 'YYYYMMDDHH24MISSFF3' );
      v_tmp_name := v_schema || v_tmp_name; -- Уже с точкой
      info( 'Таблица' || v_table_name || ' уже существует. Сравниваю структуру...' );
      v_pos      := instr( P_DDL, '(', 1, 1 ); -- finding the first left parenthis
      v_ddl      := substr( P_DDL, 1, v_pos );
      v_ddl      := replace( v_ddl, p_table_name, v_tmp_name );
      v_ddl      := v_ddl || substr( p_ddl, v_pos + 1 );
      info( v_ddl );
      execute immediate V_DDL;
      /* adding new columns */
      for R in (  select TC.COLUMN_NAME
                       , TC.DATA_TYPE
                       , DATA_LENGTH
                       , CHAR_USED
                       , DATA_PRECISION
                       , DATA_SCALE
                       , NULLABLE
                       , DATA_DEFAULT
                    from all_tab_columns TC
                   where TC.owner || '.' || TC.table_name = v_tmp_name
                         and COLUMN_NAME in (select COLUMN_NAME CN
                                               from all_tab_columns NT
                                              where NT.owner || '.' || NT.table_name = v_tmp_name
                                             minus
                                             select COLUMN_NAME CN
                                               from all_tab_columns OT
                                              where OT.owner || '.' || OT.table_name = v_table_name)
                order by COLUMN_ID ) loop
        v_ddl     := 'ALTER TABLE ' || v_table_name || ' ADD(' || r.COLUMN_NAME || ' ' || r.DATA_TYPE;
        v_ddl      :=
          v_ddl
          || case
               when r.DATA_TYPE like '%CHAR%' then
                 '(' || r.DATA_LENGTH || case r.CHAR_USED when 'Y' then ' CHAR ' else ' BYTE ' end || ')'
               when r.DATA_TYPE like '%NUMBER%' and r.DATA_PRECISION is not null then
                 '(' || r.DATA_PRECISION || case when r.DATA_SCALE is not null then ',' || r.DATA_SCALE end || ')'
               when r.DATA_TYPE like '%NUMBER%' and r.DATA_PRECISION is null then
                 ''
             end;
        v_default := r.DATA_DEFAULT;
        v_ddl     := v_ddl || case when v_default is not null then ' DEFAULT ' || v_default end;
        v_ddl     := v_ddl || case when not r.NULLABLE = 'Y' then ' NOT NULL' end;        
        v_ddl     := v_ddl || ')';
        alter_table(v_ddl);
      end loop;
      /* delete deprecated columns */
      for R in ( select 'ALTER TABLE ' || v_table_name || ' DROP COLUMN ' || TC.COLUMN_NAME CSQL
                   from all_tab_columns TC
                  where TC.owner || '.' || TC.table_name = v_table_name
                        and COLUMN_NAME in (select COLUMN_NAME CN
                                              from all_tab_columns OT
                                             where OT.owner || '.' || OT.table_name = v_table_name
                                            minus
                                            select COLUMN_NAME CN
                                              from all_tab_columns NT
                                             where NT.owner || '.' || NT.table_name = v_tmp_name) ) loop
        info( R.CSQL || ' - Удаление столбцов нужно производить вручную.' );
      end loop;
      /* alter changed columns */
        DECLARE
           v#new_dd   varchar2 ( 32767 CHAR );
           v#old_dd   varchar2 ( 32767 CHAR );
        BEGIN
           FOR r IN (SELECT 'alter table ' || OCo.OWNER || '.' || OCo.table_name || ' modify constraint ' || oco.constraint_name || ' enable' csql
                       FROM all_constraints oco
                      WHERE OCo.owner || '.' || OCo.table_name = 'FNS.DEC_TU_DOC_OUT' AND status != 'ENABLED') 
           LOOP
                 EXECUTE IMMEDIATE r.csql;
           END LOOP;
           FOR R IN (  SELECT *
                         FROM (SELECT NC.COLUMN_NAME
                                    ,  NC.COLUMN_ID
                                    ,  NC.DATA_TYPE
                                    ,  NC.DATA_LENGTH
                                    ,  NC.DATA_PRECISION
                                    ,  NC.DATA_SCALE
                                    ,  NC.CHAR_USED
                                    ,  NC.NULLABLE NULLABLE_NEW
                                    ,  OC.NULLABLE NULLABLE_OLD
                                    ,  NC.DATA_DEFAULT NEW_DD
                                    ,  OC.DATA_DEFAULT OLD_DD
                                    ,  CASE WHEN NC.DATA_TYPE != OC.DATA_TYPE THEN 'DATA_TYPE CHANGED' END Q1
                                    ,  CASE WHEN NC.DATA_LENGTH != OC.DATA_LENGTH THEN 'DATA_LENGTH CHANGED' END Q2
                                    ,  CASE WHEN NC.DATA_PRECISION != OC.DATA_PRECISION THEN 'DATA_PRECISION CHANGED' END Q3
                                    ,  CASE WHEN NC.DATA_SCALE != OC.DATA_SCALE THEN 'DATA_SCALE CHANGED' END Q4
                                    ,  CASE WHEN NC.NULLABLE != OC.NULLABLE THEN 'NULLABLE CHANGED' END Q5
                                    ,  CASE WHEN NC.CHAR_USED != OC.CHAR_USED THEN 'CHAR_USED CHANGED' END Q6
                                    ,  OCO.STATUS
                                    ,  OCC.CONSTRAINT_NAME old_constraint_name
                                    ,  COUNT (*) OVER (PARTITION BY NVL (OCc.CONSTRAINT_NAME, OC.COLUMN_NAME)) rn
                                    ,  OCO.SEARCH_CONDITION
                                 FROM all_tab_columns NC
                                      INNER JOIN all_tab_columns OC
                                         ON NC.COLUMN_NAME = OC.COLUMN_NAME
                                      LEFT JOIN (   all_cons_columns occ
                                                 INNER JOIN
                                                    all_constraints oco
                                                 ON OCO.OWNER = OCC.OWNER AND OCO.CONSTRAINT_NAME = OCC.CONSTRAINT_NAME AND OCO.CONSTRAINT_TYPE = 'C')
                                         ON OCC.COLUMN_NAME = OC.COLUMN_NAME AND OCC.OWNER = OC.OWNER AND OCC.TABLE_NAME = OC.TABLE_NAME
                                WHERE NC.owner || '.' || NC.table_name = v_tmp_name AND OC.owner || '.' || OC.table_name = v_table_name)
                        WHERE rn <= 1
                     ORDER BY COLUMN_ID)
           LOOP
              v#new_dd := r.new_dd;
              v#old_dd := r.old_dd;
              IF r.q1 || r.q2 || r.q3 || r.q4 || r.q5 || R.Q6 IS NOT NULL OR NVL (v#new_dd, '~') != NVL (v#old_dd, '~')
              THEN
                 v_ddl := 'ALTER TABLE ' || v_table_name || ' modify (' || r.COLUMN_NAME || ' ' || r.DATA_TYPE;
                 v_ddl :=
                       v_ddl
                    || CASE
                          WHEN r.DATA_TYPE LIKE '%CHAR%'
                          THEN
                             '(' || r.DATA_LENGTH || CASE WHEN r.CHAR_USED = 'C' OR r.CHAR_USED IS NULL THEN ' CHAR ' ELSE ' BYTE ' END || ')'
                          WHEN r.DATA_TYPE LIKE '%NUMBER%' AND r.DATA_PRECISION IS NOT NULL
                          THEN
                             '(' || r.DATA_PRECISION || CASE WHEN r.DATA_SCALE IS NOT NULL THEN ',' || r.DATA_SCALE END || ')'
                          WHEN r.DATA_TYPE LIKE '%NUMBER%' AND r.DATA_PRECISION IS NULL
                          THEN
                             ''
                       END;
                 IF NVL (v#new_dd, '~') != NVL (v#old_dd, '~')
                 THEN
                    v_default := v#new_dd;
                 ELSE
                    v_default := NULL;
                 END IF;
                 v_ddl := v_ddl || CASE WHEN v_default IS NOT NULL THEN ' DEFAULT ' || v_default END;
                 v_ddl :=
                       v_ddl
                    || CASE
                          WHEN NOT r.NULLABLE_NEW = 'Y' AND r.NULLABLE_OLD = 'Y' THEN ' NOT NULL '
                          WHEN r.NULLABLE_NEW = 'Y' AND NOT r.NULLABLE_OLD = 'Y' THEN ' NULL '
                       END;
                 v_ddl := v_ddl || ')';
                 alter_table(v_ddl);
              END IF;
           END LOOP;
        END;
      /* DROP TEMP TABLE */
      DROP_TABLE( v_tmp_name );
    else
      execute immediate p_ddl;
      info( 'Таблица' || v_table_name || ' успешно создана...' );
    end if;
    exception when others then
     /* DROP TEMP TABLE anyway */
      error(sqlerrm);
      DROP_TABLE( v_tmp_name );
      RAISE;
  end;
  procedure create_table( p_schema_name     in varchar2
                        , p_table_name      in varchar2
                        , p_ddl             in varchar2 ) as
  begin
    create_table( p_schema_name || '.' || p_table_name, p_ddl );
  end;
  procedure create_type( p_type_name in varchar2, p_ddl in varchar2 ) as
    v_is_exists     boolean := false;
    v_type_name     varchar2( 100 char );
  begin
    v_type_name := non( p_type_name );
    for r in cur#obj( 'TABLE', v_type_name ) loop
      v_is_exists := true;
    end loop;
    if v_is_exists then
      info( 'Тип данных ' || v_type_name || ' уже существует...' );
    else
      execute immediate p_ddl;
      info( 'Тип данных ' || v_type_name || ' успешно создан...' );
    end if;
  end;
  procedure create_type( p_schema_name     in varchar2
                       , p_type_name       in varchar2
                       , p_ddl             in varchar2 ) as
  begin
    create_type( p_schema_name || '.' || p_type_name, p_ddl );
  end;
  procedure comment_tab( p_table_name in varchar2, p_comment in varchar2 ) as
  begin
    execute immediate 'COMMENT ON TABLE ' || p_table_name || ' IS ''' || p_comment || '''';
  end;
  procedure comment_col( p_column_name in varchar2, p_comment in varchar2 ) as
  begin
    execute immediate 'COMMENT ON COLUMN ' || p_COLUMN_name || ' IS ''' || p_comment || '''';
  end;
  procedure grant_permissions( p_ddl in varchar2 ) as
  begin
    execute immediate p_ddl;
  exception
    when exc#table_does_not_exists then
      error( 'Таблица отсутствует в области видимости.' );
    when exc#priv_not_allowed_for_tab then
      error( 'Такая привилегия для таблицы не может быть выдана.' );
    when exc#priv_not_allowed_for_proc then
      error(
             'Такая привилегия для исполняемого кода(процедуры, функции, пакета) не может быть выдана.' );
    when exc#user_or_role_not_exists then
      error( 'Пользователь или роль, для которого выдается привилегия, не существует.' );
  end;
  procedure revoke_permissions( p_ddl in varchar2 ) as
  begin
    execute immediate p_ddl;
  exception
    when exc#table_does_not_exists then
      error( 'Таблица отсутствует в области видимости.' );
    when exc#priv_not_allowed_for_tab then
      error( 'Такая привилегия для таблицы не может быть выдана.' );
    when exc#priv_not_allowed_for_proc then
      error(
             'Такая привилегия для исполняемого кода(процедуры, функции, пакета) не может быть выдана.' );
    when exc#user_or_role_not_exists then
      error( 'Пользователь или роль, для которого выдается привилегия, не существует.' );
    when exc#cannot_revoke_non_granted then
      error( 'Нельзя отобрать привилегию, которая не выдавалась.' );
  end;
  procedure update_if_exists( p_ddl in varchar2 ) as
  begin
    info(p_ddl);
    execute immediate p_ddl;
    commit;
  exception
    when exc#table_does_not_exists or exc#invalid_identifier then
      null;
  end;
  procedure delete_if_exists( p_ddl in varchar2 ) as
  begin
    info(p_ddl);
    execute immediate p_ddl;
    commit;
  exception
    when exc#table_does_not_exists or exc#invalid_identifier then
      null;
  end;
end;
/

