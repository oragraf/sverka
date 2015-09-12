PL/SQL Developer Test script 3.0
57
declare
 v_exec       varchar2(4096);
 v_subpartition_name  varchar2(255);
begin
for rc in (select tname from (
(
select 'INC_PAYMENT'    v10,
       'INC_SYS_STATUS' v9,
       'INC_OPER_CLOB'  v8,
       'INC_NOTE_INFO'  v7,
       'INC_OPER_DTL_INFO' v6,
       'INC_SYSTEM_STATUS' v5,
       'INC_OPER_INFO'     v4,
       'INC_DOC_PRINT'     v3,
       'DISPUTED_REQUEST' v2,
       'INCIDENT'           v1
from dual 
)
unpivot
    (
        tname
        for value_type in
            (v1,v2,v3,v4,v5,v6,v7,v8,v9,v10)
    )
)    
where rownum<7) loop

select subpartition_name into v_subpartition_name
 from all_tab_subpartitions
  where table_owner=:p_owner 
        and table_name=rc.tname 
        and partition_name='TB99'
        and subpartition_position=(select max(subpartition_position) from all_tab_subpartitions
                                         where table_owner=:p_owner 
                                                 and partition_name='TB99'
                                                 and table_name=rc.tname); 
                                                 
                                                                     
v_exec:='ALTER TABLE '||rc.tname||' SPLIT SUBPARTITION '||v_subpartition_name||' AT (to_date('''||to_char(:p_dt,'dd.mm.yyyy')||''',''dd.mm.yyyy''))'|| 
' INTO (SUBPARTITION '||substr(v_subpartition_name,1,4)||'_'||to_char(:p_dt,'yyyymmdd')||', SUBPARTITION '||v_subpartition_name||')';
dbms_output.put_line(v_exec);
execute immediate v_exec;
select 'insert into '||rc.tname||'('||listagg(column_name,',') within group(order by column_id)||') '||
'select '||listagg(case when (column_name='TRANS_DATE' and rc.tname<>'INC_OPER_DTL_INFO') or (column_name='INC_TRANS_DATE' and rc.tname='INC_OPER_DTL_INFO')   then 'to_date('''||to_char(:p_dt,'dd.mm.yyyy')||'''||to_char('||column_name||',''hh24miss'')'||',''dd.mm.yyyyhh24miss'')' else column_name end  ,',') within group(order by column_id) ||
' from '||rc.tname||' where bank_code=99 and trans_date between :a and :b'
into
v_exec
 from all_tab_columns
  where owner=:p_owner
        and table_name=rc.tname;
dbms_output.put_line(v_exec);
execute immediate v_exec using :p_dt_start, :p_dt_end;   
end loop;
commit;
end;


4
p_owner
1
SVERKA_41_TEST_0621
5
p_dt
1
01.01.2016
12
p_dt_start
1
01.01.2015
12
p_dt_end
1
31.03.2015
12
0
