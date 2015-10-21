define script_step='0'
define script_name='prepare'
define script_full_name='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
define script_desc='Скрипт осуществляет удаление ограничений целостности в БД, создание промежуточных объектов, необходимых для работы. Запускается один раз.'

@@utl_head.sql

@@pkg_mp.pks
@@pkg_mp.pkb

select 'Начало сплита ' d, max (l.start_mark)
  from sys_manage_partition_log l;

begin
   for d in (with p
                  as (select cast (min (trans_date) as date) min_d, least (cast (max (trans_date) as date), sysdate) max_d
                        from inc_oper_dtl_info odi
                       where odi.bank_code = '99')
                 ,p1
                  as (    select min_d + level - 1 d
                            from p
                      connect by level <= max_d - min_d)
               select d
                 from p1
             order by d)
   loop
      pkg_mp.add_modify_split_partition (p_table_name             => 'INC_OPER_DTL_INFO'
                                        ,p_bank_code              => '99'
                                        ,p_part_values_list       => '99'
                                        ,p_subpart_values_range   => d.d
                                        ,p_tabspace               => 'SBRFTBS');
   end loop;
end;
/

select 'Окончание сплита ' d, max (l.end_mark)
  from sys_manage_partition_log l;
  
@@utl_foot.sql

exit