prompt Проставим признак завершения этапа &&script_step. для банков

update it$$bank_code t
   set t.last_ok_step = &&script_step.
 where is_process = 1;

prompt Проставим признак завершения этапа &&script_step. для этапа

update it$$step
   set completed = systimestamp
 where step_no = to_number (&&script_step.);

commit;

prompt Скрипт &&SCRIPT_FULL_NAME выполнен
prompt ---------------------------------------------------------------------------------------------------------------
SPOOL OFF
undefine script_step
undefine script_name
undefine script_full_name