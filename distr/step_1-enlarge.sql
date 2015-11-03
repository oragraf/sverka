define script_step='1'
define script_name='enlarge'
define script_full_name='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
define script_desc='Скрипт выполняет основную работу по увеличению объема партиций.'

@@utl_head.sql
--------------

exec it$$utl.enlarge(&&script_step., '&&TS.');   

--------------
@@utl_foot.sql

exit