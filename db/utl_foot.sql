prompt ��������� ������� ���������� ����� &&script_step. ��� ������

update it$$bank_code t
   set t.last_ok_step = &&script_step.
 where is_process = 1;

prompt ��������� ������� ���������� ����� &&script_step. ��� �����

update it$$step
   set completed = systimestamp
 where step_no = to_number (&&script_step.);

commit;

prompt ������ &&SCRIPT_FULL_NAME ��������
prompt ---------------------------------------------------------------------------------------------------------------
SPOOL OFF
undefine script_step
undefine script_name
undefine script_full_name