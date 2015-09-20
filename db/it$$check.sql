declare
   i       number;
   c_err   varchar2 (2000)
      :=    'Ошибка последовательности запуска!'
         || chr (10)
         || 'По журналу запуска видно, что этот шаг(скрипт &&script_full_name.) уже выполнен!';
begin
   execute immediate 'select count (*) from dual where exists (select null from it$$step where step_no >= &&script_step. and completed is not null)' into i;

   if i > 0
   then
      raise_application_error (-20000, c_err);
   end if;
exception
   when others
   then
      if sqlcode = -942
      then
         null;
      else
         raise;
      end if;
end;
/