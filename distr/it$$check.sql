declare
   i        number;
   c_err1   varchar2 (2000)
      :=    'Ошибка последовательности запуска!'
         || chr (10)
         || 'По журналу запуска видно, что этот шаг(скрипт &&script_full_name.) уже выполнен!';
   c_err2   varchar2 (2000)
               := 'Обнаружены запущенные задания! Необходимо их остановить на время распучивания БД!';
begin
   execute immediate 'select count (*) from dual where exists (select null from it$$step where step_no >= &&script_step. and completed is not null)' into i;

   if i > 0
   then
      raise_application_error (-20000, c_err1);
   end if;

   select count (*)
     into i
     from user_scheduler_jobs j
    where j.enabled = 'TRUE' and j.state = 'RUNNING';

   if i > 0
   then
      raise_application_error (-20000, c_err2);
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