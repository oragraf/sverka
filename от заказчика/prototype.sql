define p_base_day= "date'2015-01-01'"

drop table incident cascade constraints purge;
drop table map$t cascade constraints purge;

create table incident
(
  trans_date      timestamp
, bank_code       varchar2( 2 char )
, new_trans_date  timestamp
)
;

insert into incident( trans_date
                    , bank_code
                     )
      select sysdate + level / 24
           , '99'
        from dual
  connect by level <= 48;

create table map$t
(
  dt_start  timestamp
, dt_end    timestamp
, np        number
)
;


declare
  v_dt_start           date;
  v_previous_date      date;
  v_cntday             number := 0;
  p_max_count_rec_day  number := 3;
  v_np                 number := 0;
begin
  for rec in (  select trans_date
                  from incident
                 where bank_code = 99
              order by trans_date )
  loop                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           ---Открывается курсор  по таблице INCIDENT  отсортированной по TRANS_DATE, данные отбираются для 99 ТБ
    if v_dt_start is null then
      v_dt_start := rec.trans_date;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  --Начало периода для первой записи

      v_previous_date := rec.trans_date;
    end if;

    v_cntday := v_cntday + 1;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  -- Счетчик записей в дне

    if v_cntday > p_max_count_rec_day
   and v_previous_date < rec.trans_date then                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    --По дата проверяем, чтобы с одинаковой датой  была одинаковая партиция
      insert into map$t( dt_start
                       , dt_end
                       , np
                        )
           values ( v_dt_start
                  , v_previous_date
                  , v_np
                   );                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               -- Фиксируем период

      v_dt_start := rec.trans_date;

      v_np := v_np + 1;

      v_cntday := 1;
    end if;

    v_previous_date := rec.trans_date;
  end loop;

  if v_cntday >1 then
      insert into map$t( dt_start
                       , dt_end
                       , np
                        )
           values ( v_dt_start
                  , v_previous_date
                  , v_np
                   );                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               -- Фиксируем период
   end if;
  commit;

end;
/

update incident
   set new_trans_date =
         ( select &&p_base_day. + np
             from map$t
            where incident.trans_date between dt_start and dt_end );

commit;