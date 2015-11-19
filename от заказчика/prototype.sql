define p_base_day= "to_timestamp(date'2015-01-01')"

drop table incident cascade constraints purge;
drop table map$t cascade constraints purge;

create table incident
(
  trans_date      timestamp
, bank_code       varchar2( 2 char )
, new_trans_date  timestamp
, grp             number
, new_grp         number
)
;

insert into incident( trans_date
                    , bank_code
                     )
      select systimestamp + numtodsinterval(level / 24,'day')
           , '99'
        from dual
  connect by level <= 150;

create table map$t
(
  dt_start  timestamp
, dt_end    timestamp
, np        number
)
;


declare
  v_dt_start           timestamp;
  v_previous_date      timestamp;
  v_cntday             number := 0;
  p_max_count_rec_day  number := 7;
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
   set (new_trans_date, grp) =
         ( select &&p_base_day. + numtodsinterval(np,'day') + (incident.trans_date - trunc(incident.trans_date,'DD')), np
             from map$t
            where incident.trans_date between dt_start and dt_end );

--update incident
--   set new_grp =
--         ( select &&p_base_day. + numtodsinterval(np,'day') + (incident.trans_date - trunc(incident.trans_date,'DD'))
--             from map$t
--            where incident.trans_date between dt_start and dt_end );
commit;