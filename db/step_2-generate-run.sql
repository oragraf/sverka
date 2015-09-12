DEFINE part='&1'
----------------------------------------------
DEFINE SCRIPT_STEP='2'
DEFINE SCRIPT_NAME='generate-run'
DEFINE SCRIPT_FULL_NAME='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
DEFINE SCRIPT_DESC='Скрипт определяет инициализационный параметр для сиквенса, осуществляет генерацию заданий на копирование и выполнение по тербанка &&PART.'

@@utl_head.sql
--------------

DECLARE
   sel         VARCHAR2 (1000);
   csql        VARCHAR2 (1000);
   fr          VARCHAR2 (1000);
   imax        NUMBER;
   prev_imax   NUMBER := 0;

   PK          VARCHAR2 (4000);
   TOT_PK      VARCHAR2 (4000);
   COL_LIST    VARCHAR2 (4000);
   VAL_LIST    VARCHAR2 (4000);
   CDML        VARCHAR2 (4000);

   PROCEDURE PL (S IN VARCHAR2)
   AS
   BEGIN
      DBMS_OUTPUT.PUT_LINE (S);
   END;
BEGIN
   DBMS_OUTPUT.ENABLE (1000000);

   FOR T IN (SELECT *
               FROM it$DUP_TABLES J)
   LOOP
      sel := 'select max(';
      fr := ' from ' || t.ttbl;

      FOR C IN (  SELECT TC.TABLE_NAME
                       ,  TC.COLUMN_ID RN
                       ,  ROW_NUMBER () OVER (PARTITION BY TC.TABLE_NAME ORDER BY TC.COLUMN_ID DESC) DRN
                       ,  TC.COLUMN_NAME
                       ,  CC.CONSTRAINT_NAME
                       ,  CC.POSITION
                    FROM    USER_TAB_COLUMNS TC
                         LEFT JOIN
                            (   USER_CONS_COLUMNS CC
                             INNER JOIN
                                USER_CONSTRAINTS C
                             ON CC.CONSTRAINT_NAME = C.CONSTRAINT_NAME AND C.CONSTRAINT_TYPE = 'P')
                         ON CC.TABLE_NAME = TC.TABLE_NAME AND CC.COLUMN_NAME = TC.COLUMN_NAME
                   WHERE TC.TABLE_NAME = T.TTBL
                ORDER BY TC.TABLE_NAME, TC.COLUMN_ID)
      LOOP
         IF C.POSITION IS NOT NULL
         THEN
            sel := sel || C.COLUMN_NAME || ')';
            EXIT;
         END IF;
      END LOOP;

      csql := sel || fr;

      EXECUTE IMMEDIATE csql INTO imax;

      pl (csql || ';---->' || TO_CHAR (imax));

      IF imax > prev_imax
      THEN
         prev_imax := imax;
      END IF;
   END LOOP;

   PREV_IMAX := TRUNC (PREV_IMAX, -7) + POWER (10, 7);
   pl ('Самое максимальное значение ' || TO_CHAR (prev_imax));

   FOR T IN (SELECT *
               FROM it$DUP_TABLES J)
   LOOP
      CDML := 'INSERT /*+ APPEND */ INTO ' || T.PTBL || ' (';
      COL_LIST := '';
      VAL_LIST := '';

      FOR C IN (  SELECT TC.TABLE_NAME
                       ,  TC.COLUMN_ID RN
                       ,  ROW_NUMBER () OVER (PARTITION BY TC.TABLE_NAME ORDER BY TC.COLUMN_ID DESC) DRN
                       ,  TC.COLUMN_NAME
                       ,  CASE WHEN T.CN LIKE '%,' || TC.COLUMN_NAME || ',%' THEN '+' || TO_CHAR (PREV_IMAX) ELSE '' END DIFF
                    FROM USER_TAB_COLUMNS TC
                   WHERE TC.TABLE_NAME = T.TTBL
                ORDER BY TC.TABLE_NAME, TC.COLUMN_ID)
      LOOP
         COL_LIST := COL_LIST || CASE WHEN C.RN != 1 THEN ',' END || '"' || C.COLUMN_NAME || '"';
         VAL_LIST :=
               VAL_LIST
            || CASE WHEN C.RN != 1 THEN ',' END
            || CASE WHEN C.COLUMN_NAME = 'BANK_CODE' THEN '''&&PART.''' ELSE '"' || C.COLUMN_NAME || '"' || C.DIFF END;
      END LOOP;

      CDML := CDML || COL_LIST || ') SELECT ' || VAL_LIST || ' FROM ' || T.TTBL || ' WHERE BANK_CODE=''99'';' || 'COMMIT;';

      DECLARE
         V#JOB_NAME                        VARCHAR2 (65 CHAR);
         EXN#JOB_DOESNOT_EXISTS   CONSTANT BINARY_INTEGER := -27475;
         EXC#JOB_DOESNOT_EXISTS            EXCEPTION;
         PRAGMA EXCEPTION_INIT (EXC#JOB_DOESNOT_EXISTS, -27475);
      BEGIN
         V#JOB_NAME := 'J$' || T.TTBL;

         BEGIN
            DBMS_SCHEDULER.DROP_JOB (job_name => V#JOB_NAME, FORCE => TRUE);
         EXCEPTION
            WHEN EXC#JOB_DOESNOT_EXISTS
            THEN
               NULL;
         END;

         DBMS_SCHEDULER.CREATE_JOB (
            job_name     => V#JOB_NAME
          ,  start_date   => SYSTIMESTAMP + INTERVAL '3' MINUTE
          ,  end_date     => TRUNC (SYSTIMESTAMP, 'DD') + INTERVAL '1' DAY + INTERVAL '7' HOUR
          ,  job_class    => 'DEFAULT_JOB_CLASS'
          ,  job_type     => 'PLSQL_BLOCK'
          ,  job_action   => CDML
          ,  comments     =>    'ЗАО Ай-Теко, 2013'
                             || CHR (10)
                             || 'Самоудаляемое задание для наполнения промежуточных таблиц для задач нагрузочного тестирования.'
          ,  auto_drop    => FALSE
          ,  enabled      => TRUE);
         DBMS_SCHEDULER.DISABLE (name => V#JOB_NAME);
         DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'RESTARTABLE', VALUE => FALSE);
         DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'JOB_PRIORITY', VALUE => 3);
         DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'MAX_RUNS', VALUE => 1);     
      END;
   END LOOP;
END;
/


--------------
@@utl_foot.sql

EXIT