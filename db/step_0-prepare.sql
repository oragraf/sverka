REM TS - табличное пространство, в котором будут создаваться таблицы для миграции
DEFINE TS='&1'
----------------------------------------------
DEFINE SCRIPT_STEP='0'
DEFINE SCRIPT_NAME='prepare'
DEFINE SCRIPT_FULL_NAME='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
DEFINE SCRIPT_DESC='Скрипт осуществляет удаление ограничений целостности в БД, создание промежуточных объектов, необходимых для работы. Запускается один раз.'

@@utl_head.sql

PROMPT Создание списка таблиц для размножения данных

CREATE OR REPLACE VIEW it$dup_TABLES
AS
   SELECT tn ttbl, REGEXP_REPLACE (tn || CASE WHEN INSTR (tn, '_') > 0 THEN '' ELSE '_' END, '\_', '$', 1, 1) ptbl, cn
     FROM (SELECT 'JRN_SESSION_INFO' tn, ',SESSION_ID,JOURNAL_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'JRN_OPER_REQ_RESP' tn, ',REQUEST_ID,SESSION_ID,PAYMENT_PARENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'JRN_OPER_EVENT' tn, ',EVENT_ID,REQUEST_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'JRN_DTL_EVENT' tn, ',EVENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'JRN_DTL_EVENT_PROCESSING' tn, ',EVENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'JRN_EVENT_ERROR' tn, ',ERROR_ID,EVENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'JRN_NOTE_INFO' tn, ',NOTE_ID,EVENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'JRN_OPER_INFO' tn, ',REQUEST_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'JOURNAL_INFO' tn, ',JOURNAL_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'DISPUTED_REQUEST' tn, ',ID,INCIDENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INC_DOC_PRINT' tn, ',ID,INCIDENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INC_OPER_INFO' tn, ',INCIDENT_ID,INC_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INC_OPER_DTL_INFO' tn, ',DTL_INFO_ID,INCIDENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INC_NOTE_INFO' tn, ',NOTE_ID,DTL_INFO_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INC_OPER_CLOB' tn, ',DTL_INFO_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INC_PAYMENT' tn, ',ID,INCIDENT_ID,RBC_ID,SP_ID,DEBATE_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INC_SYS_STATUS' tn, ',DTL_INFO_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INC_SYSTEM_STATUS' tn, ',SYS_STATUS_ID,INCIDENT_ID,' cn FROM DUAL
           UNION ALL
           SELECT 'INCIDENT' tn, ',INCIDENT_ID,ACTIVE_OPER_INFO_ID,' cn FROM DUAL);

PROMPT Создание $-таблиц для сгенерированных искусственных данных

DECLARE
   exn#resource_busy   NUMBER := -54;
   exc#resource_busy   EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#resource_busy, -54);
   exn#no_table        NUMBER := -942;
   exc#no_table        EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#no_table, -942);
BEGIN
   BEGIN
      FOR t IN (SELECT * FROM it$dup_tables)
      LOOP
         BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || T.PTBL || ' REUSE STORAGE';
         EXCEPTION
            WHEN exc#resource_busy
            THEN
               DBMS_OUTPUT.PUT_LINE (
                     'Таблица '
                  || t.ptbl
                  || ' заблокирована и не может быть очищена. Устраните блокировку и запустите скрипт заново.');
            WHEN exc#no_table
            THEN
               EXECUTE IMMEDIATE 'CREATE TABLE ' || T.PTBL || ' TABLESPACE &&TS. AS SELECT * FROM ' || T.TTBL || ' WHERE 1=0';
         END;
      END LOOP;
   END;

   NULL;
END;
/

PROMPT Отключение ограничений целостности на таблицах

DECLARE
   SHOULD_REPEAT    BOOLEAN := FALSE;
   TRY_COUNT        NUMBER := 0;
   EXN#DEP_EXISTS   NUMBER := -2297;
   EXC#DEP_EXISTS   EXCEPTION;
   PRAGMA EXCEPTION_INIT (EXC#DEP_EXISTS, -2297);
BEGIN
   WHILE (SHOULD_REPEAT = TRUE OR TRY_COUNT < 10)
   LOOP
      SHOULD_REPEAT := FALSE;

      FOR C
         IN (SELECT 'ALTER TABLE ' || C.TABLE_NAME || ' MODIFY CONSTRAINT ' || C.CONSTRAINT_NAME || ' DISABLE' ALTER_DDL
               FROM user_constraints c, IT$DUP_TABLES T
              WHERE     STATUS = 'ENABLED'
                    AND (C.CONSTRAINT_TYPE IN ('P', 'R') OR (C.CONSTRAINT_TYPE = 'C' AND C.GENERATED = 'USER NAME'))
                    AND C.TABLE_NAME = T.TTBL)
      LOOP
         BEGIN
            DDL_PKG.ALTER_TABLE(C.ALTER_DDL);
         EXCEPTION
            WHEN EXC#DEP_EXISTS
            THEN
               SHOULD_REPEAT := TRUE;
         END;
      END LOOP;

      TRY_COUNT := TRY_COUNT + 1;
   END LOOP;
END;
/


@@utl_foot.sql
UNDEFINE ts

EXIT