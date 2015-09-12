PROMPT Включение ограничений целостности на таблицах
----------------------------------------------
DEFINE SCRIPT_STEP='6'
DEFINE SCRIPT_NAME='validate-constraints'
DEFINE SCRIPT_FULL_NAME='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
DEFINE SCRIPT_DESC='Скрипт включает отключенные ограничения целостности в БД. Для этого генерируются одноразовые джобы'

@@utl_head.sql

PROMPT Этап 1. Все констрайнты переводим в enabled
DECLARE
   SHOULD_REPEAT             BOOLEAN := FALSE;
   TRY_COUNT                 NUMBER := 0;
   EXN#DEP_EXISTS            NUMBER := -2297;
   EXC#DEP_EXISTS            EXCEPTION;
   PRAGMA EXCEPTION_INIT (EXC#DEP_EXISTS, -2297);

   EXN#NO_KEY_EXISTS         NUMBER := -2270;
   EXC#NO_KEY_EXISTS         EXCEPTION;
   PRAGMA EXCEPTION_INIT (EXC#NO_KEY_EXISTS, -2270);

   EXN#PKEY_DOESNOT_EXISTS   NUMBER := -2298;
   EXC#PKEY_DOESNOT_EXISTS   EXCEPTION;
   PRAGMA EXCEPTION_INIT (EXC#PKEY_DOESNOT_EXISTS, -2298);
BEGIN
   WHILE (SHOULD_REPEAT = TRUE OR TRY_COUNT < 3)
   LOOP
      SHOULD_REPEAT := FALSE;

      FOR C
         IN (  SELECT T.TTBL,
                      STATUS,
                      CONSN,
                      CONSTRAINT_NAME,
                      CONSTRAINT_TYPE,
                         'ALTER TABLE '
                      || T.TTBL
                      || ' MODIFY CONSTRAINT '
                      || CONSTRAINT_NAME
                      || ' ENABLE '
                      || 'NOVALIDATE' 
                         ALTER_DDL
                 FROM    (    SELECT C.TABLE_NAME,
                                     LPAD (' ', 8 * (LEVEL - 1)) || C.CONSTRAINT_NAME CONSN,
                                     CONSTRAINT_NAME,
                                     C.STATUS,
                                     C.CONSTRAINT_TYPE,
                                     GENERATED,
                                     LEVEL LEV,
                                     ROWNUM RN
                                FROM user_constraints c
                          START WITH C.R_CONSTRAINT_NAME IS NULL
                          CONNECT BY PRIOR C.CONSTRAINT_NAME = C.R_CONSTRAINT_NAME) Q
                      INNER JOIN
                         IT$DUP_TABLES T
                      ON TABLE_NAME = T.TTBL
                WHERE STATUS = 'DISABLED'
             ORDER BY RN)
      LOOP
         BEGIN
            DDL_PKG.ALTER_TABLE (C.ALTER_DDL);
         EXCEPTION
            WHEN EXC#DEP_EXISTS OR EXC#NO_KEY_EXISTS OR EXC#PKEY_DOESNOT_EXISTS
            THEN
               SHOULD_REPEAT := TRUE;
         END;
      END LOOP;

      TRY_COUNT := TRY_COUNT + 1;
   END LOOP;
END;
/

prompt Шаг 2. Проводим валидацию ссылочных констрейнтов. С использованием джобов
declare
sql_Stmnt varchar2(4000);
v_altr_stmnt varchar2(2000);
BEGIN

      for  TT in (select * from IT$DUP_TABLES T)
      loop
          FOR C
             IN (  SELECT q.TABLE_NAME,
                          STATUS,
                          CONSN,
                          CONSTRAINT_NAME,
                          CONSTRAINT_TYPE,
                            'ALTER TABLE '
                          || q.TABLE_NAME
                          || ' MODIFY CONSTRAINT '
                          || CONSTRAINT_NAME
                          || ' VALIDATE ' 
                           ALTER_DDL
                     FROM    (    SELECT C.TABLE_NAME,
                                         LPAD (' ', 8 * (LEVEL - 1)) || C.CONSTRAINT_NAME CONSN,
                                         CONSTRAINT_NAME,
                                         C.STATUS,
                                         C.CONSTRAINT_TYPE,
                                         GENERATED,
                                         LEVEL LEV,
                                         VALIDATED,
                                         ROWNUM RN
                                    FROM user_constraints c
                              START WITH C.R_CONSTRAINT_NAME IS NULL
                              CONNECT BY PRIOR C.CONSTRAINT_NAME = C.R_CONSTRAINT_NAME) Q
                    WHERE STATUS = 'ENABLED' and  VALIDATED = 'NOT VALIDATED' and  CONSTRAINT_TYPE !='C'
                      AND q.TABLE_NAME = TT.TTBL
                 ORDER BY RN)
          LOOP
              v_altr_stmnt := v_altr_stmnt ||
	          'DDL_PKG.ALTER_TABLE ('''  || c.alter_ddl || ''');';
          end loop;
             
            sql_Stmnt := 
             'DECLARE  '
          || '   SHOULD_REPEAT             BOOLEAN := FALSE; '
          || '   TRY_COUNT                 NUMBER := 0;      ' 
          || '   EXN#DEP_EXISTS            NUMBER := -2297;  '
          || '   EXC#DEP_EXISTS            EXCEPTION;        '
          || '   PRAGMA EXCEPTION_INIT (EXC#DEP_EXISTS, -2297);     '
          || '   EXN#NO_KEY_EXISTS         NUMBER := -2270;         '
          || '   EXC#NO_KEY_EXISTS         EXCEPTION;               '
          || '   PRAGMA EXCEPTION_INIT (EXC#NO_KEY_EXISTS, -2270);  '
          || '   EXN#PKEY_DOESNOT_EXISTS   NUMBER := -2298;         '
          || '   EXC#PKEY_DOESNOT_EXISTS   EXCEPTION;               '
          || '   PRAGMA EXCEPTION_INIT (EXC#PKEY_DOESNOT_EXISTS, -2298); '
          || 'BEGIN                                                '    
          || '   WHILE (SHOULD_REPEAT = TRUE OR TRY_COUNT < 3)      '
          || '   LOOP                                               '       
          || '       SHOULD_REPEAT := FALSE;                        '        
          || '	     BEGIN                                          '
          ||  v_altr_stmnt 
          || '       EXCEPTION                                      '
          || '            WHEN EXC#DEP_EXISTS OR EXC#NO_KEY_EXISTS OR EXC#PKEY_DOESNOT_EXISTS '
          || '            THEN                                      '
          || '               SHOULD_REPEAT := TRUE;                 '
          || '       END;                                           '
          || '       TRY_COUNT := TRY_COUNT + 1;                    '
          || '   END LOOP;                                           '
          || 'END;                                                  '
          ;                                       

              DECLARE
                 V#JOB_NAME                        VARCHAR2 (65 CHAR);
                 EXN#JOB_DOESNOT_EXISTS   CONSTANT BINARY_INTEGER := -27475;
                 EXC#JOB_DOESNOT_EXISTS            EXCEPTION;
                 PRAGMA EXCEPTION_INIT (EXC#JOB_DOESNOT_EXISTS, -27475);
              BEGIN
                 V#JOB_NAME := 'VC$' || TT.TTBL;

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
                  ,  job_action   => sql_Stmnt
                  ,  comments     =>    'ЗАО Ай-Теко, 2013'
                                     || CHR (10)
                                     || 'Самоудаляемое задание для распараллеливания валидации ограничений целостности'
                  ,  auto_drop    => FALSE
                  ,  enabled      => TRUE);
                 DBMS_SCHEDULER.DISABLE (name => V#JOB_NAME);
                 DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'RESTARTABLE', VALUE => FALSE);
                 DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'JOB_PRIORITY', VALUE => 3);
                 DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'MAX_RUNS', VALUE => 1);     
              END;
          v_altr_stmnt :='';   
      END LOOP;
END;
/


@@utl_foot.sql

EXIT
