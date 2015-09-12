DEFINE part='&1'
----------------------------------------------
DEFINE SCRIPT_STEP='4'
DEFINE SCRIPT_NAME='exchange-subpartitions'
DEFINE SCRIPT_FULL_NAME='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
DEFINE SCRIPT_DESC='Скрипт осуществляет обмен партиций по таблицам тербанка &&PART. с таблицей, содержащей сгенерированные искусственные данные.'

@@utl_head.sql

DECLARE
   EXN#CHECK_FAILED   NUMBER := -14281;
   EXC#CHECK_FAILED   EXCEPTION;
   PRAGMA EXCEPTION_INIT (EXC#CHECK_FAILED, -14281);
BEGIN
   FOR r IN (  SELECT t.ttbl
                    ,  tp.partition_name
                    ,  TSP.SUBPARTITION_NAME
                    ,  TSP.SUBPARTITION_POSITION
                    ,  'ALTER TABLE ' || t.ttbl || ' EXCHANGE SUBPARTITION ' || TSP.SUBPARTITION_NAME || ' WITH TABLE ' || T.PTBL csql
                 FROM it$dup_tables t
                      INNER JOIN user_tab_partitions tp
                         ON TP.TABLE_NAME = t.ttbl
                      INNER JOIN user_tab_subpartitions tsp
                         ON TSP.PARTITION_NAME = TP.PARTITION_NAME AND TSP.TABLE_NAME = TP.TABLE_NAME
                WHERE TP.PARTITION_NAME = 'TB&&part.' AND T.TTBL != 'INC_OPER_CLOB'
             ORDER BY ttbl)
   LOOP
      DBMS_OUTPUT.put_line (r.csql || ';');

      BEGIN
         EXECUTE IMMEDIATE r.csql;
      EXCEPTION
         WHEN EXC#CHECK_FAILED
         THEN
            NULL;
      END;
   END LOOP;
END;
/

@@utl_foot.sql

EXIT