DEFINE part='&1'
----------------------------------------------
DEFINE SCRIPT_STEP='1'
DEFINE SCRIPT_NAME='merge-subpartitions'
DEFINE SCRIPT_FULL_NAME='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
DEFINE SCRIPT_DESC='Скрипт осуществляет слияние всех подсекций по секции тербанка &&PART. в одну секцию по умолчанию.'

@@utl_head.sql

Prompt Слияние субпартиций в одну по умолчанию
BEGIN
   FOR r IN (  SELECT *
                 FROM (SELECT t.ttbl
                            ,  tp.partition_name
                            ,  TSP.SUBPARTITION_NAME
                            ,  TSP.SUBPARTITION_POSITION
                            ,     'ALTER TABLE '
                               || t.ttbl
                               || ' MERGE SUBPARTITIONS '
                               || LAG (TSP.SUBPARTITION_NAME) OVER (PARTITION BY t.ttbl ORDER BY TP.PARTITION_POSITION, TSP.SUBPARTITION_position)
                               || ','
                               || TSP.SUBPARTITION_NAME
                               || ' INTO SUBPARTITION '
                               || TSP.SUBPARTITION_NAME
                                  csql
                         FROM it$dup_tables t
                              INNER JOIN user_tab_partitions tp
                                 ON TP.TABLE_NAME = t.ttbl
                              INNER JOIN user_tab_subpartitions tsp
                                 ON TSP.PARTITION_NAME = TP.PARTITION_NAME AND TSP.TABLE_NAME = TP.TABLE_NAME
                        WHERE TP.PARTITION_NAME = 'TB&&part.') q
                WHERE subpartition_position > 1
             ORDER BY ttbl, SUBPARTITION_position)
   LOOP
      DBMS_OUTPUT.put_line (r.csql || ';');

      EXECUTE IMMEDIATE r.csql;
   END LOOP;
END;
/

@@utl_foot.sql

EXIT