----------------------------------------------
DEFINE SCRIPT_STEP='5'
DEFINE SCRIPT_NAME='rebuild-indexes'
DEFINE SCRIPT_FULL_NAME='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
DEFINE SCRIPT_DESC='Скрипт включает отключенные ограничения целостности в БД, удаляет промежуточные объекты, перекидывает сиквенсы. Запускается один раз.'

@@utl_head.sql

CREATE OR REPLACE PACKAGE PKG_MANAGE_PARTITIONS
IS
   /*****************************************************************************************************\
   * Пакет для управления разделами партиционированных таблиц
   * @author  : A.SERGEEV
   * @author  : A.Glinsky
   ******************************************************************************************************
   * ВАЖНО! Для корректной работы формат наименования партиции должен быть
   * TBXX -
   * TB - префикс названия партиции. Так и есть TB
   * XX - числовой код тербанка, либо для партиции по умолчанию XX
   *
   * Формат наименования субпартиции должен быть
   * TBXX_YYYYMMDD
   * TB - префикс названия из родительской партиции.
   * XX - код тербанка родительской партиции, либо XX для партиции по умолчанию
   * YYYYMMDD верхняя несобственная граница интервала субпартиции - дата в формате YYYYMMDD
   *
   */
   PACKAGE_VERSION                  CONSTANT VARCHAR2 (100) := '2.01';
   PACKAGE_NAME                     CONSTANT VARCHAR2 (30 CHAR) := 'PKG_MANAGE_PARTITIONS';
   /* Процедура генерации-отцепления партиций будет запускаться еженедельно.
      Исходя из этого, количество нарезаемых партиций будет 7 + 1 */
   NEW_PARTITIONS_COUNT             CONSTANT BINARY_INTEGER := 3;
   /* ПАРТИЦИИ С ДАННЫМИ СТАРШЕ ОТ ТЕКУЩЕЙ ДАТЫ БУДУТ УДАЛЕНЫ */
   KEEP_DAYS_AGO                    CONSTANT BINARY_INTEGER := 300;
   /*
    Исключения при добавлении/удалении/расщеплении/слиянии партиций
    */
   /* Попытка вставки данных в несуществующую партицию */
   exn#ins_into_non_existent_part   CONSTANT BINARY_INTEGER := -14400;
   exc#ins_into_non_existent_part            EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#ins_into_non_existent_part, -14400);
   /* Попытка добавления уже существующей партиции */
   exn#duplicate_partition_name     CONSTANT BINARY_INTEGER := -14013;
   exc#duplicate_partition_name              EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#duplicate_partition_name, -14013);
   /* Попытка добавления партиции с таким же ключом но другим именем */
   exn#part_key_already_exists      CONSTANT BINARY_INTEGER := -14312;
   exc#part_key_already_exists               EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#part_key_already_exists, -14312);
   /* Попытка добавления субпартиции с границей меньшей, чем у другой существующей */
   exn#subpart_key_already_exists   CONSTANT BINARY_INTEGER := -14211;
   exc#subpart_key_already_exists            EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#subpart_key_already_exists, -14211);
   /* Попытка добавления партиции при существующей DEFAULT */
   exn#part_default_exists          CONSTANT BINARY_INTEGER := -14323;
   exc#part_default_exists                   EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#part_default_exists, -14323);
   exn#invalid_value                CONSTANT BINARY_INTEGER := -904;
   exc#invalid_value                         EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#invalid_value, -904);
   exn#name_already_exists          CONSTANT BINARY_INTEGER := -955;
   exc#name_already_exists                   EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#name_already_exists, -955);
   exn#unique_key_exists            CONSTANT BINARY_INTEGER := -2266;
   exc#unique_key_exists                     EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#unique_key_exists, -2266);
   exn#enforcing_index              CONSTANT BINARY_INTEGER := -2429;
   exc#enforcing_index                       EXCEPTION;
   PRAGMA EXCEPTION_INIT (exc#enforcing_index, -2429);


   /* Часть имени субпартиции по умолчанию */
   DEFAULT_SUBPART_NAME             CONSTANT VARCHAR2 (8 CHAR) := '99991231';

   PROCEDURE TABLE_MAINTENANCE (p_table_name       IN VARCHAR2,
                                p_tabspace         IN VARCHAR2,
                                p_part_count       IN BINARY_INTEGER,
                                p_drop_range       IN TIMESTAMP := SYSTIMESTAMP - NUMTODSINTERVAL (KEEP_DAYS_AGO, 'DAY'),
                                p_keep_exchanged   IN BOOLEAN := FALSE);

   PROCEDURE TABLES_MAINTENANCE (p_tabspace IN VARCHAR2, p_keep_exchanged IN BOOLEAN := FALSE);

   /* Процедура генерации задач для добавления новых, на будущую неделю, партиций и отцепления старых */
   PROCEDURE GEN_JOBS_SUBPART_GENERATOR (P_TABSPACE IN VARCHAR2);

   PROCEDURE gen_future_subpartition (p_table_name IN VARCHAR2, p_part_count IN BINARY_INTEGER := 3, P_TABSPACE IN VARCHAR2);

   PROCEDURE exchange_subpartitions (p_table_name       IN VARCHAR2,
                                     P_TABSPACE         IN VARCHAR2,
                                     p_drop_range       IN TIMESTAMP := SYSTIMESTAMP - NUMTODSINTERVAL (KEEP_DAYS_AGO, 'DAY'),
                                     p_keep_exchanged   IN BOOLEAN := FALSE);

   PROCEDURE rebuild_indexes (p_table_name IN VARCHAR2, P_TABSPACE IN VARCHAR2);

   PROCEDURE rebuild_indexes (p_table_name IN VARCHAR2, P_TABSPACE IN VARCHAR2, call_old_version IN OUT BOOLEAN);

   /**
   * Создает партиции по тербанкам.
   * В качестве даты первой субпартиции берет дату, указанную в параметре p_first_date
   * Список таблиц жестко закодирован.
   *
   * @param p_first_date date default null - дата первой партиции для новых партиций
   * @param p_tablespace varchar2          - tablespace в котором будут создаваться партиции
   */
   PROCEDURE prc_create_partitions_by_tb (p_first_date DATE, p_tablespace VARCHAR2);

   /**
   * Создает субпартиции по дням для всех List-range секционированных таблиц.
   * С периодом в 1 день до указанной даты, начиная с даты последней существующей субпартиции
   * в каждой партиции. Список таблиц жестко закодирован.
   *
   * @param p_date_end date -дата до которой нужно создавать партиции
   * @param p_tablespace varchar2          - tablespace в котором будут создаваться субпартиции
   */
   PROCEDURE prc_create_subpart_to_date (p_date_end DATE, p_tablespace VARCHAR2);

   /**
   * Создает партиции для таблиц X_%. Список жестко закодирован.
   * Tablespace используется тот же, что и у первой партиции
   *
   */
   PROCEDURE create_x_partitions;

   PROCEDURE test_p;

   PROCEDURE test_gen;

   PROCEDURE test_TABLE_MAINTENANCE;
END PKG_MANAGE_PARTITIONS;
/

DROP PACKAGE BODY PKG_MANAGE_PARTITIONS;

CREATE OR REPLACE PACKAGE BODY PKG_MANAGE_PARTITIONS
IS
   g_x_tables   DBMS_UTILITY.name_array;

   TYPE REC#SUBPART IS RECORD
   (
      TABLE_NAME              VARCHAR2 (65 CHAR)                                                                       -- имя таблицы, в которой проводим работы
                                                ,
      BANK_CODE               VARCHAR2 (2 CHAR)                                                               -- Код тербанка. Будет включен в название партиции
                                               ,
      PART_VALUES_LIST        VARCHAR2 (100 CHAR)                                                            --  Список кодов тербанков для добавляемой партиции
                                                 ,
      SUBPART_VALUES_RANGE    TIMESTAMP                                                                                       -- Верхняя граница для субпартиции
                                       ,
      TABSPACE                VARCHAR2 (30 CHAR)                                                                       -- ТАБЛИЧНОЕ ПРОСТРАНСТВО ДЛЯ СУБПАРТИЦИЙ
                                                ,
      PART_NAME$              VARCHAR2 (30 CHAR),
      SUBPART_NAME$           VARCHAR2 (30 CHAR),
      DEFAULT_SUBPART_NAME$   VARCHAR2 (30 CHAR),
      PART_VALUES_LIST$       VARCHAR2 (100 CHAR),
      SUBPART_VALUES_RANGE$   VARCHAR2 (100 CHAR),
      add_part$               VARCHAR2 (2000 CHAR),
      add_subpart$            VARCHAR2 (2000 CHAR),
      split_subpart$          VARCHAR2 (2000 CHAR),
      TABSPACE_OPTION$        VARCHAR2 (100 CHAR),
      part_name_to_split$     VARCHAR2 (30 CHAR)
   );

   CURSOR get_tables_bank_codes
   IS
        SELECT t.table_name, BC.BC
          FROM it#mig_tables t, it#bank_codes bc
         WHERE t.create_partitions = 'Y'
      ORDER BY 1, 2;

   FUNCTION GET_SUBPART_NAME (P_BANK_CODE IN VARCHAR2, P_SUBP_RANGE_VALUE IN TIMESTAMP)
      RETURN VARCHAR2
   AS
   BEGIN
      RETURN 'TB' || SUBSTR (NVL (P_BANK_CODE, 'XX'), 1, 2) || '_' || TO_CHAR (NVL (P_SUBP_RANGE_VALUE, TIMESTAMP '9999-12-31 23:59:59'), 'YYYYMMDD');
   END;

   PROCEDURE GET_SUBPART_TO_SPLIT (P IN OUT NOCOPY REC#SUBPART)
   AS
      v#high_val        TIMESTAMP;
      v#prev_high_val   TIMESTAMP := TO_TIMESTAMP ('0001-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
      v#STR             VARCHAR2 (32767);
   BEGIN
      FOR r IN (  SELECT TP.TABLE_NAME,
                         TP.PARTITION_NAME,
                         TP.PARTITION_POSITION,
                         TS.SUBPARTITION_NAME,
                         TS.SUBPARTITION_POSITION,
                         TS.HIGH_VALUE,
                         TS.HIGH_VALUE_LENGTH
                    FROM user_tab_subpartitions ts INNER JOIN user_tab_partitions tp ON TP.TABLE_NAME = TS.TABLE_NAME AND TP.PARTITION_NAME = TS.PARTITION_NAME
                   WHERE tp.table_name = P.TABLE_NAME AND TP.PARTITION_NAME = P.PART_NAME$
                ORDER BY TP.PARTITION_POSITION, TS.SUBPARTITION_POSITION)
      LOOP
         v#STR := SUBSTRC (R.HIGH_VALUE, 1, R.HIGH_VALUE_LENGTH);

         BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || v#STR || ' FROM DUAL' INTO v#high_val;
         EXCEPTION
            WHEN EXC#INVALID_VALUE
            THEN
               v#high_val := TIMESTAMP '9999-12-31 23:59:59';                                                                                        -- MAXVALUE
         END;

         IF p.SUBPART_VALUES_RANGE > v#prev_high_val AND p.SUBPART_VALUES_RANGE < v#high_val
         THEN
            p.PART_NAME_TO_SPLIT$ := R.SUBPARTITION_NAME;
            EXIT;
         ELSE
            v#PREV_high_val := v#high_val;
         END IF;
      END LOOP;

      NULL;                                                                                                                                 -- контрольная точка
   END;

   /**
     Процедура добавления|модификации/сплита одной партиции с субпартицией
    */
   PROCEDURE add_modify_split_partition (P IN OUT NOCOPY REC#SUBPART)
   AS
      IS_COMPLETED   BOOLEAN := FALSE;
   BEGIN
      p.PART_NAME$ := 'TB' || NVL (SUBSTR (TRIM (P.BANK_CODE), 1, 2), 'XX');
      p.PART_VALUES_LIST$ := CASE WHEN P.PART_VALUES_LIST IS NULL THEN 'DEFAULT' ELSE '''' || P.PART_VALUES_LIST || '''' END;

      IF P.SUBPART_VALUES_RANGE IS NULL
      THEN
         p.SUBPART_NAME$ := DEFAULT_SUBPART_NAME;
         p.SUBPART_VALUES_RANGE$ := 'MAXVALUE';
      ELSE
         p.SUBPART_NAME$ := TO_CHAR (P.SUBPART_VALUES_RANGE, 'YYYYMMDD');
         p.SUBPART_VALUES_RANGE$ := 'TO_TIMESTAMP(''' || TO_CHAR (P.SUBPART_VALUES_RANGE, 'YYYYMMDD') || ''',''YYYYMMDD'')';
      END IF;

      p.SUBPART_NAME$ := p.PART_NAME$ || '_' || p.SUBPART_NAME$;
      p.TABSPACE_OPTION$ := CASE WHEN P.TABSPACE IS NOT NULL THEN ' TABLESPACE ' || P.TABSPACE END;
      p.add_part$ :=
            ('ALTER TABLE ' || P.TABLE_NAME || ' ADD PARTITION ' || p.PART_NAME$)
         || (' VALUES(' || p.PART_VALUES_LIST$ || ')' || p.TABSPACE_OPTION$ || ' NOLOGGING NOCOMPRESS ')
         || (' (SUBPARTITION ' || p.SUBPART_NAME$ || ' VALUES LESS THAN(' || p.SUBPART_VALUES_RANGE$ || ') ' || p.TABSPACE_OPTION$ || ')');
      p.add_subpart$ :=
         ('ALTER TABLE ' || P.TABLE_NAME || ' MODIFY PARTITION ' || p.PART_NAME$)
         || (' ADD SUBPARTITION ' || p.SUBPART_NAME$ || ' VALUES LESS THAN(' || p.SUBPART_VALUES_RANGE$ || ') ' || p.TABSPACE_OPTION$ || '');
      p.part_name_to_split$ := NULL;
      GET_SUBPART_TO_SPLIT (p => p);

      IF p.PART_NAME_TO_SPLIT$ IS NOT NULL                          -- явный сплит субпартиции------------------------------------------------------------------
      THEN
         p.split_subpart$ :=
               ('ALTER TABLE ' || P.TABLE_NAME)
            || (' SPLIT SUBPARTITION ' || p.PART_NAME_TO_SPLIT$ || ' AT (' || p.SUBPART_VALUES_RANGE$)
            || ') INTO ('
            || (' SUBPARTITION ' || p.SUBPART_NAME$)
            || (',SUBPARTITION ' || p.PART_NAME_TO_SPLIT$)
            || ') PARALLEL';

         BEGIN
            /* расщепляем субпартицию и выходим, если все благополучно */
            DDL_PKG.ALTER_TABLE (p.split_subpart$);
            RETURN;
         EXCEPTION
            WHEN OTHERS
            THEN
               ddl_pkg.error (SQLERRM);
               RAISE;
         END;
      ELSE
         /* пробуем добавлять партицию/субпартицию */
         BEGIN
            /* добавляем партицию */
            DDL_PKG.ALTER_TABLE (p.add_part$);
            RETURN;
         EXCEPTION
            WHEN exc#duplicate_partition_name OR exc#part_key_already_exists
            THEN
               BEGIN
                  ddl_pkg.error (SQLERRM);
                  /* такая партиция уже есть, пробуем ее модифицировать, добавив субпартицию */
                  DDL_PKG.ALTER_TABLE (p.add_subpart$);
                  RETURN;
               EXCEPTION
                  WHEN exc#duplicate_partition_name OR exc#subpart_key_already_exists
                  THEN
                     RETURN;
                  WHEN OTHERS
                  THEN
                     ddl_pkg.error (SQLERRM);
                     RAISE;
               END;
            WHEN OTHERS
            THEN
               ddl_pkg.error (SQLERRM);
               RAISE;
         END;
      END IF;
   END;

   PROCEDURE add_modify_split_partition (P_TABLE_NAME             IN VARCHAR2                                          -- имя таблицы, в которой проводим работы
                                                                             ,
                                         P_BANK_CODE              IN VARCHAR2                                 -- Код тербанка. Будет включен в название партиции
                                                                             ,
                                         P_PART_VALUES_LIST       IN VARCHAR2                                --  Список кодов тербанков для добавляемой партиции
                                                                             ,
                                         P_SUBPART_VALUES_RANGE   IN TIMESTAMP                                                -- Верхняя граница для субпартиции
                                                                              ,
                                         P_TABSPACE               IN VARCHAR2)
   AS
      P   REC#SUBPART;
   BEGIN
      p.TABLE_NAME := P_TABLE_NAME;
      p.BANK_CODE := P_BANK_CODE;
      p.PART_VALUES_LIST := P_PART_VALUES_LIST;
      p.SUBPART_VALUES_RANGE := P_SUBPART_VALUES_RANGE;
      p.TABSPACE := P_TABSPACE;
      add_modify_split_partition (p => p);
   END;

   FUNCTION create_table_like (p_table_name IN VARCHAR2, P_TABSPACE IN VARCHAR2)
      RETURN VARCHAR2
   AS
      v#ddl               VARCHAR2 (2000 CHAR);
      v#new_table_name    VARCHAR2 (65 CHAR);
      v#old_table_name    VARCHAR2 (65 CHAR);
      v#TABSPACE_OPTION   VARCHAR2 (100 CHAR);
   BEGIN
      v#new_table_name := 'IT#' || TO_CHAR (SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF3');
      v#old_table_name := p_table_name;
      v#TABSPACE_OPTION := CASE WHEN P_TABSPACE IS NOT NULL THEN ' TABLESPACE ' || P_TABSPACE END;
      v#ddl := 'CREATE TABLE ' || v#new_table_name || v#TABSPACE_OPTION || ' AS SELECT * FROM ' || v#old_table_name || ' WHERE 1=0';

      EXECUTE IMMEDIATE v#ddl;

      RETURN v#new_table_name;
   END;

   /* процедура обменивает партицию с пустой таблицей */
   PROCEDURE exchange_subpartition (p_table_name       IN VARCHAR2,
                                    p_subpart_name     IN VARCHAR2,
                                    P_TABSPACE         IN VARCHAR2,
                                    p_keep_exchanged   IN BOOLEAN := FALSE)
   AS
      v#ddl              VARCHAR2 (2000 CHAR);
      V#exchange_table   VARCHAR2 (65 CHAR);
      v#table_name       VARCHAR2 (65 CHAR);
      v#new_table_name   VARCHAR2 (65 CHAR);
   BEGIN
      v#table_name := p_table_name;

      IF p_keep_exchanged
      THEN
         V#exchange_table := create_table_like (p_table_name, P_TABSPACE);
         v#ddl :=
            ('ALTER TABLE ' || v#table_name || ' EXCHANGE SUBPARTITION ' || p_subpart_name) || (' WITH TABLE ' || V#exchange_table || ' WITHOUT VALIDATION');
         DDL_PKG.ALTER_TABLE (v#ddl);
         DDL_PKG.COMMENT_TAB (
            V#exchange_table,
            ('Отцепленная партиция ' || v#table_name || '.' || p_subpart_name)
            || ('. Отцеплено ' || TO_CHAR (SYSDATE, 'yyyy-mm-dd hh24:mi:ss')));

         BEGIN
            v#new_table_name := SUBSTR (P_TABLE_NAME, 1, 30 - LENGTH (p_subpart_name) - 1) || '#' || p_subpart_name;
            DDL_PKG.ALTER_TABLE ('ALTER TABLE ' || V#exchange_table || ' RENAME TO ' || v#new_table_name);
         EXCEPTION
            WHEN exc#name_already_exists
            THEN
               DDL_PKG.DROP_TABLE (V#exchange_table);
         END;
      END IF;

      v#ddl := 'ALTER TABLE ' || v#table_name || ' DROP SUBPARTITION ' || p_subpart_name                                       /*|| ' UPDATE INDEXES PARALLEL'*/
                                                                                        ;
      DDL_PKG.ALTER_TABLE (v#ddl);
   END;

   PROCEDURE exchange_subpartitions (p_table_name       IN VARCHAR2,
                                     P_TABSPACE         IN VARCHAR2,
                                     p_drop_range       IN TIMESTAMP := SYSTIMESTAMP - NUMTODSINTERVAL (KEEP_DAYS_AGO, 'DAY'),
                                     p_keep_exchanged   IN BOOLEAN := FALSE)
   AS
      v#high_val             TIMESTAMP;
      V#drop_range           TIMESTAMP;
      v#ddl                  VARCHAR2 (2000 CHAR);
      V#exchange_table       VARCHAR2 (65 CHAR);
      v#table_name           VARCHAR2 (65 CHAR);
      v#STR                  VARCHAR2 (32767);
      is_unique_key_exists   BOOLEAN := FALSE;
   BEGIN
      V#drop_range := NVL (p_drop_range, SYSTIMESTAMP - NUMTODSINTERVAL (KEEP_DAYS_AGO, 'DAY'));

      FOR r IN (  SELECT TP.TABLE_NAME,
                         TP.PARTITION_NAME,
                         TP.PARTITION_POSITION,
                         TS.SUBPARTITION_NAME,
                         TS.SUBPARTITION_POSITION,
                         TS.HIGH_VALUE,
                         TS.HIGH_VALUE_LENGTH
                    FROM user_tab_subpartitions ts INNER JOIN user_tab_partitions tp ON TP.TABLE_NAME = TS.TABLE_NAME AND TP.PARTITION_NAME = TS.PARTITION_NAME
                   WHERE tp.table_name = P_TABLE_NAME
                ORDER BY TS.SUBPARTITION_POSITION)
      LOOP
         v#STR := SUBSTRC (R.HIGH_VALUE, 1, R.HIGH_VALUE_LENGTH);

         BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || v#STR || ' FROM DUAL' INTO v#high_val;
         EXCEPTION
            WHEN EXC#INVALID_VALUE
            THEN
               v#high_val := TIMESTAMP '9999-12-31 23:59:59';                                                                                        -- MAXVALUE
         END;

         IF v#high_val < V#drop_range          -- субпартиция на удаление и эксчендж ---------------------------------------------------------------------------
         THEN
            BEGIN
               exchange_subpartition (P_TABLE_NAME       => r.TABLE_NAME,
                                      P_SUBPART_NAME     => r.SUBPARTITION_NAME,
                                      P_TABSPACE         => P_TABSPACE,
                                      p_keep_exchanged   => p_keep_exchanged);
            EXCEPTION
               WHEN exc#unique_key_exists
               THEN
                  is_unique_key_exists := TRUE;
            END;
         END IF;
      END LOOP;

      IF is_unique_key_exists
      THEN
         RAISE exc#unique_key_exists;
      END IF;
   END;

   FUNCTION GET_DEPENDENT_DDL (P_OBJECT_tYPE IN VARCHAR2, P_OBJECT_NAME IN VARCHAR2)
      RETURN CLOB
   IS
      V#HANDLE             NUMBER;
      V#tRANSFORM_HANDLE   NUMBER;
      V#ddl                CLOB;
   BEGIN
      V#HANDLE := DBMS_METADATA.OPEN (P_OBJECT_tYPE);
      DBMS_METADATA.SET_FILTER (V#HANDLE, 'NAME', P_OBJECT_NAME);
      V#tRANSFORM_HANDLE := DBMS_METADATA.ADD_TRANSFORM (V#HANDLE, 'DDL');
      DBMS_METADATA.SET_TRANSFORM_PARAM (V#tRANSFORM_HANDLE, 'SEGMENT_ATTRIBUTES', TRUE);

      V#ddl := DBMS_METADATA.fetch_CLOB (V#HANDLE);
      DBMS_METADATA.CLOSE (V#HANDLE);
      RETURN V#DDL;
   END;

   FUNCTION GET_INDEX_DDL (P_OBJECT_NAME IN VARCHAR2)
      RETURN CLOB
   IS
      ddl_h     NUMBER;                                                                                                    -- handle returned by OPEN for tables
      t_h       NUMBER;                                                                                           -- handle returned by ADD_TRANSFORM for tables
      idxddl    CLOB;                                                                                                              -- creation DDL for an object
      pi        SYS.ku$_parsed_items;                                               -- parse items are returned in this object which is contained in sys.ku$_ddl
      idxddls   SYS.ku$_ddls;                                                     -- metadata is returned in sys.ku$_ddls, a nested table of sys.ku$_ddl objects
      idxname   VARCHAR2 (30);                                                                                                          -- the parsed index name
   BEGIN
      ddl_h := DBMS_METADATA.OPEN ('INDEX');
      DBMS_METADATA.SET_FILTER (ddl_h, 'NAME', P_OBJECT_NAME);
      DBMS_METADATA.SET_FILTER (ddl_h, 'SYSTEM_GENERATED', FALSE);
      -- Request that the index name be returned as a parse item.
      DBMS_METADATA.SET_PARSE_ITEM (ddl_h, 'NAME');

      t_h := DBMS_METADATA.ADD_TRANSFORM (ddl_h, 'DDL');
      DBMS_METADATA.set_transform_param (t_h, 'SQLTERMINATOR', FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM (t_h, 'SEGMENT_ATTRIBUTES', TRUE);


      LOOP
         idxddls := DBMS_METADATA.fetch_ddl (ddl_h);
         -- When there are no more objects to be retrieved, FETCH_DDL returns NULL.
         EXIT WHEN idxddls IS NULL;

         FOR i IN idxddls.FIRST .. idxddls.LAST
         LOOP
            idxddl := idxddls (i).ddlText;
            pi := idxddls (i).parsedItems;

            -- Loop through the returned parse items.
            IF pi IS NOT NULL AND pi.COUNT > 0
            THEN
               FOR j IN pi.FIRST .. pi.LAST
               LOOP
                  IF pi (j).item = 'NAME' AND pi (j).VALUE = P_OBJECT_NAME
                  THEN
                     DBMS_METADATA.CLOSE (ddl_h);
                     RETURN idxddl;
                  END IF;
               END LOOP;
            END IF;
         END LOOP;                                                                                                                                   -- for loop
      END LOOP;

      DBMS_METADATA.CLOSE (ddl_h);
      RETURN EMPTY_CLOB ();
   END;

   FUNCTION GET_CONSTRAINT_DDL (P_OBJECT_NAME IN VARCHAR2)
      RETURN CLOB
   IS
      ddl_h     NUMBER;                                                                                                    -- handle returned by OPEN for tables
      t_h       NUMBER;                                                                                           -- handle returned by ADD_TRANSFORM for tables
      idxddl    CLOB;                                                                                                              -- creation DDL for an object
      pi        SYS.ku$_parsed_items;                                               -- parse items are returned in this object which is contained in sys.ku$_ddl
      idxddls   SYS.ku$_ddls;                                                     -- metadata is returned in sys.ku$_ddls, a nested table of sys.ku$_ddl objects
      idxname   VARCHAR2 (30);                                                                                                          -- the parsed index name
   BEGIN
      ddl_h := DBMS_METADATA.OPEN ('CONSTRAINT');
      DBMS_METADATA.SET_FILTER (ddl_h, 'NAME', P_OBJECT_NAME);
      -- Request that the index name be returned as a parse item.
      DBMS_METADATA.SET_PARSE_ITEM (ddl_h, 'NAME');

      t_h := DBMS_METADATA.ADD_TRANSFORM (ddl_h, 'DDL');
      DBMS_METADATA.set_transform_param (t_h, 'SQLTERMINATOR', FALSE);
      DBMS_METADATA.SET_TRANSFORM_PARAM (t_h, 'SEGMENT_ATTRIBUTES', TRUE);


      LOOP
         idxddls := DBMS_METADATA.fetch_ddl (ddl_h);
         -- When there are no more objects to be retrieved, FETCH_DDL returns NULL.
         EXIT WHEN idxddls IS NULL;

         FOR i IN idxddls.FIRST .. idxddls.LAST
         LOOP
            idxddl := idxddls (i).ddlText;
            pi := idxddls (i).parsedItems;

            -- Loop through the returned parse items.
            IF pi IS NOT NULL AND pi.COUNT > 0
            THEN
               FOR j IN pi.FIRST .. pi.LAST
               LOOP
                  IF pi (j).item = 'NAME' AND pi (j).VALUE = P_OBJECT_NAME
                  THEN
                     DBMS_METADATA.CLOSE (ddl_h);
                     RETURN idxddl;
                  END IF;
               END LOOP;
            END IF;
         END LOOP;                                                                                                                                   -- for loop
      END LOOP;

      DBMS_METADATA.CLOSE (ddl_h);
      RETURN EMPTY_CLOB ();
   END;

   PROCEDURE rebuild_indexes (p_table_name IN VARCHAR2, P_TABSPACE IN VARCHAR2)
   AS
   BEGIN
      FOR r
         IN (SELECT 'ALTER INDEX ' || INDEX_NAME || ' REBUILD '
                    || CASE
                          WHEN SUBPARTITION_NAME IS NOT NULL THEN 'SUBPARTITION ' || SUBPARTITION_NAME
                          WHEN PARTITION_NAME IS NOT NULL THEN 'PARTITION ' || PARTITION_NAME
                       END
                    || ' PARALLEL '
                    || TO_CHAR (CASE WHEN SUBPARTITION_NAME IS NOT NULL OR PARTITION_NAME IS NOT NULL THEN 1 ELSE 4 END, 'FM99')
                    || NVL2 (P_TABSPACE, ' TABLESPACE ' || P_TABSPACE, ' ')
                       REBUILD_DDL,
                    CASE WHEN SUBPARTITION_NAME IS NULL AND PARTITION_NAME IS NULL THEN 'ALTER INDEX ' || INDEX_NAME || ' NOPARALLEL' END NOPARALLEL_DDL
               FROM (SELECT ui.status IND_STATUS,
                            COALESCE (UIP.STATUS, ui.status) PART_STATUS,
                            COALESCE (UIS.STATUS, UIP.STATUS, ui.status) SUBP_STATUS,
                            UI.INDEX_NAME,
                            UI.TABLE_NAME,
                            UI.INDEX_TYPE,
                            UIP.PARTITION_NAME,
                            UIS.SUBPARTITION_NAME,
                            us.bytes
                       FROM user_indexes ui
                            LEFT JOIN USER_IND_PARTITIONS UIP
                               ON UIP.INDEX_NAME = UI.INDEX_NAME
                            LEFT JOIN USER_IND_SUBPARTITIONS UIS
                               ON UIS.INDEX_NAME = UIP.INDEX_NAME AND UIS.PARTITION_NAME = UIP.PARTITION_NAME
                            LEFT JOIN user_segments us
                               ON     US.SEGMENT_NAME = UI.INDEX_NAME
                                  AND NVL (US.PARTITION_NAME, '~') = COALESCE (UIS.SUBPARTITION_NAME, UIP.PARTITION_NAME, '~')
                                  AND US.SEGMENT_TYPE LIKE 'INDEX%'
                      WHERE generated = 'N' AND TABLE_NAME = p_table_name) Q
              WHERE (NOT (IND_STATUS = 'VALID' AND PART_STATUS = 'VALID' AND SUBP_STATUS = 'VALID')) AND (NOT SUBP_STATUS = 'USABLE'))
      LOOP
         EXECUTE IMMEDIATE R.REBUILD_DDL;

         IF R.NOPARALLEL_DDL IS NOT NULL
         THEN
            EXECUTE IMMEDIATE R.NOPARALLEL_DDL;
         END IF;
      END LOOP;
   END;

   PROCEDURE rebuild_indexes (p_table_name IN VARCHAR2, P_TABSPACE IN VARCHAR2, call_old_version IN OUT BOOLEAN)
   AS
      v#high_val                TIMESTAMP;
      v#ddl                     VARCHAR2 (2000 CHAR);
      V#exchange_table          VARCHAR2 (65 CHAR);
      should_rebuild            BOOLEAN;
      v#CREATE_INDEX_DDL        CLOB;
      v#CREATE_CONSTRAINT_DDL   CLOB;
   BEGIN
      IF call_old_version
      THEN
         rebuild_indexes (p_table_name => p_table_name, P_TABSPACE => P_TABSPACE);
         RETURN;
      END IF;

      FOR I IN (SELECT UI.INDEX_NAME,
                       UI.STATUS,
                       UC.CONSTRAINT_NAME,
                       UC.CONSTRAINT_TYPE,
                       'ALTER INDEX ' || UI.INDEX_NAME || ' PARALLEL (DEGREE 8)' PARALLEL_DDL,
                       'ALTER INDEX ' || UI.INDEX_NAME || ' NOPARALLEL' NOPARALLEL_DDL
                  FROM USER_INDEXES UI LEFT JOIN USER_CONSTRAINTS UC ON UI.INDEX_NAME = UC.INDEX_NAME
                 WHERE UI.GENERATED = 'N' AND UI.TABLE_NAME = p_table_name)
      LOOP
         should_rebuild := FALSE;

         FOR IP
            IN (SELECT UIP.PARTITION_NAME, UIS.SUBPARTITION_NAME
                  FROM    USER_IND_PARTITIONS UIP
                       LEFT JOIN
                          USER_IND_SUBPARTITIONS UIS
                       ON UIS.INDEX_NAME = UIP.INDEX_NAME AND UIS.PARTITION_NAME = UIP.PARTITION_NAME
                 WHERE     UIP.INDEX_NAME = I.INDEX_NAME
                       AND (NOT (COALESCE (UIP.STATUS, i.status) = 'VALID' AND COALESCE (UIS.STATUS, UIP.STATUS, i.status) = 'VALID'))
                       AND (NOT COALESCE (UIS.STATUS, UIP.STATUS, i.status) = 'USABLE'))
         LOOP
            should_rebuild := TRUE;
            EXIT;
         END LOOP;

         IF I.STATUS = 'UNUSABLE' OR should_rebuild
         THEN
            should_rebuild := TRUE;

            EXECUTE IMMEDIATE I.PARALLEL_DDL;                                                        -- set Parallel option. when parsed DDL - it will be there.

            v#CREATE_INDEX_DDL := get_index_ddl (I.INDEX_NAME);
         ELSE
            v#CREATE_INDEX_DDL := EMPTY_CLOB ();
         END IF;

         IF I.CONSTRAINT_NAME IS NOT NULL AND should_rebuild
         THEN
            v#CREATE_CONSTRAINT_DDL := get_constraint_ddl (i.CONSTRAINT_NAME);
         ELSE
            v#CREATE_CONSTRAINT_DDL := EMPTY_CLOB ();
         END IF;

         DECLARE
            ch   BINARY_INTEGER;
            rh   BINARY_INTEGER;
         BEGIN
            DDL_PKG.DROP_INDEX (i.index_name);
            ch := DBMS_SQL.OPEN_CURSOR;
            DBMS_SQL.PARSE (ch, v#CREATE_INDEX_DDL, DBMS_SQL.NATIVE);
            rh := DBMS_SQL.EXECUTE (ch);
            DBMS_SQL.CLOSE_CURSOR (ch);
         EXCEPTION
            WHEN exc#enforcing_index
            THEN
               call_old_version := TRUE;            -- Констрейнт мешает удалить/создать индекс. Поэтому вызовем старый метод для ребилда всех индексов/партиций
         END;

         IF i.NOPARALLEL_DDL IS NOT NULL AND should_rebuild
         THEN
            EXECUTE IMMEDIATE i.NOPARALLEL_DDL;
         END IF;
      END LOOP;

      IF call_old_version                                                   -- если есть необходимость перестроить индексы по-старому, ребилдом, то сделаем это.
      THEN
         rebuild_indexes (p_table_name => p_table_name, P_TABSPACE => P_TABSPACE);
         RETURN;
      END IF;
   END;

   PROCEDURE gen_future_subpartition (p_table_name IN VARCHAR2, p_part_count IN BINARY_INTEGER := 3, P_TABSPACE IN VARCHAR2)
   AS
      v#part_count      BINARY_INTEGER := GREATEST (NVL (p_part_count, 1), 1);
      v#subpart_name    VARCHAR2 (30 CHAR);
      V#subpart_RANGE   TIMESTAMP;
   BEGIN
      FOR bc IN (SELECT * FROM it#bank_codes)
      LOOP
         FOR d IN 1 .. v#part_count
         LOOP
            V#subpart_RANGE := TRUNC (SYSTIMESTAMP, 'DD') + NUMTODSINTERVAL (d, 'DAY');
            add_modify_split_partition (P_TABLE_NAME             => p_table_name                                       -- имя таблицы, в которой проводим работы
                                                                                ,
                                        P_BANK_CODE              => BC.BC                                     -- Код тербанка. Будет включен в название партиции
                                                                         ,
                                        P_PART_VALUES_LIST       => BC.BC                                    --  Список кодов тербанков для добавляемой партиции
                                                                         ,
                                        P_SUBPART_VALUES_RANGE   => V#subpart_RANGE                                           -- Верхняя граница для субпартиции
                                                                                   ,
                                        P_TABSPACE               => P_TABSPACE);
         END LOOP;
      END LOOP;
   END;

   PROCEDURE TABLE_MAINTENANCE (p_table_name       IN VARCHAR2,
                                p_tabspace         IN VARCHAR2,
                                p_part_count       IN BINARY_INTEGER,
                                p_drop_range       IN TIMESTAMP := SYSTIMESTAMP - NUMTODSINTERVAL (KEEP_DAYS_AGO, 'DAY'),
                                p_keep_exchanged   IN BOOLEAN := FALSE)
   AS
      is_unique_key_exists   BOOLEAN := FALSE;
      CALL_OLD_VERSION       BOOLEAN := FALSE;
   BEGIN
      /* СОЗДАЕМ ПАРТИЦИИ НА БУДУЩЕЕ */
      GEN_FUTURE_SUBPARTITION (p_table_name => P_TABLE_NAME, p_part_count => p_part_count, P_tabspace => P_TABSPACE);

      /* ПАРТИЦИИ С УСТАРЕВШИМИ ДАННЫМИ ОТЦЕПЛЯЕМ И КОВЕРТИРУЕМ В ОБЫЧНЫЕ ТАБЛИЦЫ.
      в случае ошибки невозможности удаления партиции из-за ключей, ссылающихся на партицию - перехват,
      перестройка и возбуждение исключения после перестройки индексов */
      BEGIN
         EXCHANGE_SUBPARTITIONS (p_table_name       => P_TABLE_NAME,
                                 P_tabspace         => P_TABSPACE,
                                 p_drop_range       => p_drop_range,
                                 p_keep_exchanged   => p_keep_exchanged);
      EXCEPTION
         WHEN exc#unique_key_exists
         THEN
            is_unique_key_exists := TRUE;
      END;

      /* ПЕРЕСТРАИВАЕМ ИНВАЛИДНЫЕ ИНДУКСЫ */
      REBUILD_INDEXES (p_table_name => P_TABLE_NAME, P_tabspace => P_TABSPACE, CALL_OLD_VERSION => CALL_OLD_VERSION);

      IF is_unique_key_exists
      THEN
         RAISE exc#unique_key_exists;
      END IF;
   END;

   PROCEDURE TABLES_MAINTENANCE (P_TABSPACE IN VARCHAR2, p_keep_exchanged IN BOOLEAN := FALSE)
   AS
      V#OWNER   CONSTANT VARCHAR2 (30 CHAR) := SYS_CONTEXT ('USERENV', 'CURRENT_SCHEMA');
      v#BLOCK            VARCHAR2 (2000 CHAR);
   BEGIN
      FOR t IN (SELECT Q.*, ROWNUM - 1 RN
                  FROM (  SELECT *
                            FROM it#mig_tables tt
                           WHERE TT.CREATE_PARTITIONS = 'Y'
                        ORDER BY REFRESH_ORDER DESC) Q)
      LOOP
         BEGIN
            v#BLOCK :=
                  'BEGIN '
               || (V#OWNER || '.' || PACKAGE_NAME || '.TABLE_MAINTENANCE')
               || '('
               || (' P_TABLE_NAME=>''' || T.TABLE_NAME || '''')
               || (',P_PART_COUNT=>' || TO_CHAR (NEW_PARTITIONS_COUNT, 'FM99'))
               || (',P_TABSPACE=>''' || P_TABSPACE || '''')
               || (',p_keep_exchanged=>' || CASE WHEN p_keep_exchanged THEN 'TRUE' ELSE 'FALSE' END)
               || ');'
               || 'END;';

            EXECUTE IMMEDIATE v#BLOCK;
         END;
      END LOOP moving;
   END;

   PROCEDURE GEN_JOBS_SUBPART_GENERATOR (P_TABSPACE IN VARCHAR2)
   AS
      V#OWNER                   CONSTANT VARCHAR2 (30 CHAR) := SYS_CONTEXT ('USERENV', 'CURRENT_SCHEMA');
      V#JOB_NAME                         VARCHAR2 (65 CHAR);
      V#JOB_PREFIX                       VARCHAR2 (30 CHAR);
      v#START_DATE                       TIMESTAMP := SYSTIMESTAMP + INTERVAL '3' MINUTE;
      v#END_DATE                         TIMESTAMP := TRUNC (SYSTIMESTAMP, 'DD') + INTERVAL '1' DAY + INTERVAL '7' HOUR;
      v#BLOCK_START                      VARCHAR2 (2000 CHAR);
      v#BLOCK_END                        VARCHAR2 (2000 CHAR);
      V#JOB_PRIORITY                     BINARY_INTEGER;
      V#JOB_COMMENT                      VARCHAR2 (200 CHAR);
      EXN#JOB_DOESNOT_EXISTS    CONSTANT BINARY_INTEGER := -27475;
      EXC#JOB_DOESNOT_EXISTS             EXCEPTION;
      PRAGMA EXCEPTION_INIT (EXC#JOB_DOESNOT_EXISTS, -27475);
      EXN#JOB_ALLREADY_EXISTS   CONSTANT BINARY_INTEGER := -27477;
      EXC#JOB_ALLREADY_EXISTS            EXCEPTION;
      PRAGMA EXCEPTION_INIT (EXC#JOB_ALLREADY_EXISTS, -27477);
   BEGIN
      v#BLOCK_START := 'BEGIN ';
      v#BLOCK_END := ' END;';
      V#JOB_PRIORITY := 3;
      V#JOB_COMMENT :=
         'ЗАО Ай-Теко, 2013' || CHR (10)
         || 'Самоудаляемое задание для генерации партиций/очистки устаревших данных в таблице ';

      FOR t IN (SELECT Q.*, ROWNUM - 1 RN
                  FROM (  SELECT *
                            FROM it#mig_tables tt
                           WHERE TT.CREATE_PARTITIONS = 'Y'
                        ORDER BY REFRESH_ORDER DESC) Q)
      LOOP
         V#JOB_PREFIX := 'MPJ' || TO_CHAR (T.RN, 'FM09') || '$';
         V#JOB_NAME := V#OWNER || '.' || V#JOB_PREFIX || SUBSTR (t.table_name, 1, 30 - LENGTH (V#JOB_PREFIX));

         BEGIN
            DBMS_SCHEDULER.DROP_JOB (job_name => V#JOB_NAME, FORCE => TRUE);
         EXCEPTION
            WHEN EXC#JOB_DOESNOT_EXISTS
            THEN
               NULL;
         END;

         v#START_DATE := SYSTIMESTAMP + NUMTODSINTERVAL (T.RN * 10, 'MINUTE');

         BEGIN
            DBMS_SCHEDULER.CREATE_JOB (
               job_name     => V#JOB_NAME,
               start_date   => v#START_DATE, -------------------------------------------------------------------------------------------------------------------------
               end_date     => v#END_DATE,
               job_class    => 'DEFAULT_JOB_CLASS',
               job_type     => 'PLSQL_BLOCK',
               job_action   =>   v#BLOCK_START
                              || (V#OWNER || '.' || PACKAGE_NAME || '.TABLE_MAINTENANCE')
                              || '('
                              || (' p_table_name=>''' || T.TABLE_NAME || '''')
                              || (',p_part_count=>' || TO_CHAR (NEW_PARTITIONS_COUNT, 'FM99'))
                              || (',p_tabspace=>''' || P_TABSPACE || '''')
                              || ');'
                              || v#BLOCK_END,
               comments     => V#JOB_COMMENT || T.TABLE_NAME,
               auto_drop    => TRUE,
               enabled      => TRUE);
            DBMS_SCHEDULER.DISABLE (name => V#JOB_NAME);
            DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'RESTARTABLE', VALUE => FALSE);
            DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'JOB_PRIORITY', VALUE => V#JOB_PRIORITY);
            DBMS_SCHEDULER.SET_ATTRIBUTE (name => V#JOB_NAME, attribute => 'MAX_RUNS', VALUE => 1);
         EXCEPTION
            WHEN EXC#JOB_ALLREADY_EXISTS
            THEN
               NULL;
         END;
      END LOOP moving;
   END;

   /* Процедура добавляет партиции по всем тербанкам. В каждой партиции 1 субпартиция по умолчанию (MAXVALUE)*/
   PROCEDURE prc_create_partitions_by_tb (p_first_date DATE, p_tablespace VARCHAR2)
   IS
      r        get_tables_bank_codes%ROWTYPE;
      minval   TIMESTAMP;
   BEGIN
      minval := SYSTIMESTAMP - INTERVAL '1' YEAR;

      OPEN get_tables_bank_codes;

      LOOP
         FETCH get_tables_bank_codes INTO r;

         EXIT WHEN get_tables_bank_codes%NOTFOUND;
         add_modify_split_partition (P_TABLE_NAME             => r.table_name,
                                     P_BANK_CODE              => r.bc,
                                     P_PART_VALUES_LIST       => r.bc,
                                     P_SUBPART_VALUES_RANGE   => NULL           -- NULL - будет добавлена субпартиция по умолчанию------------------------------
                                                                     ,
                                     P_TABSPACE               => p_tablespace);
         /*  Делаем субпартицию, в которую заносим совсем старые данные старше года ------------*/
         add_modify_split_partition (P_TABLE_NAME             => r.table_name,
                                     P_BANK_CODE              => r.bc,
                                     P_PART_VALUES_LIST       => r.bc,
                                     P_SUBPART_VALUES_RANGE   => minval,
                                     P_TABSPACE               => p_tablespace);
      END LOOP;

      CLOSE get_tables_bank_codes;
   /* Все необработанные исключения передаем наверх */
   END;

   PROCEDURE prc_create_subpart_to_date (p_date_end DATE, p_tablespace VARCHAR2)
   IS
      r        get_tables_bank_codes%ROWTYPE;
      minval   TIMESTAMP;
   BEGIN
      OPEN get_tables_bank_codes;

      LOOP
         FETCH get_tables_bank_codes INTO r;

         EXIT WHEN get_tables_bank_codes%NOTFOUND;

         FOR d IN (    SELECT SYSTIMESTAMP - INTERVAL '12' MONTH + NUMTODSINTERVAL (LEVEL, 'DAY') cd
                         FROM DUAL
                   CONNECT BY SYSTIMESTAMP - INTERVAL '12' MONTH + NUMTODSINTERVAL (LEVEL, 'DAY') <= SYSTIMESTAMP + INTERVAL '30' DAY)
         LOOP
            add_modify_split_partition (P_TABLE_NAME             => r.table_name,
                                        P_BANK_CODE              => r.bc,
                                        P_PART_VALUES_LIST       => r.bc,
                                        P_SUBPART_VALUES_RANGE   => d.cd                                               -- будет добавлена субпартиция с границей
                                                                        ,
                                        P_TABSPACE               => p_tablespace);
         END LOOP;
      END LOOP;

      CLOSE get_tables_bank_codes;
   /* Все необработанные исключения передаем наверх */
   END;

   /**
   * создает партиции для одной X_ таблицы
   *
   */
   PROCEDURE create_x_tab_part (p_table_name VARCHAR2)
   IS
      l_table_name   VARCHAR2 (32);
      l_tablespace   VARCHAR2 (32);
      l_ddl          VARCHAR2 (4000);
   BEGIN
      l_table_name := p_table_name;

      BEGIN
         SELECT p.table_name, p.tablespace_name
           INTO l_table_name, l_tablespace
           FROM user_tab_partitions p
          WHERE p.table_name = l_table_name AND p.partition_position = 1;

         FOR i IN 2 .. 366
         LOOP
            l_ddl :=
               'ALTER TABLE ' || l_table_name || ' ADD PARTITION ' || 'D' || TRIM (TO_CHAR (i, '000')) || ' VALUES (' || i || ') TABLESPACE ' || l_tablespace;
            DDL_PKG.ALTER_TABLE (l_ddl);
         END LOOP;

         DDL_PKG.ALTER_TABLE ('ALTER TABLE ' || l_table_name || ' ADD PARTITION DXXX VALUES (DEFAULT) TABLESPACE ' || l_tablespace);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;
   END;

   PROCEDURE create_x_partitions
   IS
   BEGIN
      FOR i IN 1 .. g_x_tables.COUNT
      LOOP
         create_x_tab_part (g_x_tables (i));
      END LOOP;
   END;

   PROCEDURE test_p
   AS
   BEGIN
      add_modify_split_partition (P_TABLE_NAME             => 'INC_DOC_PRINT',
                                  P_BANK_CODE              => '90',
                                  P_PART_VALUES_LIST       => '90',
                                  P_SUBPART_VALUES_RANGE   => NULL                                                   --TO_TIMESTAMP ('2009-01-01', 'yyyy-mm-dd')
                                                                  ,
                                  P_TABSPACE               => 'USERS');
      NULL;
   END;

   PROCEDURE test_gen
   AS
   BEGIN
      GEN_JOBS_SUBPART_GENERATOR (P_TABSPACE => 'SVERKA_HIST');
      NULL;
   END;

   PROCEDURE test_TABLE_MAINTENANCE
   AS
   BEGIN
      TABLE_MAINTENANCE (p_table_name => 'DISPUTED_REQUEST', p_tabspace => 'SVERKA_HIST', p_part_count => 1);
   END;
BEGIN
   g_x_tables (1) := 'X_OPER_INFO_SP';
   g_x_tables (2) := 'X_UV_PAYMENTVERIFICATION_RBS';
   g_x_tables (3) := 'X_DOC_WAY4';
   g_x_tables (4) := 'X_CURR_TRANS_SV';
   g_x_tables (5) := 'X_OPER_INFO_DEBATE';
END PKG_MANAGE_PARTITIONS;
/

PROMPT Удаление $-таблиц для сгенерированных искусственных данных и перестройка индексов

BEGIN
   BEGIN
      FOR t IN (SELECT * FROM it$dup_tables)
      LOOP
         DDL_PKG.DROP_TABLE (t.ptbl);
         PKG_MANAGE_PARTITIONS.REBUILD_INDEXES (P_TABLE_NAME => t.ttbl, P_TABSPACE => NULL);
      END LOOP;
   END;

   NULL;
END;
/
/*
PROMPT Включение ограничений целостности на таблицах

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
                      || CASE CONSTRAINT_TYPE WHEN 'C' THEN 'NOVALIDATE' ELSE 'VALIDATE' END
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

prompt Перекидывание счетчиков в актуальные значения, соответствующие загруженным данным
DECLARE
   CURSOR it$dup_tables_c
   IS
      SELECT ttbl,
             sequence_name,
             pk_col,
             ROW_NUMBER () OVER (PARTITION BY sequence_name ORDER BY 1) rn
        FROM (SELECT it$dup_tables.ttbl,
                     CASE WHEN it$dup_tables.ttbl = 'INC_OPER_INFO' THEN 'SEQ_INCIDENT' ELSE s.sequence_name END sequence_name,
                     cc.column_name AS pk_col
                FROM it$dup_tables
                     LEFT JOIN user_constraints c
                        ON (it$dup_tables.ttbl = c.table_name AND c.constraint_type = 'P')
                     LEFT JOIN user_cons_columns cc
                        ON (c.table_name = cc.table_name AND c.constraint_name = cc.constraint_name)
                     LEFT JOIN user_sequences s
                        ON (s.sequence_name LIKE '%' || cc.table_name || '%'));

   TYPE seq_need_val_tab_type IS TABLE OF NUMBER
                                    INDEX BY VARCHAR2 (30 CHAR);

   seq_need_val_tab   seq_need_val_tab_type;
   v_max_id           NUMBER := 0;
   v_need_val         NUMBER := 0;
   v_owner            VARCHAR2 (30 CHAR) := SYS_CONTEXT ('USERENV', 'CURRENT_USER');

   PROCEDURE alter_seq (owner IN VARCHAR2, seq_name IN VARCHAR2, need_val IN NUMBER)
   IS
      p_owner       VARCHAR2 (30) := UPPER (owner);
      p_name        VARCHAR2 (30) := UPPER (seq_name);
      p_need_val    NUMBER := need_val;
      -- процедура изменяет последовательность таким образом, чтобы при следующем вызове
      -- последовательность возвратит p_need_val
      -- процедура применима только к возрастающим последовательностям (у которых задано MINVALUE)
      -- ВНИМАНИЕ: если p_need_val меньше текущего значения, то MINVALUE изменится
      sql_str       VARCHAR2 (500);
      l_cache       VARCHAR2 (30);
      l_increment   NUMBER;
      l_last_num    NUMBER;
      l_curr_val    NUMBER;
   BEGIN
      -- получить значения cache_size, increment_by, last_number
      SELECT CASE cache_size WHEN 0 THEN ' nocache ' ELSE ' cache ' || TO_CHAR (cache_size) END, increment_by, last_number
        INTO l_cache, l_increment, l_last_num
        FROM user_sequences
       WHERE sequence_name = p_name;

      IF p_need_val = l_last_num
      THEN
         -- последовательность или ни разу не вызывалась,
         -- или смысла что-то трогать нет
         NULL;
      ELSE
         IF p_need_val > l_last_num
         THEN
            -- отключить кеширование, minvalue не трогать
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' nocache';
         ELSE
            -- отключить кеширование, сдвинуть minvalue
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' nocache' || ' minvalue ' || TO_CHAR (p_need_val - l_increment);
         END IF;

         EXECUTE IMMEDIATE sql_str;

         -- получить текущее значение последовательности
         sql_str := 'select ' || p_owner || '.' || p_name || '.nextval from dual';

         EXECUTE IMMEDIATE sql_str INTO l_curr_val;

         -- если после этого следующий вызов последовательности выдаст нужный результат,
         -- то все, иначе
         IF (p_need_val - l_increment - l_curr_val) <> 0
         THEN
            -- increment_by на нужное значение
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' increment by ' || TO_CHAR (p_need_val - l_increment - l_curr_val);

            EXECUTE IMMEDIATE sql_str;

            -- установить последовательность в нужное значение
            sql_str := 'select ' || p_owner || '.' || p_name || '.nextval from dual';

            -- после чего l_curr_val будет равен p_need_val - l_increment, т.е. MINVALUE
            EXECUTE IMMEDIATE sql_str INTO l_curr_val;
         END IF;

         -- возвращаем cache_size и increment_by
         sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' increment by ' || TO_CHAR (l_increment)          --|| ' minvalue ' || to_char(l_curr_val)
                                                                                                              || l_cache;

         EXECUTE IMMEDIATE sql_str;
      END IF;
   -- следующий вызов последовательности даст p_need_val

   END alter_seq;
BEGIN
   FOR r IN it$dup_tables_c
   LOOP
      EXECUTE IMMEDIATE 'select MAX(' || r.pk_col || ') from ' || r.ttbl INTO v_max_id;

      v_need_val := v_max_id + 1;

      seq_need_val_tab (r.sequence_name) := v_need_val;

      IF r.rn = 1
      THEN
         seq_need_val_tab (r.sequence_name) := v_need_val;
         alter_seq (owner => v_owner, seq_name => r.sequence_name, need_val => seq_need_val_tab (r.sequence_name));
      ELSIF r.rn > 1 AND seq_need_val_tab (r.sequence_name) < v_max_id
      THEN
         seq_need_val_tab (r.sequence_name) := v_need_val;
         alter_seq (owner => v_owner, seq_name => r.sequence_name, need_val => seq_need_val_tab (r.sequence_name));
      ELSIF r.rn > 1 AND seq_need_val_tab (r.sequence_name) > v_max_id
      THEN
         NULL;
      END IF;
   END LOOP;
END change_seq_val;
/

PROMPT Удаление списка таблиц для размножения данных
DROP VIEW it$dup_TABLES;
*/
@@utl_foot.sql

EXIT