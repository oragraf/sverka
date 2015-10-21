create or replace package pkg_mp
is
   package_version                  constant varchar2 (100) := '0.1';
   package_name                     constant varchar2 (30 char) := 'PKG_MP';
   part_job_generator               constant varchar2 (63 char) := 'PART_JOB_GENERATOR';
   clean_ddl_schedule               constant varchar2 (63 char) := 'CLEAN_DDL_SCHEDULE';
   arch_job_generator               constant varchar2 (63 char) := 'ARCH_JOB_GENERATOR';
   arch_ddl_schedule                constant varchar2 (63 char) := 'ARCH_DDL_SCHEDULE';
   unusable_indexes_exists          constant varchar2 (100 char) := 'Есть инвалидные индексы';
   unusable_indexes_dont_exists     constant varchar2 (100 char) := 'Инвалидных индексов нет';
   process_type_archivate           constant char (1 char) := 'A';
   process_type_partitioning        constant char (1 char) := 'P';
   message_type_error               constant char (1 char) := 'E';
   message_type_info                constant char (1 char) := 'I';
   ddl_parallel_degree              constant number := 16;
   /*
    Исключения при добавлении/удалении/расщеплении/слиянии партиций
    */
   /* Попытка вставки данных в несуществующую партицию */
   exn#ins_into_non_existent_part   constant binary_integer := -14400;
   exc#ins_into_non_existent_part            exception;
   pragma exception_init (exc#ins_into_non_existent_part, -14400);
   /* Попытка добавления уже существующей партиции */
   exn#duplicate_partition_name     constant binary_integer := -14013;
   exc#duplicate_partition_name              exception;
   pragma exception_init (exc#duplicate_partition_name, -14013);
   /* Попытка добавления партиции с таким же ключом но другим именем */
   exn#part_key_already_exists      constant binary_integer := -14312;
   exc#part_key_already_exists               exception;
   pragma exception_init (exc#part_key_already_exists, -14312);
   /* Попытка добавления субпартиции с границей меньшей, чем у другой существующей */
   exn#subpart_key_already_exists   constant binary_integer := -14211;
   exc#subpart_key_already_exists            exception;
   pragma exception_init (exc#subpart_key_already_exists, -14211);
   /* Попытка добавления партиции при существующей DEFAULT */
   exn#part_default_exists          constant binary_integer := -14323;
   exc#part_default_exists                   exception;
   pragma exception_init (exc#part_default_exists, -14323);
   exn#invalid_value                constant binary_integer := -904;
   exc#invalid_value                         exception;
   pragma exception_init (exc#invalid_value, -904);
   exn#name_already_exists          constant binary_integer := -955;
   exc#name_already_exists                   exception;
   pragma exception_init (exc#name_already_exists, -955);
   exn#unique_key_exists            constant binary_integer := -2266;
   exc#unique_key_exists                     exception;
   pragma exception_init (exc#unique_key_exists, -2266);
   exn#enforcing_index              constant binary_integer := -2429;
   exc#enforcing_index                       exception;
   pragma exception_init (exc#enforcing_index, -2429);
   exn#job_doesnot_exists           constant binary_integer := -27475;
   exc#job_doesnot_exists                    exception;
   pragma exception_init (exc#job_doesnot_exists, -27475);
   exn#job_allready_exists          constant binary_integer := -27477;
   exc#job_allready_exists                   exception;
   pragma exception_init (exc#job_allready_exists, -27477);
   /* Часть имени субпартиции по умолчанию */
   default_subpart_name             constant varchar2 (8 char) := '99991231';

   /********************************************************************************************************
   ** Стек процедур для секционирования таблиц
   *********************************************************************************************************/
   procedure table_part_maintenance (p_table_name in varchar2, p_tabspace in varchar2);

   procedure tables_part_maintenance (p_tabspace in varchar2, p_keep_exchanged in boolean := false);

   /* Процедура генерации задач для добавления новых, на будущую неделю, партиций */
   procedure gen_part_jobs (p_tabspace in varchar2 default '');

   /* Процедура генерации единого задания на автоматическое секционирование БД АС */
   procedure gen_part_generator;

   /********************************************************************************************************
   ** Стек процедур для архивирования/перемещения устаревших секций в хранилище
   *********************************************************************************************************/
   procedure table_arch_maintenance (p_table_name in varchar2);

   procedure tables_arch_maintenance;

   /********************************************************************************************************
   ** Процедура генерации потабличных задач для перемещения устаревших секций в хранилище
   ** Вызывается автоматически сгенерированным job
   *********************************************************************************************************/
   procedure gen_arch_jobs (p_tabspace in varchar2 default '');

   /********************************************************************************************************
   ** Процедура генерации единого задания для перемещения устаревших секций в хранилище
   ** Вызывается из установочного скрипта.
   *********************************************************************************************************/
   procedure gen_arch_generator;

   procedure gen_future_subpartition (p_table_name in varchar2, p_part_count in binary_integer := 3, p_tabspace in varchar2);

   procedure exchange_subpartitions (p_table_name       in varchar2
                                    ,p_tabspace         in varchar2
                                    ,p_drop_range       in timestamp := systimestamp - numtodsinterval (366 * 3, 'DAY')
                                    ,p_keep_exchanged   in boolean := false);

   procedure rebuild_indexes;

   procedure rebuild_indexes (p_table_name in varchar2, p_tabspace in varchar2);

   procedure rebuild_indexes (p_table_name in varchar2, p_tabspace in varchar2, call_old_version in out boolean);

   /* Функция проверки наличия unusable-индексов */
   function check_unusable_indexes
      return varchar2;

   /**
   * Создает партиции по тербанкам.
   * В качестве даты первой субпартиции берет дату, указанную в параметре p_first_date
   * Список таблиц жестко закодирован.
   *
   * @param p_first_date date default null - дата первой партиции для новых партиций
   * @param p_tablespace varchar2          - tablespace в котором будут создаваться партиции
   */
   procedure prc_create_partitions_by_tb (p_first_date date, p_tablespace varchar2);

   /**
   * Создает субпартиции по дням для всех List-range секционированных таблиц.
   * С периодом в 1 день до указанной даты, начиная с даты последней существующей субпартиции
   * в каждой партиции. Список таблиц жестко закодирован.
   *
   * @param p_date_end date -дата до которой нужно создавать партиции
   * @param p_tablespace varchar2          - tablespace в котором будут создаваться субпартиции
   */
   procedure prc_create_subpart_to_date (p_date_end date, p_tablespace varchar2);

   procedure add_modify_split_partition (p_table_name             in varchar2                                                              -- имя таблицы, в которой проводим работы
                                        ,p_bank_code              in varchar2                                                     -- Код тербанка. Будет включен в название партиции
                                        ,p_part_values_list       in varchar2                                                    --  Список кодов тербанков для добавляемой партиции
                                        ,p_subpart_values_range   in timestamp                                                                    -- Верхняя граница для субпартиции
                                        ,p_tabspace               in varchar2);
end pkg_mp;
/