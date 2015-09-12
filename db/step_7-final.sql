PROMPT ������������� ��������� � ���������� ��������, ��������������� ����������� ������
----------------------------------------------
DEFINE SCRIPT_STEP='7'
DEFINE SCRIPT_NAME='final'
DEFINE SCRIPT_FULL_NAME='step_&&SCRIPT_STEP.-&&SCRIPT_NAME.'
DEFINE SCRIPT_DESC='������ ������������ �������� � ���������� ��������. ����������� ���� ��� �� ��� ��������.'

@@utl_head.sql

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
                     INNER JOIN user_constraints c
                        ON (it$dup_tables.ttbl = c.table_name AND c.constraint_type = 'P')
                     INNER JOIN user_cons_columns cc
                        ON (c.table_name = cc.table_name AND c.constraint_name = cc.constraint_name)
                     INNER JOIN user_sequences s
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
      -- ��������� �������� ������������������ ����� �������, ����� ��� ��������� ������
      -- ������������������ ��������� p_need_val
      -- ��������� ��������� ������ � ������������ ������������������� (� ������� ������ MINVALUE)
      -- ��������: ���� p_need_val ������ �������� ��������, �� MINVALUE ���������
      sql_str       VARCHAR2 (500);
      l_cache       VARCHAR2 (30);
      l_increment   NUMBER;
      l_last_num    NUMBER;
      l_curr_val    NUMBER;
   BEGIN
      -- �������� �������� cache_size, increment_by, last_number
      SELECT CASE cache_size WHEN 0 THEN ' nocache ' ELSE ' cache ' || TO_CHAR (cache_size) END, increment_by, last_number
        INTO l_cache, l_increment, l_last_num
        FROM user_sequences
       WHERE sequence_name = p_name;

      IF p_need_val = l_last_num
      THEN
         -- ������������������ ��� �� ���� �� ����������,
         -- ��� ������ ���-�� ������� ���
         NULL;
      ELSE
         IF p_need_val > l_last_num
         THEN
            -- ��������� �����������, minvalue �� �������
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' nocache';
         ELSE
            -- ��������� �����������, �������� minvalue
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' nocache' || ' minvalue ' || TO_CHAR (p_need_val - l_increment);
         END IF;

         EXECUTE IMMEDIATE sql_str;

         -- �������� ������� �������� ������������������
         sql_str := 'select ' || p_owner || '.' || p_name || '.nextval from dual';

         EXECUTE IMMEDIATE sql_str INTO l_curr_val;

         -- ���� ����� ����� ��������� ����� ������������������ ������ ������ ���������,
         -- �� ���, �����
         IF (p_need_val - l_increment - l_curr_val) <> 0
         THEN
            -- increment_by �� ������ ��������
            sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' increment by ' || TO_CHAR (p_need_val - l_increment - l_curr_val);

            EXECUTE IMMEDIATE sql_str;

            -- ���������� ������������������ � ������ ��������
            sql_str := 'select ' || p_owner || '.' || p_name || '.nextval from dual';

            -- ����� ���� l_curr_val ����� ����� p_need_val - l_increment, �.�. MINVALUE
            EXECUTE IMMEDIATE sql_str INTO l_curr_val;
         END IF;

         -- ���������� cache_size � increment_by
         sql_str := 'alter sequence ' || p_owner || '.' || p_name || ' increment by ' || TO_CHAR (l_increment)          --|| ' minvalue ' || to_char(l_curr_val)
                                                                                                              || l_cache;

         EXECUTE IMMEDIATE sql_str;
      END IF;
   -- ��������� ����� ������������������ ���� p_need_val

   END alter_seq;
BEGIN
   FOR r IN it$dup_tables_c
   LOOP
      DECLARE
         CSQL   VARCHAR2 (1000);
      BEGIN
         CSQL := 'select MAX(' || r.pk_col || ') from ' || r.ttbl;

         EXECUTE IMMEDIATE CSQL INTO v_max_id;

         DDL_PKG.info (CSQL || ';');
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
            NULL; -- ������ �� ������, ��� ��� ����������
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            DDL_PKG.ERROR ('--' || CSQL || ';');
      END;
   END LOOP;
END change_seq_val;
/

PROMPT �������� ������ ������ ��� ����������� ������
DROP VIEW it$dup_TABLES;


@@utl_foot.sql

EXIT