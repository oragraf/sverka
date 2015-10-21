WHENEVER SQLERROR EXIT
SET SERVEROUTPUT ON SIZE UNLIMITED FORMAT WORD_WRAPPED
SET LINESIZE 300 PAGESIZE 0
SET VERIFY OFF feedback off
SET TIMING ON

SPOOL ./&&SCRIPT_FULL_NAME..log
PROMPT -------------------------------------------------------------------------------------------------------------------------------------------------------------------

PROMPT Запущен скрипт &&SCRIPT_FULL_NAME..sql

PROMPT -------------------------------------------------------------------------------------------------------------------------------------------------------------------

PROMPT &&SCRIPT_DESC.

PROMPT -------------------------------------------------------------------------------------------------------------------------------------------------------------------

@@_define.sql

connect &&user_owner./&&user_pwd.@&&tns_name.
