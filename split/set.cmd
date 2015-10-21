@echo off
rem выставим правильную кодовую страницу
@chcp 1251
rem очистим экран
cls
rem сделаем красивым терминал
color 1A
rem выставим переменную окружения
rem set ORACLE_HOME=C:\oracle\product\DB_client64
set nls_lang = american_cis.cl8mswin1251
set NLS_DATE_FORMAT=yyyymmddhh24miss
