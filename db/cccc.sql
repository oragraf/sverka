declare
call_old_version boolean := false;
begin
--   DBMS_OUTPUT.enable (10000000);

   for t in (select * from it$$dup_tables)
   loop
--      ddl_pkg.drop_table (t.tmp_tbl_name);
      pkg_manage_partitions.rebuild_indexes (p_table_name => t.orig_tbl_name, p_tabspace => null,call_old_version => call_old_version);
   end loop;
end;
/
