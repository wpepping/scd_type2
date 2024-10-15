CREATE OR REPLACE PROCEDURE support.sp_scd_type2(
	target_table VARCHAR(512), 
	source_table VARCHAR(512), 
	key_column VARCHAR(512), 
	scd_update_column_list VARCHAR(65535), 
	simple_update_column_list VARCHAR(65535), 
	update_date VARCHAR(512), 
	delete_missing BOOLEAN
)
LANGUAGE plpgsql AS 

$$
	-- CALL support.sp_scd_type2('public.scd_test_target', 'public.scd_test_source', 'candidate_id', 'name, email_address, job_position, lead_country', 'address', 'last_updated_date', 0)

---------------------------------------------------------------------------------------------------------------
-- Description:
-- ------------
-- Loads a source table into corresponding SCD table using SCD type 1 or 2
--
-- How to use:
-- -----------
-- The SCD table should have the same columns as the source table, plus the following 3:
-- is_current 	BOOLEAN
-- valid_from 	DATETIME
-- valid_to 	DATETIME
--  
-- Parameters:
-- -----------
-- target_table 				- The name of the SCD table (schema.table_name). Cannot be NULL or empty.
-- source_table 				- The name of the source table (schema.table_name). Cannot be NULL or empty.
-- key_column 					- The name of key column of the source table. This column has to be unique  
-- 								  for every record in the source table. If no single key column exists, it  
--								  will have to be created. Cannot be NULL or empty.
-- scd_update_column_list		- The columns in the source and SCD table that will be updated following
--								  SCD type 2. When the value of one or more of these columns changes for a 
--								  specific record, the current record in the SCD table is closed by filling 
--								  the valid_from date and a new record is created with the updated values.
--								  The newly added record will be marked as current. Cannot be NULL or empty.
-- simple_update_column_list	- The columns in the source and SCD table that will be updated by a simple
--								  update (SCD type 1). If the value of one of these columns changes, it will  
--								  be changed in all corresponding records in the SCD table (also the 
--								  historic ones). If only columns in this list and no columns in 
--								  scd_update_column_list change in a certain record, no new record will be
--								  added and the current record will stay current.
-- update_date					- This column affects the date that is inserted in the valid_from column 
--								  for newly inserted records and the valid_to column for the old record in 
--								  case of an update. This parameter behaves differently for different 
--								  types values:
--									- When the value is NULL, the current datetime when the procedure is 
--									  called will be used. This value will be the same for all records that
--									  are inserted or closed during the run.
--									- When the value is in the format 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss'
--									  the given date (time) will be used.
--									- When the value is another format, it is assumed this refers to a 
--									  column in the source table. The value cannot be an empty string, the 
--									  column needs to exist in the source table and the column needs to be
--									  type TIMESTAMP.
-- delete_missing				- This value is a boolean.
--									- When TRUE: Records that are not present in the source table are closed 
--									  in the SCD table. is_current is set to 0 and valid_to date is set to
--									  the correct update_date. Use this when the source table always contains  
--									  a full set of current records
-- 									- When FALSE: Records that are not present in the source table stay open
--									  in the SCD table. In this case nothing is updated for these records.
--									  Use this when the source table only contains records that were changed.
---------------------------------------------------------------------------------------------------------------

DECLARE
	sqlstr 								VARCHAR(65535);

	column_list_clean					VARCHAR(65535);
	column_list_formatted				VARCHAR(65535);
	column_list_with_src_prefix			VARCHAR(65535);
	column_list_with_target_prefix		VARCHAR(65535);
	compare_columns_for_scd_update		VARCHAR(65535);
	compare_columns_for_simple_update	VARCHAR(65535);
	update_columns_for_simple_update	VARCHAR(65535);
	
	update_date_type					SMALLINT;
	update_date_sqlstr					VARCHAR(65535);
	update_date_sqlstr_for_delete		VARCHAR(65535);

	c									VARCHAR(512);
	n									INT;
	i									INT = 1;

	verbose								BOOLEAN = 0;
	run_execute							BOOLEAN = 1;
	
BEGIN
	
	IF verbose = 1 THEN
		RAISE NOTICE 'target_table              = %', target_table;
		RAISE NOTICE 'source_table              = %', source_table;
		RAISE NOTICE 'key_column                = %', key_column;
		RAISE NOTICE 'scd_update_column_list    = %', scd_update_column_list;
		RAISE NOTICE 'simple_update_column_list = %', simple_update_column_list;
		RAISE NOTICE 'update_date               = %', update_date;
		RAISE NOTICE 'delete_missing            = %', delete_missing;
		RAISE NOTICE ' ';
	END IF;
	

-------------------------------------------------------
-- Pre processing
-------------------------------------------------------

	IF 	   COALESCE(TRIM(target_table), '') = '' 
		OR COALESCE(TRIM(source_table), '') = '' 
		OR COALESCE(TRIM(key_column), '') = ''
		OR COALESCE(TRIM(scd_update_column_list), '') = '' 
		OR update_date = '' THEN 
			RAISE EXCEPTION 'Error: parameters target_table, source_table, key_column, scd_update_column_list cannot be empty';
	END IF;
	
	IF update_date LIKE '____-__-__' OR update_date LIKE '____-__-__ __:__:__' THEN
		update_date_type := 2;
		update_date_sqlstr := '''' + update_date + '''::TIMESTAMP';
		update_date_sqlstr_for_delete := update_date_sqlstr;
	ELSEIF NULLIF(update_date, '') IS NOT NULL THEN 
		update_date_type := 1;
		update_date_sqlstr := 'src."' + update_date + '"';
		update_date_sqlstr_for_delete := '''' + GETDATE()::VARCHAR(19) + '''::TIMESTAMP';
	ELSE
		update_date_type := 0;
		update_date_sqlstr := '''' + GETDATE()::VARCHAR(19) + '''::TIMESTAMP';
		update_date_sqlstr_for_delete := update_date_sqlstr;
	END IF;

	IF verbose = 1 THEN
		RAISE NOTICE 'update_date_type = %', update_date_type;
		RAISE NOTICE 'update_date_sqlstr = %', update_date_sqlstr;
		RAISE NOTICE 'update_date_sqlstr_for_delete = %', update_date_sqlstr_for_delete;
		RAISE NOTICE ' ';
	END IF;


-- Check update date

	IF update_date_type = 1 THEN
		sqlstr = '
			CREATE TABLE #valid_from_errors AS
			SELECT src.' + update_date + '
			FROM ' + source_table + ' src
			INNER JOIN ' + target_table + ' target
				ON  src."' + key_column + '"  = target."' + key_column + '"
				AND target.valid_to = ''9999-12-31''
				AND src.' + update_date + ' < target.valid_from';
	ELSE
		sqlstr = '
			CREATE TABLE #valid_from_errors AS
			SELECT valid_from
			FROM ' + target_table + '
			WHERE valid_from > ''' + update_date + '''';
	END IF;

	DROP TABLE IF EXISTS #valid_from_errors;
	EXECUTE sqlstr;
	
	IF EXISTS (SELECT TOP 1 * FROM #valid_from_errors) THEN
		RAISE EXCEPTION 'Error: valid_from date is before existing record.';
	END IF;
	

-- Create column lists
	
	column_list_clean := REPLACE(REPLACE(REPLACE(REPLACE(scd_update_column_list, CHR(9), ''), CHR(10), ''), CHR(13), ''), CHR(32), '');

	n := REGEXP_COUNT(column_list_clean, ',') + 1;
	WHILE i <= n LOOP
		c := SPLIT_PART(column_list_clean, ',', i);
		
		IF column_list_formatted IS NULL THEN 
			column_list_formatted := '"' + c + '"';
			column_list_with_src_prefix := 'src."' + c + '"';
			column_list_with_target_prefix := 'target."' + c + '"';
			compare_columns_for_scd_update := 'src."' + c + '" <> target."' + c + '"';
			compare_columns_for_scd_update := compare_columns_for_scd_update 
												+ CHR(10) + REPEAT(CHR(9), 2) 
												+ 'OR    CASE WHEN src."' + c 
												+ '" IS NULL THEN ''y'' ELSE '''' END + CASE WHEN target."' + c 
												+ '" IS NULL THEN ''y'' ELSE '''' END = ''y''';
		ELSE
			column_list_formatted := column_list_formatted + ',' + CHR(10) + REPEAT(CHR(9), 3) + '"' + c + '"';
			column_list_with_src_prefix := column_list_with_src_prefix + ',' + CHR(10) + REPEAT(CHR(9), 3) + 'src."' + c + '"';
			column_list_with_target_prefix := column_list_with_target_prefix + ',' + CHR(10) + REPEAT(CHR(9), 3) + 'target."' + c + '"';
			compare_columns_for_scd_update := compare_columns_for_scd_update + CHR(10) + REPEAT(CHR(9), 2) + 'OR    src."' + c + '" <> target."' + c + '"';
			compare_columns_for_scd_update := compare_columns_for_scd_update 
												+ CHR(10) + REPEAT(CHR(9), 2) 
												+ 'OR    CASE WHEN src."' + c 
												+ '" IS NULL THEN ''y'' ELSE '''' END + CASE WHEN target."' + c 
												+ '" IS NULL THEN ''y'' ELSE '''' END = ''y''';
		END IF;

		i := i + 1;
	END LOOP;

	IF verbose = 1 THEN
		RAISE NOTICE ' ';
		RAISE NOTICE '--- column_list_formatted';
		RAISE NOTICE '%', column_list_formatted;
		RAISE NOTICE ' ';
		RAISE NOTICE '--- column_list_with_src_prefix';
		RAISE NOTICE '%', column_list_with_src_prefix;
		RAISE NOTICE ' ';
		RAISE NOTICE '--- column_list_with_target_prefix';
		RAISE NOTICE '%', column_list_with_target_prefix;
		RAISE NOTICE ' ';
		RAISE NOTICE '--- compare_columns_for_scd_update';
		RAISE NOTICE '%', compare_columns_for_scd_update;
	END IF;

	column_list_clean := REPLACE(REPLACE(REPLACE(REPLACE(simple_update_column_list, CHR(9), ''), CHR(10), ''), CHR(13), ''), CHR(32), '');

	n := REGEXP_COUNT(column_list_clean, ',') + 1;
	i = 1;
	WHILE i <= n LOOP
		c := SPLIT_PART(column_list_clean, ',', i);
		
		column_list_formatted := column_list_formatted + ',' + CHR(10) + REPEAT(CHR(9), 3) + '"' + c + '"';
		column_list_with_src_prefix := column_list_with_src_prefix + ',' + CHR(10) + REPEAT(CHR(9), 3) + 'src."' + c + '"';
		column_list_with_target_prefix := column_list_with_target_prefix + ',' + CHR(10) + REPEAT(CHR(9), 3) + 'target."' + c + '"';
	
		IF compare_columns_for_simple_update IS NULL THEN 
			compare_columns_for_simple_update := 'src."' + c + '" <> target."' + c + '"';
			compare_columns_for_simple_update := compare_columns_for_simple_update 
													+ CHR(10) + REPEAT(CHR(9), 2) 
													+ 'OR    CASE WHEN src."' + c 
													+ '" IS NULL THEN ''y'' ELSE '''' END + CASE WHEN target."' + c 
													+ '" IS NULL THEN ''y'' ELSE '''' END = ''y''';
												
			update_columns_for_simple_update := '"' + c + '" = src."' + c  + '"';
		ELSE
			compare_columns_for_simple_update := compare_columns_for_simple_update + CHR(10) + REPEAT(CHR(9), 2) + 'OR    src."' + c + '" <> target."' + c + '"';
			compare_columns_for_simple_update := compare_columns_for_simple_update 
													+ CHR(10) + REPEAT(CHR(9), 2) 
													+ 'OR    CASE WHEN src."' + c 
													+ '" IS NULL THEN ''y'' ELSE '''' END + CASE WHEN target."' + c 
													+ '" IS NULL THEN ''y'' ELSE '''' END = ''y''';
												
			update_columns_for_simple_update = update_columns_for_simple_update + ',' + CHR(10) + REPEAT(CHR(9), 3) + '"' + c + '" = src."' + c  + '"';
		END IF;

		i := i + 1;
	END LOOP;

	IF verbose = 1 THEN
		RAISE NOTICE ' ';
		RAISE NOTICE '--- compare_columns_for_simple_update';
		RAISE NOTICE '%', compare_columns_for_simple_update;
		RAISE NOTICE ' ';
		RAISE NOTICE '--- update_columns_for_simple_update';
		RAISE NOTICE '%', update_columns_for_simple_update;
	END IF;


-- Check source table

	DROP TABLE IF EXISTS #duplicates;

	sqlstr := '
		CREATE TABLE #duplicates AS
		SELECT src."' + key_column + '"
		FROM ' + source_table + ' src
		GROUP BY src."' + key_column + '"
		HAVING COUNT(*)>1;';
	
	IF verbose = 1 THEN
		RAISE NOTICE '%', sqlstr;
	END IF;
	
	EXECUTE sqlstr;

	n := (SELECT COUNT(*) FROM #duplicates);

	IF n > 0 THEN
		RAISE EXCEPTION 'Error: % duplicate keys found in source table', n;
	END IF;

-------------------------------------------------------
-- SCD update
-------------------------------------------------------

-- Prepare records to be updated
	
	IF run_execute = 1 THEN
		DROP TABLE IF EXISTS #to_be_updated;
	END IF;

	sqlstr := '
		CREATE TABLE #to_be_updated AS
		SELECT
			src."' + key_column + '",
			' + column_list_with_src_prefix + ',
			' + update_date_sqlstr + '	AS valid_from,
			''9999-12-31''::TIMESTAMP 	AS valid_to,
			TRUE::BOOLEAN				AS is_current
		FROM ' + source_table + ' src
		LEFT JOIN ' + target_table + ' target
			ON  src."' + key_column + '"  = target."' + key_column + '"
			AND target.valid_to = ''9999-12-31''
		WHERE  target."' + key_column + '" IS NOT NULL
		AND   ' + update_date_sqlstr + ' >= target.valid_from
		AND  (' + compare_columns_for_scd_update + ');';

	IF verbose = 1 THEN
		RAISE NOTICE '%', sqlstr;
	END IF;

	IF run_execute = 1 THEN
		EXECUTE sqlstr;
	END IF;


-- Close old versions (needs to be done before inserting new versions)
									
	sqlstr := '
		UPDATE ' + target_table + ' target
		SET valid_to = src.valid_from,
			is_current = FALSE
		FROM #to_be_updated src
		WHERE src."' + key_column + '" = target."' + key_column + '"
		AND   target.valid_to = ''9999-12-31'';';

	IF verbose = 1 THEN
		RAISE NOTICE '%', sqlstr;
	END IF;

	IF run_execute = 1 THEN
		EXECUTE sqlstr;
	END IF;


-- Insert new versions

	sqlstr := '
		INSERT INTO ' + target_table + ' (
			"' + key_column + '",
			' + column_list_formatted + ',
			valid_from,
			valid_to,
			is_current
		)
		SELECT 
			' + key_column + ',
			' + column_list_formatted + ',
			valid_from,
			valid_to,
			is_current
		FROM #to_be_updated;';

	IF verbose = 1 THEN
		RAISE NOTICE '%', sqlstr;
	END IF;

	IF run_execute = 1 THEN
		EXECUTE sqlstr;
	END IF;


-------------------------------------------------------
-- Simple update
-------------------------------------------------------

	sqlstr := '
		UPDATE ' + target_table + ' target
		SET ' + update_columns_for_simple_update + '
		FROM ' + source_table + ' src
		--LEFT JOIN #to_be_updated scd_update
		--	ON scd_update."' + key_column + '" = src."' + key_column + '"
		WHERE src."' + key_column + '" = target."' + key_column + '"
		--AND   scd_update."' + key_column + '" IS NULL
		AND  (' + compare_columns_for_simple_update + ');';

	IF verbose = 1 THEN
		IF sqlstr IS NULL THEN
			RAISE NOTICE ' '; 
			RAISE NOTICE 'No simple update columns.'; 
		ELSE
			RAISE NOTICE '%', sqlstr;
		END IF;
	END IF;

	IF run_execute = 1 AND sqlstr IS NOT NULL THEN
		EXECUTE sqlstr;
	END IF;


-------------------------------------------------------
-- Insert new records
-------------------------------------------------------
	
	sqlstr := '
		INSERT INTO ' + target_table + ' (
			"' + key_column + '",
			' + column_list_formatted + ',
			valid_from,
			valid_to,
			is_current
		)
		SELECT 
			src."' + key_column + '",
			' + column_list_with_src_prefix + ',
			' + update_date_sqlstr + ' 	AS valid_from,
			''9999-12-31''::TIMESTAMP 	AS valid_to,
			TRUE::BOOLEAN				AS is_current
		FROM ' + source_table + ' src
		LEFT JOIN ' + target_table + ' target
			ON src."' + key_column + '" = target."' + key_column + '"
		WHERE target."' + key_column + '" IS NULL;';
	
	IF verbose = 1 THEN
		RAISE NOTICE '%', sqlstr;
	END IF;

	IF run_execute = 1 THEN
		EXECUTE sqlstr;
	END IF;


-------------------------------------------------------
-- Close deleted records
-------------------------------------------------------
	
	sqlstr := '
		UPDATE ' + target_table + ' target
		SET valid_to = ' + update_date_sqlstr_for_delete + ',
			is_current = FALSE
		FROM (
			SELECT target."' + key_column + '"
			FROM ' + target_table + ' target
			LEFT JOIN ' + source_table + ' src
			ON  src."' + key_column + '" = target."' + key_column + '"
			WHERE src."' + key_column + '" IS NULL
		) deleted
		WHERE target."' + key_column + '" = deleted."' + key_column + '"
		AND   target.valid_to = ''9999-12-31'';';
	
	IF delete_missing = 1 THEN
		IF verbose = 1 THEN
			RAISE NOTICE '%', sqlstr;
		END IF;
	
		IF run_execute = 1 THEN
			EXECUTE sqlstr;
		END IF;
	END IF;
	
END;

$$;
