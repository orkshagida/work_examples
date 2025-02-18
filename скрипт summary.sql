DO $$ 
DECLARE 
    view_name TEXT; 
    table_suffix TEXT; 
    table_prefix TEXT; 
    corresponding_table TEXT; 
    full_table_name TEXT; 
    output_line TEXT; 
    total_columns INT; 
    current_index INT;
    full_data_type TEXT; 
    column_rec RECORD; 
    view_mappings TEXT[][] := ARRAY[
        -- Сюда необходимо вставить названия вью и индекс названия соответсвующей НТП витрины
        ['vu_y_w_p14_bal_pr', 'balpr'],
        ['vu_y_w_p14_bal_tr', 'baltr'],
        ['vu_y_w_p14_calc', 'calc'],
        ['vu_y_w_p14_calc_energy', 'calc'],
        ['vu_y_w_p14_fuel', 'fuel'],
        ['vu_y_w_p14_list_org', 'org'],
        ['vu_y_w_p14_tm1', 'tm1'],
        ['vu_y_w_p14_tm2', 'tm2'],
        ['vu_y_vo_p14_bal_pr', 'balpr'],
        ['vu_y_vo_p14_bal_tr', 'baltr'],
        ['vu_y_vo_p14_calc', 'calc'],
        ['vu_y_vo_p14_calc_energy', 'calc'],
        ['vu_y_vo_p14_list_org', 'org'],
        ['vu_y_vo_p14_tm1', 'tm1'],
        ['vu_y_vo_p14_tm2', 'tm2'],
        ['vu_y_vs_p14_bal_pr', 'balpr'],
        ['vu_y_vs_p14_bal_tr', 'baltr'],
        ['vu_y_vs_p14_calc', 'calc'],
        ['vu_y_vs_p14_calc_energy', 'calc'],
        ['vu_y_vs_p14_list_org', 'org'],
        ['vu_y_vs_p14_tm1', 'tm1'],
        ['vu_y_vs_p14_tm2', 'tm2'],
        ['vu_y_hvs_p14_list_org', 'org'],
        ['vu_y_hvs_p14_tm1', 'tm1'],
        ['vu_y_hvs_p14_tm2', 'tm2'],
        ['vu_y_otko_p14_bal_ro', 'bro'],
        ['vu_y_otko_p14_calc', 'ko'],
        ['vu_y_otko_p14_calc_ro', 'kro'],
        ['vu_y_otko_p14_list_org', 'org'],
        ['vu_y_otko_p14_tmx_o', 'to'],
        ['vu_y_otko_p14_tmx_ro', 'tro']
    ];
    current_view_code TEXT;
    assistent_schema TEXT := 'assistent_odata_ru_2_73'; -- схема с вью
BEGIN
    FOR i IN 1..array_length(view_mappings, 1) LOOP

        view_name := view_mappings[i][1];
        table_suffix := view_mappings[i][2];

IF view_name LIKE '%_hvs_%' THEN
    table_prefix := 'hvs';
ELSIF view_name LIKE '%_vs_%' THEN
    table_prefix := 'vs';
ELSIF view_name LIKE '%_vo_%' THEN
    table_prefix := 'vo';
ELSIF view_name LIKE '%_w_%' THEN
    table_prefix := 'w';
ELSE
    table_prefix := 'otko';
END IF;

 
        full_table_name := FORMAT('fdb_%s_p14_%s', table_prefix, table_suffix);


        SELECT COUNT(*)
        INTO total_columns
        FROM information_schema.columns
        WHERE table_schema = assistent_schema AND table_name = view_name;

        current_index := 0; 

        SELECT definition
        INTO current_view_code
        FROM pg_views
        WHERE schemaname = assistent_schema
        AND viewname = view_name;

        current_view_code := RTRIM(current_view_code, ';');

        RAISE NOTICE 'CREATE OR REPLACE VIEW %.% AS', assistent_schema, view_name;
        RAISE NOTICE '%', current_view_code;
        RAISE NOTICE 'UNION ALL';
        RAISE NOTICE 'SELECT';

        FOR column_rec IN 
            SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = assistent_schema AND table_name = view_name
        LOOP
            current_index := current_index + 1; 

            -- Исключения для столбцов из title
            IF column_rec.column_name = 'entity_id' THEN 
                output_line := 'r.fas_region_id AS entity_id';
            ELSIF column_rec.column_name = 'scenario_name' THEN
                output_line := 'ti.scenario_name::varchar(255) AS scenario_name';
            ELSIF column_rec.column_name = 'scenario_id' THEN
                output_line := 'ti.scenario_id::int8 AS scenario_id';
            ELSIF column_rec.column_name = 'form' THEN
                output_line := 'ti.form::character varying(64) AS form';
            ELSIF column_rec.column_name = 'start_date' THEN
                output_line := 'ti.start_date::timestamp without time zone AS start_date';
            ELSIF column_rec.column_name = 'end_date' THEN
                output_line := 'ti.end_date::timestamp without time zone AS end_date';
            ELSIF column_rec.column_name = 'report_date' THEN
                output_line := 'ti.report_date::timestamp without time zone AS report_date';
            ELSIF column_rec.column_name = 'entity_code' THEN
                output_line := 'ti.entity_code::character varying(40) AS entity_code';
            ELSIF column_rec.column_name = 'entity_name' THEN
                output_line := 'ti.entity_name::character varying(1500) AS entity_name';
            ELSE
                IF column_rec.data_type = 'character varying' THEN
                    full_data_type := column_rec.data_type || '(' || column_rec.character_maximum_length || ')';
                ELSIF column_rec.data_type = 'numeric' THEN
                    full_data_type := column_rec.data_type || '(' || column_rec.numeric_precision || ',' || column_rec.numeric_scale || ')';
                ELSE
                    full_data_type := column_rec.data_type; 
                END IF;

                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'tpl_data_jkh_ru_9_99' AND table_name = full_table_name AND column_name = column_rec.column_name
                ) THEN
                    output_line := FORMAT('a.%s::%s AS %s', column_rec.column_name, full_data_type, column_rec.column_name);
                ELSE
                    output_line := FORMAT('NULL::%s AS %s', full_data_type, column_rec.column_name);
                END IF;
            END IF;
       

            IF current_index < total_columns THEN
                output_line := output_line || ',';
            END IF;

            RAISE NOTICE '%', output_line;
        END LOOP;

        RAISE NOTICE 'FROM tpl_data_jkh_ru_9_99.% a', full_table_name;
        RAISE NOTICE 'JOIN tpl_data_jkh_ru_9_99.fdb_%_p14_title ti ON a.lgl_id = ti.lgl_id', table_prefix;
        RAISE NOTICE 'LEFT JOIN dictionaries.region r ON r.name = ti.entity_name::text';
        RAISE NOTICE 'WHERE r.fas_region_id = 2640;';
    END LOOP;
END $$;