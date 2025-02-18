DO $$ 
DECLARE
    table_name text;
    table_list text[] := ARRAY[
     'sewerage_piping_chamber',
      'sewerage_piping_plots',
      'sewerage_piping_shutoff_valve',
      'sewerage_piping_corrosion_protec',
      'sewerage_piping_device',
      'sewerage_piping_aux_equipment' -- Добавьте сюда все нужные таблицы
    ];
    table_id bigint;
    log_table_name text;
    log_guid uuid;
    org_id bigint;
    main_table_name text := 'sewerage_piping';  -- Добавьте сюда "головную" таблицу
    main_table_id_column text := 'sewerage_pipingId';  
BEGIN
    -- 1. Создаем таблицы связи для каждой из таблиц из списка
    FOREACH table_name IN ARRAY table_list
    LOOP
        -- Создаем таблицу связи с именем "_<название_таблицы>_org"
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS dictionaries_ru_6_42._%I_org (
    id SERIAL PRIMARY KEY,  
    create_date TIMESTAMP,
    audit_date TIMESTAMP,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    "orgId" BIGINT,
    "%IId" BIGINT
)', table_name, table_name);

        -- 2. Генерация лог-таблицы для каждой из таблиц
        log_guid := gen_random_uuid();
        log_table_name := format('log_%s', log_guid);

        EXECUTE format('
            CREATE TABLE IF NOT EXISTS dictionaries_ru_6_42.%I (
                "ParentId" bigint,
                "AuditChangeItemId" bigint,
                "FieldName" text,
                "Value" text,
                PRIMARY KEY ("ParentId", "AuditChangeItemId", "FieldName"),  
                FOREIGN KEY ("ParentId") REFERENCES dictionaries_ru_6_42._%I_org(id),
                FOREIGN KEY ("AuditChangeItemId") REFERENCES dictionaries_ru_6_42."$change_item"("Id")
            )', log_table_name, table_name);

       

        -- 3. Заполняем данные в таблицу связи "_<название_таблицы>_org"
        EXECUTE format('
            INSERT INTO dictionaries_ru_6_42._%I_org ( 
                create_date, audit_date, start_date, end_date, "orgId", "%IId"
            )
            SELECT 
                ''0001-01-01 00:00:00.000''::timestamp AS create_date,
                ''0001-01-01 00:00:00.000''::timestamp AS audit_date,
                ''0001-01-01 00:00:00.000''::timestamp AS start_date,
                ''9999-12-31 23:59:59.999''::timestamp AS end_date,
                main."orgId" AS orgId,
                link."%IId" AS "%IId"
            FROM dictionaries_ru_6_42._%I_%I AS link
            JOIN dictionaries_ru_6_42._%I_org AS main
                ON link."%IId" = main."%IId"
            WHERE main."%IId" IS NOT NULL',
            table_name, table_name, table_name, table_name, table_name, main_table_name, main_table_name, main_table_name, main_table_name, main_table_name);

        -- 4. Добавление внешних ключей для новых таблиц
        EXECUTE format('
            ALTER TABLE dictionaries_ru_6_42._%I_org
            ADD CONSTRAINT fk_orgId FOREIGN KEY ("orgId") REFERENCES dictionaries_ru_6_42.org(id);', table_name);
        

        EXECUTE format('
            ALTER TABLE dictionaries_ru_6_42._%I_org
            ADD CONSTRAINT fk_%IId FOREIGN KEY ("%IId") REFERENCES dictionaries_ru_6_42.%I(id);', table_name, table_name, table_name, table_name);
        
        -- 5. Добавляем информацию о созданной таблице в dictionaries_service_ru_6_42.Dictionaries
        EXECUTE format('
            INSERT INTO dictionaries_service_ru_6_42."Dictionaries" ("Name", "Type")
            VALUES (''_%I_org'', ''relation'') RETURNING "Id"', table_name) INTO table_id;

        EXECUTE format('
            INSERT INTO dictionaries_service_ru_6_42."Dictionaries" ("Name", "Type", "RelatedDictionaryId")
            VALUES (''%s'', ''history'', (SELECT "Id" FROM dictionaries_service_ru_6_42."Dictionaries" WHERE "Name" = ''_%I_org'')) ', log_table_name, table_name);
        
        -- 6. Добавляем столбцы созданной таблицы в dictionaries_service_ru_6_42.DictionariesProperties
        EXECUTE format('
            INSERT INTO dictionaries_service_ru_6_42."DictionariesProperties" (
                "Name", "DictionariesId", "DictionariesTypesId", "DictionariesRelationsId", "Order"
            ) VALUES
                (''id'', %s, 1, NULL, 0),
                (''create_date'', %s, 5, NULL, 0),
                (''audit_date'', %s, 5, NULL, 0),
                (''start_date'', %s, 5, NULL, 0),
                (''end_date'', %s, 5, NULL, 0),
                (''org'', %s, 3, 29, 0),
                (''%I'', %s, 3, (SELECT "Id" FROM dictionaries_service_ru_6_42."Dictionaries" WHERE "Name" = ''%I''), 0);',
                table_id, table_id, table_id, table_id, table_id, table_id, table_name, table_id, table_name);
        
        -- 7. Создаем представление в схеме p4e_registry_ru_6_42
        EXECUTE format('
            CREATE OR REPLACE VIEW p4e_registry_ru_6_42._%I_org AS 
            SELECT 
                COALESCE(id, 0::bigint) AS id,
                COALESCE(create_date, ''0001-01-01 00:00:00''::timestamp without time zone) AS create_date,
                COALESCE(audit_date, ''0001-01-01 00:00:00''::timestamp without time zone) AS audit_date,
                COALESCE(start_date, ''0001-01-01 00:00:00''::timestamp without time zone) AS start_date,
                COALESCE(end_date, ''0001-01-01 00:00:00''::timestamp without time zone) AS end_date,
                "orgId",
                "%IId" 
            FROM dictionaries_ru_6_42._%I_org', table_name, table_name, table_name, table_name);

    EXECUTE format('
    CREATE OR REPLACE VIEW p4e_registry_ru_6_42.%s AS 
    SELECT 
        "ParentId",
        "AuditChangeItemId",
        "FieldName",
        "Value"
    FROM dictionaries_ru_6_42.%s',
    quote_ident('$' || log_table_name), quote_ident(log_table_name));
    END LOOP;
END $$;


DO $$
DECLARE
    -- Массив имен справочников, которые нужно обработать
    dict_name text;
    dict_names text []:= ARRAY ['boiler_house', 
      'boiler',
      'boiler_device',
      'boiler_building',
      'boiler_chimney',
      'boiler_pump',
      'boiler_fan',
      'boiler_draft_device',
      'boiler_burner',
      'boiler_slag_ash_removal_system',
      'boiler_aux_equipment',
      'boiler_counter',
      'boiler_gas_facility',
      'boiler_gas_facility_levels',
      'boiler_masut_facility',
      'boiler_masut_facility_tank',
      'boiler_masut_facility_pipe',
      'boiler_masut_facility_shirts',
      'boiler_masut_facility_pump',
      'boiler_diesel_facility',
      'boiler_diesel_facility_tank',
      'boiler_diesel_facility_pipe',
      'boiler_diesel_facility_pump',
      'boiler_solidfuel_facility',
      'boiler_solidfuel_facility_belt',
      'boiler_solidfuel_facility_grinder',
      'boiler_solidfuel_facility_sift',
      'boiler_solidfuel_facility_storage',
      'boiler_solidfuel_facility_mill',
      'boiler_solidfuel_facility_pipe',
      'boiler_solidfuel_facility_tank',
      'temp_graph',
      'tpp', 
      'boiler_device',
      'boiler_building',
      'boiler_chimney',
      'boiler_pump',
      'boiler_fan',
      'boiler_draft_device',
      'boiler_burner',
      'boiler_slag_ash_removal_system',
      'boiler_aux_equipment',
      'boiler_counter',
      'tpp_power_boiler',
      'tpp_water_boiler',
      'tpp_turbine',
      'tpp_capacitor',
      'tpp_pgu_gtu',
      'tpp_generator',
      'tpp_cooling_tower',
      'boiler_gas_facility',
      'boiler_gas_facility_levels',
      'boiler_masut_facility',
      'boiler_masut_facility_tank',
      'boiler_masut_facility_pipe',
      'boiler_masut_facility_shirts',
      'boiler_masut_facility_pump',
      'boiler_diesel_facility',
      'boiler_diesel_facility_tank',
      'boiler_diesel_facility_pipe',
      'boiler_diesel_facility_pump',
      'boiler_solidfuel_facility',
      'boiler_solidfuel_facility_belt',
      'boiler_solidfuel_facility_grinder',
      'boiler_solidfuel_facility_sift',
      'boiler_solidfuel_facility_storage',
      'boiler_solidfuel_facility_mill',
      'boiler_solidfuel_facility_pipe',
      'boiler_solidfuel_facility_tank',
      'temp_graph',
    'heat_network', 
      'heat_network_node',
      'heat_network_sector',
      'heat_network_shutoff_valve',
      'temp_graph',
    'heat_points', 
      'heat_points_shutoff_valve',
      'heat_points_pump',
      'heat_points_heater',
      'heat_points_device',
      'heat_points_aux_equipment',
    'heat_pump_station', 
      'heat_pump_station_shutoff_valve',
      'heat_pump_station_pump',
      'heat_pump_station_aux_equipment',
    'st_territory', 
'water_pump_station', 
      'water_pump_ventilation',
      'water_pump_aux_pump',
      'water_pump_pumps',
      'water_pump_building',
      'water_pump_tank',
      'water_pump_shutoff_valve',
      'water_pump_aux_equipment',
      'water_pump_freq_convert',
      'water_pump_pipe',
      'water_pump_grid',
      'water_pump_device',
      'water_pump_aux_trans',
    'water_treat_facility', 
      'water_treat_facility_heater',
      'water_treat_facility_freq_convert',
      'water_treat_facility_ventilation',
      'water_treat_facility_pipe',
      'water_treat_facility_pump',
      'water_treat_facility_indicator_reagent',
      'water_treat_facility_ozone_station',
      'water_treat_facility_dryers',
      'water_treat_facility_shutoff_valve',
      'water_treat_facility_t_compressors',
      'water_treat_facility_clarifiers',
      'water_treat_facility_filters',
      'water_treat_facility_aux_equipment',
      'water_treat_facility_agitators',
      'water_treat_facility_device',
      'water_treat_facility_building',
      'water_treat_facility_stage',
      'water_treat_facility_tank',
      'artesian_well', 
      'artesian_well_treat_facility',
      'artesian_well_pipe',
      'artesian_well_shutoff_valve',
      'artesian_well_device',
      'artesian_well_freq_convert',
      'artesian_well_pump',
      'artesian_well_building',
      'artesian_well_tank',
      'artesian_well_aux_equipment',
    'water_piping', 
      'water_piping_plots',
      'water_piping_chamber',
      'water_piping_shutoff_valve',
      'water_piping_corrosion_protec',
      'water_piping_aux_equipment',
      'water_piping_device',
    'water_supply',
    'sewerage_pump_station', 
      'sewerage_pump_pumps',
      'sewerage_pump_freq_convert',
      'sewerage_pump_aux_equipment',
      'sewerage_pump_ventilation',
      'sewerage_pump_building',
      'sewerage_pump_grid',
      'sewerage_pump_pipe',
      'sewerage_pump_switchgear',
      'sewerage_pump_trans',
      'sewerage_pump_device',
      'sewerage_pump_shutoff_valve',
      'sewerage_pump_aux_pump',
    'sewerage_treat_facility', 
      'sewerage_treat_facility_scrapers',
      'sewerage_treat_facility_boiler',
      'sewerage_treat_facility_pipe',
      'sewerage_treat_facility_shutoff_valve',
      'sewerage_treat_facility_grid',
      'sewerage_treat_facility_chamber',
      'sewerage_treat_facility_filters',
      'sewerage_treat_facility_pump',
      'sewerage_treat_facility_t_compressors',
      'sewerage_treat_facility_freq_convert',
      'sewerage_treat_facility_waste_charact',
      'sewerage_treat_facility_trans',
      'sewerage_treat_facility_cable',
      'sewerage_treat_facility_agitators',
      'sewerage_treat_facility_building',
      'sewerage_treat_facility_tank',
      'sewerage_treat_facility_sandtrap',
      'sewerage_treat_facility_aerotanks',
      'sewerage_treat_facility_methane_tanks',
      'sewerage_treat_facility_generator',
      'sewerage_treat_facility_thickeners',
      'sewerage_treat_facility_ventilation',
      'sewerage_treat_facility_switchgear',
      'sewerage_treat_facility_aux_equipment',
      'sewerage_treat_facility_device',
      'sew_treat_facility_water',
    'sewerage_piping', 
      'sewerage_piping_chamber',
      'sewerage_piping_plots',
      'sewerage_piping_shutoff_valve',
      'sewerage_piping_corrosion_protec',
      'sewerage_piping_device',
      'sewerage_supply_purpose',
      'sewerage_piping_aux_equipment',
    'sewerage_supply'] ; 
    dict_id integer;
BEGIN
    -- Проходим по всем названиям справочников
    FOREACH dict_name IN ARRAY dict_names
    LOOP
        -- Получаем Id справочника по имени
        SELECT "Id" INTO dict_id
        FROM dictionaries_service_ru_6_42."Dictionaries"
        WHERE "Name" = dict_name
        LIMIT 1;

        -- Проверяем, найден ли справочник
        IF FOUND THEN
            -- Проверяем, есть ли запись 'org' в таблице DictionariesProperties для данного DictionariesId
            IF NOT EXISTS (
                SELECT 1
                FROM dictionaries_service_ru_6_42."DictionariesProperties"
                WHERE "DictionariesId" = dict_id
                  AND "Name" = 'org'
            ) THEN
                -- Вставляем новую запись в таблицу DictionariesProperties
                INSERT INTO dictionaries_service_ru_6_42."DictionariesProperties" (
                    "Name",
                    "Name_i18n",
                   "Description",
                    "DictionariesId",
                    "DictionariesTypesId",
                    "DictionariesRelationsId",
                    "IsNullable",
                    "MultipleRelation",
                    "Order",
                    "PrimaryKey"
                ) VALUES (
                    'org', -- Name
                    'Организация Реестра ФАС', -- Name_i18n
                    NULL, -- Description
                    dict_id, -- DictionariesId
                    3, -- DictionariesTypesId
                    29, -- DictionariesRelationsId
                    TRUE, -- IsNullable
                    TRUE, -- MultipleRelation
                    0, -- Order
                    FALSE -- PrimaryKey
                );
            END IF;
        END IF;
    END LOOP;
END $$;