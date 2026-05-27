import boto3
import pymysql
import sys
import time
import threading
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cadenat global per evitar que els fils col·lideixin en crear estacions/municipis nous
metadata_lock = threading.Lock()

# Diccionaris globals compartits en memòria per tots els fils
available_units = {}
available_gas = {}
available_station_types = {}
available_urban_areas = {}
available_comarques = {}
available_municipalities = {}
available_stations = {}


# --- 1. CONFIGURACIÓ I CREDENCIALS ---
def get_ssm_parameters():
    print("Recuperant paràmetres d'AWS Systems Manager...")
    ssm = boto3.client('ssm', region_name='us-east-1')
    try:
        params = {}
        names = [
            '/bdata-processing-server/env/DB_HOST',
            '/bdata-processing-server/env/DB_USER',
            '/bdata-processing-server/env/DB_PASS',
            '/bdata-processing-server/env/DB_NAME',
            '/bdata-processing-server/env/BUCKET_PROCESSED',
            '/bdata-processing-server/env/WORKERS'
        ]
        response = ssm.get_parameters(Names=names, WithDecryption=True)
        for param in response['Parameters']:
            params[param['Name'].split('/')[-1]] = param['Value']
        return params
    except Exception as e:
        print(f"Error recuperant credencials: {e}")
        sys.exit(1)


def get_db_connection(params):
    return pymysql.connect(
        host=params['DB_HOST'],
        user=params['DB_USER'],
        password=params['DB_PASS'],
        database=params['DB_NAME'],
        cursorclass=pymysql.cursors.DictCursor
    )


# --- 2. INICIALITZACIÓ DE DICCIONARIS (S'executa un sol cop a l'inici) ---
def init_shared_dictionaries(params):
    global available_units, available_gas, available_station_types, available_urban_areas, available_comarques, available_municipalities, available_stations
    print("Carregant catàlegs actuals de la base de dades a memòria...")
    conn = get_db_connection(params)
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, name_units FROM unit")
        available_units = {r['name_units']: r['id'] for r in cursor.fetchall()}

        cursor.execute("SELECT id, name_gas FROM pollution_gas")
        available_gas = {r['name_gas']: r['id'] for r in cursor.fetchall()}

        cursor.execute("SELECT id, name_type FROM station_type")
        available_station_types = {r['name_type']: r['id'] for r in cursor.fetchall()}

        cursor.execute("SELECT id, name_area FROM urban_area")
        available_urban_areas = {r['name_area']: r['id'] for r in cursor.fetchall()}

        cursor.execute("SELECT id, code_comarca FROM comarca")
        available_comarques = {int(r['code_comarca']): r['id'] for r in cursor.fetchall()}

        cursor.execute("SELECT id, ine_code FROM municipality")
        available_municipalities = {r['ine_code']: r['id'] for r in cursor.fetchall()}

        cursor.execute("SELECT id, eoi_code FROM station")
        available_stations = {r['eoi_code']: r['id'] for r in cursor.fetchall()}
    conn.close()


# --- 3. GESTIÓ DINÀMICA DE METADADES (Thread-Safe amb Lock) ---
def sync_missing_metadata(df, params):
    global available_units, available_gas, available_station_types, available_urban_areas, available_comarques, available_municipalities, available_stations

    # Adquirim el cadenat: si un altre fil està aquí, els altres s'esperen en cua
    with metadata_lock:
        conn = get_db_connection(params)
        cursor = conn.cursor()

        # Eliminem duplicats del subset per comprovar ràpidament
        df_meta = df.drop_duplicates(subset=['codi_eoi', 'contaminant']).copy()

        for _, row in df_meta.iterrows():
            # 1. Unitats
            u_name = row.get('unitats')
            if u_name and u_name not in available_units:
                cursor.execute("INSERT INTO unit (name_units) VALUES (%s)", (u_name,))
                conn.commit()
                available_units[u_name] = cursor.lastrowid

            # 2. Gasos (Contaminants)
            g_name = row.get('contaminant')
            if g_name and g_name not in available_gas:
                mag = int(row['magnitud']) if pd.notnull(row['magnitud']) else 0
                cursor.execute("INSERT INTO pollution_gas (name_gas, magnitude, id_unit) VALUES (%s, %s, %s)",
                               (g_name, mag, available_units[u_name]))
                conn.commit()
                available_gas[g_name] = cursor.lastrowid

            # 3. Tipus Estació
            t_name = row.get('tipus_estacio')
            if t_name and t_name not in available_station_types:
                cursor.execute("INSERT INTO station_type (name_type) VALUES (%s)", (t_name,))
                conn.commit()
                available_station_types[t_name] = cursor.lastrowid

            # 4. Àrees Urbanes
            a_name = row.get('area_urbana')
            if a_name and a_name not in available_urban_areas:
                cursor.execute("INSERT INTO urban_area (name_area) VALUES (%s)", (a_name,))
                conn.commit()
                available_urban_areas[a_name] = cursor.lastrowid

            # 5. Comarques
            c_code = int(row['codi_comarca']) if pd.notnull(row['codi_comarca']) else None
            c_name = row.get('nom_comarca')
            if c_code and c_code not in available_comarques:
                cursor.execute("INSERT INTO comarca (code_comarca, name_comarca) VALUES (%s, %s)", (c_code, c_name))
                conn.commit()
                available_comarques[c_code] = cursor.lastrowid

            # 6. Municipis
            m_ine = row.get('codi_ine')
            m_name = row.get('municipi')
            if m_ine and m_ine not in available_municipalities:
                id_c = available_comarques.get(c_code)
                cursor.execute("INSERT INTO municipality (ine_code, name_municipality, id_comarca) VALUES (%s, %s, %s)",
                               (m_ine, m_name, id_c))
                conn.commit()
                available_municipalities[m_ine] = cursor.lastrowid

            # 7. Estacions
            e_code = str(row.get('codi_eoi'))
            if e_code and e_code not in available_stations:
                alt = int(row['altitud']) if pd.notnull(row['altitud']) else 0
                lat = float(row['latitud']) if pd.notnull(row['latitud']) else 0.0
                lon = float(row['longitud']) if pd.notnull(row['longitud']) else 0.0

                cursor.execute("""
                               INSERT INTO station (eoi_code, name_station, id_urban_area, id_station_type,
                                                    id_municipality, altitude, latitude, longitude)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                               """, (e_code, row.get('nom_estacio'), available_urban_areas.get(a_name),
                                     available_station_types.get(t_name), available_municipalities.get(m_ine), alt, lat,
                                     lon))
                conn.commit()
                print(f"[Autodiscovery] S'ha detectat i afegit la nova estació: {row.get('nom_estacio')} ({e_code})")
                available_stations[e_code] = cursor.lastrowid

        cursor.close()
        conn.close()


# --- 4. PROCÉS DIARI DIRECTE AMB ATHENA ---
def process_single_day(process_date, params):
    fecha_str = process_date.strftime('%Y-%m-%d')
    print(f"[{fecha_str}] Iniciant enviament a Athena...")

    athena = boto3.client('athena', region_name='us-east-1')
    s3_path_output = f"s3://{params['BUCKET_PROCESSED']}/athena-results/"

    # Query dinàmica conservant metadades per al descobriment automàtic
    query = f"""
        WITH cleaned_lines AS (
            SELECT 
                regexp_replace(regexp_replace(regexp_replace(trim(linea_text), '^,', ''), '^\\[', ''), '\\]$', '') AS json_clean
            FROM la_teva_base_de_dades_glue.air_quality_raw_text
            WHERE linea_text IS NOT NULL
        ),
        parsed_data AS (
            SELECT TRY(json_parse(json_clean)) AS registre
            FROM cleaned_lines
            WHERE json_clean LIKE '{{%'
        ),
        flattened_data AS (
            SELECT 
                json_extract_scalar(registre, '$.codi_eoi') AS codi_eoi,
                json_extract_scalar(registre, '$.nom_estacio') AS nom_estacio,
                json_extract_scalar(registre, '$.tipus_estacio') AS tipus_estacio,
                json_extract_scalar(registre, '$.area_urbana') AS area_urbana,
                json_extract_scalar(registre, '$.codi_comarca') AS codi_comarca,
                json_extract_scalar(registre, '$.nom_comarca') AS nom_comarca,
                json_extract_scalar(registre, '$.codi_ine') AS codi_ine,
                json_extract_scalar(registre, '$.municipi') AS municipi,
                json_extract_scalar(registre, '$.altitud') AS altitud,
                json_extract_scalar(registre, '$.latitud') AS latitud,
                json_extract_scalar(registre, '$.longitud') AS longitud,
                json_extract_scalar(registre, '$.unitats') AS unitats,
                json_extract_scalar(registre, '$.contaminant') AS contaminant,
                json_extract_scalar(registre, '$.magnitud') AS magnitud,
                json_extract_scalar(registre, '$.data') AS data_base,
                ARRAY[
                    ROW('01:00:00', CAST(json_extract_scalar(registre, '$.h01') AS FLOAT)), ROW('02:00:00', CAST(json_extract_scalar(registre, '$.h02') AS FLOAT)),
                    ROW('03:00:00', CAST(json_extract_scalar(registre, '$.h03') AS FLOAT)), ROW('04:00:00', CAST(json_extract_scalar(registre, '$.h04') AS FLOAT)),
                    ROW('05:00:00', CAST(json_extract_scalar(registre, '$.h05') AS FLOAT)), ROW('06:00:00', CAST(json_extract_scalar(registre, '$.h06') AS FLOAT)),
                    ROW('07:00:00', CAST(json_extract_scalar(registre, '$.h07') AS FLOAT)), ROW('08:00:00', CAST(json_extract_scalar(registre, '$.h08') AS FLOAT)),
                    ROW('09:00:00', CAST(json_extract_scalar(registre, '$.h09') AS FLOAT)), ROW('10:00:00', CAST(json_extract_scalar(registre, '$.h10') AS FLOAT)),
                    ROW('11:00:00', CAST(json_extract_scalar(registre, '$.h11') AS FLOAT)), ROW('12:00:00', CAST(json_extract_scalar(registre, '$.h12') AS FLOAT)),
                    ROW('13:00:00', CAST(json_extract_scalar(registre, '$.h13') AS FLOAT)), ROW('14:00:00', CAST(json_extract_scalar(registre, '$.h14') AS FLOAT)),
                    ROW('15:00:00', CAST(json_extract_scalar(registre, '$.h15') AS FLOAT)), ROW('16:00:00', CAST(json_extract_scalar(registre, '$.h16') AS FLOAT)),
                    ROW('17:00:00', CAST(json_extract_scalar(registre, '$.h17') AS FLOAT)), ROW('18:00:00', CAST(json_extract_scalar(registre, '$.h18') AS FLOAT)),
                    ROW('19:00:00', CAST(json_extract_scalar(registre, '$.h19') AS FLOAT)), ROW('20:00:00', CAST(json_extract_scalar(registre, '$.h20') AS FLOAT)),
                    ROW('21:00:00', CAST(json_extract_scalar(registre, '$.h21') AS FLOAT)), ROW('22:00:00', CAST(json_extract_scalar(registre, '$.h22') AS FLOAT)),
                    ROW('23:00:00', CAST(json_extract_scalar(registre, '$.h23') AS FLOAT)), ROW('00:00:00', CAST(json_extract_scalar(registre, '$.h24') AS FLOAT))
                ] AS hores_array
            FROM parsed_data
            WHERE CAST(substring(json_extract_scalar(registre, '$.data'), 1, 10) AS DATE) = DATE '{fecha_str}'
        )
        SELECT 
            f.codi_eoi, f.nom_estacio, f.tipus_estacio, f.area_urbana, f.codi_comarca, f.nom_comarca,
            f.codi_ine, f.municipi, f.altitud, f.latitud, f.longitud, f.unitats, f.contaminant, f.magnitud,
            CAST(substring(f.data_base, 1, 11) || t.hora_id AS TIMESTAMP) AS date_measurement,
            t.concentration
        FROM flattened_data f
        CROSS JOIN UNNEST(f.hores_array) AS t(hora_id, concentration)
        WHERE t.concentration IS NOT NULL;
    """

    try:
        # Execution a Athena
        response = athena.start_query_execution(QueryString=query,
                                                ResultConfiguration={'OutputLocation': s3_path_output})
        q_id = response['QueryExecutionId']

        # Polling de l'estat
        status = 'RUNNING'
        while status in ['RUNNING', 'QUEUED']:
            time.sleep(3)
            status_resp = athena.get_query_execution(QueryExecutionId=q_id)
            status = status_resp['QueryExecution']['Status']['State']

        if status != 'SUCCEEDED':
            raise Exception(f"Athena error: {status_resp['QueryExecution']['Status'].get('StateChangeReason')}")

        csv_uri = status_resp['QueryExecution']['ResultConfiguration']['OutputLocation']
        df = pd.read_csv(csv_uri)

        if df.empty:
            print(f"[{fecha_str}] Sense dades per processar.")
            return fecha_str, True

        # Forcem el tipus text al codi d'estació per evitar pèrdua de zeros a l'esquerra
        df['codi_eoi'] = df['codi_eoi'].astype(str)

        # CRIDEM AL SINCRONITZADOR: Si hi ha coses noves al dataframe, les crea de manera segura
        sync_missing_metadata(df, params)

        # Mapegem ràpidament els IDs de memòria al nostre DataFrame definitiu
        df['id_station'] = df['codi_eoi'].map(available_stations)
        df['id_gas'] = df['contaminant'].map(available_gas)

        # Netegem files que hagin pogut quedar òrfenes per errors estranys
        df_final = df.dropna(subset=['id_station', 'id_gas'])
        total_records = len(df_final)

        # Inserció massiva de concentracions a RDS
        connection = get_db_connection(params)
        with connection.cursor() as cursor:
            insert_sql = """
                         INSERT \
                         IGNORE INTO pollution_concentration (id_station, pollution_gas, date_measurement, concentration) 
                VALUES ( \
                         %s, \
                         %s, \
                         %s, \
                         %s \
                         ) \
                         """
            # Extraiem les columnes en l'ordre de la taula final
            tuples_to_insert = list(
                df_final[['id_station', 'id_gas', 'date_measurement', 'concentration']].itertuples(index=False,
                                                                                                   name=None))
            cursor.executemany(insert_sql, tuples_to_insert)

            # Guardem el Log d'èxit diari
            cursor.execute("""
                           INSERT INTO batch_execution_log (processed_date, path_result_athena,
                                                            total_pollution_concentrations_added, id_status)
                           VALUES (%s, %s, %s, 4)
                           """, (fecha_str, csv_uri, total_records))
            connection.commit()

        connection.close()
        print(f"[{fecha_str}] ¡Èxit! {total_records} files inserides correctament.")
        return fecha_str, True

    except Exception as e:
        print(f"[{fecha_str}] ERROR CRÍTIC: {e}")
        try:
            conn = get_db_connection(params)
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO batch_execution_log (processed_date, id_status, log_s3) VALUES (%s, 6, %s)",
                               (fecha_str, str(e)))
                conn.commit()
            conn.close()
        except:
            pass
        return fecha_str, False


# --- 5. ORQUESTRADOR PRINCIPAL ---
def main():
    params = get_ssm_parameters()
    max_workers = int(params.get('WORKERS', 5))

    # 1. Carregar diccionaris inicials de control
    init_shared_dictionaries(params)

    # 2. Llegir última data processada
    conn = get_db_connection(params)
    with conn.cursor() as cursor:
        cursor.execute("SELECT last_processed_date FROM etl_control WHERE id = 1")
        last_processed_date = cursor.fetchone()['last_processed_date']
    conn.close()

    # Calculem el rang de dies fins ahir
    fecha_objetivo = datetime.now().date() - timedelta(days=1)
    if last_processed_date >= fecha_objetivo:
        print(f"El sistema de dades ja està al dia (Última data: {last_processed_date}).")
        return

    dias_a_procesar = []
    dia_actual = last_processed_date + timedelta(days=1)
    while dia_actual <= fecha_objetivo:
        dias_a_procesar.append(dia_actual)
        dia_actual += timedelta(days=1)

    print(f"S'han llançat {len(dias_a_procesar)} dies a la cua de processament amb {max_workers} fils.")

    dias_exito = []
    # 3. Llançar el Pool de Processos en Paral·lel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futurs = {executor.submit(process_single_day, dia, params): dia for dia in dias_a_procesar}
        for fut in as_completed(futurs):
            dia = futurs[fut]
            fecha_str, exito = fut.result()
            if exito:
                dias_exito.append(dia)

    # 4. Actualització segura de la data de control
    if dias_exito:
        max_fecha_exito = max(dias_exito)
        conn = get_db_connection(params)
        with conn.cursor() as cursor:
            cursor.execute("UPDATE etl_control SET last_processed_date = %s WHERE id = 1", (max_fecha_exito,))
            conn.commit()
        conn.close()
        print(f"\n[CONTROL] Procés finalitzat. etl_control actualitzat fins a: {max_fecha_exito}")
    else:
        print("\n[CONTROL] No s'ha pogut processar cap dia amb èxit.")


if __name__ == '__main__':
    main()