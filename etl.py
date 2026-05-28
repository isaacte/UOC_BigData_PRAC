import asyncio

import boto3
import pymysql
import sys
import time
import threading
import pandas as pd
import json
import uuid
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


# --- 0. LOGGING (S3 + RDS) ---
class ETLLogger:
    """Logger que guarda logs detallats en S3 i resums en RDS"""

    def __init__(self, db_params, s3_bucket_logs, s3_prefix='etl-logs'):
        self.db_params = db_params
        self.s3_bucket_logs = s3_bucket_logs  # Bucket dedicado para logs
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.events = []

    def get_db_connection(self):
        return pymysql.connect(
            host=self.db_params['DB_HOST'],
            user=self.db_params['DB_USER'],
            password=self.db_params['DB_PASS'],
            database=self.db_params['DB_NAME'],
            cursorclass=pymysql.cursors.DictCursor
        )

    def add_event(self, event_type, level='info', message='', data=None):
        """Añadir evento al log"""
        self.events.append({
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'level': level,
            'message': message,
            'data': data or {}
        })

    def log_batch_start(self, execution_id, process_date):
        """Registrar inicio en RDS"""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                               INSERT INTO etl_execution_summary
                                   (execution_id, process_date, started_at, status)
                               VALUES (%s, %s, NOW(), 'running')
                               """, (execution_id, process_date))
                conn.commit()
        finally:
            conn.close()

    def log_batch_success(self, execution_id, process_date, total_records, duration_seconds, s3_log_path):
        """Registrar éxito en RDS"""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                               UPDATE etl_execution_summary
                               SET status           = 'success',
                                   total_records    = %s,
                                   duration_seconds = %s,
                                   completed_at     = NOW(),
                                   s3_log_path      = %s
                               WHERE execution_id = %s
                               """, (total_records, duration_seconds, s3_log_path, execution_id))
                conn.commit()
        finally:
            conn.close()

    def log_batch_error(self, execution_id, error_message, error_type='unknown', s3_log_path=None):
        """Registrar error en RDS"""
        conn = self.get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                               UPDATE etl_execution_summary
                               SET status        = 'failed',
                                   error_message = %s,
                                   error_type    = %s,
                                   completed_at  = NOW(),
                                   s3_log_path   = %s
                               WHERE execution_id = %s
                               """, (error_message[:500], error_type, s3_log_path, execution_id))
                conn.commit()
        finally:
            conn.close()

    def save_to_s3(self, execution_id, process_date):
        """Guardar log detallado en S3 (bucket dedicado para logs)"""
        timestamp = datetime.now().isoformat()
        s3_key = f"{self.s3_prefix}/{process_date}/{execution_id}/{timestamp}.json"

        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket_logs,  # Usar bucket de logs dedicado
                Key=s3_key,
                Body=json.dumps({
                    'collected_at': datetime.now().isoformat(),
                    'total_events': len(self.events),
                    'events': self.events
                }, indent=2, default=str),
                ContentType='application/json'
            )
            return f"s3://{self.s3_bucket_logs}/{s3_key}"
        except Exception as e:
            print(f"Error guardando log en S3: {e}")
            return None


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
            '/bdata-processing-server/env/ATHENEA_DB',
            '/bdata-processing-server/env/BUCKET_ORIGIN',
            '/bdata-processing-server/env/BUCKET_PROCESSED',
            '/bdata-processing-server/env/BUCKET_LOGS',
            '/bdata-processing-server/env/CONCURRENT_ETL_WORKERS'
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

    with metadata_lock:
        conn = get_db_connection(params)
        cursor = conn.cursor()

        df_meta = df.drop_duplicates(subset=['codi_eoi', 'contaminant']).copy()

        for _, row in df_meta.iterrows():
            u_name = row.get('unitats')
            if u_name and u_name not in available_units:
                cursor.execute("INSERT INTO unit (name_units) VALUES (%s)", (u_name,))
                conn.commit()
                available_units[u_name] = cursor.lastrowid

            g_name = row.get('contaminant')
            if g_name and g_name not in available_gas:
                mag = int(row['magnitud']) if pd.notnull(row['magnitud']) else 0
                cursor.execute("INSERT INTO pollution_gas (name_gas, magnitude, id_unit) VALUES (%s, %s, %s)",
                               (g_name, mag, available_units[u_name]))
                conn.commit()
                available_gas[g_name] = cursor.lastrowid

            t_name = row.get('tipus_estacio')
            if t_name and t_name not in available_station_types:
                cursor.execute("INSERT INTO station_type (name_type) VALUES (%s)", (t_name,))
                conn.commit()
                available_station_types[t_name] = cursor.lastrowid

            a_name = row.get('area_urbana')
            if a_name and a_name not in available_urban_areas:
                cursor.execute("INSERT INTO urban_area (name_area) VALUES (%s)", (a_name,))
                conn.commit()
                available_urban_areas[a_name] = cursor.lastrowid

            c_code = int(row['codi_comarca']) if pd.notnull(row['codi_comarca']) else None
            c_name = row.get('nom_comarca')
            if c_code and c_code not in available_comarques:
                cursor.execute("INSERT INTO comarca (code_comarca, name_comarca) VALUES (%s, %s)", (c_code, c_name))
                conn.commit()
                available_comarques[c_code] = cursor.lastrowid

            m_ine = row.get('codi_ine')
            m_name = row.get('municipi')
            if m_ine and m_ine not in available_municipalities:
                id_c = available_comarques.get(c_code)
                cursor.execute("INSERT INTO municipality (ine_code, name_municipality, id_comarca) VALUES (%s, %s, %s)",
                               (m_ine, m_name, id_c))
                conn.commit()
                available_municipalities[m_ine] = cursor.lastrowid

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


# ✅ FUNCIÓ ASYNC PER ESPERAR ATHENA (sense bloquear)
async def wait_athena_query_async(athena, query_id, max_wait=3600):
    """
    Esperar Athena query SIN BLOQUEAR el hilo principal
    - asyncio.sleep() libera el hilo mientras espera
    - Exponential backoff: 1s → 2s → 4s → 8s → ... → 60s
    """
    wait_time = 1
    elapsed = 0

    while elapsed < max_wait:
        resp = athena.get_query_execution(QueryExecutionId=query_id)
        status = resp['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            print(f"✅ Query completada en {elapsed}s (sin bloquear)")
            return resp

        elif status in ['FAILED', 'CANCELLED']:
            raise Exception(f"Athena query {status}")

        # ✅ asyncio.sleep() NO bloqueja el hilo principal
        print(f"⏳ Esperant {wait_time}s...")
        await asyncio.sleep(wait_time)

        elapsed += wait_time
        wait_time = min(wait_time * 2, 60)  # Exponential backoff

    raise TimeoutError(f"Athena query timeout después de {max_wait}s")


# --- 4. PROCÉS DIARI DIRECTE AMB ATHENA ---
def process_single_day(process_date, params):
    fecha_str = process_date.strftime('%Y-%m-%d')
    execution_id = str(uuid.uuid4())
    start_time = time.time()

    # Inicializar logger con bucket dedicado para logs
    logger = ETLLogger(params, params['BUCKET_LOGS'])
    logger.add_event('batch_start', 'info', 'Batch iniciado')

    print(f"[{fecha_str}] Iniciant enviament a Athena... (ID: {execution_id[:8]}...)")

    # Log de inicio en RDS
    try:
        logger.log_batch_start(execution_id, fecha_str)
    except Exception as e:
        print(f"[{fecha_str}] Avís: No s'ha pogut registrar inici: {e}")
        logger.add_event('batch_start_error', 'error', str(e))

    athena = boto3.client('athena', region_name='us-east-1')
    s3_path_output = f"s3://{params['BUCKET_PROCESSED']}/athena-results/"

    athena_db = params['ATHENEA_DB']

    query = f"""
        WITH cleaned_lines AS (
            SELECT 
                linea_text,
                CASE
                    WHEN linea_text = '[]' THEN NULL
                    WHEN linea_text LIKE '[%' THEN 
                        substr(trim(linea_text), 2, length(trim(linea_text)) - 2)
                    WHEN linea_text LIKE ',%' THEN 
                        substr(trim(linea_text), 2)
                    ELSE trim(linea_text)
                END AS json_candidate
            FROM {athena_db}.air_quality_raw_text
            WHERE linea_text IS NOT NULL AND trim(linea_text) != '[]'
        ),
        validated_json AS (
            SELECT 
                json_candidate,
                TRY(json_parse(json_candidate)) AS registre
            FROM cleaned_lines
            WHERE json_candidate IS NOT NULL
        ),
        parsed_data AS (
            SELECT registre
            FROM validated_json
            WHERE registre IS NOT NULL
        )
        SELECT 
            json_extract_scalar(registre, '$.codi_eoi') AS codi_eoi,
            json_extract_scalar(registre, '$.nom_estacio') AS nom_estacio,
            json_extract_scalar(registre, '$.data') AS data_raw,
            TRY(CAST(SUBSTRING(json_extract_scalar(registre, '$.data'), 1, 10) AS DATE)) AS data_parsed,
            json_extract_scalar(registre, '$.magnitud') AS magnitud,
            json_extract_scalar(registre, '$.contaminant') AS contaminant,
            json_extract_scalar(registre, '$.unitats') AS unitats,
            json_extract_scalar(registre, '$.tipus_estacio') AS tipus_estacio,
            json_extract_scalar(registre, '$.area_urbana') AS area_urbana,
            json_extract_scalar(registre, '$.codi_ine') AS codi_ine,
            json_extract_scalar(registre, '$.municipi') AS municipi,
            json_extract_scalar(registre, '$.codi_comarca') AS codi_comarca,
            json_extract_scalar(registre, '$.nom_comarca') AS nom_comarca,
            json_extract_scalar(registre, '$.altitud') AS altitud,
            json_extract_scalar(registre, '$.latitud') AS latitud,
            json_extract_scalar(registre, '$.longitud') AS longitud,
            json_extract_scalar(registre, '$.h01') AS h01, json_extract_scalar(registre, '$.h02') AS h02,
            json_extract_scalar(registre, '$.h03') AS h03, json_extract_scalar(registre, '$.h04') AS h04,
            json_extract_scalar(registre, '$.h05') AS h05, json_extract_scalar(registre, '$.h06') AS h06,
            json_extract_scalar(registre, '$.h07') AS h07, json_extract_scalar(registre, '$.h08') AS h08,
            json_extract_scalar(registre, '$.h09') AS h09, json_extract_scalar(registre, '$.h10') AS h10,
            json_extract_scalar(registre, '$.h11') AS h11, json_extract_scalar(registre, '$.h12') AS h12,
            json_extract_scalar(registre, '$.h13') AS h13, json_extract_scalar(registre, '$.h14') AS h14,
            json_extract_scalar(registre, '$.h15') AS h15, json_extract_scalar(registre, '$.h16') AS h16,
            json_extract_scalar(registre, '$.h17') AS h17, json_extract_scalar(registre, '$.h18') AS h18,
            json_extract_scalar(registre, '$.h19') AS h19, json_extract_scalar(registre, '$.h20') AS h20,
            json_extract_scalar(registre, '$.h21') AS h21, json_extract_scalar(registre, '$.h22') AS h22,
            json_extract_scalar(registre, '$.h23') AS h23, json_extract_scalar(registre, '$.h24') AS h24
        FROM parsed_data
        WHERE TRY(CAST(SUBSTRING(json_extract_scalar(registre, '$.data'), 1, 10) AS DATE)) = DATE '{fecha_str}'
    """

    try:
        print(f"[{fecha_str}] Executant consulta Athena...")
        logger.add_event('athena_query_start', 'info', 'Consulta Athena iniciada')

        response = athena.start_query_execution(QueryString=query,
                                                ResultConfiguration={'OutputLocation': s3_path_output})
        q_id = response['QueryExecutionId']

        # ✅ CORRECTE: Cridar función async desde código sincron
        # asyncio.run() crea un event loop, executa la función async, i ho torna
        status_resp = asyncio.run(wait_athena_query_async(athena, q_id))

        if status_resp['QueryExecution']['Status']['State'] != 'SUCCEEDED':
            error_msg = status_resp['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            raise Exception(f"Athena error: {error_msg}")

        csv_uri = status_resp['QueryExecution']['ResultConfiguration']['OutputLocation']
        print(f"[{fecha_str}] Resultats a: {csv_uri}")
        logger.add_event('athena_query_success', 'info', 'Consulta completada', {'csv_uri': csv_uri})

        df = pd.read_csv(csv_uri)

        if df.empty:
            print(f"[{fecha_str}] Sense dades per processar.")
            logger.add_event('no_data', 'info', 'No hay datos para procesar')

            # Guardar log en S3 (bucket dedicado)
            s3_log_path = logger.save_to_s3(execution_id, fecha_str)

            # Actualizar RDS
            logger.log_batch_success(execution_id, fecha_str, 0, time.time() - start_time, s3_log_path)
            return fecha_str, True

        print(f"[{fecha_str}] Registres obtinguts: {len(df)}")
        logger.add_event('data_retrieved', 'info', f'{len(df)} registres obtinguts')

        df['codi_eoi'] = df['codi_eoi'].astype(str)

        sync_missing_metadata(df, params)
        logger.add_event('metadata_synced', 'info', 'Metadatos sincronizados')

        df['id_station'] = df['codi_eoi'].map(available_stations)
        df['id_gas'] = df['contaminant'].map(available_gas)

        df_final = df.dropna(subset=['id_station', 'id_gas'])
        total_records = len(df_final)

        if total_records == 0:
            print(f"[{fecha_str}] Cap registre valid.")
            logger.add_event('no_valid_records', 'warning', 'Cap registre vàlid')

            s3_log_path = logger.save_to_s3(execution_id, fecha_str)
            logger.log_batch_success(execution_id, fecha_str, 0, time.time() - start_time, s3_log_path)
            return fecha_str, True

        # Convertir numéricas
        numeric_cols = [col for col in df_final.columns if col.startswith('h') or col in ['altitud', 'magnitud']]
        for col in numeric_cols:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

        df_final = df_final.fillna(value=None)
        logger.add_event('data_cleaned', 'info', 'Dades netejades')

        concentration_col = None
        for col in df_final.columns:
            if col.startswith('h'):
                concentration_col = col
                break

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
                         )
                         """

            tuples_to_insert = []
            for _, row in df_final.iterrows():
                try:
                    id_station = int(row['id_station']) if pd.notnull(row['id_station']) else None
                    id_gas = int(row['id_gas']) if pd.notnull(row['id_gas']) else None
                    data_parsed = row['data_parsed'] if pd.notnull(row['data_parsed']) else None
                    concentration = float(row[concentration_col]) if pd.notnull(row[concentration_col]) else None

                    tuples_to_insert.append((id_station, id_gas, data_parsed, concentration))
                except (ValueError, TypeError) as e:
                    logger.add_event('data_conversion_error', 'warning', f'Error: {e}')
                    continue

            if tuples_to_insert:
                cursor.executemany(insert_sql, tuples_to_insert)

            cursor.execute("""
                           INSERT INTO batch_execution_log (processed_date, path_result_athena,
                                                            total_pollution_concentrations_added, id_status)
                           VALUES (%s, %s, %s, 4)
                           """, (fecha_str, csv_uri, total_records))
            connection.commit()

        connection.close()

        duration_seconds = time.time() - start_time
        print(f"[{fecha_str}] ✓ Èxit! {total_records} files inserides en {duration_seconds:.1f}s.")
        logger.add_event('batch_success', 'info', 'Batch completat', {
            'total_records': total_records,
            'duration_seconds': duration_seconds
        })

        # Guardar log en S3 (bucket dedicado)
        s3_log_path = logger.save_to_s3(execution_id, fecha_str)

        # Actualizar RDS con éxito
        logger.log_batch_success(execution_id, fecha_str, total_records, duration_seconds, s3_log_path)
        return fecha_str, True

    except Exception as e:
        duration_seconds = time.time() - start_time
        print(f"[{fecha_str}] ✗ ERROR CRÍTIC: {e}")
        logger.add_event('batch_error', 'error', str(e), {
            'error_type': type(e).__name__,
            'duration_seconds': duration_seconds
        })

        # Guardar log en S3 (bucket dedicado) incluso con error
        s3_log_path = logger.save_to_s3(execution_id, fecha_str)

        # Actualizar RDS con error
        logger.log_batch_error(execution_id, str(e)[:500], type(e).__name__, s3_log_path)

        # También actualizar batch_execution_log (si existe)
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
    max_workers = int(params.get('CONCURRENT_ETL_WORKERS', 5))

    init_shared_dictionaries(params)

    conn = get_db_connection(params)
    with conn.cursor() as cursor:
        cursor.execute("SELECT last_processed_date FROM etl_control WHERE id = 1")
        last_processed_date = cursor.fetchone()['last_processed_date']
    conn.close()

    fecha_objetivo = datetime.now().date() - timedelta(days=1)
    if last_processed_date >= fecha_objetivo:
        print(f"El sistema de dades ja està al dia (Última data: {last_processed_date}).")
        return

    dias_a_procesar = []
    dia_actual = last_processed_date + timedelta(days=1)
    while dia_actual <= fecha_objetivo:
        dias_a_procesar.append(dia_actual)
        dia_actual += timedelta(days=1)

    print(f"S'han llançat {len(dias_a_procesar)} dies a la cua de processament amb {max_workers} processos.")

    dias_exito = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futurs = {executor.submit(process_single_day, dia, params): dia for dia in dias_a_procesar}
        for fut in as_completed(futurs):
            dia = futurs[fut]
            fecha_str, exito = fut.result()
            if exito:
                dias_exito.append(dia)

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