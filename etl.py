import queue
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
from dataclasses import dataclass

metadata_lock = threading.Lock()

available_units = {}
available_gas = {}
available_station_types = {}
available_urban_areas = {}
available_comarques = {}
available_municipalities = {}
available_stations = {}


class ETLLogger:
    def __init__(self, db_params, s3_bucket_logs, s3_prefix='etl-logs'):
        self.db_params = db_params
        self.s3_bucket_logs = s3_bucket_logs
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
        self.events.append({
            'timestamp': datetime.now().isoformat(),
            'event_type': event_type,
            'level': level,
            'message': message,
            'data': data or {}
        })

    def log_batch_start(self, execution_id, process_date):
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
        timestamp = datetime.now().isoformat()
        s3_key = f"{self.s3_prefix}/{process_date}/{execution_id}/{timestamp}.json"

        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket_logs,
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
            print(f"Error guardant log a S3: {e}")
            return None


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


def init_shared_dictionaries(params):
    global available_units, available_gas, available_station_types, available_urban_areas, available_comarques, available_municipalities, available_stations
    print("Carregant catàlegs de la base de dades a memòria...")
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


@dataclass
class AthenaQueryTask:
    process_date: str
    query_id: str
    execution_id: str
    logger: ETLLogger
    start_time: float
    query_string: str


class AthenaQueryOrchestrator:
    def __init__(self, athena_client, max_wait=3600):
        self.athena = athena_client
        self.max_wait = max_wait
        self.queries = {}
        self.results_queue = queue.Queue()
        self.lock = threading.Lock()

    def submit_query(self, task: AthenaQueryTask):
        with self.lock:
            self.queries[task.query_id] = task
            print(f"[SUBMIT] Query {task.query_id[:8]}... per a {task.process_date} iniciada")

    def poll_all(self):
        thread = threading.Thread(
            target=self._polling_background,
            daemon=True
        )
        thread.start()
        print(f"[CONSULTA] Iniciada consulta de {len(self.queries)} queries")

    def _polling_background(self):
        completed = set()
        elapsed = 0
        wait_time = 1

        while len(completed) < len(self.queries) and elapsed < self.max_wait:
            with self.lock:
                for query_id, task in list(self.queries.items()):
                    if query_id in completed:
                        continue

                    try:
                        resp = self.athena.get_query_execution(QueryExecutionId=query_id)
                        status = resp['QueryExecution']['Status']['State']

                        if status == 'SUCCEEDED':
                            self.results_queue.put((task, resp))
                            completed.add(query_id)
                            print(f"[COMPLETADA] Query {query_id[:8]}... finalitzada en {elapsed}s")

                        elif status in ['FAILED', 'CANCELLED']:
                            error = resp['QueryExecution']['Status'].get('StateChangeReason')
                            self.results_queue.put((task, Exception(f"Query {status}: {error}")))
                            completed.add(query_id)
                            print(f"[FALLIDA] Query {query_id[:8]}... {status}")

                    except Exception as e:
                        self.results_queue.put((task, e))
                        completed.add(query_id)
                        print(f"[ERROR] Query {query_id[:8]}... error: {e}")

            print(f"⏳ Esperant {wait_time}s... ({len(completed)}/{len(self.queries)} completades)")
            time.sleep(wait_time)

            elapsed += wait_time
            wait_time = min(wait_time * 2, 10)

    def get_result(self, timeout=None):
        try:
            task, result = self.results_queue.get(timeout=timeout)

            if isinstance(result, Exception):
                raise result

            return task, result

        except queue.Empty:
            return None, None


def process_task(task: AthenaQueryTask, result, params):
    process_date = task.process_date
    execution_id = task.execution_id
    logger = task.logger
    start_time = task.start_time

    try:
        csv_uri = result['QueryExecution']['ResultConfiguration']['OutputLocation']
        print(f"[{process_date}] Resultats a: {csv_uri[:60]}...")
        logger.add_event('athena_query_success', 'info', 'Consulta completada')

        df = pd.read_csv(csv_uri)

        if df.empty:
            print(f"[{process_date}] Sense dades per processar.")
            logger.add_event('no_data', 'info', 'Sense dades per processar')

            s3_log_path = logger.save_to_s3(execution_id, process_date)
            logger.log_batch_success(execution_id, process_date, 0, time.time() - start_time, s3_log_path)
            return True

        print(f"[{process_date}] Registres obtinguts: {len(df)}")
        logger.add_event('data_retrieved', 'info', f'{len(df)} registres obtinguts')

        df['codi_eoi'] = df['codi_eoi'].astype(str)

        sync_missing_metadata(df, params)
        logger.add_event('metadata_synced', 'info', 'Metadatos sincronitzats')

        df['id_station'] = df['codi_eoi'].map(available_stations)
        df['id_gas'] = df['contaminant'].map(available_gas)

        df_final = df.dropna(subset=['id_station', 'id_gas'])
        total_records = len(df_final)

        if total_records == 0:
            print(f"[{process_date}] Cap registre vàlid.")
            logger.add_event('no_valid_records', 'warning', 'Cap registre vàlid')

            s3_log_path = logger.save_to_s3(execution_id, process_date)
            logger.log_batch_success(execution_id, process_date, 0, time.time() - start_time, s3_log_path)
            return True

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
                         INSERT IGNORE INTO pollution_concentration (id_station, pollution_gas, date_measurement, concentration)
                         VALUES (%s, %s, %s, %s)
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
                           INSERT INTO batch_execution_log (processed_date, path_result_athena, total_pollution_concentrations_added, id_status)
                           VALUES (%s, %s, %s, 4)
                           """, (process_date, csv_uri, total_records))
            connection.commit()

        connection.close()

        duration_seconds = time.time() - start_time
        print(f"[{process_date}] ✓ Èxit! {total_records} registres insertats en {duration_seconds:.1f}s.")
        logger.add_event('batch_success', 'info', 'Batch completat')

        s3_log_path = logger.save_to_s3(execution_id, process_date)
        logger.log_batch_success(execution_id, process_date, total_records, duration_seconds, s3_log_path)
        return True

    except Exception as e:
        duration_seconds = time.time() - start_time
        print(f"[{process_date}] ✗ ERROR CRÍTIC: {e}")
        logger.add_event('batch_error', 'error', str(e))

        s3_log_path = logger.save_to_s3(execution_id, process_date)
        logger.log_batch_error(execution_id, str(e)[:500], type(e).__name__, s3_log_path)

        try:
            conn = get_db_connection(params)
            with conn.cursor() as cursor:
                cursor.execute("INSERT INTO batch_execution_log (processed_date, id_status, log_s3) VALUES (%s, 6, %s)",
                               (process_date, str(e)))
                conn.commit()
            conn.close()
        except:
            pass

        return False


def main():
    params = get_ssm_parameters()
    max_workers = int(params.get('CONCURRENT_ETL_WORKERS', 5))

    init_shared_dictionaries(params)

    conn = get_db_connection(params)
    with conn.cursor() as cursor:
        cursor.execute("SELECT last_processed_date FROM etl_control WHERE id = 1")
        last_processed_date = cursor.fetchone()['last_processed_date']
    conn.close()

    target_date = datetime.now().date() - timedelta(days=1)
    if last_processed_date >= target_date:
        print(f"El sistema de dades ja està al dia (Última data: {last_processed_date}).")
        return

    days_to_process = []
    current_day = last_processed_date + timedelta(days=1)
    while current_day <= target_date:
        days_to_process.append(current_day)
        current_day += timedelta(days=1)

    print(f"\n[FASE 1] S'han iniciado {len(days_to_process)} queries a Athena...")

    athena = boto3.client('athena', region_name='us-east-1')
    s3_path_output = f"s3://{params['BUCKET_PROCESSED']}/athena-results/"
    athena_db = params['ATHENEA_DB']

    orchestrator = AthenaQueryOrchestrator(athena)

    for day in days_to_process:
        date_str = day.strftime('%Y-%m-%d')
        execution_id = str(uuid.uuid4())

        logger = ETLLogger(params, params['BUCKET_LOGS'])
        logger.add_event('batch_start', 'info', 'Batch iniciado')

        try:
            logger.log_batch_start(execution_id, date_str)
        except:
            pass

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
            WHERE TRY(CAST(SUBSTRING(json_extract_scalar(registre, '$.data'), 1, 10) AS DATE)) = DATE '{date_str}'
        """

        try:
            print(f"[{date_str}] Executant consulta Athena...")
            logger.add_event('athena_query_start', 'info', 'Consulta Athena iniciada')

            response = athena.start_query_execution(
                QueryString=query,
                ResultConfiguration={'OutputLocation': s3_path_output}
            )
            query_id = response['QueryExecutionId']

            task = AthenaQueryTask(
                process_date=date_str,
                query_id=query_id,
                execution_id=execution_id,
                logger=logger,
                start_time=time.time(),
                query_string=query
            )
            orchestrator.submit_query(task)

        except Exception as e:
            print(f"[{date_str}] ✗ Error iniciando query: {e}")
            logger.log_batch_error(execution_id, str(e)[:500], type(e).__name__, None)

    print(f"\n[FASE 2] Consultant {len(orchestrator.queries)} queries en paral·lel...")
    orchestrator.poll_all()

    print(f"\n[FASE 3] Processant resultats conforme es completen...")

    processed_count = 0
    successful_days = []

    while processed_count < len(days_to_process):
        task, result = orchestrator.get_result(timeout=60)

        if task is None:
            print("[TIMEOUT] Esperant resultats...")
            continue

        try:
            if isinstance(result, Exception):
                raise result

            success = process_task(task, result, params)
            processed_count += 1

            if success:
                successful_days.append(datetime.strptime(task.process_date, '%Y-%m-%d').date())

        except Exception as e:
            print(f"[{task.process_date}] ✗ Error processant: {e}")
            task.logger.log_batch_error(task.execution_id, str(e)[:500], type(e).__name__, None)
            processed_count += 1

    if successful_days:
        max_successful_date = max(successful_days)
        conn = get_db_connection(params)
        with conn.cursor() as cursor:
            cursor.execute("UPDATE etl_control SET last_processed_date = %s WHERE id = 1", (max_successful_date,))
            conn.commit()
        conn.close()
        print(f"\n[CONTROL] Procés finalitzat. etl_control actualitzat fins a: {max_successful_date}")
    else:
        print("\n[CONTROL] No s'ha pogut processar cap dia amb èxit.")


if __name__ == '__main__':
    main()