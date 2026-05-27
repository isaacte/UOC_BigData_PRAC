import boto3
import pymysql
import sys
import argparse


def get_db_credentials():
    print("Recuperant credencials de Parameter Store...")
    ssm = boto3.client('ssm', region_name='us-east-1')
    try:
        db_host = ssm.get_parameter(Name='/bdata-processing-server/env/DB_HOST', WithDecryption=False)['Parameter']['Value']
        db_user = ssm.get_parameter(Name='/bdata-processing-server/env/DB_USER', WithDecryption=False)['Parameter']['Value']
        db_password = ssm.get_parameter(Name='/bdata-processing-server/env/DB_PASS', WithDecryption=False)['Parameter']['Value']
        db_name = ssm.get_parameter(Name='/bdata-processing-server/env/DB_NAME', WithDecryption=False)['Parameter']['Value']
        return db_host, db_user, db_password, db_name
    except Exception as e:
        print(f"Error recuperant les credencials: {e}")
        sys.exit(1)

def init_database(clean=False):
    host, user, password, db_name = get_db_credentials()

    print(f"Connectant a la base de dades a {host}...")
    try:
        # Ens connectem sense especificar la db_name per poder esborrar-la/crear-la
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            cursorclass=pymysql.cursors.DictCursor
        )
    except pymysql.MySQLError as e:
        print(f"Error de connexió a MySQL: {e}")
        sys.exit(1)

    sql_commands = []

    # Si s'ha passat el paràmetre --clean, afegim el DROP DATABASE primer
    if clean:
        print(f"⚠️ ATENCIÓ: Mode --clean activat. Esborrant la base de dades '{db_name}' si existeix...")
        sql_commands.append(f"DROP DATABASE IF EXISTS {db_name};")

    print(f"Creant l'esquema de la base de dades '{db_name}' (si no existeix)...")

    # Afegim la resta de comandos de creació
    sql_commands.extend([
        f"CREATE DATABASE IF NOT EXISTS {db_name};",

        f"USE {db_name};",

        """CREATE TABLE IF NOT EXISTS unit (
            id TINYINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            name_units VARCHAR(10)
        );""",

        """CREATE TABLE IF NOT EXISTS pollution_gas (
            id TINYINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            name_gas VARCHAR(10),
            magnitude SMALLINT,
            id_unit TINYINT NOT NULL,
            FOREIGN KEY (id_unit) REFERENCES unit(id)
        );""",

        """CREATE TABLE IF NOT EXISTS station_type (
            id TINYINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            name_type VARCHAR(30)
        );""",

        """CREATE TABLE IF NOT EXISTS urban_area (
            id TINYINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            name_area VARCHAR(30)
        );""",

        """CREATE TABLE IF NOT EXISTS comarca (
            id SMALLINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            code_comarca SMALLINT,
            name_comarca VARCHAR(30)
        );""",

        """CREATE TABLE IF NOT EXISTS municipality (
            id SMALLINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            ine_code VARCHAR(6),
            name_municipality VARCHAR(30),
            id_comarca SMALLINT NOT NULL,
            FOREIGN KEY (id_comarca) REFERENCES comarca(id)
        );""",

        """CREATE TABLE IF NOT EXISTS station (
            id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            eoi_code VARCHAR(10),
            name_station VARCHAR(100),
            id_urban_area TINYINT NOT NULL,
            id_station_type TINYINT NOT NULL,
            id_municipality SMALLINT NOT NULL,
            altitude INT,
            latitude FLOAT,
            longitude FLOAT,
            FOREIGN KEY (id_urban_area) REFERENCES urban_area(id),
            FOREIGN KEY (id_station_type) REFERENCES station_type(id),
            FOREIGN KEY (id_municipality) REFERENCES municipality(id)
        );""",

        """CREATE TABLE IF NOT EXISTS pollution_concentration (
            id_station INT NOT NULL,
            pollution_gas TINYINT NOT NULL,
            date_measurement TIMESTAMP NOT NULL,
            concentration FLOAT NOT NULL,
            PRIMARY KEY (id_station, pollution_gas, date_measurement),
            FOREIGN KEY (id_station) REFERENCES station(id),
            FOREIGN KEY (pollution_gas) REFERENCES pollution_gas(id)
        );""",

        """CREATE TABLE IF NOT EXISTS etl_control (
            id TINYINT PRIMARY KEY,
            last_processed_date DATE NOT NULL,
            date_last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            is_running BOOLEAN NOT NULL DEFAULT TRUE
        );""",

        "INSERT IGNORE INTO etl_control(id, last_processed_date) VALUES (1, '2025-12-31');",

        """CREATE TABLE IF NOT EXISTS status_batch_execution (
            id TINYINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
            status_name VARCHAR(10)
        );""",

        """INSERT IGNORE INTO status_batch_execution (id, status_name) VALUES 
            (1, 'Idle'), (2, 'Running'), (3, 'Executing'), 
            (4, 'Success'), (5, 'Error-lmb'), (6, 'Error');""",

        """CREATE TABLE IF NOT EXISTS batch_execution_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            update_date_status TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            processed_date DATE NOT NULL,
            path_result_athena varchar(255) DEFAULT NULL,
            total_pollution_concentrations_added INT DEFAULT 0,
            id_status TINYINT NOT NULL DEFAULT 1,
            log_s3 VARCHAR(255) DEFAULT NULL,
            FOREIGN KEY (id_status) REFERENCES status_batch_execution(id)
        );"""
    ])

    with connection.cursor() as cursor:
        for index, query in enumerate(sql_commands):
            try:
                cursor.execute(query)
            except Exception as e:
                print(f"Error executant la consulta {index}:\n{query}\nError: {e}")
                sys.exit(1)

        connection.commit()

    connection.close()
    print("Procés finalitzat! L'esquema de la base de dades s'ha aplicat correctament.")

if __name__ == '__main__':
    # Configurem argparse per acceptar paràmetres per terminal
    parser = argparse.ArgumentParser(description="Inicialitza la base de dades del projecte ETL.")
    parser.add_argument('--clean', action='store_true', help="Esborra la base de dades existent abans de crear-la de nou.")
    args = parser.parse_args()

    # Cridem a la funció passant-li el valor del paràmetre
    init_database(clean=args.clean)