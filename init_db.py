import boto3
import pymysql
import sys


def get_db_credentials():
    print("Recuperant credencials de Parameter Store...")
    ssm = boto3.client('ssm', region_name='us-east-1')
    try:
        db_host = ssm.get_parameter(Name='/bdata-processing-server/env/DB_HOST', WithDecryption=False)['Parameter'][
            'Value']
        db_user = ssm.get_parameter(Name='/bdata-processing-server/env/DB_USER', WithDecryption=False)['Parameter'][
            'Value']
        db_password = ssm.get_parameter(Name='/bdata-processing-server/env/DB_PASS', WithDecryption=False)['Parameter'][
            'Value']
        db_name = ssm.get_parameter(Name='/bdata-processing-server/env/DB_NAME', WithDecryption=False)['Parameter'][
            'Value']
        return db_host, db_user, db_password, db_name
    except Exception as e:
        print(f"Error recuperant les credencials: {e}")
        sys.exit(1)


def init_database():
    host, user, password, db_name = get_db_credentials()

    print(f"Connectant a la base de dades '{db_name}' a {host}...")
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
    except pymysql.MySQLError as e:
        print(f"Error de connexió a MySQL: {e}")
        sys.exit(1)

    print("Connexió establerta. Comprovant i creant taules si cal...")

    # Llista de consultes SQL per crear les taules (l'ordre importa per les Foreign Keys)
    taules_sql = [
        """
        CREATE TABLE IF NOT EXISTS global_variables
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            start_date
            DATE
            NOT
            NULL,
            end_date
            DATE
            NOT
            NULL,
            is_running
            TINYINT
        (
            1
        ) DEFAULT 1
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS batch_execution_log
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            start_date
            DATE,
            end_date
            DATE,
            id_status
            INT
            DEFAULT
            1,
            update_date_status
            DATETIME
            DEFAULT
            CURRENT_TIMESTAMP
            ON
            UPDATE
            CURRENT_TIMESTAMP,
            path_result_athena
            VARCHAR
        (
            255
        ),
            log_s3 VARCHAR
        (
            255
        ),
            total_pollution_concentrations_added INT DEFAULT 0
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS unit
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            name_units
            VARCHAR
        (
            50
        ) UNIQUE NOT NULL
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS pollution_gas
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            name_gas
            VARCHAR
        (
            50
        ) UNIQUE NOT NULL,
            magnitude INT,
            id_unit INT,
            FOREIGN KEY
        (
            id_unit
        ) REFERENCES unit
        (
            id
        )
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS station_type
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            name_type
            VARCHAR
        (
            50
        ) UNIQUE NOT NULL
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS urban_area
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            name_area
            VARCHAR
        (
            50
        ) UNIQUE NOT NULL
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS comarca
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            code_comarca
            INT
            UNIQUE
            NOT
            NULL,
            name_comarca
            VARCHAR
        (
            100
        ) NOT NULL
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS municipality
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            ine_code
            VARCHAR
        (
            20
        ) UNIQUE NOT NULL,
            name_municipality VARCHAR
        (
            100
        ) NOT NULL,
            id_comarca INT,
            FOREIGN KEY
        (
            id_comarca
        ) REFERENCES comarca
        (
            id
        )
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS station
        (
            id
            INT
            PRIMARY
            KEY
            AUTO_INCREMENT,
            eoi_code
            VARCHAR
        (
            20
        ) UNIQUE NOT NULL,
            name_station VARCHAR
        (
            150
        ),
            id_urban_area INT,
            id_station_type INT,
            id_municipality INT,
            altitude INT,
            latitude DECIMAL
        (
            10,
            6
        ),
            longitude DECIMAL
        (
            10,
            6
        ),
            FOREIGN KEY
        (
            id_urban_area
        ) REFERENCES urban_area
        (
            id
        ),
            FOREIGN KEY
        (
            id_station_type
        ) REFERENCES station_type
        (
            id
        ),
            FOREIGN KEY
        (
            id_municipality
        ) REFERENCES municipality
        (
            id
        )
            );
        """,
        """
        CREATE TABLE IF NOT EXISTS pollution_concentration
        (
            id_station
            INT,
            pollution_gas
            INT,
            date_measurement
            DATETIME,
            concentration
            DECIMAL
        (
            10,
            4
        ),
            PRIMARY KEY
        (
            id_station,
            pollution_gas,
            date_measurement
        ),
            FOREIGN KEY
        (
            id_station
        ) REFERENCES station
        (
            id
        ),
            FOREIGN KEY
        (
            pollution_gas
        ) REFERENCES pollution_gas
        (
            id
        )
            );
        """
    ]

    with connection.cursor() as cursor:
        # 1. Crear l'esquema
        for query in taules_sql:
            cursor.execute(query)

        # 2. Inicialitzar global_variables si està buida (necessari per a l'script ETL)
        cursor.execute("SELECT COUNT(*) as count FROM global_variables")
        result = cursor.fetchone()
        if result['count'] == 0:
            print("Inicialitzant la taula global_variables amb la data per defecte...")
            cursor.execute("""
                           INSERT INTO global_variables (start_date, end_date, is_running) 
                VALUES ('2026-05-17', '2026-05-19', 1)
            """)

        connection.commit()

    connection.close()
    print("Process finalitzat. La base de dades està llesta per ser utilitzada!")

if __name__ == '__main__':
    init_database()