import logging


class StatisticsQueries:
    def __init__(self, conn, cursor, start_date, end_date, pollution_gas, stations):
        self.conn = conn
        self.cursor = cursor
        self.start_date = start_date
        self.end_date = end_date
        self.pollution_gas = pollution_gas
        self.stations = stations
    
    def check_if_exists_data(self) -> bool:
        """
        Funció que permet saber si hi ha informació entre les dues dates
        """


        query = """
            SELECT COUNT(*) as num
            FROM pollution_concentration pc
            WHERE
                date(pc.date_measurement) BETWEEN '@@start_date@@' AND '@@end_date@@'
        """

        query = query \
            .replace("@@start_date@@", self.start_date) \
            .replace("@@end_date@@", self.end_date)
        
        self.cursor.execute(query)
        query_result = int(self.cursor.fetchone()['num'])

        return query_result > 0
            


    def execute_avg_concentration_gas(self) -> list:
        """
        Funció que permet obtenir la mitjana de gasos emesos entre dates.
        
        Returns:
            list: Llistat resultant dels gasos mitjans emesos.
        """

        query_avg_concentration = """
            SELECT AVG(pc.concentration) as avg_concentration, g.name_gas as name_gas, g.id as id_gas, u.name_units, s.name_station
            FROM pollution_concentration pc
            INNER JOIN pollution_gas g ON g.id = pc.pollution_gas
            INNER JOIN unit u ON g.id_unit = u.id
            INNER JOIN station s ON s.id = pc.id_station
            WHERE
                date(pc.date_measurement) BETWEEN '@@start_date@@' AND '@@end_date@@'
                AND pollution_gas IN (@@pollution_gas@@)
                @@stations_query@@
            @@group_by@@
        """

        query_avg_concentration = query_avg_concentration \
        .replace("@@start_date@@", self.start_date) \
        .replace("@@end_date@@", self.end_date) \
        .replace("@@pollution_gas@@", ",".join(self.pollution_gas))

        
        if self.stations:
            query_avg_concentration = query_avg_concentration.replace("@@stations_query@@", " AND id_station IN (" + ",".join(self.stations) + ")")
            query_avg_concentration = query_avg_concentration.replace("@@group_by@@", "GROUP BY id_gas, id_station;")
        else:
            query_avg_concentration = query_avg_concentration.replace("@@stations_query@@", "")
            query_avg_concentration = query_avg_concentration.replace("@@group_by@@", "GROUP BY id_gas;")


        self.cursor.execute(query_avg_concentration)
        query_result = self.cursor.fetchall()

        result = []

        for row in query_result:
            result_dict = {
                'avg_concentration': round(row['avg_concentration'], 2),
                'name_gas': row['name_gas'],
                'name_units': row['name_units'],
            }

            if self.stations:
                result_dict['name_station'] = row['name_station']

            result.append(result_dict)
            

        return result
    
    def maximum_concentration_gas(self) -> list:
        """
        Funció que permet obtenir la concentració màxima de gasos emesos entre dates.
        
        Returns:
            list: Llistat resultant dels gasos màxims emesos.
        """

        # Utilitzem una CTE (WITH) i ROW_NUMBER() per assegurar-nos que agafem la fila real
        query_max_concentration = """
            WITH RankedData AS (
                SELECT pc.concentration as max_concentration, pc.date_measurement, pg.name_gas, u.name_units, 
                       s.name_station, st.name_type, ua.name_area, m.name_municipality, c.name_comarca,
                       ROW_NUMBER() OVER(PARTITION BY pc.pollution_gas @@partition_station@@ ORDER BY pc.concentration DESC) as rn
                FROM pollution_concentration pc
                INNER JOIN pollution_gas pg on pg.id = pc.pollution_gas
                INNER JOIN unit u on u.id = pg.id_unit
                INNER JOIN station s on s.id = pc.id_station
                INNER JOIN station_type st on st.id = s.id_station_type
                INNER JOIN urban_area ua on ua.id = s.id_urban_area
                INNER JOIN municipality m on m.id = s.id_municipality
                INNER JOIN comarca c on c.id = m.id_comarca
                WHERE
                    date(pc.date_measurement) BETWEEN '@@start_date@@' AND '@@end_date@@'
                    AND pc.pollution_gas IN (@@pollution_gas@@)
                    @@stations_query@@
            )
            SELECT max_concentration, date_measurement, name_gas, name_units, name_station, 
                   name_type, name_area, name_municipality, name_comarca
            FROM RankedData
            WHERE rn = 1;
            """
        
        query_max_concentration = query_max_concentration \
        .replace("@@start_date@@", self.start_date) \
        .replace("@@end_date@@", self.end_date) \
        .replace("@@pollution_gas@@", ",".join(self.pollution_gas))

        if self.stations:
            query_max_concentration = query_max_concentration.replace("@@stations_query@@", " AND pc.id_station IN (" + ",".join(self.stations) + ")")
            query_max_concentration = query_max_concentration.replace("@@partition_station@@", ", pc.id_station")
        else:
            query_max_concentration = query_max_concentration.replace("@@stations_query@@", "")
            query_max_concentration = query_max_concentration.replace("@@partition_station@@", "")

        self.cursor.execute(query_max_concentration)
        query_result = self.cursor.fetchall()

        result = []
        for row in query_result:
            result.append({
                'max_concentration': round(row['max_concentration'], 2),
                'date_measurement': row['date_measurement'],
                'name_gas': row['name_gas'],
                'name_units': row['name_units'],
                'name_station': row['name_station'],
                'name_type': row['name_type'],
                'name_area': row['name_area'],
                'name_municipality': row['name_municipality'],
                'name_comarca': row['name_comarca']
            })

        return result
    
    def minimum_concentration_gas(self) -> list:
        """
        Funció que permet obtenir la concentració mínima de gasos emesos entre dates.
        
        Returns:
            list: Llistat resultant dels gasos mínims emesos.
        """

        # Mateixa lògica, però ordenem de forma ascendent (ASC) per buscar el mínim
        query_min_concentration = """
            WITH RankedData AS (
                SELECT pc.concentration as min_concentration, pc.date_measurement, pg.name_gas, u.name_units, 
                       s.name_station, st.name_type, ua.name_area, m.name_municipality, c.name_comarca,
                       ROW_NUMBER() OVER(PARTITION BY pc.pollution_gas @@partition_station@@ ORDER BY pc.concentration ASC) as rn
                FROM pollution_concentration pc
                INNER JOIN pollution_gas pg on pg.id = pc.pollution_gas
                INNER JOIN unit u on u.id = pg.id_unit
                INNER JOIN station s on s.id = pc.id_station
                INNER JOIN station_type st on st.id = s.id_station_type
                INNER JOIN urban_area ua on ua.id = s.id_urban_area
                INNER JOIN municipality m on m.id = s.id_municipality
                INNER JOIN comarca c on c.id = m.id_comarca
                WHERE
                    date(pc.date_measurement) BETWEEN '@@start_date@@' AND '@@end_date@@'
                    AND pc.pollution_gas IN (@@pollution_gas@@)
                    @@stations_query@@
            )
            SELECT min_concentration, date_measurement, name_gas, name_units, name_station, 
                   name_type, name_area, name_municipality, name_comarca
            FROM RankedData
            WHERE rn = 1;
            """
        
        query_min_concentration = query_min_concentration \
        .replace("@@start_date@@", self.start_date) \
        .replace("@@end_date@@", self.end_date) \
        .replace("@@pollution_gas@@", ",".join(self.pollution_gas))

        if self.stations:
            query_min_concentration = query_min_concentration.replace("@@stations_query@@", " AND pc.id_station IN (" + ",".join(self.stations) + ")")
            query_min_concentration = query_min_concentration.replace("@@partition_station@@", ", pc.id_station")
        else:
            query_min_concentration = query_min_concentration.replace("@@stations_query@@", "")
            query_min_concentration = query_min_concentration.replace("@@partition_station@@", "")

        self.cursor.execute(query_min_concentration)
        query_result = self.cursor.fetchall()
        
        result = []
        for row in query_result:
            result.append({
                'min_concentration': round(row['min_concentration'], 2),
                'date_measurement': row['date_measurement'],
                'name_gas': row['name_gas'],
                'name_units': row['name_units'],
                'name_station': row['name_station'],
                'name_type': row['name_type'],
                'name_area': row['name_area'],
                'name_municipality': row['name_municipality'],
                'name_comarca': row['name_comarca']
            })

        return result
    
    def get_time_series_concentration_gas(self) -> list:
        """
        Funció que permet obtenir la sèrie temporal de concentració de gasos emesos entre dates.

        Returns:
            list: Llistat resultant de la sèrie temporal de concentració de gasos emesos.
        """

        query = """
            SELECT 
                pc.date_measurement AS timestamp,
                CONCAT(g.name_gas, ' - ', u.name_units) AS serie_key,
                AVG(pc.concentration) AS concentration,
                u.name_units AS unit
            FROM pollution_concentration pc
            INNER JOIN pollution_gas g ON g.id = pc.pollution_gas
            INNER JOIN unit u ON g.id_unit = u.id
            WHERE 
                DATE(pc.date_measurement) BETWEEN '@@start_date@@' AND '@@end_date@@'
                AND pc.pollution_gas IN (@@pollution_gas@@)
                @@stations_query@@
            GROUP BY pc.date_measurement, serie_key, unit
            ORDER BY pc.date_measurement ASC;
        """
        

        query = query \
        .replace("@@start_date@@", self.start_date) \
        .replace("@@end_date@@", self.end_date) \
        .replace("@@pollution_gas@@", ",".join(self.pollution_gas))
        
        if self.stations:
            query = query.replace("@@stations_query@@", " AND id_station IN (" + ",".join(self.stations) + ")")
        else:
            query = query.replace("@@stations_query@@", "")

        self.cursor.execute(query)

        query_result = self.cursor.fetchall()

        result = []
        for row in query_result:
            result.append({
                'timestamp': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'serie_key': row['serie_key'],
                'unit': row['unit'],
                'concentration': round(row['concentration'], 2)
            })

        return result
    
    def get_data_to_compare_pollutions_per_station_type(self) -> list:
        """
        Funció que permet obtenir la informació de concentració de gasos emesos entre dates per tipus d'estació.
        """

        query = """
            SELECT 
                st.name_type AS tipus_estacio,
                g.name_gas AS contaminant,
                AVG(pc.concentration) AS mitjana_concentracio
            FROM pollution_concentration pc
            INNER JOIN station s ON pc.id_station = s.id
            INNER JOIN station_type st ON s.id_station_type = st.id
            INNER JOIN pollution_gas g ON pc.pollution_gas = g.id
            WHERE DATE(pc.date_measurement) BETWEEN '@@start_date@@' AND '@@end_date@@'
                AND pc.pollution_gas IN (@@pollution_gas@@)
                @@stations_query@@
            GROUP BY g.name_gas, st.name_type
            ORDER BY g.name_gas ASC, mitjana_concentracio DESC;
        """

        query = query \
        .replace("@@start_date@@", self.start_date) \
        .replace("@@end_date@@", self.end_date) \
        .replace("@@pollution_gas@@", ",".join(self.pollution_gas))

        if self.stations:
            query = query.replace("@@stations_query@@", " AND id_station IN (" + ",".join(self.stations) + ")")
        else:
            query = query.replace("@@stations_query@@", "")

        self.cursor.execute(query)

        query_result = self.cursor.fetchall()

        result = []
        for row in query_result:
            result.append({
                'tipus_estacio': row['tipus_estacio'],
                'contaminant': row['contaminant'],
                'mitjana_concentracio': round(row['mitjana_concentracio'], 2)
            })
        
        return result
    
    def get_stations_latest_data(self) -> list:
        """
        Retorna la llista d'estacions amb la seva última lectura dins del rang filtrat
        aplicant els llindars oficials de l'ICQA per colors i estats.

        Returns:
            list: Llistat d'estacions amb la seva última lectura i estat segons l'ICQA.
        """
        
        query = """
            WITH LatestMeasurements AS (
                SELECT 
                    s.id AS id_station,
                    s.name_station,
                    s.latitude,  
                    s.longitude, 
                    st.name_type AS tipus,
                    ua.name_area AS area,
                    pc.concentration,
                    pg.name_gas,
                    u.name_units,
                    pc.date_measurement,
                    ROW_NUMBER() OVER(PARTITION BY pc.id_station, pc.pollution_gas ORDER BY pc.date_measurement DESC) AS rn
                FROM pollution_concentration pc
                INNER JOIN station s ON pc.id_station = s.id
                INNER JOIN station_type st ON s.id_station_type = st.id
                INNER JOIN urban_area ua ON s.id_urban_area = ua.id
                INNER JOIN pollution_gas pg ON pc.pollution_gas = pg.id
                INNER JOIN unit u ON pg.id_unit = u.id
                WHERE 
                    DATE(pc.date_measurement) BETWEEN '@@start_date@@' AND '@@end_date@@'
                    AND pc.pollution_gas IN (@@pollution_gas@@)
                    @@stations_query@@
            )
            SELECT * FROM LatestMeasurements WHERE rn = 1;
        """

        query = query \
            .replace("@@start_date@@", self.start_date) \
            .replace("@@end_date@@", self.end_date) \
            .replace("@@pollution_gas@@", ",".join(self.pollution_gas))

        if self.stations:
            query = query.replace("@@stations_query@@", " AND pc.id_station IN (" + ",".join(self.stations) + ")")
        else:
            query = query.replace("@@stations_query@@", "")

        self.cursor.execute(query)
        raw_estacions = self.cursor.fetchall()

        # Llindars oficials ICQA
        ICQA_THRESHOLDS = {
            'NO2':   [(40, "Bona", "#3a9ad9"), (90, "Raonablement bona", "#4daf4a"), (120, "Regular", "#ffcc00"), (230, "Desfavorable", "#e41a1c"), (340, "Molt desfavorable", "#800000"), (float('inf'), "Extremadament desfavorable", "#7a297a")],
            'PM10':  [(20, "Bona", "#3a9ad9"), (40, "Raonablement bona", "#4daf4a"), (50, "Regular", "#ffcc00"), (100, "Desfavorable", "#e41a1c"), (150, "Molt desfavorable", "#800000"), (float('inf'), "Extremadament desfavorable", "#7a297a")],
            'PM2.5': [(10, "Bona", "#3a9ad9"), (20, "Raonablement bona", "#4daf4a"), (25, "Regular", "#ffcc00"), (50, "Desfavorable", "#e41a1c"), (75, "Molt desfavorable", "#800000"), (float('inf'), "Extremadament desfavorable", "#7a297a")],
            'O3':    [(50, "Bona", "#3a9ad9"), (100, "Raonablement bona", "#4daf4a"), (130, "Regular", "#ffcc00"), (240, "Desfavorable", "#e41a1c"), (380, "Molt desfavorable", "#800000"), (float('inf'), "Extremadament desfavorable", "#7a297a")],
            'SO2':   [(100, "Bona", "#3a9ad9"), (200, "Raonablement bona", "#4daf4a"), (350, "Regular", "#ffcc00"), (500, "Desfavorable", "#e41a1c"), (750, "Molt desfavorable", "#800000"), (float('inf'), "Extremadament desfavorable", "#7a297a")],
            'CO':    [(2, "Bona", "#3a9ad9"), (5, "Raonablement bona", "#4daf4a"), (10, "Regular", "#ffcc00"), (20, "Desfavorable", "#e41a1c"), (50, "Molt desfavorable", "#800000"), (float('inf'), "Extremadament desfavorable", "#7a297a")],
            'C6H6':  [(5, "Bona", "#3a9ad9"), (10, "Raonablement bona", "#4daf4a"), (20, "Regular", "#ffcc00"), (50, "Desfavorable", "#e41a1c"), (100, "Molt desfavorable", "#800000"), (float('inf'), "Extremadament desfavorable", "#7a297a")],
            'H2S':   [(25, "Bona", "#3a9ad9"), (50, "Raonablement bona", "#4daf4a"), (100, "Regular", "#ffcc00"), (200, "Desfavorable", "#e41a1c"), (500, "Molt desfavorable", "#800000"), (float('inf'), "Extremadament desfavorable", "#7a297a")]
        }

        stations_map = {}

        for row in raw_estacions:
            sid = row['id_station']
            conc = row['concentration']
            gas_name = row['name_gas']
            date_str = row['date_measurement'].strftime('%d/%m/%Y %H:%M') if row['date_measurement'] else "Sense data"
            
            gas_key = gas_name.split()[0].upper().replace(',', '.')
            
            # Rang per defecte
            estat_gas, color_gas = ("Regular", "#ffcc00")
            if gas_key in ICQA_THRESHOLDS:
                for limit, status_name, hex_color in ICQA_THRESHOLDS[gas_key]:
                    if conc <= limit:
                        estat_gas, color_gas = (status_name, hex_color)
                        break
            elif conc < 20: 
                estat_gas, color_gas = ("Bona", "#3a9ad9")

            if sid not in stations_map:
                stations_map[sid] = {
                    'id': sid,
                    'nom': row['name_station'],
                    'lat': float(row['latitude']) if row['latitude'] else 41.75,
                    'lng': float(row['longitude']) if row['longitude'] else 1.75,
                    'tipus': row['tipus'],
                    'area': row['area'],
                    'lectures': [],
                    'ultima_actualitzacio': date_str
                }
            
            # Creem el component visual de la línia amb un quadrat de color estil "badge" per a cada gas
            html_linea = (
                f"<div style='margin-bottom: 4px; display: flex; align-items: center; justify-content: space-between; gap: 10px; color:white;'>"
                f"  <span>• <strong>{gas_name}:</strong> {round(conc, 2)} {row['name_units']}</span>"
                f"  <span style='background: {color_gas}; color: #fff; font-size: 10px; font-weight: bold; "
                f"  padding: 1px 6px; border-radius: 4px; white-space: nowrap;'>{estat_gas}</span>"
                f"</div>"
            )
            stations_map[sid]['lectures'].append(html_linea)

        station_list = []
        for sid, s in stations_map.items():
            station_list.append({
                'id': s['id'],
                'nom': s['nom'],
                'lat': s['lat'],
                'lng': s['lng'],
                'tipus': s['tipus'],
                'area': s['area'],
                'ultima_lectura': "".join(s['lectures']), 
                'data_actualitzacio': s['ultima_actualitzacio']
            })

        return station_list

    def get_pollution_gas_concentration_boxplot_data(self) -> list:
        """
        Funció que permet obtenir la informació de concentració de gasos emesos entre dates per fer el boxplot.

        Returns:
            list: Llistat resultant de la informació de concentració de gasos emesos entre dates per fer el boxplot.
        """

        query = """
            SELECT 
                g.name_gas AS contaminant,
                pc.concentration AS valor,
                pc.date_measurement AS timestamp,
                u.name_units AS unitat
            FROM 
                pollution_concentration pc
            INNER JOIN pollution_gas g ON g.id = pc.pollution_gas
            INNER JOIN unit u ON g.id_unit = u.id
            WHERE 
                pc.date_measurement BETWEEN '@@start_date@@' AND '@@end_date@@'
                AND pc.pollution_gas IN (@@pollution_gas@@)
                @@stations_query@@
            """
        
        query = query \
        .replace("@@start_date@@", self.start_date) \
        .replace("@@end_date@@", self.end_date) \
        .replace("@@pollution_gas@@", ",".join(self.pollution_gas))

        if self.stations:
            query = query.replace("@@stations_query@@", f" AND id_station IN ({','.join(self.stations)})")
        else:
            query = query.replace("@@stations_query@@", "")

        self.cursor.execute(query)
        query_result = self.cursor.fetchall()

        result = []
        for row in query_result:
            result.append({
                'contaminant': row['contaminant'],
                'concentration': round(row['valor'], 2),
                'unitat': row['unitat'],
                'timestamp': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            })

        return result