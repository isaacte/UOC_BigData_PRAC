# Scripts i configuració de la instància d'EC2

Assignatura: Anàlisi de dades en entorns Big Data / Semestre: 2 / Data: 28-05-2026

# Autors

* Francesc Ferré Tarrés - fferretar@uoc.edu
* Isaac Torres Espuña - isaacte@uoc.edu

# Descripció del repositori

En aquest repositori hi ha tots els arxius i scripts necessaris per a la instància d'EC2 responsable de processar les dades de la qualitat de l'aire que estan disponibles en un *data lake* a S3.

# Estructura del repositori

* `templates/`: Conté les vistes HTML per a visualitzar la informació que es processa i consultar les dades.
  * `dashboard.html`: Pantalla que informa de l'estat de la ingesta de les dades.
  * `statistics.html`: Pantalla que permet filtrar les dades carregades a base de dades i genera visualitzacions i taules informatives.
  * `statistics_data_graphs.html`: Conté els gràfics i les taules informatives que apareixen després de fer l'AJAX del formulari.
* `app.py`: Fitxer principal que conté l'aplicació Flask, la qual ens proveeix la pàgina web.
* `etl.py`: Script python que executa tot el procediment de recuperació de les dades originals (en el *data lake* d'S3) a través d'Athena i converteix el CSV resultant d'Athena a inserts a la base de dades.
* `flaskapp.serice`: Servei que permet que l'aplicació flask s'executi en background dins de l'EC2.
* `init.sh`: Script que s'executa inicialment a la instància d'EC2 i que prepara el desplegament de la màquina d'EC2.
* `requirements.txt`: Fitxer de requeriments de llibreries python necessaries per a que la màquina funcioni correctament.
* `statistics_queries.py`: Fitxer auxiliar que conté totes les consules necessaries per a generar els gràfics i les taules resum de les dades desades a base de dades relatives a la qualitat de l'aire de Catalunya.
