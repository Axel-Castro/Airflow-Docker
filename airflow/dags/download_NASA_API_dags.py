from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import requests
import os
from datetime import datetime, timedelta

input_coord_estado = 'https://raw.githubusercontent.com/Axel-Castro/GeoJson-Mex/main/Geojson_Mex/Coordenadas_Centroide_Estado.csv'
coord_estado = pd.read_csv(input_coord_estado)
estado_nom = coord_estado['Estado'].tolist()
estado_ids = coord_estado['Estado_ID'].tolist()
locations = [(lat, lon) for lat, lon in zip(coord_estado['centroid_lat'], coord_estado['centroid_lon'])]

output_estado_raw = '/opt/airflow/data/Estados_Raw/'
os.makedirs(output_estado_raw, exist_ok=True)
output_estado_mod = '/opt/airflow/data/Estados_Mod/'
os.makedirs(output_estado_mod, exist_ok=True)
output_estado_tydy = '/opt/airflow/data/Estados_Final/'
os.makedirs(output_estado_tydy, exist_ok=True)

parameter = 'TS,TS_MAX,TS_MIN,T2M,T2M_MAX,T2M_MIN,T2MDEW,T2MWET,PS,WS2M,RH2M,EVPTRNS,GWETPROF,CLRSKY_DAYS,PRECTOTCORR,SG_DAY_HOURS,MIDDAY_INSOL'
end_day = datetime.today().date().strftime('%Y%m%d')
start_day = (datetime.today().date() - timedelta(days=7)).strftime('%Y%m%d')
base_url = r"https://power.larc.nasa.gov/api/temporal/daily/point?parameters={parameter}&community=RE&longitude={longitude}&latitude={latitude}&start={start_day}&end={end_day}&format=CSV"



def download_data(**kwargs):

    for estado_nom, latitude, longitude in zip(coord_estado['Estado'], coord_estado['centroid_lat'], coord_estado['centroid_lon']):
        api_request_url = base_url.format(longitude=longitude, latitude=latitude, parameter=parameter, start_day=start_day, end_day=end_day)

        response = requests.get(url=api_request_url, verify=True, timeout=30.00)

        if response.status_code == 200:
            content = response.content.decode('utf-8')
            filename = f"Data_{estado_nom}.csv"
            filepath = os.path.join(output_estado_raw, filename)

            with open(filepath, 'wb') as file_object:
                file_object.write(response.content)

    #logging.info('Data downloaded successfully')


def process_data(**kwargs):
    # Obtén la lista de archivos CSV en la carpeta
    archivos_csv = [archivo for archivo in os.listdir(output_estado_raw) if archivo.endswith('.csv')]
    n = 0 # Contador de toda vida 

    # Procesa y transforma cada archivo CSV
    for archivo_csv in archivos_csv:

        # Lee el archivo CSV, salta las primeras 25 filas y agrega una columna con el nombre del estado
        ruta_completa = os.path.join(output_estado_raw, archivo_csv)
        df = pd.read_csv(ruta_completa, skiprows=25)
        estado_nom = archivo_csv.split('_')[1].split('.')[0]
        df.insert(0, 'Estado_Nom', estado_nom)

        # Agrega una columna con el ID del estado
        df.insert(1, 'Estado_ID', estado_ids[n])
        n+=1
 
        # Guarda el archivo CSV procesado en la carpeta de salida
        ruta_completa_salida = os.path.join(output_estado_mod, archivo_csv)
        with open(ruta_completa_salida, 'w') as file_object:
            df.to_csv(file_object, index=False,lineterminator='\n')

    #logging.info('Data processed successfully')


def combine_data(**kwargs):
    # Obtener la lista de archivos CSV en la carpeta
    archivos_csv = [archivo for archivo in os.listdir(output_estado_mod)]
    # Lista para almacenar los DataFrames individuales
    dataframes = []
    # Lee y combina los archivos CSV en un solo DataFrame
    for archivo_csv in archivos_csv:
        ruta_completa = os.path.join(output_estado_mod, archivo_csv)
        df = pd.read_csv(ruta_completa)
        dataframes.append(df)
        # Combina los DataFrames en uno solo
        df_combinado = pd.concat(dataframes, ignore_index=True)
    return df_combinado

    #logging.info('Data combined successfully')

def standardize_data(df_combinado):
    # Definir el nuevo nombre de las columnas
    new_columns = ['Estado', 'Estado_ID', 'Año', 'Mes', 'Día', 'Temp_Superficial', 'Temp_Superficial_MAX',
                    'Temp_Superficial_MIN', 'Temp_2_Metros', 'Temp_2_Metros_MAX', 'Temp_2_Metros_MIN',
                    'Temp_2_Metros_Pto_Congelación', 'Temp_2_Metros_Pto_Húmedo', 'Presión_Superficial',
                    'Velocidad_Viento', 'Humedad_Relativa', 'Flujo_Evapotranspiración', 'Perfil_Humedad_Suelo',
                    'Dias_Sin_Nubosidad', 'Precipitacion', 'Horas_De_Sol', 'Insolacion_Mediodia']

    # Cambiar los nombres de las columnas
    df_combinado.columns = new_columns
    return df_combinado

    #logging.info('Data standardized successfully')

def save_data(df_combinado):
    output_filename = 'Data_Climatica_Diaria_Estados.csv'  # Nombre del archivo de salida
    output_path = os.path.join(output_estado_tydy, output_filename)

    # Guarda el DataFrame combinado en el archivo CSV utilizando with open
    with open(output_path, 'w', newline='') as file_object:
        df_combinado.to_csv(file_object, index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    description='A DAG for data processing tasks',
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=datetime(2024, 3, 20),
    catchup=False,  # Prevents backfilling for past dates
)

download_data_task = PythonOperator(
    task_id='download_data',
    python_callable=download_data,
    provide_context=True,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

combine_data_task = PythonOperator(
    task_id='combine_data',
    python_callable=combine_data,
    provide_context=True,
    dag=dag,
)

standardize_data_task = PythonOperator(
    task_id='standardize_data',
    python_callable=standardize_data,
    provide_context=True,
    dag=dag,
)

save_data_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    provide_context=True,
    dag=dag,
)

download_data_task >> process_data_task >> combine_data_task >> standardize_data_task >> save_data_task
