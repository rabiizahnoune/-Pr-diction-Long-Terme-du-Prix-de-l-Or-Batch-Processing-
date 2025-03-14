from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import json
import subprocess
from hdfs import InsecureClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

# Arguments par défaut
default_args = {
    'owner': 'etudiant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

def fetch_gold_data_daily(**context):
    """Récupère les données quotidiennes de l'or (GC=F) pour la dernière journée."""
    symbol = 'GC=F'
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    
    try:
        gold = yf.Ticker(symbol)
        df = gold.history(
            start=start_time,
            end=end_time,
            interval='1d'
        )
        
        if df.empty:
            print(f"Aucune donnée pour {symbol}")
            raise ValueError(f"Aucune donnée récupérée pour {symbol}")
        
        latest_data = df.iloc[-1].to_frame().T.reset_index()
        latest_data = latest_data.rename(columns={
            'index': 'datetime',
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume': 'volume'
        })
        latest_data['asset'] = 'GOLD'
        
        columns_to_keep = ['datetime', 'open_price', 'high_price', 'low_price', 
                          'close_price', 'volume', 'asset']
        latest_data = latest_data[columns_to_keep]
        
        latest_data['datetime'] = latest_data['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        json_data = latest_data.to_dict(orient='records')
        
        print(json.dumps(json_data, indent=4))
        context['ti'].xcom_push(key='raw_data', value=json_data[0])
    
    except Exception as e:
        print(f"Erreur lors de la récupération de {symbol}: {e}")
        raise ValueError(f"Erreur lors de la récupération des données: {e}")

def update_historic_file_in_hdfs(**context):
    """Ajoute la nouvelle ligne au fichier historique existant dans HDFS."""
    new_data = context['ti'].xcom_pull(key='raw_data')
    hdfs_client = InsecureClient('http://namenode:9870', user='etudiant')
    
    hdfs_historic_dir = "/user/etudiant/crypto/raw/historique_gold"
    hdfs_historic_file = f"{hdfs_historic_dir}/yfinance_raw_historique.json"
    
    # Lire le fichier existant (si il existe)
    existing_data = []
    try:
        with hdfs_client.read(hdfs_historic_file) as reader:
            for line in reader:
                existing_data.append(json.loads(line.strip()))
        print(f"Fichier historique chargé, {len(existing_data)} lignes existantes.")
    except Exception as e:
        print(f"Aucun fichier historique existant ou erreur de lecture : {e}, création d'un nouveau.")
    
    # Ajouter la nouvelle ligne (éviter les doublons par datetime)
    new_datetime = new_data['datetime']
    if not any(entry['datetime'] == new_datetime for entry in existing_data):
        existing_data.append(new_data)
        print(f"Nouvelle ligne ajoutée, total : {len(existing_data)} lignes.")
    else:
        print(f"La ligne pour {new_datetime} existe déjà, pas d'ajout.")
    
    # Écrire dans un fichier temporaire dans le volume partagé
    temp_file = '/mnt/hadoop_data/yfinance_raw_historique.json'
    try:
        # Assurer les permissions sur le volume partagé
        subprocess.run(["docker","exec","-i","-u","root","file_rouge-airflow-webserver-1","chmod","775","/mnt/hadoop_data/"])
        with open(temp_file, 'w') as f:
            for record in existing_data:
                f.write(json.dumps(record) + '\n')
        print(f"Fichier temporaire {temp_file} créé avec succès.")
    except Exception as e:
        print(f"Erreur lors de la création du fichier temporaire : {e}")
        raise ValueError(f"Échec de la création du fichier temporaire : {e}")
    
    # Transférer vers HDFS
    try:
        subprocess.run(["docker", "exec", "-i", "-u", "root", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_historic_dir], check=True)
        subprocess.run(["docker", "exec", "-i", "-u", "root", "namenode", "hdfs", "dfs", "-put", "-f", temp_file, hdfs_historic_file], check=True)
        print(f"Fichier {hdfs_historic_file} mis à jour avec succès dans HDFS.")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors du transfert vers HDFS : {e}")
        raise ValueError(f"Échec du transfert vers HDFS : {e}")
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"Fichier temporaire {temp_file} supprimé.")

def store_partitioned_data_in_hdfs(**context):
    """Stocke les données dans une structure hiérarchique YYYY=/MM=/DD=."""
    new_data = context['ti'].xcom_pull(key='raw_data')
    execution_date = context['ds']
    year, month, day = execution_date.split('-')
    hdfs_dir = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}"
    hdfs_file_path = f"{hdfs_dir}/yfinance_raw_historique.json"
    
    # Écrire dans un fichier temporaire dans le volume partagé
    temp_file = '/mnt/hadoop_data/yfinance_raw_historique_partitioned.json'
    try:
        # Assurer les permissions sur le volume partagé
        subprocess.run(["docker","exec","-i","-u","root","file_rouge-airflow-webserver-1","chmod","775","/mnt/hadoop_data/"])
        with open(temp_file, 'w') as f:
            f.write(json.dumps(new_data) + '\n')
        print(f"Fichier temporaire {temp_file} créé avec succès.")
    except Exception as e:
        print(f"Erreur lors de la création du fichier temporaire : {e}")
        raise ValueError(f"Échec de la création du fichier temporaire : {e}")
    
    # Transférer vers HDFS
    try:
        subprocess.run(["docker", "exec", "-i", "-u", "root", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        subprocess.run(["docker", "exec", "-i", "-u", "root", "namenode", "hdfs", "dfs", "-put", "-f", temp_file, hdfs_file_path], check=True)
        print(f"Fichier {hdfs_file_path} écrit avec succès dans HDFS.")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'écriture du fichier partitionné dans HDFS : {e}")
        raise ValueError(f"Échec de l'écriture dans HDFS : {e}")
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"Fichier temporaire {temp_file} supprimé.")

def transform_data(**context):
    """Transforme les données en ajoutant des features pour le modèle LSTM."""
    hdfs_client = InsecureClient('http://namenode:9870', user='etudiant')
    hdfs_historic_file = "/user/etudiant/crypto/raw/historique_gold/yfinance_raw_historique.json"
    
    # Lire toutes les données historiques
    existing_data = []
    try:
        with hdfs_client.read(hdfs_historic_file) as reader:
            for line in reader:
                existing_data.append(json.loads(line.strip()))
        print(f"Données historiques chargées : {len(existing_data)} lignes.")
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier historique : {e}")
        raise ValueError(f"Échec de la lecture du fichier historique : {e}")
    
    # Convertir en DataFrame
    df = pd.DataFrame(existing_data)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.sort_values('datetime')
    df.set_index('datetime', inplace=True)
    
    # Feature engineering
    df['ma_5'] = df['close_price'].rolling(window=5).mean()
    df['ma_20'] = df['close_price'].rolling(window=20).mean()
    
    def calculate_rsi(data, periods=14):
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    df['rsi'] = calculate_rsi(df['close_price'])
    
    for lag in range(1, 4):
        df[f'lag_{lag}'] = df['close_price'].shift(lag)
    
    # Supprimer les NaN
    df = df.dropna()
    
    # Convertir en JSON pour stockage
    df_reset = df.reset_index()
    df_reset['datetime'] = df_reset['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    transformed_data = df_reset.to_dict(orient='records')
    
    # Push dans XCom
    context['ti'].xcom_push(key='transformed_data', value=transformed_data)

def store_transformed_data_in_cassandra(**context):
    """Stocke les données transformées dans Cassandra."""
    transformed_data = context['ti'].xcom_pull(key='transformed_data')
    
    # Connexion à Cassandra
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect()
    
    # Créer keyspace et table si nécessaire
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS crypto WITH replication = 
        {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace('crypto')
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS gold_transformed (
            datetime timestamp PRIMARY KEY,
            open_price float,
            high_price float,
            low_price float,
            close_price float,
            volume bigint,
            asset text,
            ma_5 float,
            ma_20 float,
            rsi float,
            lag_1 float,
            lag_2 float,
            lag_3 float
        )
    """)
    
    # Insérer les données
    insert_query = """
        INSERT INTO gold_transformed (datetime, open_price, high_price, low_price, close_price, 
        volume, asset, ma_5, ma_20, rsi, lag_1, lag_2, lag_3)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared_stmt = session.prepare(insert_query)
    
    for record in transformed_data:
        record_datetime = pd.to_datetime(record['datetime'])
        session.execute(prepared_stmt, (
            record_datetime,
            record['open_price'],
            record['high_price'],
            record['low_price'],
            record['close_price'],
            int(record['volume']),
            record['asset'],
            record.get('ma_5', None),
            record.get('ma_20', None),
            record.get('rsi', None),
            record.get('lag_1', None),
            record.get('lag_2', None),
            record.get('lag_3', None)
        ))
    
    cluster.shutdown()

# Définir le DAG
with DAG(
    'yfinance_ingestion_dag',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_gold_data_daily,
        provide_context=True
    )

    update_historic_file = PythonOperator(
        task_id='update_historic_file_in_hdfs',
        python_callable=update_historic_file_in_hdfs,
        provide_context=True
    )

    store_partitioned_data = PythonOperator(
        task_id='store_partitioned_data_in_hdfs',
        python_callable=store_partitioned_data_in_hdfs,
        provide_context=True
    )

    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    store_in_cassandra = PythonOperator(
        task_id='store_transformed_data_in_cassandra',
        python_callable=store_transformed_data_in_cassandra,
        provide_context=True
    )

    fetch_data >> update_historic_file >> store_partitioned_data >> transform_data_task >> store_in_cassandra