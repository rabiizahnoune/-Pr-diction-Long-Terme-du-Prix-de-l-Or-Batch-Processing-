from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import json
import subprocess


default_args = {
    'owner': 'etudiant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import json

def fetch_gold_data_hourly(**context):
    """Récupère les données horaires de l'or (GC=F) depuis 2006 à aujourd'hui."""
    symbol = 'GC=F'
    end_time = datetime.now()
    start_time = datetime(2002, 1, 1)
    
    try:
        gold = yf.Ticker(symbol)
        df = gold.history(
            start=start_time,
            end=end_time,
            interval='1D'
        )
        
        if df.empty:
            print("Aucune donnée pour {}".format(symbol))
            raise ValueError("Aucune donnée récupérée pour {}".format(symbol))
        
        # Réinitialiser l'index pour transformer les timestamps en colonne
        df = df.reset_index()
        
        # Afficher les colonnes disponibles pour diagnostic
        print("Colonnes disponibles après reset_index :")
        print(df.columns.tolist())
        
        # Adapter le renommage en fonction du nom réel de la colonne timestamp
        # Vérifiez la sortie ci-dessus et ajustez si nécessaire
        if 'Date' in df.columns:
            df = df.rename(columns={'Date': 'datetime'})
        elif 'Datetime' in df.columns:
            df = df.rename(columns={'Datetime': 'datetime'})
        else:
            raise ValueError("Colonne timestamp introuvable dans les données")
        
        df['asset'] = 'GOLD'
        
        # Colonnes à conserver
        columns_to_keep = ['datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'asset']
        df = df[columns_to_keep]
        
        # Renommer les colonnes selon votre convention
        df = df.rename(columns={
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume': 'volume'
        })
        
        df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        json_data = df.to_dict(orient='records')
        
        # Push dans XCom
        context['ti'].xcom_push(key='raw_data', value=json_data)
    
    except Exception as e:
        print(f"Erreur lors de la récupération de {symbol}: {e}")
        raise ValueError(f"Erreur lors de la récupération des données: {e}")

def store_raw_data_in_hdfs(**context):
    data = context['ti'].xcom_pull(key='raw_data')
    # chmod 775 /mnt/hadoop_data/
    subprocess.run(["docker","exec","-i","-u","root","file_rouge-airflow-webserver-1","chmod","775","/mnt/hadoop_data/"])
    local_file = '/mnt/hadoop_data/yfinance_raw_historique.json'
    with open(local_file, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')  # Écrire chaque objet sur une nouvelle ligne
    
    execution_date = context['ds']
    year, month, day = execution_date.split('-')
    hdfs_dir = f"/user/etudiant/crypto/raw/historique_gold"
    hdfs_file_path = f"{hdfs_dir}/yfinance_raw_historique.json"
    subprocess.run(["docker", "exec", "-i", "-u", "root", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    subprocess.run(["docker", "exec", "-i", "-u", "root", "namenode", "hdfs", "dfs", "-put", "-f", local_file, hdfs_file_path])
with DAG(
    'yfinance_ingestion_historique_gold',  # Nom corrigé
    default_args=default_args
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_gold_data_hourly,
        provide_context=True
    )

    store_raw_data = PythonOperator(
        task_id='store_raw_data_in_hdfs',
        python_callable=store_raw_data_in_hdfs,
        provide_context=True
    )
    fetch_data >> store_raw_data

