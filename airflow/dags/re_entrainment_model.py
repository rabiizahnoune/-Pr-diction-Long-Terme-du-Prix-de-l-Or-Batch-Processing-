from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout
import os
import subprocess

# Arguments par défaut pour le DAG hebdomadaire
default_args = {
    'owner': 'etudiant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def prepare_data_for_lstm(**context):
    """Prépare les données transformées pour l'entraînement du LSTM."""
    try:
        # Connexion à Cassandra avec une politique de répartition de charge et un protocole explicite
        cluster = Cluster(
            ['cassandra'],
            port=9042,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
            protocol_version=4
        )
        session = cluster.connect('crypto')
        
        # Récupérer toutes les données sans ORDER BY
        rows = session.execute("SELECT * FROM gold_transformed")
        df = pd.DataFrame(list(rows))
        
        # Trier les données par datetime avec Pandas
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.sort_values('datetime')
        
        cluster.shutdown()

        # Normaliser uniquement la colonne close_price
        scaler = MinMaxScaler()
        scaled_data = scaler.fit_transform(df[['close_price']])

        # Créer des séquences pour LSTM (60 jours pour prédire le suivant)
        def create_sequences(data, seq_length):
            X, y = [], []
            for i in range(len(data) - seq_length):
                X.append(data[i:i + seq_length])
                y.append(data[i + seq_length])
            return np.array(X), np.array(y)

        seq_length = 60
        X, y = create_sequences(scaled_data, seq_length)

        # Diviser en ensembles d'entraînement et de test
        train_size = int(len(X) * 0.8)
        X_train, X_test = X[:train_size], X[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]

        # Sauvegarder les données préparées dans XCom
        context['ti'].xcom_push(key='X_train', value=X_train.tolist())
        context['ti'].xcom_push(key='y_train', value=y_train.tolist())
        context['ti'].xcom_push(key='X_test', value=X_test.tolist())
        context['ti'].xcom_push(key='y_test', value=y_test.tolist())
        # Sauvegarder les paramètres du scaler au lieu de l'objet scaler
        scaler_params = {
            'scale_': scaler.scale_.tolist(),
            'min_': scaler.min_.tolist(),
            'data_range_': scaler.data_range_.tolist()
        }
        context['ti'].xcom_push(key='scaler_params', value=scaler_params)
        print("Données préparées avec succès.")
    except Exception as e:
        print(f"Erreur dans prepare_data_for_lstm : {e}")
        raise

def train_lstm(**context):
    """Entraîne ou fine-tune un modèle LSTM avec les données préparées."""
    try:
        # Récupérer les données préparées depuis XCom
        X_train = np.array(context['ti'].xcom_pull(key='X_train'))
        y_train = np.array(context['ti'].xcom_pull(key='y_train'))
        X_test = np.array(context['ti'].xcom_pull(key='X_test'))
        y_test = np.array(context['ti'].xcom_pull(key='y_test'))
        scaler_params = context['ti'].xcom_pull(key='scaler_params')

        # Reconstruire le scaler à partir des paramètres
        scaler = MinMaxScaler()
        scaler.scale_ = np.array(scaler_params['scale_'])
        scaler.min_ = np.array(scaler_params['min_'])
        scaler.data_range_ = np.array(scaler_params['data_range_'])

        # Reshape des données pour LSTM
        X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))
        X_test = X_test.reshape((X_test.shape[0], X_test.shape[1], 1))
        
        subprocess.run([
            "docker","exec","-i","-u","root","file_rouge-airflow-webserver-1","chmod","775","/mnt/hadoop_data/"
            ])
        
        subprocess.run([
            "docker","exec","-i","-u","root","file_rouge-airflow-webserver-1","chmod","775","/mnt/hadoop_data/lstm_gold_model.h5"
            ])
        

        # Chemin du modèle dans le volume partagé
        model_path = '/mnt/hadoop_data/lstm_gold_model.h5'

        # Vérifier si un modèle existe déjà pour fine-tuning
        if os.path.exists(model_path):
            print("Chargement du modèle existant pour fine-tuning...")
            model = load_model(model_path)
        else:
            print("Création d'un nouveau modèle LSTM...")
            model = Sequential([
                LSTM(50, return_sequences=True, input_shape=(60, 1)),
                Dropout(0.2),
                LSTM(50),
                Dropout(0.2),
                Dense(25),
                Dense(1)
            ])
            model.compile(optimizer='adam', loss='mean_squared_error')

        # Entraîner ou fine-tuner le modèle
        model.fit(X_train, y_train, epochs=10, batch_size=32, validation_split=0.1, verbose=1)

        # Évaluer le modèle
        y_pred = model.predict(X_test)
        # Corriger l'appel à inverse_transform pour éviter une dimension supplémentaire
        y_test_inv = scaler.inverse_transform(y_test)  # Supprimer [y_test]
        y_pred_inv = scaler.inverse_transform(y_pred)
        mse = np.mean((y_test_inv - y_pred_inv) ** 2)
        print(f"Test MSE: {mse:.2f}")

        # Sauvegarder le modèle mis à jour
        model.save(model_path)
        print("Modèle entraîné et sauvegardé avec succès.")
    except Exception as e:
        print(f"Erreur dans train_lstm : {e}")
        raise

# Définir le DAG hebdomadaire
with DAG(
    'yfinance_lstm_training_dag',
    default_args=default_args,
    schedule_interval='0 0 * * 0',
    catchup=False
) as dag:
    prepare_data_task = PythonOperator(
        task_id='prepare_data_for_lstm',
        python_callable=prepare_data_for_lstm,
        provide_context=True
    )

    train_lstm_task = PythonOperator(
        task_id='train_lstm',
        python_callable=train_lstm,
        provide_context=True
    )

    # Définir les dépendances
    prepare_data_task >> train_lstm_task