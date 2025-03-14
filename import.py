from datetime import datetime
from dateutil.relativedelta import relativedelta  # Ajout pour gérer les années
import yfinance as yf
import json

def fetch_gold_data_hourly(**context):
    """Récupère les données horaires de l'or (GC=F) depuis 2006 à aujourd'hui."""
    symbol = 'GC=F'
    end_time = datetime.now()
    start_time = datetime(2006, 1, 1)  # Date de début fixée à janvier 2006
    
    try:
        gold = yf.Ticker(symbol)
        # Récupération des données avec intervalle horaire
        df = gold.history(
            start=start_time,
            end=end_time,
            interval='1h'  # Intervalle de 1 heure
        )
        
        if df.empty:
            print(f"Aucune donnée pour {symbol}")
            raise ValueError(f"Aucune donnée récupérée pour {symbol}")
        
        # Réinitialisation de l'index et renommage des colonnes
        df = df.reset_index()
        df = df.rename(columns={
            'Datetime': 'datetime',
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Volume': 'volume'
        })
        df['asset'] = 'GOLD'
        
        # Colonnes à conserver
        columns_to_keep = ['datetime', 'open_price', 'high_price', 'low_price', 
                         'close_price', 'volume', 'asset']
        df = df[columns_to_keep]
        
        # Conversion des timestamps en chaînes ISO pour JSON
        df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        # Conversion en liste de dictionnaires
        json_data = df.to_dict(orient='records')
        
        # Aperçu des 5 premières lignes
        print("Aperçu des 5 premières lignes :")
        print(json.dumps(json_data[:5], indent=4))
        
        # Push dans XCom
        context['ti'].xcom_push(key='raw_data', value=json_data)
        
        print(f"Données récupérées avec succès : {len(json_data)} enregistrements")
        
    except Exception as e:
        print(f"Erreur lors de la récupération de {symbol}: {e}")
        raise ValueError(f"Erreur lors de la récupération des données: {str(e)}")
