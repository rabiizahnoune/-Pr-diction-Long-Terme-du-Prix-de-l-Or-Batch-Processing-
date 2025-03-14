# -*- coding: utf-8 -*-
from datetime import datetime
from dateutil.relativedelta import relativedelta
import yfinance as yf
import json

def fetch_gold_data_hourly():
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
        
        print("Aperçu des 5 premières lignes :")
        print(json.dumps(json_data[:5], indent=4))
        
        print("Données récupérées avec succès : {} enregistrements".format(len(json_data)))
        
    except Exception as e:
        print("Erreur lors de la récupération de {}: {}".format(symbol, e))
        raise ValueError("Erreur lors de la récupération des données: {}".format(str(e)))

if __name__ == "__main__":
    fetch_gold_data_hourly()