import yfinance as yf
import pandas as pd
import numpy as np
import ta  # Pour les indicateurs techniques
import requests

# -------------------- 1️ Extraction des données de l'or --------------------
def get_gold_data(start_date="2024-01-01", end_date="2024-04-01", interval="1h"):
    """Télécharge les données historiques de l'or (XAU/USD)"""
    gold = yf.download("GC=F", start=start_date, end=end_date, interval=interval)
    gold.dropna(inplace=True)  # Supprime les valeurs manquantes
    return gold

# -------------------- 2️ Calcul des indicateurs techniques --------------------
def add_technical_indicators(df):
    """Ajoute les indicateurs techniques aux données de l'or"""

    df['MA50'] = df['Close'].rolling(window=50).mean()  
    df['MA200'] = df['Close'].rolling(window=200).mean()  

    df['RSI'] = ta.momentum.RSIIndicator(df['Close'].squeeze(), window=14).rsi()

    macd = ta.trend.MACD(df['Close'].squeeze())
    df['MACD'] = macd.macd()

    bb = ta.volatility.BollingerBands(df['Close'].squeeze())
    df['BB_High'] = bb.bollinger_hband()
    df['BB_Low'] = bb.bollinger_lband()

    return df

# -------------------- 4️ Fusion des données et exportation --------------------
def main():
    print("📥 Téléchargement des données de l'or...")
    gold_data = get_gold_data()

    print("📊 Ajout des indicateurs techniques...")
    gold_data = add_technical_indicators(gold_data)
    
    # Enregistrement des données dans un fichier CSV
    gold_data.to_csv("gold_data_complete.csv")
    print(" Données enregistrées dans 'gold_data_complete.csv'")

if __name__ == "__main__":
    main()
