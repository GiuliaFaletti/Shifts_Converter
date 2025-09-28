# ============================================================================
# Convertitore turni in Layout Standard 
# Created by: Giulia Faletti
# 27/09/2025
# ============================================================================
# Scopo:
#   - Caricare un Excel contenete i turni delle ditte clienti
#   - Effettuare parsing e conversione verso il Layout Standard 
#   - Restituire CSV di output ed eventuale report degli errori riscontrati
# ============================================================================

Avvio locale:
1) python3 -m venv venv && source venv/bin/activate
2) pip install -r requirements.txt
3) streamlit run app.py