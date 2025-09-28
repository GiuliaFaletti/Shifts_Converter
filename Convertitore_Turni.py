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

from __future__ import annotations

import io
import os
import re
import sys
import tempfile
import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st


# ============================================================================
# CONFIGURAZIONE DI BASE
# ============================================================================

# Numero massimo di eventi per giorno supportati dal parser
# Aumentabile se in futuro il template avrà più slot evento
MAX_EVENTS: int = 5

# Righe mostrate in anteprima (UI)
PREVIEW_ROWS: int = 50

# Regex precompilate
RE_TIME = re.compile(r"^\d{1,2}:\d{2}$")      # es. "01:30"

# Setup logging 
logger = logging.getLogger("shifts_converter")
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
    _handler.setFormatter(_fmt)
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


# ============================================================================
# DATACLASS
# ============================================================================

@dataclass
class ConversionOutput:
    """Contenitore del risultato di conversione."""
    df_out: pd.DataFrame         # Layout pronto per export
    df_err: pd.DataFrame         # Avvisi/errori raccolti durante il processo


# ============================================================================
# UTILITY DI CONVERSIONE
# ============================================================================

def to_ddmmyy_from_token(tok) -> str:
    """
    Converte qualsiasi "token data" del template in input in stringa 'ddmmyy' (6 caratteri, con zeri).
    Ordine dei tentativi:
      1) Se stringa di sole cifre (5 o 6), interpreta come "ddmmyy" da destra:
         - yy = ultimi 2 caratteri
         - mm = due caratteri prima di yy
         - dd = tutto ciò che resta a sinistra (1–2 cifre)
         Esempi: '10825' → '010825' ; '200825' → '200825'
         Condizione: 1<=dd<=31 e 1<=mm<=12
      2) Se non plausibile come ddmmyy, prova il seriale Excel
      3) Altrimenti parser generale pandas (prima dayfirst=True, poi False)

    Ritorna:
        stringa 'ddmmyy'
    Solleva:
        ValueError se il token non è interpretabile
    """
    if tok is None or (isinstance(tok, float) and pd.isna(tok)) or str(tok).strip() == "":
        raise ValueError("Data mancante")

    s = str(tok).strip()
    # Rimuove eventuale terminazione ".0" introdotta da cast Excel/string -> float
    s = re.sub(r"\.0$", "", s)

    if s.isdigit():
        # 5-6 cifre trattate come ddmmyy (da destra)
        if len(s) in (5, 6):
            yy = int(s[-2:])
            mm = int(s[-4:-2])
            dd_str = s[:-4]
            if dd_str != "":
                try:
                    dd = int(dd_str)
                    if 1 <= mm <= 12 and 1 <= dd <= 31:
                        return f"{dd:02d}{mm:02d}{yy:02d}"
                except Exception:
                    pass
        # Seriale Excel plausibile
        try:
            val = int(s)
            if 30000 <= val <= 60000:
                d = pd.Timestamp("1899-12-30") + pd.to_timedelta(val, unit="D")
                return d.strftime("%d%m%y")
        except Exception:
            pass

    if RE_TIME.match(s):
        raise ValueError(f"Data non valida (trovato orario): {s}")

    # Parser generale
    try:
        d = pd.to_datetime(s, dayfirst=True, errors="raise")
        return d.strftime("%d%m%y")
    except Exception:
        d = pd.to_datetime(s, dayfirst=False, errors="raise")
        return d.strftime("%d%m%y")


def hours_to_int100(x) -> Optional[int]:
    """
    Converte "ore" in intero *100 (nullable):
      - numeri: 1 → 100; 7.5 → 750; '2,5' → 250
      - orari 'HH:MM': '01:30' → 150
      - vuoto/NaN → None (gestito da dtype Int64)
    """
    if x is None:
        return None
    if isinstance(x, float) and pd.isna(x):
        return None

    s = str(x).strip()
    if s == "":
        return None

    if RE_TIME.match(s):
        hh, mm = s.split(":")
        return int(round((int(hh) + int(mm) / 60.0) * 100))

    return int(round(float(s.replace(",", ".")) * 100))


def load_causali_from_sheet(xl_path: str) -> Dict[str, str]:
    """
    Se esiste un foglio 'Causali' con colonne 'Evento' e 'Codice Causale'
    restituisce una mappa Evento → Codice Causale, altrimenti {}.
    """
    try:
        tab = pd.read_excel(xl_path, sheet_name="Causali", engine="openpyxl")
        cols = [c.strip().lower() for c in tab.columns]
        tab.columns = cols
        if "evento" in cols and "codice causale" in cols:
            return dict(zip(
                tab["evento"].astype(str).str.strip(),
                tab["codice causale"].astype(str).str.strip()
            ))
    except Exception:
        pass
    return {}


def load_causali_from_csv(file) -> Dict[str, str]:
    """
    Legge un CSV di mapping causali con separatore auto-detect (',' o ';').
    Accetta intestazioni 'Evento' e 'Codice Causale',
    altrimenti usa le prime due colonne come mapping.
    Ritorna: dict {evento -> codice}. Se non valido, ritorna {}.
    """
    import csv
    import io as _io

    raw = file.read()
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    lines = raw.splitlines()
    if not lines:
        return {}

    # Prova separatore; fallback semplice
    try:
        dialect = csv.Sniffer().sniff(lines[0])
        sep = dialect.delimiter
    except Exception:
        sep = ";" if ";" in lines[0] else ","

    df = pd.read_csv(_io.StringIO(raw), sep=sep, dtype=str).fillna("")
    if df.shape[1] < 2:
        return {}

    cols = [c.strip().lower() for c in df.columns]
    df.columns = cols

    if "evento" in cols and ("codice causale" in cols or "codice" in cols):
        code_col = "codice causale" if "codice causale" in cols else "codice"
        mapping = dict(zip(df["evento"].astype(str).str.strip(),
                           df[code_col].astype(str).str.strip()))
    else:
        # Prime due colonne come fallback
        mapping = dict(zip(df.iloc[:, 0].astype(str).str.strip(),
                           df.iloc[:, 1].astype(str).str.strip()))
    # Pulisci righe vuote
    mapping = {k: v for k, v in mapping.items() if k and v}
    return mapping


# ============================================================================
# PARSING DEL TEMPLATE (EXCEL)
# ============================================================================

def parse_acme_template(excel_path: str, max_events: int = MAX_EVENTS) -> pd.DataFrame:
    """
    Legge il foglio turni (primo non chiamato 'Causali') con struttura a blocchi:
      Nome | Giorno | Data | Ore lavorate | Evento [/Ore evento] ... | '------'
    Ritorna un DataFrame del tipo:
        ['Employee','DateTok','HoursWorked','Ev1','Ev1Hours',...,'EvN','EvNHours'] (N = max_events)
    """
    xl = pd.ExcelFile(excel_path, engine="openpyxl")
    sheets = [s for s in xl.sheet_names if s.lower() != "causali"]
    if not sheets:
        raise ValueError("Nessun foglio turni trovato (solo 'Causali' presente).")
    sheet = sheets[0]  # usiamo il primo foglio utile

    raw = pd.read_excel(excel_path, sheet_name=sheet, header=None, engine="openpyxl").fillna("")
    R, C = raw.shape

    # Per riconoscere inizio blocchi: colonna 0 in lower
    col0_lower = raw.iloc[:, 0].astype(str).str.strip().str.lower()

    out_rows: List[dict] = []
    r = 0
    while r < R:
        # Inizio blocco: riga non vuota + le due successive 'giorno' e 'data'
        first = str(raw.iat[r, 0]).strip()
        if first and r + 2 < R and col0_lower.iat[r + 1].startswith("giorno") and col0_lower.iat[r + 2].startswith("data"):
            employee = first

            # Termina al separatore '------' o EOF
            r_end = r + 1
            while r_end < R and str(raw.iat[r_end, 0]).strip() != "------":
                r_end += 1

            # Mappa "label → riga"
            label_to_row: Dict[str, int] = {}
            for rr in range(r, r_end):
                key = str(raw.iat[rr, 0]).strip().lower()
                if key and key not in label_to_row:
                    label_to_row[key] = rr

            # Righe principali nel blocco
            row_dates = r + 2  # "Data"
            row_hours = label_to_row.get("ore lavorate", None)

            # Individua coppie (Evento k / Ore evento) nell'ordine
            ev_pairs: List[Tuple[int, int]] = []
            for k in range(1, max_events + 1):
                base = "evento" if k == 1 else f"evento {k}"
                re_ev = label_to_row.get(base, None)
                if re_ev is None:
                    continue
                re_hrs = -1
                for rr in range(re_ev + 1, r_end):
                    if str(raw.iat[rr, 0]).strip().lower() == "ore evento":
                        re_hrs = rr
                        break
                ev_pairs.append((re_ev, re_hrs))

            # Ultima colonna "giorno" non vuota nella riga "Data" (scansione da destra)
            last_day_col = 0
            for cc in range(C - 1, 0, -1):
                if str(raw.iat[row_dates, cc]).strip() != "":
                    last_day_col = cc
                    break
            if last_day_col == 0:
                # blocco senza giorni valorizzati -> skip
                r = r_end + 1
                continue

            # Build delle righe (una per giorno)
            for cc in range(1, last_day_col + 1):
                if cc >= C:
                    break  # guard-rail
                date_tok = raw.iat[row_dates, cc]
                if str(date_tok).strip() == "":
                    continue

                row = {
                    "Employee": employee,
                    "DateTok": date_tok,
                    "HoursWorked": (raw.iat[row_hours, cc] if row_hours is not None and row_hours < R else "")
                }
                # Eventi da 1 a max_events
                for i, (re_ev, re_hrs) in enumerate(ev_pairs, start=1):
                    if i > max_events:
                        break
                    ev_label = str(raw.iat[re_ev, cc]).strip() if (0 <= re_ev < R and cc < C) else ""
                    ev_hours = raw.iat[re_hrs, cc] if (re_hrs >= 0 and re_hrs < R and cc < C) else ""
                    row[f"Ev{i}"] = ev_label
                    row[f"Ev{i}Hours"] = ev_hours
                # Completa eventuali slot mancanti
                for j in range(len(ev_pairs) + 1, max_events + 1):
                    row[f"Ev{j}"] = ""
                    row[f"Ev{j}Hours"] = ""
                out_rows.append(row)

            r = r_end + 1
        else:
            r += 1

    if not out_rows:
        raise ValueError("Nessun blocco dipendente riconosciuto nel template. Per favore verifica l'ordinamento e le intestazioni dei blocchi.")

    df = pd.DataFrame(out_rows)

    # Check esistenza di tutte le colonne evento da 1 a max_events
    for k in range(1, max_events + 1):
        if f"Ev{k}" not in df.columns:
            df[f"Ev{k}"] = ""
        if f"Ev{k}Hours" not in df.columns:
            df[f"Ev{k}Hours"] = ""

    logger.info("Parsing completato: %s righe giorno rilevate.", len(df))
    return df


# ============================================================================
# PARSING DEL TEMPLATE (CSV) 
# ============================================================================

def read_turni_csv_as_grid(file) -> pd.DataFrame:
    """
    Legge un CSV dei turni e lo restituisce come 'griglia' (header=None),
    in modo simile a come leggiamo l'Excel. Auto-detect del separatore (',' o ';').
    Non richiede intestazioni: considera la prima colonna come etichetta riga ('Giorno','Data',...).
    """
    import csv
    import io as _io

    raw = file.read()
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    lines = raw.splitlines()
    if not lines:
        return pd.DataFrame()

    # sniff separatore; fallback semplice
    try:
        dialect = csv.Sniffer().sniff(lines[0])
        sep = dialect.delimiter
    except Exception:
        sep = ";" if ";" in lines[0] else ","

    df = pd.read_csv(_io.StringIO(raw), sep=sep, header=None, dtype=str)
    df = df.fillna("")
    return df


def parse_acme_from_raw_grid(raw: pd.DataFrame, max_events: int = MAX_EVENTS) -> pd.DataFrame:
    """
    Parser identico a parse_acme_template, ma parte da una DataFrame 'raw' già letto
    (sia da CSV che da Excel). Ritorna lo stesso df_wide.
    """
    if raw is None or raw.empty:
        raise ValueError("Il file CSV dei turni risulta vuoto o non leggibile.")

    R, C = raw.shape
    col0_lower = raw.iloc[:, 0].astype(str).str.strip().str.lower()

    out_rows: List[dict] = []
    r = 0
    while r < R:
        first = str(raw.iat[r, 0]).strip()
        if first and r + 2 < R and col0_lower.iat[r + 1].startswith("giorno") and col0_lower.iat[r + 2].startswith("data"):
            employee = first
            r_end = r + 1
            while r_end < R and str(raw.iat[r_end, 0]).strip() != "------":
                r_end += 1

            label_to_row: Dict[str, int] = {}
            for rr in range(r, r_end):
                key = str(raw.iat[rr, 0]).strip().lower()
                if key and key not in label_to_row:
                    label_to_row[key] = rr

            row_dates = r + 2
            row_hours = label_to_row.get("ore lavorate", None)

            ev_pairs: List[Tuple[int, int]] = []
            for k in range(1, max_events + 1):
                base = "evento" if k == 1 else f"evento {k}"
                re_ev = label_to_row.get(base, None)
                if re_ev is None:
                    continue
                re_hrs = -1
                for rr in range(re_ev + 1, r_end):
                    if str(raw.iat[rr, 0]).strip().lower() == "ore evento":
                        re_hrs = rr
                        break
                ev_pairs.append((re_ev, re_hrs))

            # ultima colonna Data non vuota
            last_day_col = 0
            for cc in range(C - 1, 0, -1):
                if str(raw.iat[row_dates, cc]).strip() != "":
                    last_day_col = cc
                    break
            if last_day_col == 0:
                r = r_end + 1
                continue

            for cc in range(1, last_day_col + 1):
                if cc >= C:
                    break
                date_tok = raw.iat[row_dates, cc]
                if str(date_tok).strip() == "":
                    continue

                row = {
                    "Employee": employee,
                    "DateTok": date_tok,
                    "HoursWorked": (raw.iat[row_hours, cc] if row_hours is not None and row_hours < R else "")
                }
                for i, (re_ev, re_hrs) in enumerate(ev_pairs, start=1):
                    if i > max_events:
                        break
                    ev_label = str(raw.iat[re_ev, cc]).strip() if (0 <= re_ev < R and cc < C) else ""
                    ev_hours = raw.iat[re_hrs, cc] if (re_hrs >= 0 and re_hrs < R and cc < C) else ""
                    row[f"Ev{i}"] = ev_label
                    row[f"Ev{i}Hours"] = ev_hours
                for j in range(len(ev_pairs) + 1, max_events + 1):
                    row[f"Ev{j}"] = ""
                    row[f"Ev{j}Hours"] = ""
                out_rows.append(row)

            r = r_end + 1
        else:
            r += 1

    if not out_rows:
        raise ValueError("Nessun blocco dipendente riconosciuto nel CSV dei turni. Verificare etichette e struttura.")
    df = pd.DataFrame(out_rows)
    for k in range(1, max_events + 1):
        if f"Ev{k}" not in df.columns:
            df[f"Ev{k}"] = ""
        if f"Ev{k}Hours" not in df.columns:
            df[f"Ev{k}Hours"] = ""
    return df


# ============================================================================
# CONVERSIONE -> LAYOUT STANDARD
# ============================================================================

OUT_COLS = [
    "Codice ditta",
    "Codice dipendente",
    "Data di rilevazione",
    "Codice causale",
    "Ore dell'evento",
    "Ore lavorate",
    "Giorno lavorativo",
    "Tipo elaborazione",
]

def convert_layout(
    df_wide: pd.DataFrame,
    company: str,
    causali_map: Dict[str, str],
    max_events: int = MAX_EVENTS
) -> ConversionOutput:
    """
    Converte il DataFrame "wide" per-giorno nel Layout Standard.
    Regole:
      - 1 riga se nessun evento; N righe se N eventi (ore lavorate replicate)
      - Data: ddmmyy (stringa, 6 caratteri)
      - Ore lavorate / Ore evento: *100, dtype Int64 (nullable)
      - Giorno lavorativo: 0 se ore lavorate > 0, altrimenti 1
      - Codice dipendente: progressivo in ordine di apparizione
    """
    emp_order = pd.Index(df_wide["Employee"].astype(str).unique())
    emp_to_code = {e: i + 1 for i, e in enumerate(emp_order)}

    out_rows: List[List[object]] = []
    err_rows: List[Dict[str, object]] = []

    for i, row in df_wide.iterrows():
        emp = str(row["Employee"]).strip()
        empc = emp_to_code.get(emp)
        if empc is None:
            err_rows.append({"row": i, "employee": emp, "issue": "Dipendente non mappabile"})
            continue

        try:
            ddmmyy = to_ddmmyy_from_token(row["DateTok"])
        except Exception as e:
            err_rows.append({"row": i, "employee": emp, "issue": f"Data non valida: {row['DateTok']} ({e})"})
            continue

        try:
            ow = hours_to_int100(row.get("HoursWorked", "")) or 0
        except Exception as e:
            ow = 0
            err_rows.append({"row": i, "employee": emp, "date": ddmmyy,
                             "issue": f"Ore lavorate non valide: {row.get('HoursWorked','')} ({e})"})

        gl = 0 if ow > 0 else 1

        # Colleziona eventi
        events: List[Tuple[str, Optional[int]]] = []
        for k in range(1, max_events + 1):
            ev_label = str(row.get(f"Ev{k}", "")).strip()
            ev_hrs = row.get(f"Ev{k}Hours", "")
            if ev_label == "" and str(ev_hrs).strip() == "":
                continue
            code = causali_map.get(ev_label, "")
            if ev_label and not code:
                err_rows.append({"row": i, "employee": emp, "date": ddmmyy,
                                 "cause_input": ev_label, "issue": "Causale non mappata"})
            try:
                ev100 = hours_to_int100(ev_hrs)
            except Exception as e:
                ev100 = None
                err_rows.append({"row": i, "employee": emp, "date": ddmmyy,
                                 "cause_input": ev_label, "issue": f"Ore evento non valide: {ev_hrs} ({e})"})
            events.append((code, ev100))

        # Emissione righe output
        if not events:
            out_rows.append([company, empc, ddmmyy, "", None, ow, gl, ""])
        else:
            for code, ev100 in events:
                out_rows.append([company, empc, ddmmyy, code or "", (None if ev100 is None else ev100), ow, gl, ""])

    df_out = pd.DataFrame(out_rows, columns=OUT_COLS)
    df_err = pd.DataFrame(err_rows)

    # Tipi corretti per compatibilità
    for col in ["Ore dell'evento", "Ore lavorate", "Giorno lavorativo"]:
        df_out[col] = pd.to_numeric(df_out[col], errors="coerce").astype("Int64")

    # data come stringa ddmmyy a 6 caratteri (mantiene zeri davanti)
    df_out["Data di rilevazione"] = df_out["Data di rilevazione"].astype(str).str.zfill(6)

    logger.info("Conversione completata: %s righe di output; %s avvisi/errori.",
                len(df_out), len(df_err))
    return ConversionOutput(df_out=df_out, df_err=df_err)


# ============================================================================
# UI STREAMLIT 
# ============================================================================

# -------------------- BASIC AUTH (Opzionale) --------------------
# try:
#     import hmac
#     user = st.text_input("User")
#     pwd = st.text_input("Password", type="password")
#     if not (hmac.compare_digest(user, st.secrets["APP_USER"]) and hmac.compare_digest(pwd, st.secrets["APP_PASS"])):
#         st.stop()
# except Exception:
#     # Se non sono presenti secrets, prosegui senza auth
#     pass

# Config base della pagina Streamlit
st.set_page_config(
    page_title="Convertitore Turni Cliente → Layout Standard",
    page_icon="🗂️",
    layout="centered"
)

# Titolo e sottotitolo
st.title("🗂️ Convertitore Turni Cliente → Layout Standard")
st.markdown(
    "Strumento per la trasformazione automatica dei file contenenti i turni delle ditte clienti"
    " nel formato standard richiesto dal gestionale"
)

# Sezione istruzioni (aggiornata: accetta anche CSV turni e priorità CSV causali)
with st.expander("Come utilizzare il convertitore", expanded=True):
    st.markdown("""
1. Caricare il file **Excel mensile con i turni** oppure un **CSV** con la stessa struttura; 
2. *(Opzionale)* caricare una **tabella di mapping delle causali** in formato CSV (`Evento;Codice Causale`). Se viene caricato un **CSV valido, questo ha priorità** sul foglio **Causali** dell’Excel.  
   In assenza di CSV valido, verrà utilizzato il foglio **Causali** (se presente); 
3. Inserire il **Codice azienda**;
4. Avviare la conversione tramite il pulsante **Converti**; 
5. Scaricare il file **CSV di output** e, se presenti, il **report degli errori**.
    """)

# Input principali
company_code = st.text_input(
    "Codice Azienda",
    placeholder="Es. 999",
    help="Codice identificativo della ditta nel gestionale."
)

file_in = st.file_uploader(
    "File Turni",
    type=["xlsx", "xls", "csv"],
    help="Caricare il file turni mensile (Excel ACME a blocchi o CSV equivalente)."
)

map_causali = st.file_uploader(
    "(Opzionale) Mappa Causali",
    type=["csv"],
    help="File CSV con due colonne: `Evento;Codice Causale`. "
         "Se non caricato o non valido, verrà utilizzato il foglio 'Causali' dell’Excel (se presente)."
)

# Pulsante azione
run_btn = st.button("Converti", type="primary", use_container_width=True)

if run_btn:
    try:
        # Validazioni base
        if not company_code:
            st.error("Inserire il codice azienda prima di procedere.")
            st.stop()
        if not file_in:
            st.error("Caricare il file turni (.xlsx, .xls o .csv) prima di procedere.")
            st.stop()

        progress = st.progress(0, text="Conversione in corso...")

        with tempfile.TemporaryDirectory() as tmpd:
            filename = file_in.name
            ext = os.path.splitext(filename)[1].lower()

            df_wide = None
            causali: Dict[str, str] = {}
            source_label = "Nessuna"

            if ext in [".xlsx", ".xls"]:
                # Scriviamo input su disco temporaneo per openpyxl
                in_path = os.path.join(tmpd, filename)
                with open(in_path, "wb") as f:
                    f.write(file_in.read())
                progress.progress(10, text="File Excel caricato…")

                # --- Sorgente mappa causali con priorità esplicita (CSV → Causali sheet) ---
                if map_causali is not None:
                    try:
                        causali = load_causali_from_csv(map_causali)
                    except Exception:
                        causali = {}
                    if causali:
                        source_label = f"CSV caricato ({len(causali)} voci)"
                    else:
                        st.warning(
                            "Il CSV di mappatura causali è stato caricato ma non è valido "
                            "(colonne non riconosciute o vuote). Verrà ignorato. "
                            "Caricare un CSV valido oppure usare il foglio 'Causali' dell’Excel."
                        )
                        source_label = "CSV non valido"

                if not causali:
                    causali = load_causali_from_sheet(in_path)
                    if causali:
                        source_label = f"Foglio 'Causali' ({len(causali)} voci)"

                st.info(f"Sorgente mappa causali: **{source_label}**")
                if causali:
                    st.caption("Anteprima mappa causali (prime 10 voci):")
                    _prev = pd.DataFrame(list(causali.items()), columns=["Evento", "Codice Causale"]).head(10)
                    st.dataframe(_prev, use_container_width=True)

                progress.progress(35, text="Parsing template (Excel)…")
                df_wide = parse_acme_template(in_path, max_events=MAX_EVENTS)

            elif ext == ".csv":
                progress.progress(10, text="File CSV caricato…")

                # Per un CSV dei turni, NON esiste un foglio 'Causali' → solo CSV esterno (se fornito)
                if map_causali is not None:
                    try:
                        causali = load_causali_from_csv(map_causali)
                    except Exception:
                        causali = {}
                    if causali:
                        source_label = f"CSV caricato ({len(causali)} voci)"
                    else:
                        st.warning(
                            "Il CSV di mappatura causali è stato caricato ma non è valido "
                            "(colonne non riconosciute o vuote). Verrà ignorato."
                        )
                        source_label = "CSV non valido"

                st.info(f"Sorgente mappa causali: **{source_label}**")
                if causali:
                    st.caption("Anteprima mappa causali (prime 10 voci):")
                    _prev = pd.DataFrame(list(causali.items()), columns=["Evento", "Codice Causale"]).head(10)
                    st.dataframe(_prev, use_container_width=True)

                progress.progress(35, text="Parsing template (CSV)…")
                raw_grid = read_turni_csv_as_grid(file_in)
                df_wide = parse_acme_from_raw_grid(raw_grid, max_events=MAX_EVENTS)

            else:
                st.error("Formato non supportato. Caricare un file .xlsx, .xls oppure .csv.")
                st.stop()

            # Conversione + output
            progress.progress(65, text="Conversione in Layout Standard…")
            conv = convert_layout(df_wide, company_code.strip(), causali, max_events=MAX_EVENTS)

            progress.progress(85, text="Preparazione anteprima e download…")

            # Anteprima: cast a string per estetica
            df_prev = conv.df_out.copy()
            df_prev["Ore dell'evento"] = df_prev["Ore dell'evento"].astype("string")
            df_prev["Ore lavorate"] = df_prev["Ore lavorate"].astype("string")
            df_prev["Giorno lavorativo"] = df_prev["Giorno lavorativo"].astype("string")

            st.subheader("Anteprima output")
            st.dataframe(df_prev.head(PREVIEW_ROWS), use_container_width=True)

            # Download CSV
            out_buf = io.StringIO()
            conv.df_out.to_csv(out_buf, index=False)
            st.download_button(
                "⬇️ Scarica CSV",
                data=out_buf.getvalue().encode("utf-8"),
                file_name=f"Turni_Jet_{company_code.strip()}.csv",
                mime="text/csv",
                use_container_width=True
            )

            # Report errori (se presenti)
            if not conv.df_err.empty:
                st.warning(f"{len(conv.df_err)} record non sono stati convertiti correttamente. È disponibile un report dettagliato.")
                err_buf = io.StringIO()
                conv.df_err.to_csv(err_buf, index=False)
                st.download_button(
                    "⬇️ Report errori",
                    data=err_buf.getvalue().encode("utf-8"),
                    file_name="report_errori.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            else:
                st.success("Conversione completata. Il file è pronto per il download.")

            progress.progress(100, text="Fatto ✅")

    except Exception as e:
        logger.exception("Errore inatteso")
        st.error("Si è verificato un errore inatteso. Dettagli qui sotto:")
        st.exception(e)