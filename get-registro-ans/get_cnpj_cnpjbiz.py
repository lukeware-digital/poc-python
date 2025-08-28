# -*- coding: utf-8 -*-
"""
Coleta Localização e 1º telefone do cnpj.biz para cada CNPJ do CSV.

Requisitos:
  pip install selenium webdriver-manager pandas beautifulsoup4 lxml tqdm

Entrada:
  registro-ans-unificado-com-cnpj.csv  (precisa ter a coluna "CNPJ")

Saída (incremental, salva em lotes de 10):
  registro-ans-unificado-com-todos-os-dados.csv
"""

import os
import re
import time
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
BASE_URL = "https://cnpj.biz/{cnpj}"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV  = os.path.join(SCRIPT_DIR, "registro-ans-unificado-com-cnpj.csv")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "registro-ans-unificado-com-todos-os-dados.csv")

REQUEST_SLEEP = 1   # intervalo entre páginas (educado)
BATCH_SIZE    = 10    # salva a cada N registros

PHONE_REGEX = re.compile(r"\(?\d{2}\)?\s?\d{4,5}-?\d{4}")

# ------------------------------------------------------------
# Selenium
# ------------------------------------------------------------
def new_driver(headless=True):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1600,1200")
    opts.add_argument(f"user-agent={USER_AGENT}")
    # reduz ruído
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def wait_h2_text(driver, text, timeout=20):
    """
    Espera por um <h2> cujo texto (normalize-space) contenha 'text'.
    """
    xpath = f"//h2[contains(normalize-space(.), '{text}')]"
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath)))

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def ensure_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "Logradouro", "Complemento", "Bairro", "CEP",
        "Município", "Estado", "Telefone 1"
    ]
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    return out

def merge_with_existing(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Se OUTPUT_CSV existir, preserva colunas já coletadas.
    """
    df_out = ensure_output_columns(df_in)
    if not os.path.exists(OUTPUT_CSV):
        return df_out

    old = pd.read_csv(OUTPUT_CSV, dtype=str).fillna("")
    old = ensure_output_columns(old)

    # chave de merge: por padrão usamos "Registro ANS" + "CNPJ" se existir
    keys = [k for k in ["Registro ANS", "CNPJ"] if k in df_out.columns and k in old.columns]
    if not keys:
        keys = ["CNPJ"] if "CNPJ" in df_out.columns and "CNPJ" in old.columns else df_out.columns[:1].tolist()

    cols_to_bring = ["Logradouro", "Complemento", "Bairro", "CEP", "Município", "Estado", "Telefone 1"]
    merged = pd.merge(df_out, old[keys + cols_to_bring], on=keys, how="left", suffixes=("", "_old"))
    for c in cols_to_bring:
        # mantém valor novo se existir; senão, usa _old
        merged[c] = merged[c].where(merged[c].astype(str).str.len() > 0, merged[c + "_old"].fillna(""))
        merged.drop(columns=[c + "_old"], inplace=True)
    return merged

def parse_location_and_phone(html: str):
    """
    Retorna dict com campos de localização e o primeiro telefone.
    """
    soup = BeautifulSoup(html, "lxml")
    out = {
        "Logradouro": "", "Complemento": "", "Bairro": "",
        "CEP": "", "Município": "", "Estado": "", "Telefone 1": ""
    }

    # --------- Localização ---------
    h2_loc = soup.find("h2", string=lambda s: s and "Localização" in s)
    if h2_loc:
        # percorre <p> até o próximo h2
        for sib in h2_loc.find_all_next():
            if sib.name == "h2":
                break
            if sib.name == "p":
                txt = clean(sib.get_text(" ", strip=True))
                # busca o <b> (valor)
                b = sib.find("b")
                val = clean(b.get_text(" ", strip=True)) if b else ""
                if txt.startswith("Logradouro"):
                    out["Logradouro"] = val
                elif txt.startswith("Complemento"):
                    out["Complemento"] = val
                elif txt.startswith("Bairro"):
                    out["Bairro"] = val
                elif txt.startswith("CEP"):
                    out["CEP"] = val
                elif txt.startswith("Município"):
                    out["Município"] = clean(b.get_text(" ", strip=True) if b else val)
                elif txt.startswith("Estado"):
                    out["Estado"] = clean(b.get_text(" ", strip=True) if b else val)

    # --------- Telefone (primeiro) ---------
    # Estratégia: ir até a seção Contatos e varrer os <p> logo abaixo
    h2_cont = soup.find("h2", id="contato") or soup.find("h2", string=lambda s: s and "Contatos" in s)
    if h2_cont:
        # examine próximos <p> até próximo h2
        for sib in h2_cont.find_all_next():
            if sib.name == "h2":
                break
            if sib.name == "p":
                txt = sib.get_text(" ", strip=True)
                m = PHONE_REGEX.search(txt or "")
                if m:
                    out["Telefone 1"] = m.group(0)
                    break

    return out

def fetch_from_cnpjbiz(driver, cnpj_digits: str, timeout=25):
    """
    Abre a página do cnpj.biz/<cnpj> e retorna dict com localização + telefone.
    """
    url = BASE_URL.format(cnpj=cnpj_digits)
    driver.get(url)

    # Espera alguma âncora de conteúdo. Preferimos "Informações de Registro" OU "Localização".
    try:
        # qualquer um que chegue primeiro já serve para ter page_source "pronto"
        WebDriverWait(driver, timeout).until(
            EC.any_of(
                EC.presence_of_element_located((By.XPATH, "//h2[contains(normalize-space(.),'Informações de Registro')]")),
                EC.presence_of_element_located((By.XPATH, "//h2[contains(normalize-space(.),'Localização')]")),
                EC.presence_of_element_located((By.XPATH, "//h2[@id='contato']"))
            )
        )
    except TimeoutException:
        # mesmo se time-out, tentamos parsear o que tiver
        pass

    html = driver.page_source
    return parse_location_and_phone(html)

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"CSV de entrada não encontrado: {INPUT_CSV}")

    df_in = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    if "CNPJ" not in df_in.columns:
        raise ValueError("Coluna 'CNPJ' não encontrada no CSV de entrada.")

    df_out = merge_with_existing(df_in)

    # garante arquivo de saída desde o início
    if not os.path.exists(OUTPUT_CSV):
        df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    # linhas pendentes = onde algum dos campos alvo ainda está vazio
    target_cols = ["Logradouro", "Complemento", "Bairro", "CEP", "Município", "Estado", "Telefone 1"]
    pending_idx = [i for i in range(len(df_out))
                   if any(not clean(str(df_out.at[i, c])) for c in target_cols)]

    if not pending_idx:
        print(f"✅ Nada a fazer. Todos os campos já preenchidos em {OUTPUT_CSV}")
        return

    driver = new_driver(headless=True)
    processed_since_save = 0
    try:
        for i in tqdm(pending_idx, desc="Coletando do cnpj.biz"):
            cnpj_masked = clean(str(df_out.at[i, "CNPJ"]))
            cnpj_digits = only_digits(cnpj_masked)

            if not cnpj_digits or len(cnpj_digits) != 14:
                tqdm.write(f"✘ CNPJ inválido na linha {i}: '{cnpj_masked}'")
                # marca como vazio e segue
                processed_since_save += 1
                if processed_since_save >= BATCH_SIZE:
                    df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
                    processed_since_save = 0
                continue

            try:
                data = fetch_from_cnpjbiz(driver, cnpj_digits)
            except (TimeoutException, WebDriverException):
                # tenta uma segunda vez “leve”
                time.sleep(1.2)
                try:
                    data = fetch_from_cnpjbiz(driver, cnpj_digits)
                except Exception:
                    data = {k: "" for k in ["Logradouro","Complemento","Bairro","CEP","Município","Estado","Telefone 1"]}

            # escreve no DF
            for k, v in data.items():
                df_out.at[i, k] = clean(v)

            # log amigável
            log_end = data.get("Logradouro", "") or "—"
            log_tel = data.get("Telefone 1", "") or "—"
            tqdm.write(f"✔ {cnpj_digits} → {log_end} | Tel: {log_tel}")

            processed_since_save += 1
            if processed_since_save >= BATCH_SIZE:
                df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
                processed_since_save = 0

            time.sleep(REQUEST_SLEEP)

        # salva o restante
        if processed_since_save > 0:
            df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

        print(f"\n✅ Arquivo atualizado: {OUTPUT_CSV}")
        print(f"Linhas: {len(df_out)}")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
