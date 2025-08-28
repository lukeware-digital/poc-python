# -*- coding: utf-8 -*-
import os
import time
import re
import pandas as pd
from tqdm import tqdm

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
BASE_URL = "https://www.ans.gov.br/ConsultaPlanosConsumidor/pages/ConsultaPlanos.xhtml"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112 Safari/537.36"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_CSV = os.path.join(SCRIPT_DIR, "registro-ans-unificado.csv")
OUTPUT_CSV = os.path.join(SCRIPT_DIR, "registro-ans-unificado-com-cnpj.csv")

REQUEST_SLEEP = 0.5  # intervalo entre requisições (segundos)
BATCH_SIZE = 10  # salva a cada N registros processados
WAIT_SECS = 15  # timeout de espera do Selenium

CNPJ_REGEX = re.compile(r"\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b")


# ------------------------------------------------------------
# Selenium setup
# ------------------------------------------------------------
def make_driver():
    chrome_opts = webdriver.ChromeOptions()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument(f"--user-agent={USER_AGENT}")
    chrome_opts.add_argument("--window-size=1200,900")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_opts)


# ------------------------------------------------------------
# CSV helpers
# ------------------------------------------------------------
def _clean(s: str) -> str:
    import re as _re

    return _re.sub(r"\s+", " ", (s or "")).strip()


def load_inputs() -> pd.DataFrame:
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"CSV de entrada não encontrado: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    if "Registro ANS" not in df.columns:
        raise ValueError("Coluna 'Registro ANS' não encontrada no CSV de entrada.")
    return df


def merge_with_existing(df_in: pd.DataFrame) -> pd.DataFrame:
    if not os.path.exists(OUTPUT_CSV):
        out = df_in.copy()
        if "CNPJ" not in out.columns:
            out["CNPJ"] = ""
        return out

    df_out = pd.read_csv(OUTPUT_CSV, dtype=str).fillna("")
    if "CNPJ" not in df_out.columns:
        df_out["CNPJ"] = ""

    merge_keys = [
        c
        for c in ["Registro ANS", "Razão Social", "Nome Fantasia"]
        if c in df_in.columns and c in df_out.columns
    ] or ["Registro ANS"]

    merged = pd.merge(df_in, df_out[merge_keys + ["CNPJ"]], on=merge_keys, how="left")
    merged["CNPJ"] = merged["CNPJ"].fillna("")
    return merged


# ------------------------------------------------------------
# Scraping com Selenium
# ------------------------------------------------------------
def extract_cnpj_text(text: str) -> str:
    m = CNPJ_REGEX.search(text or "")
    return m.group(0) if m else ""


def fetch_cnpj(driver: webdriver.Chrome, registro_ans: str) -> str:
    """
    Acessa a URL da operadora e tenta extrair o CNPJ:
      1) por ID direto: formConsulta:outCNPJ
      2) por XPath relativo ao rótulo "CNPJ:"
      3) fallback: regex no HTML inteiro
    Retorna string do CNPJ ou ''.
    """
    url = f"{BASE_URL}?coOperadora={str(registro_ans)}"
    driver.get(url)
    wait = WebDriverWait(driver, WAIT_SECS)

    # 1) Tenta por ID direto
    for attempt in range(2):  # pequena tolerância a staleness
        try:
            el = wait.until(
                EC.presence_of_element_located((By.ID, "formConsulta:outCNPJ"))
            )
            cnpj = extract_cnpj_text(el.text)
            if cnpj:
                return cnpj
        except (TimeoutException, StaleElementReferenceException):
            if attempt == 0:
                try:
                    driver.refresh()
                except Exception:
                    pass

    # 2) XPath relativo ao rótulo "CNPJ:"
    try:
        # pega a TD do valor do CNPJ (rótulo seguido do valor)
        val_el = wait.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    "//table[contains(@class,'tabela-cpc')]//tr[.//label[normalize-space()='CNPJ:']]/td[2]//label",
                )
            )
        )
        cnpj = extract_cnpj_text(val_el.text)
        if cnpj:
            return cnpj
    except (TimeoutException, StaleElementReferenceException):
        pass

    # 3) Fallback regex no HTML
    try:
        html = driver.page_source or ""
        cnpj = extract_cnpj_text(html)
        if cnpj:
            return cnpj
    except Exception:
        pass

    return ""


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    df_base = load_inputs()
    df_out = merge_with_existing(df_base)

    # Garante criar arquivo de saída desde o início (facilita retomada)
    if not os.path.exists(OUTPUT_CSV):
        df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    # Índices que ainda precisam ser preenchidos (CNPJ vazio)
    pending_idx = [i for i, v in enumerate(df_out["CNPJ"].tolist()) if not _clean(v)]

    if not pending_idx:
        print("✅ Nada a fazer. Todos os CNPJs já estão preenchidos em", OUTPUT_CSV)
        return

    print(f"🔎 Buscando CNPJ para {len(pending_idx)} registros...")

    processed_since_save = 0
    driver = make_driver()
    try:
        for i in tqdm(pending_idx, desc="Processando"):
            reg = _clean(str(df_out.at[i, "Registro ANS"]))
            cnpj = ""

            try:
                cnpj = fetch_cnpj(driver, reg)
                if not cnpj:
                    # segunda tentativa "light"
                    time.sleep(1.0)
                    cnpj = fetch_cnpj(driver, reg)
            except Exception:
                cnpj = ""

            df_out.at[i, "CNPJ"] = cnpj
            processed_since_save += 1

            if cnpj:
                tqdm.write(f"✔ Registro {reg} → CNPJ {cnpj}")
            else:
                tqdm.write(f"✘ Registro {reg} → CNPJ não encontrado")

            # salva em lote
            if processed_since_save >= BATCH_SIZE:
                df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
                processed_since_save = 0

            time.sleep(REQUEST_SLEEP)

        # salva o restante
        if processed_since_save > 0:
            df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
            time.sleep(REQUEST_SLEEP * 3)

        total_ok = (df_out["CNPJ"].str.len() > 0).sum()
        print(f"\n✅ Arquivo atualizado: {OUTPUT_CSV}")
        print(f"Linhas: {len(df_out)} | Com CNPJ preenchido: {total_ok}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
