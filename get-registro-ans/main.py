# pip install selenium webdriver-manager pandas beautifulsoup4 lxml

from datetime import date
import re
import time
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

URL = "https://www.ans.gov.br/ConsultaPlanosConsumidor/pages/home.xhtml"

# === Seletores específicos do seu HTML ===
TBODY_SEL = "#formHome\\:tabOperadora\\:tblOperadoras_data"
SEARCH_BTN = "#formHome\\:tabOperadora\\:j_idt16"
PAGINATOR_CURRENT = "#formHome\\:tabOperadora\\:tblOperadoras_paginator_bottom .ui-paginator-current"
BTN_NEXT = "#formHome\\:tabOperadora\\:tblOperadoras_paginator_bottom a.ui-paginator-next"

def new_driver(headless=True):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1600,1200")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def wait_clickable_and_click(driver, by, sel, timeout=30):
    el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, sel)))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    el.click()
    return el

def get_tbody(driver, timeout=30):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, TBODY_SEL))
    )

def parse_current_total(texto):
    """
    Converte '1 de 139' -> (1, 139)
    """
    # tolerante a espaços
    m = re.search(r"(\d+)\s*de\s*(\d+)", texto)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))

def get_current_and_total(driver):
    try:
        span = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, PAGINATOR_CURRENT))
        )
        return parse_current_total(span.text.strip())
    except TimeoutException:
        return None, None

def extract_page_data(driver):
    """
    Snapshot do TBODY -> parse com BeautifulSoup.
    Evita StaleElementReferenceException em DOM PrimeFaces.
    """
    tbody = get_tbody(driver)
    html = driver.execute_script("return arguments[0].outerHTML;", tbody)
    soup = BeautifulSoup(html, "lxml")

    data = []
    for tr in soup.select("tbody > tr"):
        tds = tr.select("td")
        if len(tds) < 3:
            continue

        a = tds[0].find("a")
        registro = (a.get_text(strip=True) if a else tds[0].get_text(strip=True))
        razao = tds[1].get_text(strip=True)
        fantasia = tds[2].get_text(strip=True)

        data.append({
            "Registro ANS": registro,
            "Razão Social": razao,
            "Nome Fantasia": fantasia,
        })
    return data

def click_next_and_wait(driver, expected_next_page=None):
    """
    Clica 'Próximo' e sincroniza:
    - espera o TBODY antigo ficar stale (trocado via Ajax)
    - espera novo TBODY aparecer
    - (opcional) espera current page == expected_next_page
    """
    old_tbody = get_tbody(driver)

    nxt = WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, BTN_NEXT))
    )
    # Se o botão estiver disabled por classe, aborta
    if "ui-state-disabled" in (nxt.get_attribute("class") or ""):
        return False

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
    nxt.click()

    # 1) espera o tbody antigo "morrer"
    try:
        WebDriverWait(driver, 30).until(EC.staleness_of(old_tbody))
    except TimeoutException:
        # fallback: aguarda um tico
        time.sleep(1.2)

    # 2) espera o novo tbody "nascer"
    get_tbody(driver)

    # 3) opcional: garante que a página avançou
    if expected_next_page is not None:
        try:
            WebDriverWait(driver, 30).until(
                lambda d: (get_current_and_total(d)[0] or 0) >= expected_next_page
            )
        except TimeoutException:
            pass
    return True

def main():
    today = date.today().strftime("%Y-%m-%d")
    driver = new_driver(headless=True)
    try:
        driver.get(URL)

        # Clicar "Pesquisar" (Por Operadora)
        wait_clickable_and_click(driver, By.CSS_SELECTOR, SEARCH_BTN)
        get_tbody(driver)  # bloqueia até tabela existir

        current, total = get_current_and_total(driver)
        if not current or not total:
            # fallback leve, mas normalmente já vem "1 de N"
            current, total = 1, 1

        while True:
            # Revalida a cada iteração
            current, total = get_current_and_total(driver)
            if current is None:
                current = 1

            print(f"Coletando página {current} de {total}…")
            rows = extract_page_data(driver)
            df = pd.DataFrame(rows, columns=["Registro ANS", "Razão Social", "Nome Fantasia"])
            out = f"{current}-{today}.csv"
            df.to_csv(out, index=False, encoding="utf-8")
            print(f"Salvo: {out} ({len(df)} linhas)")

            if total is None or current >= total:
                print("Finalizado. 🚀")
                break

            # Avança e sincroniza esperando ir pra current+1
            ok = click_next_and_wait(driver, expected_next_page=current + 1)
            if not ok:
                print("Botão 'Próximo' indisponível. Encerrando.")
                break

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
