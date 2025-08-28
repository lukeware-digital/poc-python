# -*- coding: utf-8 -*-
import os
import time
import pandas as pd
import requests
from tqdm import tqdm

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT = os.path.join(BASE_DIR, "registro-ans-unificado-com-cnpj.csv")
OUTPUT = os.path.join(BASE_DIR, "registro-ans-dados-completo.csv")

def fetch_ans_data(registro):
    url = f"https://www.ans.gov.br/operadoras-entity/v1/operadoras/{registro}"
    try:
        resp = requests.get(url, headers={"accept": "*/*"}, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        else:
            return {"erro": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"erro": str(e)}

def main():
    # Lê CSV original
    df = pd.read_csv(INPUT)

    results = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        registro = row["Registro ANS"]
        data = fetch_ans_data(registro)
        merged = {**row.to_dict(), **data}
        results.append(merged)
        time.sleep(0.8)  # pequena pausa p/ não sobrecarregar a API

    # Salva CSV na mesma pasta do script
    df_out = pd.DataFrame(results)
    df_out.to_csv(OUTPUT, index=False, encoding="utf-8")

if __name__ == "__main__":
    main()
