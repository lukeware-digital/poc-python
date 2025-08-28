import os
import pandas as pd

# pega a pasta onde o script está rodando
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# lista todos os CSVs da pasta
arquivos = [os.path.join(BASE_DIR, f) for f in os.listdir(BASE_DIR) if f.endswith(".csv")]

# ordena para garantir sequência 1, 2, 3...
arquivos.sort(key=lambda x: int(os.path.basename(x).split("-")[0]))

print(f"Vou juntar {len(arquivos)} arquivos...")

# carrega todos em dataframes
dfs = [pd.read_csv(arq) for arq in arquivos]

# concatena em um único dataframe
df_final = pd.concat(dfs, ignore_index=True)

# salva em um único CSV final
output_file = os.path.join(BASE_DIR, "registro-ans-unificado.csv")
df_final.to_csv(output_file, index=False, encoding="utf-8")

print(f"Arquivo final salvo em: {output_file}")
print(f"Total de linhas: {len(df_final)}")
