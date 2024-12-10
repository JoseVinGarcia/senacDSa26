# AULA 26 - ATIVIDADE 2

# Bibliotecas
import polars as pl
from datetime import datetime
import gc
import os

# Lendo e processando arquivos

try:
    ENDERECO_DADOS=r'./dados/'
    inicio = datetime.now()
    print("Processando dados...")

    # Processamento
    lista = ["202404_NovoBolsaFamilia.csv","202405_NovoBolsaFamilia.csv"]

    # 1. Concatenando arquivo
    for arquivo in lista:
        print(f"Processando arquivo {arquivo}...")
        df = pl.read_csv(ENDERECO_DADOS + arquivo,separator=';',encoding='iso-8859-1')
        # Verificação
        if "df_bolsa_familia" in locals():
            df_bolsa_familia = pl.concat([df_bolsa_familia, df])
        else:
            df_bolsa_familia = df

    # 2. Convertendo VALOR PARCELA para float
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col("VALOR PARCELA").str.replace(",",".").cast(pl.Float64)
    )

    # 3. Criando Parquet
    df_bolsa_familia.write_parquet(ENDERECO_DADOS + "bolsa_familia.parquet")

    # Print e finais
    print(df.head())

    # Deletando df e coletando dados
    del df
    gc.collect()

    # Print final
    fim = datetime.now()
    print(f"Tempo de processamento: {fim - inicio}")

except ImportError as e:
    print(f"Erro {e}.")

