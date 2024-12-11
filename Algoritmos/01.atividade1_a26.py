# AULA 26 - ATIVIDADE 1

# Bibliotecas
import polars as pl
from datetime import datetime
import gc
import os


ENDERECO_DADOS=r'./dados/'

#Lendo os arquivos
try:
    # Hora inicial
    inicio = datetime.now()
    print("Processando arquivo...")

    # Processando dados
    arquivo = '202402_NovoBolsaFamilia.csv'
    df = pl.read_csv(ENDERECO_DADOS + arquivo,separator=';',encoding='iso-8859-1')

    print(df.head(12))

    #Deletando df e coletando residuos
    del df
    gc.collect()

    #hora final
    fim = datetime.now()

    print("Dados Carregados!")
    print(f"Tempo de execução: {fim - inicio}")

except ImportError as e:
    print(f"Erro: {e}.")
