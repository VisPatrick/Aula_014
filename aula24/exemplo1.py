# A CRIAÇÃO DO ARQUIVO PARQUET É IMPORTANTE PARA PODER FAZER O PROCESSAMENTO,  ARQUIVO CSV NÃO É RECOMENDADO PARA PROCESSAMENTO, É CONSIDERADO UM PRÉ-PROCESSAMENTO
# pip install pyarrow fastparquet # PARA INSTALAR O PARQUET

from datetime import datetime
import pandas as pd
# import polars as pl

ENDERECO_ARQUIVO = r'../../bronze/'

try:
    print('\nIniciando a leitura do arquivo Parquet...')
    inicio = datetime.now()

    df_bolsa_familia = pd.read_parquet(ENDERECO_ARQUIVO + 'bolsa_familia.parquet')
    print(df_bolsa_familia.head())

    fim = datetime.now()
    print(f'\nLeitura do arquivo Parquet concluída em {fim - inicio} segundos.')
    print('Arquivo Parquet lido com sucesso!')
except Exception as e:
    print(f'Erro ao ler o arquivo: {e}')