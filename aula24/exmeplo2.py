# pip install pyarrow fastparquet # PARA INSTALAR O PARQUET

from datetime import datetime
import polars as pl
import numpy as np

ENDERECO_ARQUIVO = r'../../bronze/'
# TEMPO PANDAS 0:01:57
# TEMPO POLARS 0:00:12
try:
    print('\nIniciando a leitura do arquivo Parquet...')
    inicio = datetime.now()

    # pandas
    # df_bolsa_familia = pd.read_parquet(ENDERECO_ARQUIVO + 'bolsa_familia.parquet')
    # print(df_bolsa_familia.head())

    # polars # 0:00:12
    df_bolsa_familia = pl.scan_parquet(ENDERECO_ARQUIVO + 'bolsa_familia.parquet') # Usa scan_parquet para leitura eficiente
    df_bolsa_familia = df_bolsa_familia.collect()  # Coleta os dados do DataFrame
    print(df_bolsa_familia.head())

    fim = datetime.now()
    print('Leitura do arquivo Parquet concluída em {fim - inicio} segundos.')
    print('Arquivo Parquet lido com sucesso!')
except Exception as e:
    print(f'Erro ao ler o arquivo: {e}')

#   PROCESSAMENTO DE DADOS
try:
    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])
    media = np.mean(array_valor_parcela)
    mediana = np.median(array_valor_parcela)
    distancia_media_mediana = abs(media - mediana) / mediana 

#   Medidas
    print(f'\nMÉDIA E MEDIANA DO VALOR DA PARCELA: ')
    print(50*'~')
    print(f'Média do valor da parcela: {media}')
    print(f'Mediana do valor da parcela: {mediana}')
    print(f'Distância entre média e mediana: {distancia_media_mediana:.2%}')



except Exception as e:
    print(f'Erro ao ler o arquivo: {e}')