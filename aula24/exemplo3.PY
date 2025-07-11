# pip install pyarrow fastparquet # PARA INSTALAR O PARQUET
# dados de BIG DATA, RODAMOS  SEMPRE SEPARADO DO RESTO DO CÓDIGO
# trabalha com grandes volumes de dados, não carrega tudo na memória

from datetime import datetime
import polars as pl
import numpy as np
import matplotlib.pyplot as plt

ENDERECO_ARQUIVO = r'../../bronze/'
# TEMPO PANDAS 0:01:57
# TEMPO POLARS 0:00:12
try:
    print('\nIniciando a leitura do arquivo Parquet...')
    inicio = datetime.now()

    # polars
    # gera um plano de execução para leitura eficiente
    df_bolsa_familia = (pl.scan_parquet(ENDERECO_ARQUIVO + 'bolsa_familia.parquet')
                        .select(pl.col(['NOME MUNICÍPIO', 'VALOR PARCELA']))
                        .filter(pl.col('VALOR PARCELA') > 1518)  # Filtra valores maiores q
    )  # Seleciona apenas a coluna 'VALOR PARCELA'

    df_bolsa_familia = df_bolsa_familia.collect()
    print(df_bolsa_familia.head())

    print(df_bolsa_familia.columns)  # Exibe as colunas do DataFrame
    print(df_bolsa_familia.dtypes)  # Exibe os tipos de dados das colunas

    fim = datetime.now()
    print(f'Leitura do arquivo Parquet concluída em {fim - inicio} segundos.')
    print('Arquivo Parquet lido com sucesso!')
except Exception as e:
    print(f'Erro ao ler o arquivo: {e}')

#   PROCESSAMENTO DE DADOS
try:
    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])

except Exception as e:
    print(f'Erro ao processar os dados: {e}')

#   VISUALIZAÇÃO DISTRIBUIÇÃO
try:
    print('\nVISUALIZANDO DISTRIBUIÇÃO...')
    plt.boxplot(array_valor_parcela, showmeans=True, vert=False)
    plt.title('Distribuição do Valor da Parcela')

    plt.tight_layout()
    plt.show()
    plt

except Exception as e:
    print(f'Erro ao gerar o histograma: {e}')
