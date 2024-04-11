# bibliotecas utilizadas (pandas, sqlalchemy, pyodbc, Openpyxl)


import pandas as pd
from sqlalchemy import create_engine, exc, URL

def formatar_dt(df, colunas_datas):
    for coluna in colunas_datas:
        df[coluna] = pd.to_datetime(df[coluna], format='%d/%m/%Y')
        df[coluna] = df[coluna].dt.strftime('%Y-%m-%d')
    return df

def format_with_commas(number):
    return '{:,}'.format(number)

arquivo = 'C:/output/Plano de Ação - BI.xlsx'

df = pd.read_excel(arquivo)

df.rename(columns={'nº da Ação': 'NR_ACAO',
                   'Nº da Ação - Código': 'COD_ACAO',
                   'Módulo Origem': 'MODULO_ORIGEM',
                   'Assunto': 'ASSUNTO',
                   'Descrição da Ação': 'DESC_ACAO',
                   'Responsável': 'RESPONSAVEL',
                   'Área do Responsável': 'AREA_RESPONSAVEL',
                   'Gerência do Responsável': 'GERENCIA_RESPONSAVEL',
                   'Data - Prazo': 'DT_PRAZO',
                   'Data - Realização': 'DT_REALIZACAO',
                   'Data - Criação': 'DT_CRIACAO',
                   'Status': 'STATUS'}, inplace=True)


formatar_dt(df, ['DT_PRAZO', 'DT_REALIZACAO', 'DT_CRIACAO'])

ETL_FINAL = df

#'''
host = '192.168.2.65'
user = 'sqluser.ssis'
password = 'SoBiPode@4'
db = 'DB_RELATORIOS'



def insert_data(ETL_FINAL, host, user, password, db, driver="SQL Server"):
    try:
        connection_string = f'DRIVER={driver};SERVER={host};PORT={1433};DATABASE={db};UID={user};PWD={password};&autocommit=true'
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine = create_engine(connection_url, use_setinputsizes=False, echo=False)
    except exc.SQLAlchemyError as e:
        print(f"Erro ao conectar na base: {str(e)}")

    try:

        with engine.connect() as db_connection:
            print("Conectado ao banco de dados. Iniciando a inserção...")
            ETL_FINAL.to_sql(name='PLANEJAMENTO_TESTE', schema='dbo', if_exists='append', index=False, con=db_connection)
        num_rows_inserted = len(ETL_FINAL)
        linhas_inseridas = format_with_commas(num_rows_inserted)
        print(f"Inserção bem-sucedida! Total de {linhas_inseridas}")
    except exc.SQLAlchemyError as e:
        print(f"Erro durante a inserção: {str(e)}")
insert_data(ETL_FINAL, host, user, password, db)