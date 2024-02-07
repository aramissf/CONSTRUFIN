import re
import pandas as pd
import pyodbc as db
import os

def extract_data_from_line(line):
    contrato_pattern = re.compile(r'(\d+)\s+(\S[^\d]+)\s+(\d{5})\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d{2}/\d{2}/\d{2})\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)')

    match_contrato = contrato_pattern.match(line)
    
    if match_contrato:
        return {
            'CONTRATO': match_contrato.group(1),
            'NOME_MUTUARIO': match_contrato.group(2),
            'UNO': match_contrato.group(3),
            'ORR': match_contrato.group(4),
            'TO': match_contrato.group(5),
            'COD': match_contrato.group(6),
            'DT_ASSIN': match_contrato.group(7),
            'TIPO_UND': match_contrato.group(8),
            'GAR_AUT': match_contrato.group(9),
            'DT_INC_CTR': match_contrato.group(10),
            'DT_INC_REG': match_contrato.group(11),
            'VR_RETIDO': match_contrato.group(12),
            'VR_AMORTIZ': match_contrato.group(13),
        }
    else:
        return None
    
# Diretório contendo os arquivos
caminho_pasta = "c:/cef/Aquivos/EPR/"
# Lista todos os arquivos no diretório
arquivos_na_pasta = os.listdir(caminho_pasta)

for arquivo_nome in arquivos_na_pasta:
    print(arquivo_nome)
    #Verifica se o arquivo e um arquivo de texto 
    if arquivo_nome.endswith(".txt"):
        caminho_arquivo = os.path.join(caminho_pasta, arquivo_nome)
        dados_contratos = []

        with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
            for linha in arquivo:
                dados_contrato = extract_data_from_line(linha.strip())
                if dados_contrato:
                    dados_contratos.append(dados_contrato)

        df_dados_arquivo = pd.DataFrame(dados_contratos)


        #Criando um DataFrame com os dados
        df = pd.DataFrame(df_dados_arquivo)

        #Conectando ao banco de dados

        server = 'NB021977'
        database = 'CEF'
        username = 'cef'
        password = 'teste'

        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

        try :
            conn = db.connect(conn_str)
            cursor = conn.cursor()

            nome_tab = 'dados_epr'
            nome_bd = 'CEF2'

            conn = db.connect(conn_str, autocommit=True)

            # Cria um cursor para executar consultas SQL
            cursor = conn.cursor()

            # Verificar se o banco de dados já existe
            query = f"SELECT * FROM sys.databases WHERE name = '{nome_bd}'"
            result = cursor.execute(query).fetchone()

            if not result:
                # Se o banco de dados não existir, cria o banco de dados
                cursor.execute(f'CREATE DATABASE {nome_bd}')

            cursor.execute(f'USE {nome_bd}')
        

            if not cursor.tables(table=nome_tab).fetchone():
                    # Se a tabela não existir, cria a tabela
                    cursor.execute('''
                        CREATE TABLE {tabela} (
                            NUMERO_CONTRATO VARCHAR(255),
                            NOME_MUTUARIO VARCHAR(255),
                            UNO VARCHAR(255),
                            ORR VARCHAR(255),
                            [TO] VARCHAR(255), 
                            COD VARCHAR(255),
                            DT_ASSIN VARCHAR(255),
                            TIPO_UND VARCHAR(255),
                            GAR_AUT VARCHAR(255),
                            DT_INC_CTR VARCHAR(255),
                            DT_INC_REG VARCHAR(255),
                            VR_RETIDO VARCHAR(255),
                            VR_AMORTIZ VARCHAR(255)
                        )
                    '''.format(tabela=nome_tab))

                    # Commit para efetivar a criação da tabela
                    conn.commit()

            for index, row in df.iterrows():
                cursor.execute('''
                    INSERT INTO {tabela} (
                        NUMERO_CONTRATO, NOME_MUTUARIO, UNO, ORR, [TO],
                        COD, DT_ASSIN, TIPO_UND, GAR_AUT, DT_INC_CTR,
                        DT_INC_REG, VR_RETIDO, VR_AMORTIZ
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''.format(tabela=nome_tab), *row)

            # Commit para efetivar as inserções
            conn.commit()

            # Fechar o cursor e a conexão
            cursor.close()
            conn.close()

        except Exception as e:
            print(f'erro : {e}')