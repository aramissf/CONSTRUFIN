import re
import pandas as pd
import pyodbc as db

# Lendo o conteúdo do arquivo txt
with open( "c:/cef/Aquivos/EPR/20231122_134300_411_TX_177770012300_CONTRATOS_EMPREEN.txt", 'r') as file:
    arquivo = file.read()

# Definindo padrões de regex para diferentes tipos de informações
padrao_id = r'\(ID\)\s+(\S+)'
padrao_numero_contrato = r'NR.CONTRATO:\s+(\S+)+(.+)'
padrao_UNO = r'UNO:\s+(\S+)+(.+)'
padrao_CONSULTA_MC =  r'CONSULTA \(M/C\):\s+(\S+)'
padrao_PEDIDO_C_ERRO = r'PEDIDO COM ERRO (S/N):\s+(\S+)+(.+)'
padrao_NUMERO_APF = r'NUMERO APF.:\s+(\S+)+(.+)'
padrao_nome_empreendimento = r'NOME EMPR.:\s+(.+?)\s+UN\.FIN:'
padrao_UN_FIN = r'UN\.FIN:\s+(\S+)'
padrao_SEG_SGC = r'SEG. SGC.:\s+(\S+)+(.+)'
padrao_VIG_SEG_SGC = r'VIG:\s+(\S+)+(.+)'
padrao_INIC_OBRA = r'INIC.OBRA:\s+(\S+)'
padrao_LF = r'LF:\s+(\S+)'
padrao_SEG_SRE = r'SEG. SRE.:\s+(\S+)+(.+)'
padrao_VIG_SEG_SRE = r'SEG\. SRE\.:.*?VIG:\s+(\S+)'
padrao_FIM_OBRA = r'FIM OBRA:\s+(\S+)'
padrao_TF = r'TF:\s+(\S+)'
padrao_SEG_SGP = r'SEG. SGP.:\s+(\S+)+(.+)'
padrao_VIG_SEG_SGP = r'SEG\. SGP\.:(?:\s+\S+)+\s+VIG:\s+(\S+)'
padrao_SEG_SGT = r'SEG. SGT.:\s+(\S+)+(.)'
padrao_VIG_SEG_SGT = r'SEG. SGT.:(?:\s+\S+)+\s+VIG:\s+(\S+)'


# Procurando por correspondências nos padrões
id_match = re.search(padrao_id, arquivo)
numero_contrato_match = re.search(padrao_numero_contrato, arquivo)
UNO_match = re.search(padrao_UNO, arquivo)
CONSULTA_MC_match = re.search(padrao_CONSULTA_MC, arquivo)
PEDIDO_C_ERRO_match = re.search(padrao_PEDIDO_C_ERRO, arquivo)
NUMERO_APF_match = re.search(padrao_NUMERO_APF, arquivo)
nome_empreendimento_match = re.search(padrao_nome_empreendimento, arquivo)
UN_FIN_match = re.search(padrao_UN_FIN, arquivo)
SEG_SGC_match =re.search(padrao_SEG_SGC, arquivo)
VIG_SEG_SGC_match =re.search(padrao_VIG_SEG_SGC, arquivo)
INIC_OBRA_match = re.search(padrao_INIC_OBRA, arquivo)
LF_match = re.search(padrao_LF,arquivo)
SEG_SRE_match = re.search(padrao_SEG_SRE, arquivo)
VIG_SEG_SRE_match = re.search(padrao_VIG_SEG_SRE, arquivo)
FIM_OBRA_match = re.search(padrao_FIM_OBRA, arquivo)
TF_match = re.search(padrao_TF, arquivo)
SEG_SGP_match = re.search(padrao_SEG_SGP, arquivo)
VIG_SEG_SGP_match = re.search(padrao_VIG_SEG_SGP, arquivo)
SEG_SGT_match = re.search(padrao_SEG_SGT, arquivo)
VIG_SEG_SGT_match = re.search(padrao_VIG_SEG_SGT, arquivo)


# Extraindo os dados correspondentes, se encontrados
id_dados = id_match.group(1) if id_match else None
numero_contrato_dados = numero_contrato_match.group(1) if numero_contrato_match else None
UNO_dados = UNO_match.group(1) if UNO_match else None
CONSULTA_MC_dados = CONSULTA_MC_match.group(1) if CONSULTA_MC_match else None
PEDIDO_C_ERRO_dados = PEDIDO_C_ERRO_match.group(1) if PEDIDO_C_ERRO_match else None
NUMERO_APF_dados = NUMERO_APF_match.group(1) if NUMERO_APF_match else None
nome_empreendimento_dados = nome_empreendimento_match.group(1) if nome_empreendimento_match else None
UN_FIN_dados = UN_FIN_match.group(1) if UN_FIN_match else None
SEG_SGC_dados = SEG_SGC_match.group(1) if SEG_SGC_match else None
VIG_SEG_SGC_dados = VIG_SEG_SGC_match.group(1) if VIG_SEG_SGC_match else None
INIC_OBRA_dados = INIC_OBRA_match.group(1) if INIC_OBRA_match else None
LF_dados = LF_match.group(1) if LF_match else None
SEG_SRE_dados = SEG_SRE_match.group(1) if SEG_SRE_match else None
VIG_SEG_SRE_dados = VIG_SEG_SRE_match.group(1) if VIG_SEG_SRE_match else None
FIM_OBRA_dados = FIM_OBRA_match.group(1) if FIM_OBRA_match else None
TF_dados = TF_match.group(1) if TF_match else None
SEG_SGP_dados = SEG_SGP_match.group(1) if SEG_SGP_match else None
VIG_SEG_SGP_dados = VIG_SEG_SGP_match.group(1)  if VIG_SEG_SGP_match else None
SEG_SGT_dados = SEG_SGT_match.group(1) if SEG_SGT_match else None
VIG_SEG_SGT_dados = VIG_SEG_SGT_match.group(1) if VIG_SEG_SGT_match else None



dados_extraidos = {
    'ID': [id_dados],
    'NUMERO DO CONTRATO': [numero_contrato_dados],
    'UNO': [UNO_dados],
    'CONSULTA (M/C)': [CONSULTA_MC_dados],
    'PEDIDO COM ERRO (S/N)': [PEDIDO_C_ERRO_dados],
    'NUMERO APF': [NUMERO_APF_dados],
    'NOME DO EMPREENDIMENTO': [nome_empreendimento_dados],
    'UN.FIN': [UN_FIN_dados],
    'SEG. SGC.': [SEG_SGC_dados],
    'VIG SEG. SGC. ': [VIG_SEG_SGC_dados],
    'INIC.OBRA': [INIC_OBRA_dados],
    'LF': [LF_dados],
    'SEG. SRE.': [SEG_SRE_dados],
    'VIG SEG. SRE.': [VIG_SEG_SRE_dados],
    'FIM OBRA': [FIM_OBRA_dados],
    'TF': [TF_dados],
    'SEG. SGP.': [SEG_SGP_dados],
    'VIG SEG. SGP.': [VIG_SEG_SGP_dados],
    'SEG. SGT.': [SEG_SGT_dados],
    'VIG SEG. SGT.': [VIG_SEG_SGT_dados]
}

# Criando um DataFrame com os dados
df = pd.DataFrame(dados_extraidos)

# Criando um DataFrame com os dados
df = pd.DataFrame(dados_extraidos)

#Conectando ao banco de dados

server = 'NB021977'
database = 'CEF'
username = 'cef'
password = 'teste'

conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

try :
    conn = db.connect(conn_str)
    cursor = conn.cursor()

    nome_tab = 'cabecalho_epr'
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
                    ID VARCHAR(50),
                    NUMERO_CONTRATO VARCHAR(255),
                    UNO VARCHAR(255),
                    CONSULTA_MC VARCHAR(255),
                    PEDIDO_C_ERRO VARCHAR(255),
                    NUMERO_APF VARCHAR(255),
                    NOME_EMPREENDIMENTO VARCHAR(255),
                    UN_FIN VARCHAR(255),
                    SEG_SGC VARCHAR(255),
                    VIG_SEG_SGC VARCHAR(255),
                    INIC_OBRA VARCHAR(255),
                    LF VARCHAR(255),
                    SEG_SRE VARCHAR(255),
                    VIG_SEG_SRE VARCHAR(255),
                    FIM_OBRA VARCHAR(255),
                    TF VARCHAR(255),
                    SEG_SGP VARCHAR(255),
                    VIG_SEG_SGP VARCHAR(255),
                    SEG_SGT VARCHAR(255),
                    VIG_SEG_SGT VARCHAR(255)
                )
            '''.format(tabela=nome_tab))

            # Commit para efetivar a criação da tabela
            conn.commit()

    for index, row in df.iterrows():
        cursor.execute('''
            INSERT INTO {tabela} (
                ID, NUMERO_CONTRATO, UNO, CONSULTA_MC, PEDIDO_C_ERRO,
                NUMERO_APF, NOME_EMPREENDIMENTO, UN_FIN, SEG_SGC,
                VIG_SEG_SGC, INIC_OBRA, LF, SEG_SRE, VIG_SEG_SRE,
                FIM_OBRA, TF, SEG_SGP, VIG_SEG_SGP, SEG_SGT, VIG_SEG_SGT
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''.format(tabela=nome_tab), *row)

    # Commit para efetivar as inserções
    conn.commit()

    # Fechar o cursor e a conexão
    cursor.close()
    conn.close()

except Exception as e:
    print(f'erro : {e}')