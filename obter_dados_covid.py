import os
import pandas as pd
import requests
from sqlalchemy import create_engine

# Função genérica para obter os dados da API do IBGE
def obter_dados_ibge(url):
    """
    Obtém os dados através da API do IBGE.
    Retorna um dataframe com os dados.
    Em caso de falha na obtenção dos dados, lança uma exceção.
    """
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        return df
    else:
        raise Exception(f'Falha ao obter dados da API do IBGE. Código de status: {response.status_code}')

# Função para obter os dados de covid do banco de dados
def obter_dados_covid(engine, query):
    """
    Obtém os dados de covid do banco de dados.
    Retorna um dataframe com os dados.
    """
    df = pd.read_sql(query, engine)
    df.drop(['city', 'place_type', 'state'], axis=1, inplace=True)
    return df

# Função para salvar um dataframe em um arquivo CSV
def salvar_em_csv(df, path):
    """
    Salva um dataframe em um arquivo CSV.
    """
    df.to_csv(path, index=False)
    print(f'Dados salvos em {path}')

# Função principal
def main():
    # Definir informações de conexão com o banco de dados MySQL a partir de variáveis de ambiente
    user = os.getenv('DB_USER', 'teste-dados-leitura')
    password = os.getenv('DB_PASSWORD', 'o7c4Cc8NDeXYbAMH')
    host = os.getenv('DB_HOST', '34.95.170.227')
    port = os.getenv('DB_PORT', '3306')  # '3306' is the default port for MySQL
    database = os.getenv('DB_NAME', 'teste_dados')

    connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

    # Criar o objeto de conexão
    engine = create_engine(connection_string)

    # Consulta SQL para obter os dados de covid
    query = "SELECT * FROM DADOS_COVID"

    # Definir os caminhos dos arquivos CSV
    df_covid = obter_dados_covid(engine, query)
    df_ibge_municipios = obter_dados_ibge('https://servicodados.ibge.gov.br/api/v1/localidades/municipios')
    df_ibge_ufs = obter_dados_ibge('https://servicodados.ibge.gov.br/api/v1/localidades/estados')

    # Adicionando as duas linhas ao dataframe df_ibge_municipios
    new_rows = pd.DataFrame([
        {'id': 12, 'nome': 'Não Localiz.', 'microrregiao.id': 9999991, 'microrregiao.nome': 'Não Localiz.', 'microrregiao.mesorregiao.id': 9999991, 'microrregiao.mesorregiao.nome': 'Não Localiz.', 'microrregiao.mesorregiao.UF.id': 9999991, 'microrregiao.mesorregiao.UF.sigla': 'AC', 'microrregiao.mesorregiao.UF.nome': 'Acre', 'microrregiao.mesorregiao.UF.regiao.id': 9999991, 'microrregiao.mesorregiao.UF.regiao.sigla': 'NE', 'microrregiao.mesorregiao.UF.regiao.nome': 'Norte', 'regiao-imediata.id': 9999991, 'regiao-imediata.nome': 'Não Localiz.', 'regiao-imediata.regiao-intermediaria.id': 9999991, 'regiao-imediata.regiao-intermediaria.nome': 'Não Localiz.', 'regiao-imediata.regiao-intermediaria.UF.id': 9999991, 'regiao-imediata.regiao-intermediaria.UF.sigla': 'AL', 'regiao-imediata.regiao-intermediaria.UF.nome': 'Alagoas', 'regiao-imediata.regiao-intermediaria.UF.regiao.id': 9999991, 'regiao-imediata.regiao-intermediaria.UF.regiao.sigla': 'NE', 'regiao-imediata.regiao-intermediaria.UF.regiao.nome': 'Nordeste'},
        {'id': 27, 'nome': 'Não Localiz.', 'microrregiao.id': 9999992, 'microrregiao.nome': 'Não Localiz.', 'microrregiao.mesorregiao.id': 9999992, 'microrregiao.mesorregiao.nome': 'Não Localiz.', 'microrregiao.mesorregiao.UF.id': 9999992, 'microrregiao.mesorregiao.UF.sigla': 'AL', 'microrregiao.mesorregiao.UF.nome': 'Alagoas', 'microrregiao.mesorregiao.UF.regiao.id': 9999992, 'microrregiao.mesorregiao.UF.regiao.sigla': 'NE', 'microrregiao.mesorregiao.UF.regiao.nome': 'Nordeste', 'regiao-imediata.id': 9999992, 'regiao-imediata.nome': 'Não Localiz.', 'regiao-imediata.regiao-intermediaria.id': 9999992, 'regiao-imediata.regiao-intermediaria.nome': 'Não Localiz.', 'regiao-imediata.regiao-intermediaria.UF.id': 9999992, 'regiao-imediata.regiao-intermediaria.UF.sigla': 'AL', 'regiao-imediata.regiao-intermediaria.UF.nome': 'Alagoas', 'regiao-imediata.regiao-intermediaria.UF.regiao.id': 9999992, 'regiao-imediata.regiao-intermediaria.UF.regiao.sigla': 'NE', 'regiao-imediata.regiao-intermediaria.UF.regiao.nome': 'Nordeste'},
    ], columns=df_ibge_municipios.columns)

    df_ibge_municipios = pd.concat([df_ibge_municipios, new_rows], ignore_index=True)

    path_ibge_municipios = r'C:\Users\elder\OneDrive\Documentos\Entrevista Builder\Banco\dados\modelo\dim_municipios.csv'
    path_ibge_ufs = r'C:\Users\elder\OneDrive\Documentos\Entrevista Builder\Banco\dados\modelo\dim_ufs.csv'
    path_covid = r'C:\Users\elder\OneDrive\Documentos\Entrevista Builder\Banco\dados\modelo\f_covid.csv'

    # Chamar a função para salvar os dados em arquivos CSV
    salvar_em_csv(df_covid, path_covid)
    salvar_em_csv(df_ibge_municipios, path_ibge_municipios)
    salvar_em_csv(df_ibge_ufs, path_ibge_ufs)

# Chamar a função principal
if __name__ == '__main__':
    main()
