import pandas as pd
import datetime
import locale

# Função para criar colunas de datas
def create_date_columns(df):
    # Extrai as informações de ano, mês e dia
    df['Ano'] = df['Data'].dt.year
    df['Mês'] = df['Data'].dt.month
    df['Dia'] = df['Data'].dt.day

    # Adiciona outras colunas úteis para análises de tempo
    df['DiaSemana'] = df['Data'].dt.dayofweek
    df['DiaSemanaNome'] = df['Data'].dt.strftime('%A').str.capitalize()
    df['DiaSemanaAbrev'] = df['Data'].dt.strftime('%a').str.capitalize()
    df['MêsNome'] = df['Data'].dt.strftime('%B').str.capitalize()
    df['Trimestre'] = df['Data'].dt.quarter
    df['Semestre'] = (df['Trimestre'] + 1) // 2
    df['AnoMês'] = df['Data'].dt.strftime('%Y-%m')
    df['AnoSemana'] = df['Data'].dt.strftime('%Y-%U')
    df['TrimestreAno'] = df.apply(lambda row: f"Q{row['Trimestre']}-{row['Ano']}", axis=1)
    df['SemestreAno'] = df.apply(lambda row: f"S{row['Semestre']}-{row['Ano']}", axis=1)
    df['DiaMesAno'] = df['Data'].dt.strftime('%d-%m-%Y')
    df['TrimestreMesAno'] = df.apply(lambda row: f"Q{row['Trimestre']}-{row['Data'].strftime('%b-%Y')}", axis=1)
    df['SemestreMesAno'] = df.apply(lambda row: f"S{row['Semestre']}-{row['Data'].strftime('%b-%Y')}", axis=1)
    df['AnoMesDia'] = df['Data'].dt.strftime('%Y-%m-%d')
    df['TrimestreAnoMesDia'] = df.apply(lambda row: f"Q{row['Trimestre']}-{row['Data'].strftime('%Y-%m-%d')}", axis=1)
    df['SemestreAnoMesDia'] = df.apply(lambda row: f"S{row['Semestre']}-{row['Data'].strftime('%Y-%m-%d')}", axis=1)
    df['AnoMesDiaSemana'] = df.apply(lambda row: f"{row['Data'].strftime('%Y-%m-%d')}-{row['DiaSemanaAbrev']}", axis=1)
    df['TrimestreAnoMesDiaSemana'] = df.apply(lambda row: f"Q{row['Trimestre']}-{row['Data'].strftime('%Y-%m-%d')}-{row['DiaSemanaAbrev']}", axis=1)
    df['SemestreAnoMesDiaSemana'] = df.apply(lambda row: f"S{row['Semestre']}-{row['Data'].strftime('%Y-%m-%d')}-{row['DiaSemanaAbrev']}", axis=1)
    df['SemanaNome'] = df['Data'].dt.isocalendar().week.map(lambda x: f'{x}º Sem')
    df['TrimestreNome'] = df['Trimestre'].map(lambda x: f'{x}º Trim')

# Função principal
def main():
    # Define a localidade para o Brasil
    locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')

    # Cria uma lista de datas começando a partir de 01/01/2000 até 31/12/2030
    data_inicio = datetime.date(2020, 3, 29)
    data_fim = datetime.date(2021, 11, 22)
    dias = pd.date_range(data_inicio, data_fim, freq='D')

    # Cria um dataframe vazio
    dCalendario = pd.DataFrame()

    # Adiciona as colunas de data ao dataframe
    dCalendario['Data'] = dias

    # Cria colunas de datas
    create_date_columns(dCalendario)

    # Exibe o dataframe
    print(dCalendario.head())

    path = r'C:\Users\elder\OneDrive\Documentos\Entrevista Builder\Banco\dados\modelo\d_Calendario.csv'
    dCalendario.to_csv(path, index=False)

# Execute a função principal
if __name__ == "__main__":
    main()