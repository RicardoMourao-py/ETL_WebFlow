import os
import pandas as pd
import time
from datetime import date, timedelta, datetime
from google.cloud import bigquery
# para extrair informações de páginas HTML
from bs4 import BeautifulSoup
# para paginas dinamicas
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import zipfile
import pytz
from flask import Flask
from unidecode import unidecode

app = Flask(__name__)

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1024,768")
chrome_options.add_argument("--no-sandbox")
current_path = os.path.expanduser(os.getcwd())
prefs = {"download.default_directory": current_path, 
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
}
chrome_options.add_experimental_option("prefs", prefs)

# Criando um driver
driver = webdriver.Chrome(ChromeDriverManager().install(), chrome_options=chrome_options)

# Definindo as variáveis
PROJECT = 'dado-publico-provider'
dataset = "DS_PORTAL_TRANSPARENCIA"

def scrapping():
    print("comecei o scrapping")

    # Entrando na Página
    url = 'https://portaldatransparencia.gov.br/download-de-dados/servidores'
    driver.get(url)
    driver.implicitly_wait(10) # tempo de espera de execução

    # Pegando último mês
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    value_ultimo_mes = list(soup.find(id = "links-meses"))[-1]['value']
    click_ultimo_mes = driver.find_element("css selector", f'option[value="{value_ultimo_mes}"]')   
    click_ultimo_mes.click()

    # Baixando todos os tipos
    lista_values_tipos = list(soup.find(id = "links-origens-mes"))
    for i in lista_values_tipos:
        print(f"tipos: {i}")
        click_tipo = driver.find_element("css selector", f'option[value="{i.text}"]')
        click_tipo.click()
        
        # click para baixar o tipo no ultimo mes
        click_baixar = driver.find_element(By.ID, "btn")
        click_baixar.click()

    # Espera dos 11 arquivos serem baixados
    all_files = filter(os.path.isfile, os.listdir( os.curdir ))
    files_zip = list(filter(lambda f: f.endswith('.zip'), all_files))
    while len(files_zip) < 11:
        all_files = filter(os.path.isfile, os.listdir( os.curdir ))
        files_zip = list(filter(lambda f: f.endswith('.zip'), all_files))
        
    print(os.getcwd())
    print(os.listdir( os.curdir ))

def get_zip():
    try:
        print("cheguei getzip")
        file_bkp = []
        all_files = filter(os.path.isfile, os.listdir( os.curdir ))
        files_zip = list(filter(lambda f: f.endswith('.zip'), all_files))
        print(f"files_zip: {files_zip}")
        for i in files_zip:
            with zipfile.ZipFile(i, 'r') as zip_ref:
                zip_ref.extractall()
            all_files = filter(os.path.isfile, os.listdir( os.curdir ))
            files_csv = list(filter(lambda f: f.endswith('.csv'), all_files))
            print(f"files_csv: {files_csv}")
            for j in files_csv:
                if j not in file_bkp:
                    new_filename = '_'.join(j.split(".")[0].split("_")[1:]) +'_'+'_'.join(i.split('.')[0].split('_')[1:])+'.csv' # nome_arquivo + tipo
                    os.rename(j, new_filename)
                    file_bkp.append(new_filename)
                    new_filename = ''
        print("terminei")
    except Exception as e:
        print(f"Something went wrong with scrapping, files could not be downloaded")
        print(e)
        raise e

def add_dt_extracao_csv(csv_file):

    timezone = pytz.timezone('America/Sao_Paulo')
    data_execucao = datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')
    df = pd.read_csv(csv_file, encoding='latin-1', sep = ';', low_memory=False)

    # Tirando acentos nos nomes das colunas e espaços
    for i in list(df.columns):
        new_column = unidecode(i).replace(" ", "_")
        if "_(U$)" in new_column:
            new_column = new_column.replace("_(U$)", '_US')
        if "_(R$)" in new_column: 
            new_column = new_column.replace("_(R$)", '_RS')
        if "(*)" in new_column:
            new_column = new_column.replace("(*)", '')
        if "/" in new_column:
            new_column = new_column.replace("/", '_')
        if "-" in new_column:
            new_column = new_column.replace("-", '')
        df = df.rename(columns={f'{i}': f'{new_column.replace("__","_")}'})

    # Adicionando coluna de data de extração
    df["DT_EXTRACAO"] = data_execucao
    df.to_csv(csv_file, index=False)

    print("Included DT_EXTRACAO into ", csv_file)

def job_config():
    config = bigquery.LoadJobConfig()
    config.source_format = bigquery.SourceFormat.CSV
    config.autodetect = True
    config.create_disposition = "CREATE_IF_NEEDED"
    config.field_delimiter = ","
    config.skip_leading_rows = 1
    config.write_disposition = "WRITE_APPEND"
    config.allow_jagged_rows = True
    config.allow_quoted_newlines = True
    config.max_bad_records = 1000000
    config.ignore_unknown_values = True
    return config

def bigquery_load(list_of_infos):
    client = bigquery.Client(project=PROJECT)
    print(os.getcwd())
    print(os.listdir( os.curdir ))
    all_files = filter(os.path.isfile, os.listdir( os.curdir ))
    files = list(filter(lambda f: f.endswith('.csv'), all_files))
    print(f"CSVs found{files}")
    
    for file_infos in list_of_infos:
        file_name = file_infos['file_name']
        table_name = file_infos['table']
        dataset = file_infos['dataset']
        print(f"Starting {file_name} with the following infos {file_infos}...")
        try:
            file = str(list(filter(lambda f: f.startswith(file_name), files))[0])
            print(f"Found the correspoding file {file}")
            with open(file, "rb") as source_file:
                print(f"Loading {file} into BigQuery {dataset}.{table_name}")
                table = PROJECT + "." + dataset + "." + table_name
                load_job = client.load_table_from_file(source_file, table, job_config=job_config())
                load_job.result()
        except Exception as e:
            print(f"Something went wrong with dict {file_infos}, {file_name} couldn't be loaded into {dataset}.{table_name}")
            print(e)
            raise e
        
@app.route("/")
def call_functions():
    scrapping()
    get_zip()
    data= [{'file_name': 'Afastamentos_Servidores_BACEN.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_AFASTAMENTOS_BACEN'},
            {'file_name': 'Afastamentos_Servidores_SIAPE.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_AFASTAMENTOS_SIAPE'},
            {'file_name': 'Cadastro_Aposentados_BACEN.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_APOSENTADOS_BACEN'},
            {'file_name': 'Cadastro_Aposentados_SIAPE.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_APOSENTADOS_SIAPE'},
            {'file_name': 'Cadastro_Militares.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_MILITARES'},
            {'file_name': 'Cadastro_Pensionistas_BACEN.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_PENSIONISTAS_BACEN'},
            {'file_name': 'Cadastro_Pensionistas_DEFESA.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_PENSIONISTAS_DEFESA'},
            {'file_name': 'Cadastro_Pensionistas_SIAPE.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_PENSIONISTAS_SIAPE'},
            {'file_name': 'Cadastro_Reserva_Reforma_Militares.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_RESERVA_REFORMA_MILITARES'},
            {'file_name': 'Cadastro_Servidores_BACEN.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_SERVIDORES_BACEN'},
            {'file_name': 'Cadastro_Servidores_SIAPE.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_CADASTRO_SERVIDORES_SIAPE'},
            {'file_name': 'Honorarios(Jetons)_Honorarios_Jetons.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_HONORARIOS_JETONS'},
            {'file_name': 'HonorariosAdvocaticios_Honorarios_Advocaticios.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_HONORARIOS_ADVOCATICIOS'},
            {'file_name': 'Remuneracao_Aposentados_BACEN.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_APOSENTADOS_BACEN'},
            {'file_name': 'Remuneracao_Aposentados_SIAPE.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_APOSENTADOS_SIAPE'},
            {'file_name': 'Remuneracao_Militares.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_MILITARES'},
            {'file_name': 'Remuneracao_Pensionistas_BACEN.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_PENSIONISTAS_BACEN'},
            {'file_name': 'Remuneracao_Pensionistas_DEFESA.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_PENSIONISTAS_DEFESA'},
            {'file_name': 'Remuneracao_Pensionistas_SIAPE.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_PENSIONISTAS_SIAPE'},
            {'file_name': 'Remuneracao_Reserva_Reforma_Militares.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_RESERVA_REFORMA_MILITARES'},
            {'file_name': 'Remuneracao_Servidores_BACEN.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_BACEN'},
            {'file_name': 'Remuneracao_Servidores_SIAPE.csv',
            'dataset': dataset,
            'table': 'TB_SERVS_REMUNERACAO_SIAPE'}]

    for file in data:
        add_dt_extracao_csv(file["file_name"])
    bigquery_load(data)
    return "Job complete"
