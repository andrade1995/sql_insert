# -*- coding: utf-8 -*-
"""
Created on Fri May 12 09:44:07 2023

@author: PauloAndrade
"""

import pandas as pd
import psycopg2
from psycopg2 import Error
import os
from utils import host,db,schema,user,passwd,port,table
from datetime import datetime
from sqlalchemy import create_engine
from urllib.parse import quote
from sqlalchemy import sql


#Carregamento dos arquivos .xlsx

path = r'{url_path}' #Caminho Onde estão presentes os arquivos .xlsx
files = os.listdir(path)
files_xlsx = [path + '\\' + f for f in files if f[-4:] == 'xlsx']


#ARQUIVO EM ARQUIVO CONSULTADO ID E INSERINDO O RESULTADO NO BANCO
try:
    for f in files_xlsx:
        df = pd.read_excel(f)
        
        engine = create_engine(f"postgresql://{user}:{quote(passwd)}@{host}:{port}/{db}")
        try:
            conexao = engine.connect()
            print('Conexao realizada com sucesso !!')
        except Error as err:
            print(f'Falha na Conexao: {err}')
        
        df.to_sql(table, engine , index = False , if_exists = 'append' , schema = schema)
        print('Insert realizado com sucesso !!')

    conexao.close()
except Error as err:
    print(f'Error: {err}')






