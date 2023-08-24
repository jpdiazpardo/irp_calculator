# -------------------------------------------------------------------- Importar paquetes-----------------------------------------------------------------------------------------------------------------------
import time                                                                    #Paquete para medir el tiempo de ejecución del script 
start_time = time.time()                                                       #Variable que guarda el momento de inicio del script
import pandas as pd                                                            #Paquete pandas para poder trabajar con tablas eficientemente
import re                                                                      #Paquete para poder usar expresiones regulares
from functools import reduce                                                   #Paquete para consolidar todos los df finales de cada país  
from pandas_ods_reader import read_ods                                         #Paquete para leer archivos .ods
import numpy as np                                                             #Paquete numpy
import math                                                                    #Paquete math
import pdftotext                                                               #Paquete para convertir archivos .pdf a .txt
import os                                                                      #Paquete para extraer la ruta de la carpeta donde está el script
import openpyxl                                                                #Paquete para exportar los resultados a excel
import glob                                                                    #Paquete para encontrar archivos de manera recursiva
from selenium import webdriver                                                 #Paquete para navegar de manera automática por internet
from datetime import date, timedelta                                           #Paquete para importar la funciones de fechas
from dateutil.relativedelta import relativedelta                               #Paquete para funciones de fechas   
from webdriver_manager.chrome import ChromeDriverManager                       #Paquete para utilizar chrome como navegador
from selenium.webdriver.common.by import By                                    #Paquete para definir la forma de buscar elementos HTML en cada página
from selenium.webdriver.support.ui import WebDriverWait                        #Paquete para garantizar tiempos de espera entre descargas de páginas
from selenium.webdriver.support import expected_conditions as EC               #Paquete para verificar conditioes en cada página 
from selenium.webdriver.support.select import Select                           #Paquete para seleccionar elementos de las páginas web 
from selenium.webdriver.common.action_chains import ActionChains               #Paquete para hacer hover sobre elementos en una página
from selenium.common.exceptions import ElementNotInteractableException         #Importar las excepciones de Selenium 
from selenium.common.exceptions import TimeoutException                        #Importar las excepciones de Selenium
from selenium.common.exceptions import NoSuchElementException                  #Importar las excepciones de Selenium
from selenium.common.exceptions import ElementClickInterceptedException        #Importar las excepciones de Selenium
from selenium.common.exceptions import StaleElementReferenceException          #Importar las excepciones de Selenium
from selenium.common.exceptions import NoAlertPresentException                 #Importar las excepciones de Selenium
from bs4 import BeautifulSoup                                                  #Paquete para procesar data HTML
from send2trash import send2trash                                              #Paquete para enviar archivos a la papelera de reciclaje
import tabula                                                                  #Paquete para leer tablas de los .pdf

###########################################################################################################################################################################################

########################################################## Crear las funciones que usaremos #######################################################################################################

###########################################################################################################################################################################################

# 1) Función que limpia la información que no necesitamos y deja un conjunto de medicamentos dado
def limpiar_df(df,precio,ff):
    """Crea todas las columnas auxiliares que serán necesarias para el cálculo del PRI en cada país de referencia, si no han sido ecreadas aún. 
    Asigna valores a la columna de "FF", recibe como parámetro la columna donde se almacena la info. de la FF. 
    Elimina los precios <= 0 de la columna (precio). 
    Traduce los valores de la columna "PA" de acuerdo al diccionario (traductor) y conserva los valores incluidos dentro de la lista (medicamentos). 
    Finalmente ordena los valores por PA y FF, restableciendo el índice.
    df: Data Frame
    precio: str
    ff: str""" 
    
    #Conjunto de palabras que identifican las formas farmacéuticas (FF)
    tabletas = "TABL\s|FTBL|KAPS|TABB|TABR|TABMD|Tablett|Kapsel|Depottablett|Tablet|capsule|capsula|\stab|tab\s|\scap|cap\s|pwdr|pwdr|CPR|CPS|COMPRIMIDO|SUPP|BUST|COM\s|COMP\s|SOLIDO ORAL|SÓLIDO ORAL|CARAMELO|Cápsula|gélule|gelu|gastro|granulés|COMPRIMÉ|COMPRIME|Película bucal|CM REC|CONJUNTO"
    inyectables = "PULV|RSUSP|INYECTABLE|INYECCI.N|infusjonsvæske|Injeksjonsvæske|suspensjon|injection|infusion|INFUSIÓN|inj|INJETÁVEL|syringe|powder|sir|pen|PO\sLIOF|PARENTERAL|PARENTAL|[*]EV|LIOFILIZADO|PERFUSION|Perfusión|RECONSTITUCIÓN|seringue|perf|I[.]\s*V[.]|IV[.]|JERINGA|AMP|SOLUCIONES Y SUSTANCIAS|AMPD|AMPT|AUGS"
    parches = "Depotplaster|patch|[*]\d+CER|TRANSD|SACHE|parche|S.LIDO CUT.NEO|SACHÊ"
    sol_oral = "DSTF|LSG|Mikstur|soln.oral|susp.oral|oral sol|SOLUZ|GTT|SOSP|[*]OS|SUS\sOR|SOL\sOR|LÍQUIDO ORAL|oral liquid|oral suspension|oral solution|SOL GOT OR|GOTAS|SOLUCION ORAL|Solución Oral|solution buvable|suspension buvable|Solução oral|SOL[.]ORAL|SUSPENSÃO ORAL"
    nasal_spray = "soln.nasal|Nesespray|SPR NAS|nasal|NSPR"
    lozenge = "lozenge|MUCOSA OS"
    sublingual_spray = "SPRAY,SUBLINGUAL"
    
    #Deja las columnas intactas si ya existen dentro del df
    if set(['FF','UMC (mg)',"Quantity"]).issubset(df.columns):
       df["FF"] = df["FF"] 
       df["UMC (mg)"] = df["UMC (mg)"] 
       df["Quantity"] = df["Quantity"] 
    
    #Crea las columnas auxiliares si no existen
    else:
        df["FF"] = ""
        df["UMC (mg)"] = ""
        df["Quantity"] = ""
    
        #Asigna valores a la columna con la FF de acuerdo a las palabras definidas, si ya lleno el valor no entra a la siguiente línea
        df.loc[(df[ff].str.contains(tabletas,flags=re.IGNORECASE, regex=True)) & (df["FF"]==""),"FF"] = "TAB"
        df.loc[(df[ff].str.contains(inyectables,flags=re.IGNORECASE, regex=True)) & (df["FF"]==""),"FF"] = "INJ"
        df.loc[(df[ff].str.contains(sol_oral,flags=re.IGNORECASE, regex=True)) & (df["FF"]==""),"FF"] = "SOL ORAL"
        df.loc[(df[ff].str.contains(parches,flags=re.IGNORECASE, regex=True)) & (df["FF"]==""),"FF"] = "PATCH"
        df.loc[(df[ff].str.contains(lozenge,flags=re.IGNORECASE, regex=True)) & (df["FF"]==""),"FF"] = "LOZENGE"
        df.loc[(df[ff].str.contains(nasal_spray,flags=re.IGNORECASE, regex=True)) & (df["FF"]==""),"FF"] = "NASAL SPRAY"
        df.loc[(df[ff].str.contains(sublingual_spray,flags=re.IGNORECASE, regex=True)) & (df["FF"]==""),"FF"] = "SUBLINGUAL SPRAY"
    
    df["Precio UMC (mg)"] = "" #Crea la columna auxiliar con el precio por UMC (mg)
    df[precio] = df[precio].astype(str) #Limpia la columna con el precio para evitar errores
    df[precio] = df[precio].str.replace(" ","") #Eliminar espacios en el precio
    df[precio] = df[precio].str.replace("$","").str.replace("€", "").str.replace("B/.","").str.replace("USD","") #Quitar texto de los precios
    df[precio] = df[precio].str.replace(",",".") #Reemplazar comas por punto
    
    for ind in df.index: #Eliminar puntos innecesarios, en caso de que se hayan creado en la parte anterior (puntos para expresar miles)
        if df[precio][ind].count(".")==2:
           df[precio][ind] = df[precio][ind].replace(".","", 1)
           
        elif df[precio][ind].count(".")==3:
            df[precio][ind] = df[precio][ind].replace(".","", 2)
           
    df[precio] = df[precio].astype(float) #Volver a convertir el precio a float   
        
    #Eliminar precios cero o negativos 
    df.drop(df[df[precio]<= 0].index,axis=0, inplace=True)
    
    #Traduce los PA de acuerdo al traductor y convserva los medicamentos relevantes definidos en los parámetros
    df["PA"] = df["PA"].str.upper().replace(traductor)
    df = df[df["PA"].isin(medicamentos)]
    
    #Organiza el df de acuerdo a las columnas "PA" y "FF", finalmente reestablece el índice
    df = df.sort_values(['PA',"FF"])  
    df.reset_index(inplace = True, drop=True)
    
    #La función retorna un df "limpio" y con la información necesaria para poder calcular las UMC y cantidades de cada medicamento
    return df

#2) Función que remplaza las "," por "." en un conjunto de columnas y después las convierte a flotante, para poder hacer cálculos. Finalmente convierte las unidades de paliperidona inyectable y fentanyl parche de acuerdo a su tasa de conversión especial
def ajustar_columnas(df, columnas):
    """Reemplaza las comas (,) dentro de las columnas especifcadas de un data frame (df) por un punto (.) y luego las convierte a tipo flotante. Arregla también Fentanyl parche y paliperidona inyectable
    df: Data Frame
    columnas: list[str]"""
    for i in columnas: 
        df[i] = df[i].astype(str).str.replace(",",".").astype(float)
    
    #Arreglar Fentanyl Parche y paliperidona inyectable de acuerdo a una tasa de conversion especial
    df.loc[(df["PA"]=="FENTANYL") & (df["FF"]=="PATCH"), "UMC (mg)"] = df.loc[(df["PA"]=="FENTANYL") & (df["FF"]=="PATCH"), "UMC (mg)"].replace(fentanyl)
    df.loc[(df["PA"]=="PALIPERIDONE") & (df["FF"]=="INJ"), "UMC (mg)"] = df.loc[(df["PA"]=="PALIPERIDONE") & (df["FF"]=="INJ"), "UMC (mg)"].replace(paliperidona)

#3) Función que crea un data frame "final" con los resultados agrupados por molécula
def final(df, precio, bd):
    """Agrupa los medicamentos por PA y FF para calcular el precio mínimo por UMC de cada uno de estos grupos.
    Recibe como parámetros: un df (dataframe), el nombre de la columna (string) donde está reportado el precio y el nombre de la base de datos original (string)
    df: Data Frame
    precio: str
    bd: str"""
    df["Precio UMC (mg)"] = df[precio]/(df["UMC (mg)"]*df["Quantity"])
    final = df.groupby(['PA', "FF"])["Precio UMC (mg)"].min().reset_index().rename(columns={"Precio UMC (mg)": bd})
    final["PA"] = final["PA"] + " - " + final["FF"]
    final.drop(columns=['FF'],inplace=True)
    return final


#4) Función que identifica automáticamente el parámetro "skip rows" de los data frames importados de excel
def skip_rows(df):
    """Recibe como parámetro un data frame
    df: data frame"""
    #Hay data frames con columnas date-time y esto literalmente genera un error desconocido en spyder(¿?), es necesario eliminarlas para evitar errores
    df = df.select_dtypes(exclude=['datetime64[ns]'])
    df.dropna(axis=1,how="all",inplace=True)
    df = df[df.columns[df.isnull().mean() < 0.95]] #Borrar columnas que tengan más del 95% de las filas con NaN para eliminar ruido del df
    
    try:
        if (True in df.columns.str.contains("unnamed", flags=re.IGNORECASE, regex=True))==True:
    
            #Caso más general
            if (df.count(axis = 1) >= df.shape[1]).idxmax() > 0:
                first_row = (df.count(axis = 1) >= df.shape[1]).idxmax()
                df.columns = df.loc[first_row]
                df = df.loc[first_row+1:]
    
            #Caso menos general
            elif df.count(axis = 1).idxmax() > 0:
                first_row = df.count(axis = 1).idxmax()
                df.columns = df.loc[first_row]
                df = df.loc[first_row+1:]
    
            #Casos particular en el cual es necesario "saltar" una sola columna
            else:  
                first_row = 0
                df.columns = df.loc[first_row]
                df = df.loc[first_row+1:]
    
        return df
    
    except AttributeError:
        return df
    

#5) Función que convierte en Data Frame la información de monedas extranjeras del Banco de la República
def tasas_de_cambio(source, t0, t1):
    """Recibe como parámetros la fuente de la página (source) y dos fechas tipo texto que indican una ventana de tiempo. 
    Siendo (t0) la fecha inicial y (t1) la fecha final. 
    Retorna un data frame con la info correspondiente a cada divisa en la venta de tiempo escogida.
    source: str
    t0, t1: str"""
    
    #Crear la tabla con base a los elementos HTML
    soup = BeautifulSoup(source, 'html.parser')
    tables = soup.find_all("table")
    table = tables[0]
    tab_data = [[cell.text for cell in row.find_all(["th","td"])]
                        for row in table.find_all("tr")]

    #Crear el data frame
    df = pd.DataFrame(tab_data)
    #Limpiar el dataframe
    df = df.iloc[:,1:len(monedas)+2]
    #Crear el encabezado del data frame
    df.columns = ["FECHA"] + monedas
    #Conservar la data de las fechas de interés
    df = df.iloc[df[df["FECHA"]==t0].index.values[0]:df[df["FECHA"]==t1].index.values[0]+1,:].dropna()
    #Reemplazar comas por puntos
    df = df.apply(lambda x: x.str.replace(',','.'))
    #Convertir la fecha de texto a tipo date
    df['FECHA'] =  pd.to_datetime(df['FECHA'], format='%d/%m/%Y').dt.date
    #Reemplazar valores vacíos por NaN
    df = df.replace(r'^\s*$', np.NaN, regex=True)
    #Convertir las tasas a tipo flotante 
    df[monedas] = df[monedas].astype(float)

    return df

#6) Función que genera un tiempo de espera en la ejecución del script acutal, hasta que las descargas de Chrome finalicen.
def download_wait(directory, timeout, nfiles=None):
    """Args: directory : str (The path to the folder where the files will be downloaded)
    timeout : int (How many seconds to wait until timing out)
    nfiles : int, defaults to None (If provided, also wait for the expected number of files)."""

    seconds = 0
    dl_wait = True
    while dl_wait and seconds < timeout:
        time.sleep(1)
        dl_wait = False
        files = os.listdir(directory)
        if nfiles and len(files) != nfiles:
            dl_wait = True

        for fname in files:
            if fname.endswith('.crdownload'):
                dl_wait = True
        seconds += 1
    return seconds

#7) Función que borra versiones anteriores de archivos en el directorio de trabajo, buscándolos de acuerdo a una expresión regular en la lista de "archivos".
def borrar(archivos):
    """archivos: list[str]""" 
    for archivo in archivos:
        for filename in os.listdir(carpeta):
            if re.search(archivo, filename):
                send2trash(filename)

#8) Función que renombra archivos en el directorio de trabajo, buscándolos de acuerdo a una expresión regular "archivo".
def renombrar(archivo,nuevo_nombre):
    """archivo: str
    nuevo_nombre: str"""
    for filename in os.listdir(carpeta):
        if re.search(archivo,filename): 
            os.rename(filename,nuevo_nombre)
            
#9) Función que regresa una lista con todos los archivos encontrados en el directorio de trabajo de acuerdo a una expresión regular "archivo".
def buscar_archivo(archivo):
    """archivo: str"""
    lista = []
    for filename in os.listdir(carpeta):
        if re.findall(archivo,filename): 
            lista.append(filename)
    return lista


#10) Función que regresa el archivo más reciente en el directorio de trabajo
def archivo_reciente():
    list_of_files = glob.glob(carpeta + "/*")         
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file

#11) Función que borra archivos vacíos del directorio de trabajo
def borrar_empty():
    for filename in os.listdir(carpeta):
        if os.path.getsize(filename) == 0:    
            send2trash(filename)

###########################################################################################################################################################################################

########################################################## Definir los parámetros de la calculadora #######################################################################################################

###########################################################################################################################################################################################

#Carpeta donde esta el script de la calculadora, en esta ruta guardaremos todos nuestros resultados
carpeta = os.getcwd() +"\\"

#Usaremos estos encabezados para evitar posibles problemas a la hora de descargar los archivos de manera automática
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:66.0) Gecko/20100101 Firefox/66.0", "Accept-Encoding": "*", "Connection": "keep-alive"}

#Definir los parámetros del navegador de Google Chrome, el cual usaremos para hacer consultas en diversas fuentes de internet.
chromeOptions = webdriver.ChromeOptions()
prefs = {"profile.default_content_setting_values.notifications" : 2, "download.default_directory" : carpeta, "download.prompt_for_download": False,"download.directory_upgrade": True, "plugins.always_open_pdf_externally": True} 
chromeOptions.add_experimental_option("prefs",prefs)

#------------------------------------------------------------------ URL -----------------------------------------------------------------------------------------------------------------------

#Definir los url y los respectivos países donde están las bases de datos
url = pd.read_excel(carpeta + "Parámetros.xlsx", sheet_name = "URL")
url["PAÍS"] = url["PAÍS"].str.lower()

#------------------------------------------------------------------------ TRADUCCION ----------------------------------------------------------------------------------------------------------------------------

#Definir el diccionario con los parámetros para traducir 
traductor = pd.read_excel(carpeta + "Parámetros.xlsx",sheet_name="TRADUCTOR")
traductor = dict(zip(traductor["TRADUCCIÓN"],traductor["PA"]))

#------------------------------------------------------------------------ MEDICAMENTOS ----------------------------------------------------------------------------------------------------------------------------

#Definir los medicamentos a consultar
medicamentos = pd.read_excel(carpeta + "Parámetros.xlsx",sheet_name="MEDICAMENTOS")

#Definimos un diccionario que mapea los valores del nombre comercial al principio activo en inglés
comercial_pa = dict(zip(medicamentos["NOMBRE COMERCIAL"], medicamentos["MOLECULE"]))

#Definimos un diccionario que mapea los valores del nombre de la molécula en portugues al principio activo en inglés
traductor_por = dict(zip(medicamentos["MOLECULA (POR)"], medicamentos["MOLECULE"]))

#Definimos un diccionario que mapea los valores del nombre de la molécula en español al principio activo en inglés
traductor_esp = dict(zip(medicamentos["MOLECULA (ESP)"], medicamentos["MOLECULE"]))

#Creamos una lista con los nombres comerciales de los medicamentos a consultar (LATAM y EUROPA)
marca_medicamentos = medicamentos.drop(medicamentos[medicamentos["INCLUIR"]!="X"].index, axis=0)["NOMBRE COMERCIAL"].values.tolist() #LATAM
marca_medicamentos_eur = medicamentos.drop(medicamentos[medicamentos["INCLUIR"]!="X"].index, axis=0)["NOMBRE COMERCIAL (EUR)"].values.tolist() #EUR

#Creamos una lista con los nombres de la moléculas en español a consultar 
medicamentos_esp = medicamentos.drop(medicamentos[medicamentos["INCLUIR"]!="X"].index, axis=0)["MOLECULA (ESP)"].values.tolist()

#Creamos una lista de tuples con la marca del medicamentos y el nombre de la molécula en español
medicamentos_per = list(zip(marca_medicamentos, medicamentos_esp))

medicamentos_esp = list(set(medicamentos_esp)) #Borrar posibles duplicados para no repetir el nombre de las moléculas

#Creamos una lista con los nombres de la moléculas en español a consultar 
medicamentos_por = medicamentos.drop(medicamentos[medicamentos["INCLUIR"]!="X"].index, axis=0)["MOLECULA (POR)"].values.tolist()

#Creamos una lista con los nombres de la moléculas en inglés a consultar
medicamentos = medicamentos.drop(medicamentos[medicamentos["INCLUIR"]!="X"].index, axis=0)["MOLECULE"].values.tolist()

#Creamos una lista de tuples con la molécula y la marca del medicamento (eur)
medicamentos_fr = list(zip(medicamentos,marca_medicamentos_eur))

#--------------------------------------------------------------------- FENTANYL (PARCHE) ---------------------------------------------------------------------------------------------------------------------------------------

#Durogesic tiene una tasa de conversión especial por ser un parche (mcg/h a mg):
fentanyl = {0.012:2.1, 0.025:4.2, 0.037:6.3, 0.0375:6.3, 0.05:8.4, 0.0625:10.5, 0.075:12.6, 0.0875:14.7, 0.1:16.8}   

#--------------------------------------------------------------------- PALIPERIDONA (INYECTABLE) ---------------------------------------------------------------------------------------------------------------------------------------

#la paliperidona inyectable (trinza e invega) tienen una tasa de conversión especial de palmitato de paliperidona a paliperidona:
invega = {39:25, 78:50, 117:75, 156:100, 234:150}   
trinza = {273:175 ,410:263, 546:350, 819:525}
paliperidona = {**invega, **trinza}

#----------------------------------------------------------------- DIVISAS EXTRANJERAS ----------------------------------------------------------------------------------------------------------------------------------------

#Código ISO de las monedas locales en cada uno de los países de referencia
monedas = ["ARS","AUD","BRL","CAD","CLP","EUR","GBP","MXN","NOK","PEN","UYU"] #BanRep las ordena en orden alfabético de acuerdo al ISO

#---------------------------------------------------------------- PERIODO DE REFERENCIA ----------------------------------------------------------------------------------------------------------------------------------------

#Definir las medicamentos a consultar utilizando la pestaña FECHAS
periodo = pd.read_excel(carpeta + "Parámetros.xlsx",sheet_name="FECHAS")

#Definir la ventana tiempo con las siguientes fechas: actual (today), un día menos (yesterday) y con un año menos (last_year)
#Asignar valores a today
try: 
    if math.isnan(periodo["VALORES"][1]): #Si la fecha está vacía, asignar la fecha de hoy 
        today = date.today()
except TypeError:
    today = periodo["VALORES"][1].date() #De lo contrario utilizar el valor de la fila

#Asignar valores a last_year
try: 
    if math.isnan(periodo["VALORES"][0]): #Si la fecha está vacía, asignar la fecha de hoy con un año menos
        last_year = date.today().replace(date.today().year - 1)
except TypeError:
    last_year = periodo["VALORES"][0].date() #De lo contrario utilizar el valor de la fila

#Asignar valores a yesterday (today con un día menos)
yesterday = today - timedelta(days=1)

#Convertir las fechas a string con formato %d/%m/%Y
today_str = today.strftime('%d/%m/%Y')
yesterday_str = yesterday.strftime('%d/%m/%Y')
last_year_str = last_year.strftime('%d/%m/%Y')

#Obtener el año de las fechas (en tipo str)
año_inicial = str(today.year)
año_final = str(last_year.year)

#####################################################################################################################################################################

############################################################## Descargar bases de datos de internet #################################################################

#####################################################################################################################################################################

#Inicializar el navegador de Google Chrome y configurar la carpeta de descargas usando la carpeta del script
driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions)

########################################################################################################################################################################

#-------------------------------------------------------------  CREAR LOS DATA FRAMES DE CADA PAÍS ---------------------------------------------------------------------

##########################################################################################################################################################################
#Usamos este bucle para importar los archivos a nuestra carpeta de trabajo
for ind in url.index:
    borrar(["crdownload"])
    
    #Crear data frames con los archivos existentes (excel u .ods) de cada país en caso de que no hayn sido seleccionados por el usuario
    if url["INCLUIR"][ind]!= "X":
        for pais in buscar_archivo(url["PAÍS"][ind]):
            
            #Crear cada variable dinámicamente, de acuerdo al nombre del archivo de Excel
            nombre = pais[:pais.find(".")]
            
            if pais.find(".xls")!= -1:
                vars()[nombre] = skip_rows(pd.read_excel(pais))
            
            elif pais.find(".ods")!= -1:
                vars()[nombre] = skip_rows(read_ods(pais, sheet = 1))

############################################################ BASES DE DATOS CON FUENTES DESCARGABLES ###################################################################################
                        
#--- Buscar la info. de los países seleccionados por el usuario usando el navegador de Chrome ------------------------------------------------------------------------------------------

########################################################################################################################################################################################    
    elif url["PAÍS"][ind]=="aus" and url["INCLUIR"][ind]=="X": #PBS (AUS) -------------------------------------------------------------------------------------------------------------------------------------------       
        borrar(["aus","manufacturer"]) #################################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, 'Efficient Funding of Chemotherapy')))
        driver.find_element_by_partial_link_text('Efficient Funding of Chemotherapy').click()
        driver.find_element_by_partial_link_text('excluding Efficient Funding of Chemotherapy').click()
        download_wait(directory = carpeta, timeout = 60*5)
        renombrar("non-efc","aus.xlsx")
        renombrar("prices-efc","aus1.xlsx")
        aus = skip_rows(pd.read_excel("aus.xlsx"))
        aus1 = skip_rows(pd.read_excel("aus1.xlsx"))

############################################################################################################################################################################################       
    elif url["PAÍS"][ind]=="bra" and url["INCLUIR"][ind]=="X": #ANVISA (BRA) --------------------------------------------------------------------------------------------------------------------------------------------
        borrar(["bra",'conformidade']) #####################################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.XPATH, '//a[contains(@href,"conformidade")]')))
        
        try: #Aceptar los cookies de la página
            driver.find_element_by_xpath('//button[contains(.,"ACEITO")]').click()
        except:
            pass
        
        driver.find_element_by_xpath('(//a[contains(@href,"xls")])[1]').click()
        download_wait(directory = carpeta, timeout = 60*20)
        renombrar("conformidade","bra.xls")
        bra = skip_rows(pd.read_excel("bra.xls"))
        driver.find_element_by_xpath('(//a[contains(@href,"xls")])[2]').click()
        download_wait(directory = carpeta, timeout = 60*20)
        renombrar("conformidade","bra1.xls")
        bra1 = skip_rows(pd.read_excel("bra1.xls"))
       
#############################################################################################################################################################################    
    elif url["PAÍS"][ind]=="can" and url["INCLUIR"][ind]=="X": #RAMQ (CAN) -------------------------------------------------------------------------------------------------------------------------------
        borrar(["can",'liste']) #############################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, 'Liste des médicaments')))
        driver.find_element_by_partial_link_text('Liste des médicaments').click()
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.CLASS_NAME, 'file-download')))
        driver.find_element_by_class_name('file-download').click()
        download_wait(directory = carpeta, timeout = 60*10)
        renombrar("liste","can.pdf")

#############################################################################################################################################################################    
    elif url["PAÍS"][ind]=="ger" and url["INCLUIR"][ind]=="X": #DIMDI (GER) -------------------------------------------------------------------------------------------------------------------------------
        borrar(["ger",'festbetraege']) #############################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.XPATH, "//*[@onclick ='setCookieAndHide()']")))
        driver.find_element_by_xpath("//*[@onclick ='setCookieAndHide()']").click()
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.XPATH, '//a[contains(@href,"pdf")]')))
        driver.find_element_by_xpath('//a[contains(@href,"pdf")]').click()
        download_wait(directory = carpeta, timeout = 60*10)
        renombrar("festbetraege","ger.pdf")

###############################################################################################################################################################################################            
    elif url["PAÍS"][ind]=="ecu" and url["INCLUIR"][ind]=="X": #CONSEJO DE FIJACIÓN DE PRECIOS (ECU) -----------------------------------------------------------------------------------------------------------------------
        borrar(["ecu[.]",'onsolidado']) ##########################################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href,'onsolidado')]")))
        driver.find_element_by_xpath('//a[contains(@href,"onsolidado")]').click()
        while True:
            try:
                download_wait(directory = carpeta, timeout = 60*10)
                renombrar("onsolidado","ecu.xls")
                ecu = skip_rows(pd.read_excel("ecu.xls"))
                break
            except FileNotFoundError:
                time.sleep(1)
                continue

##############################################################################################################################################################################################                        
    elif url["PAÍS"][ind]=="nor" and url["INCLUIR"][ind]=="X": #NOMA (NOR) ------------------------------------------------------------------------------------------------------------------------------------------------
        borrar(["nor","ackage prices"]) ######################################################################################################################################################
        driver.get(url["URL"][ind])
        while True:
            try:
                WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.XPATH, '//a[contains(.,"reimbursement list")]')))
                driver.find_element_by_xpath('//a[contains(.,"reimbursement list")]').click()
                download_wait(directory = carpeta, timeout = 60*10)
                renombrar("ackage prices","nor.xlsx")
                nor = skip_rows(pd.read_excel("nor.xlsx"))
                break
            except (ElementClickInterceptedException, FileNotFoundError):
                time.sleep(1)
                continue

##############################################################################################################################################################################################       
    elif url["PAÍS"][ind]=="uk" and url["INCLUIR"][ind]=="X": #eMIT (UK) --------------------------------------------------------------------------------------------------------------------------------------------------        
        borrar(["uk",'eMIT']) ################################################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.LINK_TEXT, 'eMIT national database')))
        driver.find_element_by_link_text('eMIT national database').click()
        download_wait(directory = carpeta, timeout = 60*10)
        renombrar("eMIT","uk.ods")
        uk = skip_rows(read_ods("uk.ods", sheet = 1))
    
##############################################################################################################################################################################################       
    elif url["PAÍS"][ind]=="nhs" and url["INCLUIR"][ind]=="X": #NHS (UK) --------------------------------------------------------------------------------------------------------------------------------------------------        
        borrar(["nhs",'Drug Tariff']) ################################################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.ID, 'ccc-close')))
        driver.find_element_by_id('ccc-close').click()
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.XPATH, '//*[contains(@href, "pdf")]')))
        driver.find_element_by_xpath('//*[contains(@href, "pdf")]').click()
        download_wait(directory = carpeta, timeout = 60*10)
        renombrar("Drug Tariff","nhs.pdf")
        
#############################################################################################################################################################################################    
    elif url["PAÍS"][ind]=="eeuu" and url["INCLUIR"][ind]=="X": #FSS (EEUU) ----------------------------------------------------------------------------------------------------------------------------------------------        
        borrar(["eeuu","FssPharmPrices"]) ###################################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.LINK_TEXT, 'pricing data')))
        driver.find_element_by_link_text('pricing data').click()
        download_wait(directory = carpeta, timeout = 60*10)
        renombrar("FssPharmPrices","eeuu.xlsx")
        eeuu = skip_rows(pd.read_excel("eeuu.xlsx"))

#############################################################################################################################################################################################    
    elif url["PAÍS"][ind]=="esp" and url["INCLUIR"][ind]=="X": #PETRONE (ESP) --------------------------------------------------------------------------------------------------------------------------------------------       
        borrar(["esp","ESH","ITH"]) #########################################################################################################################################################
        driver.get(url["URL"][ind])
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.XPATH, "//a[@href = '/xpet/media/ESH.xls']")))
        driver.find_element_by_xpath("//a[@href = '/xpet/media/ESH.xls']").click()
        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.XPATH, "//a[@href = '/xpet/media/ITH.xls']")))
        driver.find_element_by_xpath("//a[@href = '/xpet/media/ITH.xls']").click()
        download_wait(directory = carpeta, timeout = 60*10)
        renombrar("ESH","esp.xls")
        renombrar("ITH","esp1.xls")
        esp = skip_rows(pd.read_excel("esp.xls"))
        esp1 = skip_rows(pd.read_excel("esp1.xls"))

######################################################################################################################################################################################        

################################################################## BASES DE DATOS DE CONSULTA ONLINE #################################################################################

######################################################################################################################################################################################
    elif url["PAÍS"][ind] == "per" and url["INCLUIR"][ind]=="X": #DIGEMID (PER) ----------------------------------------------------------------------------------------------------
        per = pd.DataFrame() #df donde se guardará toda la info. consolidada #########################################################################################################
        for medicamento in medicamentos_per: #Iterar sobre los medicamentos de interés
            i_inicial = 1
            m_inicial = 0
            while True:
                try:    
                    driver.get(url["URL"][ind]) #Ingresar a la página de DIGEMID
                    driver.maximize_window()
                    WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.ID, 'btnBuscar'))) #Esperar a que los elementos de la página sean clickeables               
                    siguiente = False
                    for m in range(m_inicial,2):
                        driver.find_element_by_id('txtBuscador').clear() #Borrar los elementos de la lista de búsqueda
                        driver.find_element_by_id('txtBuscador').send_keys(medicamento[m]) #Ingresar el nombre del medicamento
                        time.sleep(3) #Esperar a que carguen los elementos de la lista desplegable                   
                        total = len(driver.find_elements_by_xpath("//*[@class = 'ui-menu-item']"))
                        if total > 0: #Verificar si el medicamento existe en la base de datos
                            break
                        elif m==1: #Si el medicamento no existe, intentar con el nombre de la molécula    
                            siguiente = True
                    
                    if siguiente == True:
                        break #Salimos del while infinito si no existe el medicamento ni por nombre comercial ni por PA
                        
                    for i in range(i_inicial, total+1): #Iterar sobre el total de resultados
                        validar = False
                        while True: #Crear una espera dinámica hasta que se despliguen nuevamente todos los elementos de la lista
                            try:
                                driver.find_element_by_xpath("(//*[@class = 'ui-menu-item'])" + "["+ str(i) + "]").click()
                                break
                        
                            except (NoSuchElementException, ElementClickInterceptedException):
                                time.sleep(1)
                                continue
                        
                        id_medicamento = driver.find_element_by_id('txtBuscador').get_attribute('value')
                        if  re.search("/\s*ml" , id_medicamento , flags = re.IGNORECASE) is not None:
                            validar = True #Si el medicamento está en XXMG/ML es necesario validar la cantidad de ML totales 
                        
                        driver.find_element_by_id("btnBuscar").click()
                            
                        try: #Hay medicamentos que aparecen en la lista despegable pero después de dar click la página aparece vacía (error de DIGEMID)
                            #Exportar todos los resultados de la búsqueda a un archivo de excel
                            WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//*[@class = 'dataTables_info']"))) #Esperar a que carguen los elementos de la página
                            
                            while True: #Crear un espera dinámica hasta que se desplieguen los resultados
                                resultados = driver.find_element_by_xpath("//*[@class = 'dataTables_info']").text
                                if re.search("\d" , resultados) is not None:
                                    break
                                else:
                                    time.sleep(1)
                                    continue
                                
                            if resultados == "Mostrando 0 a 0 de 0 registros": #Si la búsqueda arroja cero resultados
                                driver.get(url["URL"][ind]) #Volver a la pagína inicial para continuar iterando
                                WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.ID, 'btnBuscar')))
                                driver.find_element_by_id('txtBuscador').send_keys(medicamento[m])
                                continue
                                                                                    
                            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href, 'exportar')]")))
                            driver.find_element_by_xpath("//a[contains(@href, 'exportar')]").click() #Dar click en exportar       
                            download_wait(directory = carpeta, timeout = 60*10) #Esperar a que se descargue el elemento
                            per1 = skip_rows(pd.read_excel(archivo_reciente())) #leer el excel dentro de un df
                                    
                            
                            per1.dropna(inplace = True) #Borrar filas con valores vacíos
                            per1.reset_index(drop = True , inplace = True) #Resetear índice
                            per1["indice"] = per1.index + 1 #Guardar la posición del medicamento dentro del resultado de la búsqueda
                            #Eliminar duplicados
                            per1.drop_duplicates(subset = ["Nombre de Producto","Precio Unit","Titular"], inplace = True)
                            per1.reset_index(drop = True , inplace = True) #Resetear el índice
                            per1["PA"] = driver.find_element_by_xpath('//span[contains(@id, "PA")]').text  #Asignar a la columna "PA" el principio activo del medicamento
                            per1["Cantidades"] = 1 #Asignar un valor default de 1 para las cantidades
                                
                            for k in per1.index: #Iterar sobre el df auxiliar para llenar los ML de los medicamentos que necesitan ser validados
                                if (validar == True) and (per1["indice"][k]<=150): #Si es necesario validar, hacerlo máximo hasta el resultado 150
                                    #Buscar el link con los detalles del producto
                                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "(//*[contains(@href ,'FichaProducto')])[" + str(per1["indice"][k]) + "]")))
                                    ventana_1 = driver.window_handles[0] #Guardamos la ventana principal (ventana 1)
                                    
                                    #Hacemos click en el hipervínculo (hiper)
                                    hiper = driver.find_element_by_xpath("(//*[contains(@href ,'FichaProducto')])[" + str(per1["indice"][k]) + "]")
                                    driver.execute_script('arguments[0].scrollIntoView()', hiper) #Hacemos scroll para hacer visible el hiperviculo y que no ocurra un error (¿?)
                                    hiper.click()
                                        
                                    ventana_2 = driver.window_handles[1] #Guardamos la ventana auxiliar (ventana 2)
                                    driver.switch_to.window(ventana_2) #Cambiamos a la ventana 2 
                                    WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.TAG_NAME, "body"))) #Esperamos a que cargue el contenido 
                                    detalle = driver.find_element_by_tag_name('body').text #Guardar la info. de la ficha del producto
                                    per1["Cantidades"][k] = detalle #Escribir esto en el df auxiliar
                                    driver.close() #Cerrar la ventana auxiliar
                                    driver.switch_to.window(ventana_1) #Cambiamos a la ventana 1 
                                    #Extraer la info.con los ML de todo el BODY de acuerdo con una expresión regular         
                                    if re.search("[\d,.]+\s*ml", per1["Cantidades"][k], flags = re.IGNORECASE) is not None:
                                        per1["Cantidades"][k] = re.search("[\d,.]+", re.search("[\d,.]+\s*ml", per1["Cantidades"][k], flags = re.IGNORECASE).group()).group()             
                                    else: #Si no la encuentra asignamos un valor por defecto de 1
                                        per1["Cantidades"][k] = "1"
                                    
                                elif (validar == True) and (per1["indice"][k]>150): 
                                    per1["Cantidades"][k] = "" #Para los resultados > 150, cogeremos la info. de los medicamentos anteriores
                                
                            per = per.append(per1) #Unir el df auxiliar con el df principal
                            #Mandar a la papelera de reciclaje el excel descargado para no llenar innecesariamente de archivos nuestro wd
                            send2trash(archivo_reciente()) 
                        
                        except TimeoutException: #Dejamos pasar la excepción para errores en la página de DIGEMID
                            pass
                            
                        driver.get(url["URL"][ind]) #Volver a la pagína inicial para continuar iterando
                        WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.ID, 'btnBuscar')))
                        driver.find_element_by_id('txtBuscador').send_keys(medicamento[m])
                    
                except TimeoutException:
                    i_inicial = i
                    m_inicial = m
                    driver.close()
                    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions) #Inicializamos otra ventana
                    continue #Reiniciamos la búsqueda PERO empezando desde i-inicial y m-inicial
                
                break #Si todo sale bien llegamos a esta línea saliendo del loop y continuando con el siguiente medicamento de la lista
                                
        per.drop_duplicates(subset=['Nombre de Producto','Titular', 'Precio Unit'], inplace=True) #Eliminar duplicados
        per.reset_index(drop = True, inplace = True) #Resetear el ínidice
        per.to_excel("per.xlsx" , index = False) #Exportar df a excel
                          
#########################################################################################################################################################################################            
    elif url["PAÍS"][ind]=="arg" and url["INCLUIR"][ind]=="X": #ANMAT (ARG) -------------------------------------------------------------------------------------------------------------
        arg = pd.DataFrame() #Crear df prinicipal #######################################################################################################################################       
        driver.get(url["URL"][ind]) #Ingresar a la página de ANMAT
        WebDriverWait(driver, 25).until(EC.visibility_of_element_located((By.CLASS_NAME, "z-textbox"))) #Esperar a que carguen los elementos de la página
                
        for medicamento in medicamentos_esp: #Iterar sobre los medicamentos de interés
            pagina = 1
            terminar = False
            while True:
                try: #Crear una espera dinámica en caso de que ocurra la siguiente excepción
                    driver.find_element_by_id('zk_comp_81').click() #Borrar búsqueda anterior
                    time.sleep(2)
                    #Iniciar nueva búsqueda
                    buscar= driver.find_element_by_class_name('z-textbox')
                    buscar.clear() #Borrar el texto en la barra de búsquda
                    buscar.send_keys(medicamento) #Ingresar el nombre del medicamento en la barra de búsqueda
                    driver.find_element_by_id("zk_comp_80").click()
                    break
                
                except ElementClickInterceptedException:
                     time.sleep(1)
                     continue
            
            time.sleep(12)
            
            #Revisar si la búsqueda arrojo resultados y de lo contrario continuar
            if driver.find_element_by_xpath("//*[@id = 'zk_comp_86-empty']").text == 'No se han encontrado resultados':
                continue
            
            attrs= [] #Lista para guardar los atributos de la flecha
                    
            #Extraer los elementos de la tabla e navegar sobre el panel de resultados
            while True: #Navegar sobre los resultados hasta que el atributo de la flecha se vuelva "disabled"
                arg1 = pd.DataFrame() #Crear df auxiliar para ir guardando los resultados en cada iteración
                headers = [] #Crear una lista vacía para guardar los encabezados de la tabla
                rows = driver.find_elements_by_xpath("//*[@id='zk_comp_86-cave']/tbody/tr") #Número de filas
                columns = driver.find_elements_by_xpath("//*[@id='zk_comp_86-cave']/tbody/tr[1]/td") #Número de columnas
                    
                for i in range(1,len(rows)): #Llenar el df iterando sobre los elementos de la tabla
                    for j in range(2,len(columns)-2):
                        if j == 8: #La columna 8 está oculta y tiene información basura que no es de interés
                            continue
                        else:
                            data = "//*[@id='zk_comp_86-cave']/tbody/tr[" + str(i) + "]/td["  + str(j) + "]"
                            try:
                                arg1.loc[i,j]= driver.find_element_by_xpath(data).text
                            except:
                                continue
                            if i==1: #Guardar el nombre de las columnas en la lista headers
                                columna = "//*[@id='zk_comp_86-headtbl']/tbody/tr/th[" + str(j) + "]"
                                headers.append(driver.find_element_by_xpath(columna).text)
                    
                arg = arg.append(arg1) #Combinar el df auxiliar con el df principal
                siguiente = driver.find_element_by_name('zk_comp_99-next') #Buscar la flecha de "siguiente"
                
                #Examinar las propiedades de la flecha
                for attr in siguiente.get_property('attributes'):
                    if attr['name']=='disabled':
                        terminar = True
                   
                #Parar de iterar sobre el panel de navegación cuando la flecha este "disabled"
                if terminar==True:
                    break
    
                else: #Hacer click en "siguiente" si hay más resultados disponibles
                    while True:
                        try:
                            driver.find_element_by_name('zk_comp_99-next').click()
                            pagina = pagina + 1
                            while int(driver.find_element_by_name("zk_comp_99-real").get_attribute("value")) != pagina:
                                time.sleep(1)
                            
                            break
                        
                        except StaleElementReferenceException:
                            time.sleep(1)
                            
        arg.columns = headers #Asignar nombres a las columnas del df con headers  
        arg.reset_index(drop = True , inplace = True) #Resetar el índice del df
        arg.to_excel("arg.xlsx" , index = False) #Exportar df principal a excel

############################################################################################################################################################################################
    elif url["PAÍS"][ind]=="por" and url["INCLUIR"][ind]=="X": #INFARMED (POR) -------------------------------------------------------------------------------------------------------------
        k = 1 ##############################################################################################################################################################################
        por = pd.DataFrame() #Crear un df frame principal donde guardaremos la info
        for medicamento in medicamentos_por: #Iterar sobre la lista de medicamentos seleccionados por el usuario
            #Cuando hay muchos medicamentos en la búsqueda, el servidor de INFOMED puede caerse y la idea no es comenzar desde el principio
            k_inicial = 1 #Creamos un variable auxiliar que nos guarde la iteración en la que ibamos
            while True: #Usamos un while para continuar iterando hasta que sea exitoso
                try:
                    driver.get(url["URL"][ind]) #Entramos a la página
                    #Esperamos a que carguen los elementos (barra de búsqueda)
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='mainForm:dci_input']")))
                    #Covertimos el medicamento del archvio parámetros a la forma en que aparece en INFARMED
                    medicamento = medicamento.split(" ")
                    medicamento = [palabra.lower().capitalize() for palabra in medicamento]                
                    separator = " "
                    medicamento = separator.join(medicamento)
            
                    #Limpiamos la barra de búsqueda e iniciamos una nueva consulta
                    driver.find_element_by_xpath("//*[@id='mainForm:dci_input']").clear()
                    driver.find_element_by_xpath("//*[@id='mainForm:dci_input']").send_keys(medicamento)
            
                    try: #Revisamos si el medicamento aparece en la página, si no, forzamos un break y continuamos con el siguiente
                        WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.XPATH, "//li[starts-with(., '" + medicamento + "')]"))) 
            
                    except TimeoutException: #Si el medicamento no aparece se generará un time out y forzamos un break
                        break

                    #Hacemos click en el medicamento que aparece
                    driver.find_element_by_xpath("//li[starts-with(., '" + medicamento + "')]").click() 
            
                    #Búscamos sólo las presentaciones que son actualmente comercializadas - 'Comercialização das Apresentações'
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//*[@title ='Comercialização das Apresentações']")))
                    estado = driver.find_element_by_xpath("//*[@title ='Comercialização das Apresentações']")
                    estado.click()
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//li[starts-with(., 'Comercializado')]")))
                    driver.find_element_by_xpath("//li[starts-with(., 'Comercializado')]").click()
            
                    #Damos click en el botón buscar
                    driver.find_element_by_xpath("//*[@id='mainForm:btnDoSearch']").click()
                    time.sleep(10) #Esperamos 10 segundos a que se actualicen los resultados
    
                    #Revisamos si existen resultados para esa búsqueda
                    WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//*[@class = 'ui-paginator-current']")))
                    existe = driver.find_element_by_xpath("//*[@class = 'ui-paginator-current']").text
                    if existe == "A mostrar 0 - 0 de um total de 0 registos. Está a visualizar a página 1 de 1": #Si no existen registros forzamos un break y continuamos con el siguiente medicamento
                        break
                    
                    #Desplegamos la opción de mostrar 100 resultados
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@id,'linkNome')]")))
                    num_resultados = driver.find_element_by_xpath('//select[contains(@class, "paginator")]')
                    Select(num_resultados).select_by_visible_text("100")
                    time.sleep(10) #Esperamos 10 seg a que se despliguen todos los resultados
            
                    for k in range(k_inicial, len(driver.find_elements_by_xpath("//a[contains(@id,'linkNome')]"))+1):
                        #Crear una nueva ventana
                        link = driver.current_url
                        ventana_1 = driver.window_handles[0] #La ventana principal se llamará ventana_1
                        driver.execute_script("window.open('');") #Abrir una ventana nueva
                        ventana_2 = driver.window_handles[1] #La ventana auxiliar se llamará ventana_2
                        driver.switch_to.window(ventana_2)
                        driver.get(link)
                        
                        #Iteramos sobre todos los resultados disponibles, iniciamos desde k-inicial
                        WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@id,'linkNome')]")))
                        
                        headers_por = [] #Lista donde se guardan los encabezados de la tabla
                        datos = [] #Lista donde se guarda la información de la k-ésima fila

                        #Llenar la listas de los encabezados y la k-ésima fila 
                        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//*[contains(@id, 'medicamentos_data')]/tr["+ str(k)+ "]"))) 
                        for j in range(2,7): #Las columnas 2 a 6 son las que importan
                            header = driver.find_element_by_xpath('(//*[@role = "columnheader"])['+ str(j)+ ']').text
                            headers_por.append(header) 
                            dato = driver.find_element_by_xpath("//*[contains(@id, 'medicamentos_data')]/tr["+ str(k)+ "]/td["+ str(j)+ "]").text
                            datos.append(dato)

                        #A veces los las filas no son clicekables por una animación que sale en la página. Creamos una espera dinámica para evitar posibles errores
                        while True: 
                            try: #Damos click sobre el vínculo de la k-ésima fila
                                driver.find_element_by_xpath("(//a[contains(@id,'linkNome')])[" + str(k) + "]").click()
                                break
                            except:
                                time.sleep(1)
                                continue

                        #Examinamos el elemento 'card body' el cual tiene información sobre la cantidad de presentaciones
                        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "(//*[@class='card-body'])[5]/div"))) 
                        num_resultados = driver.find_element_by_xpath("(//*[@class='card-body'])[5]/div").text 
                        #A lo sumo pueden haber tres presentaciones comercializadas, por eso escogemos el mínimo entre lo que diga la página y tres
                        num_resultados = min(3, int(num_resultados[:num_resultados.find(" ")])) 

                        #Extraemos la información de precios y cantidades de las presentaciones comercizalizadas
                        for i in range(1, num_resultados + 1): 
                            #Las cantidades están en el elemento panel grid header
                            cantidades = driver.find_element_by_xpath("(//*[contains(@class,'panelgrid-header')])[" + str(i) + "]").text 
                            #Los precios están en el elemento panel de precios
                            precios = driver.find_element_by_xpath("(//*[@id = 'preco-panel'])["+ str(i)+ "]").text  
                            #Creamos una lista con todos los precios de acuerdo a una expresión regular
                            precios = re.findall("[\d,.]+\s*€", precios)

                            por1 = pd.DataFrame([datos]) #Creamos un data frame auxiliar y le asignamos los valores de la k-ésima fila
                            #Copiamos las filas en función de la cantidad de precios encontrados, si no se encontraron precios, el dataframe tendra cero filas 
                            por1 = pd.DataFrame(np.repeat(por1.values,len(precios),axis=0), columns = headers_por)
                            por1["Presentacion"] = cantidades #Creamos la columna "Presentacion" donde se guardarán las cantidades
                            por1["Preco"] = "" #Creamos la columna Preco donde se guardarán los precios

                            for z in por1.index: #Iteramos sobre las filas
                                por1["Preco"][z] = precios[z] #Asignamos a cada fila el precio encontrado 
    
                            por = por.append(por1) #Unimos el df auxiliar con el df principal    
    
                        #Nos devolvemos a la página inicial donde estaban todos los resultados
                        driver.close() #Cerrar la ventana auxiliar 
                        driver.switch_to.window(ventana_1) #Volver a la ventana principal
                            
                except TimeoutException: #Si la página muere y se hace un timeout, hacemos lo siguiente
                    k_inicial = k #Guardamos la k iteración en la que ibamos en nuestra variable auxiliar k-inicial
                    driver.quit() #Cerramos el navegador
                    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions) #Inicializamos otra ventana
                    continue #Reiniciamos la búsqueda PERO empezando desde k-inicial

                break #Si todo sale bien llegamos a esta línea saliendo del loop y continuando con el siguiente medicamento de la lista

        por.drop_duplicates(inplace=True) #Borramos posibles duplicados
        por.reset_index(drop = True, inplace = True) #Reseteamos el ínidice
        por.to_excel("por.xlsx" , index = False) #Exportamos los resultados de la consulta a un excel

############################################################################################################################################################################################    
    elif url["PAÍS"][ind]=="fra" and url["INCLUIR"][ind]=="X": #L'AM (FRA) -----------------------------------------------------------------------------------------------------------------
        #Crear los data frame auxiliares para todas las consultas CIP y UCD #################################################################################################################
        fra_cip = pd.DataFrame()
        fra_ucd = pd.DataFrame()

        #Iterar sobre una lista de medicamentos que contiene el nombre comercial y el principio activo
        for medicamento in medicamentos_fr:
            #Creamos estas variables auxiliares por si la consulta llega a fallar
            i_inicial = 1
            k_inicial = 0
            z_inicial = 1
            while True:
                try:
                    #Hay dos tipos de búsqueda: CIP o UCD, es importante hacer los dos tipos de consulta    
                    #i=1 para CIP e i = 2 para UCD
                    for i in range(i_inicial,3):
                        #k=0 es el nombre de la molécula y k=1 para el nombre comercial
                        for k in range(k_inicial,2):
                            driver.get(url["URL"][ind]) #Entramos a la página
                            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.NAME, "p_nom_commercial")))
                            driver.find_element_by_name("p_nom_commercial").clear()
                            driver.find_element_by_name("p_nom_commercial").send_keys(medicamento[k].replace(" / "," ")) #Ingresar el medicamento en la barra de búsqueda
                            if i==1: #Seleccionar todas las opciones de CIP
                                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.NAME, "p_cip")))
                                driver.find_element_by_name("p_cip").click()
                                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.NAME, "p_homol_ass")))
                                driver.find_element_by_name("p_homol_ass").click()
                                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.NAME, "p_homol_coll")))
                                driver.find_element_by_name("p_homol_coll").click()
                            elif i==2: #Seleccionar todas las opciones de UCD
                                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.NAME, "p_ucd")))
                                driver.find_element_by_name("p_ucd").click()
                                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.NAME, "p_homol_retro")))
                                driver.find_element_by_name("p_homol_retro").click()
                                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.NAME, "p_homol_taa")))
                                driver.find_element_by_name("p_homol_taa").click()

                            #Click en el botón validar
                            WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '*//input[@value = "Valider"]')))
                            driver.find_element_by_xpath('*//input[@value = "Valider"]').click()
                            #Si la búsqueda no retorna ningún resultado continuar iterando
                            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//*[@class ='titreActu']")))            
                            if driver.find_element_by_xpath("//*[@class ='titreActu']").text == "Aucune donnée ne correspond à votre sélection !":
                                continue
            
                            #Continuar con el algoritmo dependiendo si la consulta tiene uno o varios registros
                            muchos_registros = False
                            #No sumamos +1 en el bucle porque no queremos llegar al último elemento (que es un link de descarga a Excel)
                            for z in range(z_inicial, max(2,len(driver.find_elements_by_xpath("//*[@class = 'liensoul']")))):
                                if len(driver.find_elements_by_xpath("//*[@class = 'liensoul']"))>0:
                                    muchos_registros = True  #Avisar que hay más de un registro 
                                    #Hacer click en cada registro si hay más de uno
                                    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, "(//*[@class = 'liensoul'])[" + str(z) + "]")))
                                    codigo = driver.find_element_by_xpath("(//*[@class = 'liensoul'])["+ str(z) +"]").get_attribute("href") #Obtener el hipervínculo con el detalle del medicamento
                                    ventana_1 = driver.window_handles[0] #La ventana principal se llamará ventana_1
                                    driver.execute_script("window.open('');") #Abrir una ventana nueva
                                    ventana_2 = driver.window_handles[1] #La ventana auxiliar se llamará ventana_2
                                    driver.switch_to.window(ventana_2) #Cambiar a la ventana 2
                                    driver.get(codigo) #Buscar el detalle del medicamento en una ventana nueva
                                                                        
                                #Crear df auxiliares en cada iteración para consolidar los data frames principales
                                fra1_cip = pd.DataFrame()
                                fra1_ucd = pd.DataFrame()
                                #Crear listas con los encabezados de cada tabla
                                header_cip = []
                                header_ucd = []
                                        
                                if i==1: #CIP
                                    try: #Si no existen precios para ese registro, volver a la página anterior con todos los registros
                                        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//table")))
                                        if driver.find_element_by_xpath("//table[13]").text != "Historique Remboursement :":
                                            if muchos_registros == True:
                                                driver.close() #Cerrar la ventana auxiliar
                                                driver.switch_to.window(ventana_1) #Volver a la ventana prinicipal
                                            continue
                                        
                                    except NoSuchElementException: #Si no existe la tabla 13, continuar iterando
                                        if muchos_registros == True:
                                            driver.close() #Cerrar la ventana auxiliar
                                            driver.switch_to.window(ventana_1) #Volver a la ventana principal
                                        continue
                            
                                    #Llenar el data frame iterando sobre las filas y columnas de la tabla
                                    rows = len(driver.find_elements_by_xpath("//table[14]/tbody/tr"))
                                    columns = len(driver.find_elements_by_xpath("//table[14]/tbody/tr[1]/td"))
                                    for x in range(2 , rows+1):
                                        for y in range(1 , columns+1):
                                            if x==2:
                                                nombre_columna = "//table[14]/tbody/tr[1]/td["  + str(y) + "]"
                                                header_cip.append(driver.find_element_by_xpath(nombre_columna).text)
                                                data = "//table[14]/tbody/tr[" + str(x) + "]/td["  + str(y) + "]"
                                                fra1_cip.loc[x,y] = driver.find_element_by_xpath(data).text
                            
                                    fra1_cip.columns = header_cip #Asignar nombre de columnas al df                
                                    #Llenar información correspondiente a la descripción del medicamento
                                    fra1_cip["Désignation"] = driver.find_element_by_xpath("//table[2]/tbody/tr[4]/td[3]").text
                                    fra1_cip["Conditionnement"] = driver.find_element_by_xpath("//table[2]/tbody/tr[5]/td[3]").text
                                    fra1_cip["Type"] = "CIP"
                                    fra1_cip["PA"] = medicamento[0] #Guardamos en la columna "PA" el principio activo
                                    fra_cip = fra_cip.append(fra1_cip) #Combinar el df principal con el auxiliar
                                    #Eliminar duplicados y columnas que no son de interés. Finalmente cambiar el nombre las columnas
                                    fra_cip.drop_duplicates(subset = ["Prix Fabricant HT €","Désignation","Conditionnement"], inplace = True)
                                
                                elif i==2: #UCD
                                    try: #Si no existen precios para ese registro, volver a la página anterior con todos los reistros
                                        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//table")))
                                        if driver.find_element_by_xpath("//table[12]").text != "Historique Remboursement :":
                                            if muchos_registros == True:
                                                driver.close() #Cerrar la ventana auxiliar
                                                driver.switch_to.window(ventana_1) #Volver a la ventana prinicipal
                                            continue
                        
                                    except NoSuchElementException: #Si no existe la tabla 12, continuar iterando
                                        if muchos_registros == True:
                                            driver.close() #Cerrar la ventana auxiliar
                                            driver.switch_to.window(ventana_1) #Volver a la ventana principal
                                        continue
                            
                                    rows = len(driver.find_elements_by_xpath("//table[13]/tbody/tr"))
                                    columns = len(driver.find_elements_by_xpath("//table[13]/tbody/tr[1]/td"))
                                    for x in range(2 , rows+1):
                                        for y in range(1 , columns+1):
                                            if x ==2:
                                                nombre_columna = "//table[13]/tbody/tr[1]/td["  + str(y) + "]"
                                                header_ucd.append(driver.find_element_by_xpath(nombre_columna).text)
                                                data = "//table[13]/tbody/tr[" + str(x) + "]/td["  + str(y) + "]"
                                                fra1_ucd.loc[x,y]= driver.find_element_by_xpath(data).text    
            
                                    header_ucd = [header.replace("\n"," ") for header in header_ucd] #Remplazar espaciados por espacio 
                                    fra1_ucd.columns = header_ucd #Asignar nombre de columnas al df
                                    fra1_ucd["Désignation"] = driver.find_element_by_xpath("//table[2]/tbody/tr[3]/td[3]").text
                                    fra1_ucd["Conditionnement"] = driver.find_element_by_xpath("//table[2]/tbody/tr[4]/td[3]").text
                                    fra1_ucd["Type"] = "UCD"
                                    fra1_ucd["PA"] = medicamento[0] #Guardamos en la columna "PA" el principio activo
                                    fra_ucd = fra_ucd.append(fra1_ucd)
                                    fra_ucd.drop_duplicates(subset = ["Prix HT", "Désignation","Conditionnement"], inplace = True)
                                        
                                if muchos_registros == True:
                                    driver.close()
                                    driver.switch_to.window(ventana_1)

                except TimeoutException:
                    i_inicial = i #Guardamos la consulta en la que ibamos CIP o UCD
                    k_inicial = k #Guardamos el tipo de consulta por PA o nombre comercial
                    z_inicial = z #Guardamos el número de registro en el que ibamos
                    driver.close() #Cerramos el navegador que murió
                    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions) #Inicializamos otra ventana
                    continue #Reiniciamos la búsqueda PERO empezando desde i-inicial, k-inicial, z-inicial
                
                break #Si todo sale bien llegamos a esta línea saliendo del loop y continuando con el siguiente medicamento de la lista

        try: #Guardar las columnas relevantes del df cip
            fra_cip = fra_cip[['Prix Fabricant HT €','Prix Public TTC €',"Désignation","Conditionnement","Type","PA"]]
            #Cambiar los nombres para poder consolidar en un solo df prinicipal
            fra_cip.rename(columns={"Prix Fabricant HT €": "Prix HT", "Prix Public TTC €": "Prix TTC"}, inplace = True)
        except KeyError: #Si el df está vacío porque no hay info, ignorar el error
                pass
    
        try: #Guardar las columnas relevantes del df ucd
            fra_ucd = fra_ucd[['Prix HT','Prix TTC',"Désignation","Conditionnement","Type","PA"]]
        except KeyError: #Si el df está vacío porque no hay info, ignorar el error
            pass
        
        fra = fra_cip.append(fra_ucd) #Cambinar los df cip y ucd en un df prinicipal
        fra.reset_index(drop = True, inplace = True) #Resetar el ínidice
        fra.to_excel("fra.xlsx" , index = False) #Exportar los resultados de la consulta a Excel    
    
####################################################################################################################################################################################################
    elif url["PAÍS"][ind]=="chi" and url["INCLUIR"][ind]=="X": #CHILE COMPRA (CHI) -----------------------------------------------------------------------------------------------------------------
        chi = pd.DataFrame() #######################################################################################################################################################################
        for medicamento in medicamentos_esp:
            #Inicializar las siguientes variables por si se cae la consulta
            pagina_inicial = 1 
            orden_inicial = 1
            while True: #Crear un while infinito, en caso de que se genere un time out guardaremos la iteración en la que ibamos
                try:
                    driver.get(url["URL"][ind]) #Ingresar a la página
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.NAME, "txtSearch")))
                    driver.find_element_by_name("txtSearch").send_keys(medicamento) #Ingresar el PA para la consulta
                    
                    driver.find_element_by_id("chkEstado").click() #Buscar licitaciones con estado adjudicado
                    estado = driver.find_element_by_xpath('//select[contains(@name,"Adquisition")]')
                    Select(estado).select_by_visible_text("Adjudicada")
                    
                    driver.find_element_by_id("chkFecha").click() #Seleccionar Fecha
                    tipo_fecha = driver.find_element_by_name("ddlDateType")
                    Select(tipo_fecha).select_by_visible_text("Fecha de Adjudicacíón")
                    driver.find_element_by_name("txtFecha1").clear()
                    driver.find_element_by_name("txtFecha2").clear()
                    
                    driver.find_element_by_name("txtFecha1").send_keys(last_year_str.replace("/","-")) #Seleccionar Fecha Inicial
                    driver.find_element_by_name("txtFecha2").send_keys(today_str.replace("/","-")) #Seleccionar Fecha Final
                                                            
                    driver.find_element_by_name('btnBusqueda').click() #Dar click en el botón buscar
        
                    WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.CLASS_NAME, "cssResultDate")))
                    if driver.find_element_by_class_name("cssResultDate").text == "No se encontraron resultados para su búsqueda.":
                        break #Si el medicamento no existe salir del while
        
                    if pagina_inicial > 1: #Ingresar a la página en que ibamos en caso de que se haya caído la consulta
                        for num_pagina in driver.find_elements_by_xpath("//*[contains(@onclick, 'PaginadorBusqueda')]"):
                            if num_pagina.text == str(pagina_inicial):
                                num_pagina.click()
                                break
            
                    num_paginas = len(driver.find_elements_by_xpath("//*[contains(@onclick, 'PaginadorBusqueda')]"))
                    for k in range(pagina_inicial, num_paginas+2):            
                        WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//*[contains(@id,'lnkCodigo')]")))
                        licitaciones = driver.find_elements_by_xpath("//*[contains(@id,'lnkCodigo')]") #Todas las licitaciones
                        num_resultados = len(licitaciones) #Número total de licitaciones
                   
                        for licitacion in licitaciones:
                            num_licitacion = licitacion.text #Guardar el número de la licitación
                            num = driver.find_element_by_xpath("//*[contains(@id,'lnkCodigo')]").get_attribute("onclick") #Guardar el hipervínculo para abrirlo en otra ventana
                            ventana_1 = driver.window_handles[0] #Guardamos la ventana principal (ventana 1)
                            habilitado = True
                            num = "https://www.mercadopublico.cl" + num[re.search("/[A-Z]",num).start():re.search("',",num).start()]
                            driver.execute_script("window.open('');") #Abrir una segunda ventana
                            ventana_2 = driver.window_handles[1] #La ventana 2 se llamará ventana_2
                            driver.switch_to.window(ventana_2) #Cambiar a la ventana 2
                            driver.get(num) #Abrir el hipervínculo en la ventana 2
                
                            WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.NAME, "imgOrdenCompra")))
                            imagen = driver.find_element_by_name("imgOrdenCompra") #Buscar el el elemento Orden de Compra
                            #Revisar los atributos de la imagen para saber si está habilitada
                            for attr in imagen.get_property('attributes'):
                                if attr['name']=='disabled':
                                    habilitado = False #Si está deshabilitado, "habilitado" se vuelve False
                                
                            if habilitado == False:
                                driver.close() #Si las OC no están habilitadas para consulta, cerrar la ventana 2
                                driver.switch_to.window(ventana_1) #Volver a la ventana prinicipal
                                continue #Continuar iterando sobre las licitaciones
                            
                            detalle = str(driver.find_element_by_name("imgOrdenCompra").get_attribute('href')) #Buscar el vínculo de la orden las OC
                            detalle = "https://www.mercadopublico.cl/Procurement" + detalle[5:]
                            driver.get(detalle) #Abrir el hipervínculo en la misma ventana
                
                            WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.ID, "lblSeachResult")))
                            if driver.find_element_by_id("lblSeachResult").text == "No se encontraron ordenes de compra para esta licitación.":
                                driver.close() #Si no se encontraron OC para esa licitación, cerrar la ventana 2  
                                driver.switch_to.window(ventana_1) #Volver a la ventana prinicipal
                                continue #Continuar iterando sobre k-licitaciones
                
                            WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, '//*[contains(@onclick, "PurchaseOrder")]')))
                            num_oc = len(driver.find_elements_by_xpath('//*[contains(@onclick, "PurchaseOrder")]')) #Guardar el número total de OC asociadas a esa licitación
                        
                            for order in range(orden_inicial, num_oc+1): #Iterar sobre el total de órdenes
                                headers = [] #Lista de encabezados
                                chi1 = pd.DataFrame() #Crear un df auxiliar
                                orden_de_compra = driver.find_element_by_xpath('(//*[contains(@onclick, "PurchaseOrder")])[' + str(order) + ']').text #Guardar el número de la OC 
                                link = driver.find_element_by_xpath('(//*[contains(@onclick, "PurchaseOrder")])[' + str(order) + ']').get_attribute("onclick") #Guardar el hipervínculo para abrirlo en otra ventana
                                driver.execute_script("window.open('');") #Abrir una tercera ventana
                                ventana_3 = driver.window_handles[2] #La ventana 3 se llamará ventana_3
                                driver.switch_to.window(ventana_3) #Cambiar a la ventana 3
                                driver.get("https://www.mercadopublico.cl" + link.split("'")[1]) #Abrir el hipervínculo en la ventana 3                    
                            
                                #Al tratar de abrir el hipervínculo puede aparecer una alerta que diga que hay un error con esa OC
                                try: #Revisar si la ventana genera una alerta
                                    driver.switch_to.alert.accept() #Aceptar la alerta
                                    driver.close() #Cerrar la ventana 3
                                    driver.switch_to.window(ventana_2) #Volver a la ventana 2
                                
                                    if order == num_oc: #Si casualmente era la última OC hacer lo siguiente 
                                        driver.close() #Cerrar la ventana 2
                                        driver.switch_to.window(ventana_1) #Volver a la ventana 1
                                
                                    continue #Continuar iterando
                                                
                                except NoAlertPresentException: #Si la ventana no genera una alerta, ignorar el error y continuar ejectuando el código
                                        pass 
                                            
                                #Al tratar de abrir el hipervínculo puede aparecer un error que dice que en estos momentos no se puede atender la solicitud.
                                try:
                                    if driver.find_element_by_id("_lblMessaje").text == "En estos momentos no podemos atender su solicitud.":
                                        driver.close() #Cerrar la ventana 3
                                        driver.switch_to.window(ventana_2) #Volver a la ventana 2
                                                                    
                                        if order == num_oc: #Si casualmente era la última OC hacer lo siguiente 
                                            driver.close() #Cerrar la ventana 2
                                            driver.switch_to.window(ventana_1) #Volver a la ventana 1
                                
                                        continue
                                
                                except NoSuchElementException:
                                    pass
                                
                                WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//table[2]"))) #Esperar a que cargen los elementos de la página
                            
                                #Buscar el número de columnas
                                columns = len(driver.find_elements_by_xpath("//*[@scope = 'col']"))
                                #Buscar el número de filas (contiene encabezados)
                                rows = len(driver.find_elements_by_xpath("//*[@id='gv']/tbody/tr"))
                    
                                #Llenar el df
                                for i in range(2, rows + 1): #Iteramos desde la segunda fila para no usar los encabezdos
                                    for j in range(1, columns + 1):
                                        if i == 2: #Llenar los encabezados sólo para la primera iteración de i
                                            header = "(//*[@scope = 'col'])[" + str(j) + "]"
                                            headers.append(driver.find_element_by_xpath(header).text)
                            
                                            data = "//*[@id='gv']/tbody/tr[" + str(i) + "]/td[" + str(j) + "]"
                                            chi1.loc[i,j] = driver.find_element_by_xpath(data).text
                                    
                                chi1.columns = headers #Asignar el nombre de las columnas
                                chi1["Licitación"] = num_licitacion #Escribir el nombre de la licitación
                                chi1["PA"] = medicamento #Guardar el medicamento se consulto
                                chi1["OC"] = orden_de_compra #Guardar el nombre de la OC consultada
                                chi = chi.append(chi1) #Unir el df auxiliar con el df principal
                                driver.close() #Cerrar la ventana 3
                                driver.switch_to.window(ventana_2) #Volver a la ventana 2
                                if order == num_oc: #Entrar cuando se terminen de revisar todas las OC 
                                    driver.close() #Cerrar la ventana 2
                                    driver.switch_to.window(ventana_1) #Volver a la ventana prinicipal (ventana 1)

                        #Cambiar de página   
                        for num_pagina in driver.find_elements_by_xpath("//*[contains(@onclick, 'PaginadorBusqueda')]"):
                            if num_pagina.text == str(pagina_inicial + 1): #Buscar la siguiente página
                                num_pagina.click() #Hacer click sobre la siguiente página
                                pagina_inicial = pagina_inicial + 1 #Actualizar la página en la que vamos
                                break #Parar de iterar sobre el Selector de páginas
                
                except TimeoutException: #Si se genera un Timeout guardar los parámetros donde ibamos
                    try:
                        orden_inicial = order #Orden
                    except NameError:
                        orden_inicial = 1
                        
                    driver.quit() #Cerrar todas las ventana abiertas
                    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions) #Inicializar un nuevo navegador
                    continue #Continuar

                break #Si todo sale bien, salir del bucle inifinito
        
        chi.to_excel("chi.xlsx", index = False) #Exportar los resultados a Excel

####################################################################################################################################################################################################
    elif url["PAÍS"][ind]=="nice" and url["INCLUIR"][ind]=="X": #NICE (UK) -----------------------------------------------------------------------------------------------------------------
        nice = pd.DataFrame() #######################################################################################################################################################################
        iteracion_1 = True
        for medicamento in medicamentos: #Iterar sobre los medicamentos
            while True: #Crear un while infinito en caso de que la página muera
                try:
                    driver.get(url["URL"][ind]) #Ingresar a la página de NICE
                    
                    if iteracion_1 == True: #Aceptar los cookies de la página, pero sólo en la iteración 1
                        try: 
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'Accept')]"))) #Esperar a que cargue la página de inicio
                            driver.find_element_by_xpath("//button[contains(.,'Accept')]").click()
                        except TimeoutException:
                            pass
                    
                    iteracion_1 = False
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//*[@id='autocomplete']"))) #Esperar a que cargue la página de inicio
                    driver.find_element_by_xpath("//*[@id='autocomplete']").send_keys(medicamento.replace(" / "," ")) #Ingresar el medicamento en la barra de búsqueda
                    driver.find_element_by_xpath('//*[@type = "submit"]').click() #Dar click en el botón "buscar"

                    WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='results-title']"))) #Esperar a que carguen los resultados 
                    if driver.find_element_by_xpath('//*[@id="results-title"]').text == "No results found": #Si la búsqueda no retorna ningún resultado, salir del while infinito
                        break
    
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//*[contains(@href, 'Date')]")))
                    driver.find_element_by_xpath("//*[contains(@href, 'Date')]").click() #Organizar los resultados de la búsqueda por fecha
                    time.sleep(3) #Esperar 3 seg. a que los resultados se organicen
                    while True: #Crear una espera dinámica en caso de que ocurra un StaleElementReferenceException
                        try:
                            resultados = [resultado.text for resultado in driver.find_elements_by_xpath("//*[@class = 'card']")] #Guardar todos los resultados en una lista
                            break
                        except StaleElementReferenceException: #En caso de que ocurra esta excepción, esperar un segundo
                            time.sleep(1)
                            continue
                            
                    matching = [] #Creamos una lista auxilar en la que guardaremos los índices de los resultados que cumplen con la condición que nos interesa
                    for i in range(len(resultados)): #Los resultados que nos interesan cumplen las siguientes dos condiciones: 1) Tienen el nombre del medicamento y 2) Son Technology appraisal guidance publicadas
                        if (re.search(medicamento, resultados[i], flags = re.IGNORECASE) is not None) and (re.search("Technology appraisal guidance (Last updated|Published)", resultados[i]) is not None):
                            matching.append(i) #Guardamos todos los índices que cumplan con esta condición
                            
                    for match in matching: #Iteramos sobre los indices que guardamos "matches"
                        if re.search("(This guidance has been updated and replaced)|(This guidance has been replaced)", resultados[match], flags = re.IGNORECASE) is not None:
                            continue #Si la guía fue reemplazada continuamos con el siguiente "match"
                        
                        #Buscamos el código de la guía: TAXXX y lo concatenamos al link de la página de NICE
                        driver.get("https://www.nice.org.uk/guidance/" + re.search("TA\d+", resultados[match]).group().lower())
                        WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//*[contains(@href, 'chapter')]"))) #Buscamos y esperamos a que cargue el elemento chapter, que son las diferentes "pestañas" dentro de la guía
                        try: #Generalmente la pestaña 2 es la que tiene la información de precios, no obstante hay algunas excepciones
                            chapter = driver.find_element_by_xpath("//*[contains(@href, 'chapter/2')]").text #Verificamos el texto de la pestaña 2
                            if chapter == "2 Clinical need and practice":  #Si el texto es: "2 Clinical need and practice", hacer click en la pestaña 3 
                                driver.find_element_by_xpath("//*[contains(@href, 'chapter/3')]").click()
                    
                            else: #En cualquier otro caso hacer click en la pestaña 2
                                driver.find_element_by_xpath("//*[contains(@href, 'chapter/2')]").click()
                    
                            try: #Esperar a que cargue la información de precios, puede que no este disponible y en este caso hará un timeout
                                WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//p[contains(.,'£')]"))) #Los precios siempre tendrán un signo de libra esterlina
                                precios = [x.text for x in driver.find_elements_by_xpath("//p[contains(.,'£')]")] #Crear una lista con todos los precios encontrados (a veces la guía puede incluir más de un medicamento)
                                for precio in precios: 
                                    if (re.search(medicamento, precio, flags = re.IGNORECASE) is not None and len(precios)>1) or (len(precios)==1): #Sólo nos interesa el precio del medicamento que inicialmente estabamos buscando
                                        nice1 = pd.DataFrame([[medicamento, precio]], columns = ["PA","Resultado"]) #Crear el df auxiliar
                                        oraciones = re.split("[.]\s", nice1["Resultado"][0]) #Separar las oraciones dentro del texto
                                        nice1 = pd.DataFrame(np.repeat(nice1.values,len(oraciones),axis=0), columns = ["PA","Resultado"]) #Crear tantas filas como oraciones
                                        nice1["Medicamento"] = ""                            
                                        for z in nice1.index: #Iteramos sobre las filas
                                            nice1["Medicamento"][z] = oraciones[z] #Asignamos a cada fila una oración 
                                        nice = nice.append(nice1) #Combinar el df auxiliar con el df principal
                                
                                break #Dejar de iterar sobre los "matches" pues sólo nos interesa la guía más reciente (igual no es que haya muchas...)
                            
                            except TimeoutException: #Si los precios no están disponibles continuar con el siguiente "match"
                                continue
                        
                        except NoSuchElementException: #Si la guía no tiene una pestaña 2 continuar con el siguiente "match"
                            continue 

                except TimeoutException: #En caso de que la página muera, reiniciamos la búsqueda
                    driver.close() #Cerrar el navegador
                    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions) #Abrir uno nuevo
                    continue #Continuar con el medicamento en el que ibamos
        
                break #Si todo sale bien, salir del while infinito

        nice.reset_index(drop = True, inplace = True) #Limpiar los indices del df
        for ind in nice.index: #Borrar precios que NO son de interés
            if nice["Medicamento"][ind].find("£")==-1:
                nice.drop(ind, inplace = True)
            elif re.search("annual cost|kg|m2|wastage", nice["Medicamento"][ind]) is not None: #Costos por m2, kg, anuales, desperdicio, etc los borramos
                nice.drop(ind, inplace = True)
                
            elif re.search("\AThe list price of \w+", nice["Medicamento"][ind]) is not None: #Precios de otros de medicamentos también los borramos
                if re.split("\s", re.search("\AThe list price of \w+", nice["Medicamento"][ind]).group())[-1].upper() != nice["PA"][ind]:
                    nice.drop(ind, inplace = True)
                            
        nice.reset_index(drop = True, inplace = True) #Limpiar los indices del df
        nice.to_excel("nice.xlsx", index = False) #Exportar los resultados de la consulta a excel
        
############################################################################################################################################################################################
    elif url["PAÍS"][ind]=="mex" and url["INCLUIR"][ind]=="X": #IMSS (MEX) -----------------------------------------------------------------------------------------------------------------
        mex = pd.DataFrame() ###############################################################################################################################################################
        for medicamento in medicamentos_esp: #Iterar sobre los medicamentos
            while True: #Crear un while infinito por si la consulta se cae
                try:
                    driver.get(url["URL"][ind]) #Ingresar a la página del IMSS
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.ID, "q_compro_desc"))) #Esperar a que cargue la página
                    driver.find_element_by_id("search_prod").click() #Seleccionar la pestaña de búsqueda por medicamento

                    barra= driver.find_element_by_id("q_prod") #Buscar la barra de búsqueda
                    barra.clear() #Limpiar el texto de la barra de búsqueda
                    barra.send_keys(medicamento) #Ingresar el medicamento a consultar
                    time.sleep(3) #Esperar a que carguen los resultados

                    resultado_busqueda = driver.find_element_by_xpath('//*[@id="div_resultados_prod"]').text #Guardar el número de resultados
                    if "0 resultados" in resultado_busqueda: #Si la búsqueda tiene cero resultados continuar con el siguiente medicamento 
                        break
                
                    #Si la búsqueda retorna resultados hacer lo siguiente
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@href , 'imsscomproprod')]")))
                    num_resultados = driver.find_elements_by_xpath("//a[contains(@href , 'imsscomproprod')]") #Guardar el número de resultados
                    num_resultados = [x.get_attribute("href") + "&pr=" for x in num_resultados] #Obtener el hipervículo de cada resultado
                    ventana_1 = driver.window_handles[0] #Guardar la ventana actual 
                    driver.execute_script("window.open('');") #Crear una nueva pestaña
                    ventana_2 = driver.window_handles[1] 
                    driver.switch_to.window(ventana_2)
        
                    for resultado in num_resultados:
                        for periodo in [año_inicial, año_final]:
                            while True:
                                driver.get(resultado + periodo)
                                try: #La página se traba, reingresamos la URL hasta que cargue
                                    WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='divcontenidos']")))
                                    break
                                except TimeoutException:
                                    continue
                
                            datos = driver.find_element_by_xpath("//*[@id='divcontenidos']").text
                            if "No contiene datos" in datos:
                                continue
                            else:
                                producto = driver.find_element_by_xpath('//*[@class = "txtcajacompra"]').text
                                precios = [t.replace("\n", "") for t in re.findall("\n[$]\s*[\d,.]+\n", datos)]
                                mex1 = pd.DataFrame([[medicamento, producto, ""]], columns = ["Molécula","Producto","Precio"])
                                mex1 = pd.DataFrame(np.repeat(mex1.values,len(precios),axis=0), columns = ["Molécula","Producto","Precio"])
                                for z in mex1.index:
                                    mex1["Precio"][z] = precios[z]
                        
                                mex1.drop_duplicates(inplace=True)
                                mex = mex.append(mex1)
    
                    driver.close() #Cerrar las ventana 2
                    driver.switch_to.window(ventana_1) #Volver a la ventana principal
    
                except TimeoutException: #Si la página se cae, cerramos el navegador de Chrome y abrimos uno nuevo
                    driver.quit()
                    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions)
                    continue
        
                break

        mex.reset_index(drop = True, inplace = True)
        mex.to_excel("mex.xlsx",index=False)            
        
############################################################################################################################################################################################
    elif url["PAÍS"][ind]=="pan" and url["INCLUIR"][ind]=="X": #PANAMA COMPRA (PAN) --------------------------------------------------------------------------------------------------------
        pan = pd.DataFrame() #Crear un df principal ########################################################################################################################################
        driver.get(url["URL"][ind]) #Ingresar a la página
        WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, '(//button[contains(.,"Buscar")])[2]'))) #Esperar a que cargue
        for medicamento in medicamentos_esp: #Iterar sobre los medicamentos
            while True:
                try: 
                    driver.find_element_by_xpath('//*[contains(@name, "descripcion")]').clear() #limpiar la barra de búsqueda
                    driver.find_element_by_xpath('//*[contains(@name, "descripcion")]').send_keys(medicamento) #Ingresar el medicamento
                    desde = driver.find_element_by_xpath('(//*[@current-text = "Hoy"])[1]') #Buscar la barra con la fecha inicial
                    hasta = driver.find_element_by_xpath('(//*[@current-text = "Hoy"])[2]') #Buscar la barra con la fecha final

                    desde.clear() #Limpiar la barra con la fecha inicial
                    desde.send_keys(last_year_str.replace("/","-")) #Ingresar la fecha inicial
                    hasta.clear() #limpiar la barra con la fecha final
                    hasta.send_keys(today_str.replace("/","-")) #Ingresar la fecha final

                    tipo_compra = driver.find_element_by_xpath('//select[contains(@id, "tcompra")]') #Buscar el tipo de compra
                    Select(tipo_compra).select_by_index(11) #Seleccionar Licitaciones públicas

                    estado = driver.find_element_by_xpath("//select[contains(@name,'estado')]") #Buscar el estado de la licitación
                    Select(estado).select_by_visible_text("Adjudicado") #Seleccionar "Adjudicado"
                    
                    buscar = driver.find_element_by_xpath('(//button[contains(.,"Buscar")])[2]') 
                    buscar.click() #Hacer click en el botón Buscar
                    #Esperar a que carguen los resultados
                    WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//*[@id='toTopBA']")))
                    while True: #La búsqueda finaliza cuando aparece un número en este elemento, crear una esper dinámica
                        resultados = driver.find_element_by_xpath("//*[@id='toTopBA']").text #Obtener el texto
                        if re.search("\d+", resultados) is not None: 
                            break #Cuando aparezca un número, salir del while
                        else: #De lo contrario esperar 1 segundo y volver a iterar
                            time.sleep(1)
                            continue
                        
                    if re.search("\d+", resultados).group() == "0": #Si la búsqueda arroja cero resultados, continuar con el siguiente medicamento
                        break
            
                    pagina = driver.find_element_by_xpath('//select[contains(@id, "numPerPage")]')
                    Select(pagina).select_by_visible_text("50") #Desplegar 50 resultados
                    time.sleep(3) #Esperar 3 segundos a que cargue
                
                    licitaciones = driver.find_elements_by_xpath('//*[contains(@href,"NumLc")]') #buscar los links de los medicamentos
                    licitaciones = [x.get_attribute("href") for x in licitaciones] #Convertir a texto
                    ventana_1 = driver.window_handles[0] #Guardar la ventana actual 
                    driver.execute_script("window.open('');") #Crear una nueva pestaña 
                    ventana_2 = driver.window_handles[1] #Guardar la pestana como ventana 2
                    driver.switch_to.window(ventana_2) #Cambiar a la ventana 2
        
                    for licitacion in licitaciones: #Iterar sobre las iteraciones
                        driver.get(licitacion) #Ingresar el link de la licitación
                        #Esperar a que carguen los elementos
                        WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//*[contains(@ng-if, 'especificaciones')]")))
                        #Guardar el número de columnas
                        columns = len(driver.find_elements_by_xpath("//*[contains(@ng-if, 'especificaciones')]//table//th"))
                        #Guardar el número de filas (incluye encabezados)
                        rows = len(driver.find_elements_by_xpath("//*[contains(@ng-if, 'especificaciones')]//table//tr"))
                        headers = [] #Crear una lista vacía para guardar los encabezados
                        pan1 = pd.DataFrame() #Crear un df auxiliar
                        #Llenar el df auxiliar
                        for i in range(1, rows): #Iterar hasta rows, pues tiene encabezados
                            for j in range(1, columns+1):
                                if i == 1: #Cuando i = 1, llenar la lista con los encabezados 
                                    header = driver.find_element_by_xpath('//*[contains(@ng-if, "especificaciones")]//table//th[' + str(j) + ']').text
                                    headers.append(header)
                                pan1.loc[i,j] = driver.find_element_by_xpath("//*[contains(@ng-if, 'especificaciones')]//table//tr["+ str(i) + "]/td["+ str(j) + "]").text
        
                        pan1.columns = headers #Asignar encabezados al df auxiliar
                        filas = driver.find_elements_by_xpath('//*[contains(@ng-if, "aviso")]//table//tr/td[2]') #Iterar sobre las filas de esta tabla para encontrar el precio de referencia
                        filas = [x.text for x in filas] #Guardar el texto de cada elemento
                        precio = [x for x in filas if re.search("B/.\s[\d,.]+", x) is not None] #Filtrar el precio de la lista anterior
                        pan1["Precio Referencia"] = precio[0] #Asignar el precio a la columna Precio Referencia 
                        pan1["Medicamento"] = medicamento #Asignar el medicamento a la columna medicamento
                        pan = pan.append(pan1) #Combinar el df prinicpal y el df auxiliar
                        pan.drop_duplicates(inplace = True) #Borrar duplicados
           
                    driver.close() #Cerrar la ventana 2
                    driver.switch_to.window(ventana_1) #Volver a la ventana principal

                except TimeoutException: #Si el navegador falla, cerrar y abrir uno nuevo
                    driver.quit() #Cerrar todas las pestañas
                    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions) #Crear una nueva ventana
                    continue #Continuar iterando
    
                break #Si todo sale bien, salir del while infinito

        pan.reset_index(drop = True, inplace = True) #Resetear el ínidice del df prinicpal
        pan.to_excel("pan.xlsx",index=False) #Exportar los resultados de la consulta a Excel            
        
############################################################################################################################################################################################
    elif url["PAÍS"][ind]=="ecu_sp" and url["INCLUIR"][ind]=="X": #SISTEMA DE COMPRAS PÚBLICAS (ECU) --------------------------------------------------------------------------------------------------------
        iteracion_1 = True
        ecu_sp = pd.DataFrame() #Crear un df principal ########################################################################################################################################
        for medicamento in medicamentos_esp: #Iterar sobre la lista de medicamentos a consultar
            while True: #Crear un bucle infinito en caso de que la página se caiga
                try:
                    driver.get(url["URL"][ind]) #Ingresar a la página
                    if iteracion_1 == True: #Aceptar los cookies de la página en dado caso que aparezca el mensaje en la iteración 1
                        try: #Esperar a que carguen los elementos
                            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'Aceptar')]")))
                            driver.find_element_by_xpath("//button[contains(.,'Aceptar')]").click()

                        except TimeoutException: #De lo contrario dejar pasar el error
                            pass
                    
                    iteracion_1 = False
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.NAME, "btnBuscar"))) #Esperar a que carguen los elementos
                    driver.find_element_by_name("txtPalabrasClaves").send_keys(medicamento) #Ingresar el medicamento  en la barra de búsqueda 
                    inicial_default = date.today() - relativedelta(months=+6) #Fecha inicial que aparece por defecto en la página (fecha de hoy menos 6 meses)
                    fecha_inicial = last_year #Empezaremos a iterar desde la fecha inicial igual a last_year
                    fecha_final = "" #Declaramos la vriable fecha inicial con un valor vacío
                    #########################################################################################################
                    #-LA PÁGINA SOLO PERMITE UN INTERVALO ENTREE FECHAS MÁXIMO DE 6 MESES, DE LO CONTRARIO ARROJA UN ERROR!!!
                    #########################################################################################################
                    while fecha_final !=today: #Iterar hasta que la fecha final sea igual a today (fecha escogida por el usuario)
                        ###############################################################################
                        #Ajustar la fecha inicial -----------------------------------------------------
                        ###############################################################################
                        driver.find_element_by_name("ico_f_inicio").click() #Damos click en el ícono de fecha de inicio
                        diferencia_año_inicial = abs(inicial_default.year - fecha_inicial.year) #Guardamos la diferencia entre el año inicial default y el año al que queremos llegar
                        diferencia_mes_inicial = abs(inicial_default.month - fecha_inicial.month) #Guardamos la diferencia entre el mes inicial default y el mes al que queremos llegar

                        for i in range(1, diferencia_año_inicial + 1): #Ajustar el año de la fecha inicial
                            if fecha_inicial.year < inicial_default.year: #Hacer click (atras o adelante) proporcional a la diferencia de años
                                driver.find_element_by_xpath("//td[contains(., '«')]").click()
                            else:
                                driver.find_element_by_xpath("//td[contains(., '»')]").click()
   
                        for i in range(1, diferencia_mes_inicial + 1): #Ajustar el mes de la fecha inicial
                            if fecha_inicial.month < inicial_default.month: #Hacer click (atras o adelante) proporcional a la diferencia de meses
                                driver.find_element_by_xpath("//td[contains(., '‹')]").click()
                            else:
                                driver.find_element_by_xpath("//td[contains(., '›')]").click()

                        dias = driver.find_elements_by_xpath('//*[@class = "day" or @class = "day weekend"]') #Ajustar el dia de la fecha inicial
                        for dia in dias: #Seleccionar el día de la fecha inicial
                            if dia.text == str(last_year.day):
                                dia.click()
    
                        ###############################################################################
                        #Ajustar la fecha final -------------------------------------------------------
                        ###############################################################################
                        driver.find_element_by_name("ico_f_fin").click() #Damos click en el ícono de la fecha de fin
                        fecha_final = min(fecha_inicial + relativedelta(months=+6), today) #Fecha final es igual al mínimo entre today (fecha de fin definida por el usuario y la fecha inicial + 6 meses)
                        diferencia_año_final = abs(date.today().year - fecha_final.year)
                        diferencia_mes_final = abs(date.today().month - fecha_final.month)

                        for i in range(1, diferencia_año_final + 1): #Ajustar el año de la fecha final
                            if fecha_final.year < date.today().year: #Hacer click (atras o adelante) proporcional a la diferencia de años
                                driver.find_element_by_xpath("(//td[contains(., '«')])[2]").click()
                            else:
                                driver.find_element_by_xpath("(//td[contains(., '»')])[2]").click()
   
                        for i in range(1, diferencia_mes_final + 1): #Ajustar el mes de la fecha final
                            if fecha_final.month < date.today().month: #Hacer click (atras o adelante) proporcional a la diferencia de meses
                                driver.find_element_by_xpath("(//td[contains(., '‹')])[2]").click()
                            else:
                                driver.find_element_by_xpath("(//td[contains(., '›')])[2]").click()

                        dias = driver.find_elements_by_xpath('//*[@class = "day" or @class = "day weekend"]') #Ajustar el dia de la fecha final
                        for dia in dias: #Seleccionar el día de la fecha final
                            if dia.text == str(last_year.day):
                                dia.click()

                        texto = driver.find_element_by_xpath('//input[@name = "image"]') #Buscar la barra de texto que contiene el CAPTCHA
                        driver.execute_script('arguments[0].scrollIntoView()', texto) #Hacemos scroll para bajar al texto
                        texto.click() #Hacer click en la barra de texto que contiene el CAPTCHA
                        texto.send_keys("") #Ingresar un valor vacío
                        
                        while True: #Crear una espera infinita hasta que el usuario digite el CAPTCHA correspondiente
                            try:
                                WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.ID, "divProcesos"))) #Cuando aparece el elemento divProcesos quiere decir que ya se introdujo el CAPCTHA
                                if driver.find_element_by_id("divProcesos").text == "Captcha Incorrecto": #Si el CAPTCHA es incorrecto, continuar esperando
                                    texto.send_keys("") #Ingresar un valor vacío
                                    continue
                                else:
                                    break #Si no sale ningún mensaje salir del bucle infinito
    
                            except TimeoutException:
                                time.sleep(1) #De lo contrario continuar esperando
                                continue

                        #Si no existen procesos para esa búsqueda continuar iterando sobre las fechas
                        if driver.find_element_by_id("divProcesos").text == "No existen procesos para la consulta ingresada":
                            driver.find_element_by_xpath("//*[@name = 'btnLimpiar']").click() #Limpiar los datos de la consulta
                            fecha_inicial = fecha_final #Fecha inicial se convierte ahora en la fecha final
                            driver.find_element_by_name("txtPalabrasClaves").send_keys(medicamento) #Ingresar el medicamento en la barra de búsqueda 
                            continue #Continuar iterando sobre las fechas
   
                        #Si se despliegan los resultados iterar sobre las filas que contengan el valor de la compra ($) 
                        resultados = driver.find_elements_by_xpath("//tr[contains(.,'$')]//td[1]/a")
                        resultados = [x.get_attribute('href') for x in resultados] #Guardar los hipervínculos de las licitaciones de interés
                        descripciones = driver.find_elements_by_xpath("//tr[contains(.,'$')]//td[3]") #Guardar la descripción de las licitaciones de interés
                        descripciones = [x.text for x in descripciones] #Guardar las descricpiones como texto
                        result_desc = list(zip(resultados,descripciones)) #Zippear dos listas en un conjunto de tuples, donde el primer elemento es el hipervínculo y el segundo es la descripción 
    
                        ventana_1 = driver.window_handles[0] #Guardar la ventana actual 
                        driver.execute_script("window.open('');") #Abrir una nueva pestaña
                        ventana_2 = driver.window_handles[1] #Nombrar la ventana auxiliar como ventana_2
                        driver.switch_to.window(ventana_2) #Cambiar a la ventana auxiliar
        
                        for link in result_desc: #Iterar sobre la lista zippeada que contiene los links[0] y descripciones[1]
                            driver.get(link[0]) #Ingresar el link
                            ecu_sp1 = pd.DataFrame() #Crear un df auxiliar
                            WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.ID, "tab1"))) #Esperar a que los tabs sean clickeables
                            tabs = driver.find_elements_by_xpath("//*[contains(@id , 'tab') and not(contains(@id,'menu'))]") #Buscar todos los tabs disponibles
                            [tab.click() for tab in tabs if tab.text == "Productos"] #Hacer click sobre el tab que tenga la información de los productos
                            
                            WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, '//*[@id = "rounded-corner"]//th'))) #Esperar a que cargue la tabla con la información correspondiente
                            headers = driver.find_elements_by_xpath('//*[@id = "rounded-corner"]//th') #Guardar los encabezados de la tabla
                            headers = [x.text for x in headers] #Convertir los encabezados a texto
                            filas = driver.find_elements_by_xpath("//*[@id = 'rounded-corner']//td[1]") #Buscar el total de filas de la tabla
                            for i in range(1, len(filas)+1): #Iterar sobre el total de las filas
                                for j in range(1, len(headers)+1): #Iterar sobre el total de las columnas
                                        #Sólo nos interesan las filas que en la primera columna tengan un código númerico
                                        if re.search("[a-zA-Z]",driver.find_element_by_xpath("//*[@id = 'rounded-corner']//tr[" + str(i) + "]/td[1]").text) is None:
                                            dato = driver.find_element_by_xpath("//*[@id = 'rounded-corner']//tr[" + str(i) + "]/td[" + str(j)+ "]")
                                            ecu_sp1.loc[i,j] = dato.text
                                     
                            ecu_sp1.columns = headers #Asignar los encabezados al df auxiliar
                            ecu_sp1["Descripción"] = link[1] #Llenar la columna descripción con el segundo elemento del tuple result_desc
                            ecu_sp1["Medicamento"] = medicamento #Llenar la columna medicamento con el nombre del medicamento que ingresamos en la barra de búsqueda
                            ecu_sp = ecu_sp.append(ecu_sp1) #Combinar el df principal con el df auxiliar
        
                        driver.close() #Al terminar de iterar sobre todos los links disponibles, cerrar la ventana auxiliar
                        driver.switch_to.window(ventana_1) #Volver a la ventana principal
                        driver.find_element_by_xpath("//*[@name = 'btnLimpiar']").click() #Dar click en el botón limpiar 
                        fecha_inicial = fecha_final #Ahora la fecha inicial será la fecha final
                        driver.find_element_by_name("txtPalabrasClaves").send_keys(medicamento) #Ingresar el medicamento en la barra de búsqueda 

                except TimeoutException: #Si la página se cae abrir una ventana nueva
                    driver.quit() #Cerrar todos los navegadores abiertos
                    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions) #Abrir un navegador nuevo
                    continue #Continuar iterando
            
                break #Si todo sale bien salir del bucle infinito    
                                                
        ecu_sp.reset_index(drop = True, inplace = True) #Resetear el ínidice del df prinicpal
        #Dentro de la consulta podemos "arrastrar" otros medicamentos que no eran de interés, pero que aparecen al tener un nombre muy parecido al que estabamos búscando 
        for ind in ecu_sp.index: #Borrar estos medicamentos usando la siguiente expresión regular
            if re.search("[a-zA-Z]" + ecu_sp["Medicamento"][ind], ecu_sp["Descripción"][ind], flags = re.IGNORECASE)  is not None: 
                ecu_sp.drop(ind, inplace = True)

        ecu_sp.reset_index(drop = True, inplace = True) #Volver a resetear el ínidice del df prinicpal
        ecu_sp.to_excel("ecu_sp.xlsx",index=False) #Exportar los resultados de la consulta a Excel            
        
############################################################################################################################################################################################
    elif url["PAÍS"][ind]=="bps" and url["INCLUIR"][ind]=="X": #BPS (BRA) --------------------------------------------------------------------------------------------------------
        bps = pd.DataFrame() #Crear un df prinicipal donde se consolidaran los resultados de todas las búsquedas
        k_inicial = 0 #Creamos un k-inicial en caso de que se caíga la página, se retomara en el medicamento que ibamos
        while True: #Creamos un bucle inifinito en caso de que se caíga la página
            try:
                driver.get(url["URL"][ind]) #Ingresar a la página del banco de precios en salud (BPS)
                WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//input[contains(@id, 'Acessar')]"))) #Esperar a que carguen los elementos
                driver.find_element_by_xpath('//input[contains(@id, "txtEmail1")]').send_keys("jdiazpar@its.jnj.com") #Ingresar un usuario (usare el mi correo de J&J)
                driver.find_element_by_xpath('//input[contains(@id, "Acessar")]').click() #Accesar a la página

                WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//p[contains(.,'Relatórios')]"))) #Esperar a que carguen los elementos
                element_to_hover_over = driver.find_element_by_xpath("//p[contains(.,'Relatórios')]") #Buscar la sección de Relatorios
                hover = ActionChains(driver).move_to_element(element_to_hover_over) #Hacer hover sobre la sección de relatorios
                hover.perform() 
                driver.find_element_by_xpath('//a[contains(.,"Geral")]').click() #Hacer click en la sección de Geral
                                    
                for k in range(k_inicial, len(medicamentos_por)): #Empezar a iterar sobre los medicamentos, pero desde k_inicial (por si la página se cae)
                    WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//input[@value = 'Limpar']"))) #Esperar a que carguen los elementos
                    driver.find_element_by_xpath("//input[@value = 'Limpar']").click() #Limpiar todos los elementos de búsqueda anteriores
                    #Seleccionar los checkbox que están en la guía
                    driver.find_element_by_xpath('//input[contains(@id, "checkPeriodo")]').click() #PERIODO
                    driver.find_element_by_id("formItensBPS:dados").click()  #BASE SIASG
                    driver.find_element_by_xpath('//input[contains(@id, "checkInstituicao")]').click()  #INSTITUIÇÃO
                    #Ingresar en la barra de búsqueda los medicamentos con el nombre en portugues (eliminamos el +)
                    driver.find_element_by_xpath('//input[contains(@id , "descricaoItem")]').send_keys(medicamentos_por[k].replace(" + "," "))
                    time.sleep(5) #Esperamos 5 segundos a que carguen las posibles opciones
                    resultados = len(driver.find_elements_by_xpath("//*[@nowrap]")) #Guardamos la cantidad de elementos disponibles

                    if resultados == 0: #Si la búsqueda no arrojo resultados, limpiamos la barra de búsqueda y continuamos con el siguiente medicamento
                        driver.find_element_by_xpath('//input[contains(@id , "descricaoItem")]').clear()
                        continue
    
                    for resultado in range(1, resultados+1): #Si la búsqueda arroja resultados ejecutamos el siguiente código
                        if resultado > 1: #Si la búsqueda arroja más de un resultado llenar la barra de búsqueda nuevamente (OJO: Si se presiona buscar y hay algo escrito en la barra de búsqueda, la página se daña o no arroja ningún resultado!!!!)
                            driver.find_element_by_xpath('//input[contains(@id , "descricaoItem")]').send_keys(medicamentos_por[k].replace(" + "," "))
                                               
                        while True: #Creamos una espera dinámica, hasta que sea posible clickear en el elemento
                            try:
                                driver.find_element_by_xpath("(//*[@nowrap])[" + str(resultado) + "]").click()
                                break
            
                            except (NoSuchElementException, ElementNotInteractableException):
                                time.sleep(1)
                                continue
            
                        while True: #Creamos una espera dinámica hasta que cargue el código BR (OJO: SIN ESTE CÓDIGO lA CONSULTA ARROJA ERROR!)
                            try:
                                if driver.find_element_by_xpath('//input[contains(@id,"codigo2")]').get_attribute('value') != "":
                                    break
                            
                            except StaleElementReferenceException:
                                time.sleep(1)
                                continue
                                
                        driver.find_element_by_xpath("//input[@value = 'S']").click() #Medicamento génerico: SIM 
                        driver.find_element_by_xpath("//input[@value = 'N']").click() #Medicamento génerico: NO
                        driver.find_element_by_xpath("//input[@value = 'Adicionar']").click() #Adicionar el medicamento 
                                        
                    #Hacemos la búsqueda uno a uno por medicamento, porque si buscamos muchos a la vez, es muy probable que la página se dañe
                    buscar = driver.find_element_by_xpath("//input[@value = 'Pesquisar']") #Buscar el botón de Pesquisar
                    driver.execute_script('arguments[0].scrollIntoView()', buscar) #Bajar hasta el botón
                    buscar.click() #Dar click en Buscar
                        
                    try: #Esperar a que carguen los resultados de la búsqueda
                        WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, '//input[@value= "Gerar Planilha"]'))) #Esperar a que carguen los elementos
                    except TimeoutException: #Si hace Timeout contiuamos con el siguiente medicamento
                        driver.find_element_by_xpath("//input[@value = 'Limpar']").click()  
                        continue
                
                    for i in range(1,3): #Exportamos dos archivos de Excel (uno se llama Geral y el otro SIASG)
                        driver.find_element_by_xpath('(//input[@value= "Gerar Planilha"])['+ str(i) +']').click()
                        download_wait(directory = carpeta, timeout = 60*10)
                        
                    try: #SIASG
                        bps_siasg = skip_rows(pd.read_csv("Geral_SIASG.csv", sep=';', encoding= 'unicode_escape')) #Convertimos el archivo SIASG a un df
                        bps_siasg["Base"] = "SIASG" #Creamos una columna auxiliar que indica a cual de los dos pertenecía
                    except:
                        bps_siasg = pd.DataFrame() #Si la búsqueda no tenía resultados en SIASG, se crea un df vacío
    
                    try: #GERAL
                        bps_geral = skip_rows(pd.read_csv("Geral_BPS.csv", sep=';', encoding= 'unicode_escape')) #Convertimos el archivo Geral a un df
                        bps_geral["Base"] = "GERAL" #Creamos una columna auxiliar que indica a cual de los dos pertenecía
                    except:
                        bps_geral = pd.DataFrame() #Si la búsqueda no tenía resultados en GERAL, se crea un df vacío

                    borrar(["Geral_SIASG.csv", "Geral_BPS.csv"]) #Borramos los archivos que se pudieron guardar, para que no se acumulen en el directorio de trabajo                    
                    bps = bps.append(bps_siasg) #Combinar SIASG con el df principal
                    bps = bps.append(bps_geral) #Combinar GERAL con el df principal
                       
            except TimeoutException: #Si cae la página, crear un navegador nuevo
                driver.close() #Cerrar el driver
                driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions) #Abrir un driver nuevo
                k_inicial = k #Guardar el índice del medicamento en el que ibamos
                continue #Continuar
            
            break #Si todo sale bien, salir del bucle infinito

        bps.to_excel("bps.xlsx", index = False) #Exportar el df final a Excel


#############################################################################################################################################################################
#--------------------- Detener el script si no se encuentran todos los archivos necesarios e imprimir un mensaje de error ---------------------------------------------------

#completo = 0
#faltantes = ""
#parameters = ["ecu_sp","ecu[.]","eeuu","aus[.]","aus1","esp[.]","esp1","bra[.]","bra1","bps","nice","nhs[.]pdf","uk","can[.]pdf","arg","per","fra","por","pan","mex","chi"]
#for param in parameters:
    #for filename in os.listdir(carpeta):
        #if re.search(param,filename): 
            #completo = completo + 1
        #else:
            #faltantes = faltantes + ", " + param[:3]
            
#if len(parameters) != completo:
    #print("No se puede ejecutar el algoritmo, por favor descargar los siguientes archivo(s): " + faltantes)
    #exit()    
    
############################################################################################################################################################################

#----------------------------------------------------------------------MONEDAS Y TRM---------------------------------------------------------------------------------------------------------------

#############################################################################################################################################################################

#--------------------------------------------------------------------------- TRM---------------------------------------------------------------------------------------------------------------

if url.iloc[-1,-1] == "X":
    #Borrar versiones viejas
    borrar(["1.1.1.TCM_Serie histórica IQY.xlsx"])
    while True:
        try:
            #Paso 1: Ingresar a la página del Banco de la República que contiene la info. histórica de la TRM
            driver.get(url['URL'].iloc[-1])
            WebDriverWait(driver, 120).until(EC.element_to_be_clickable((By.LINK_TEXT, 'Serie histórica completa (desde 27/11/1991)')))
            driver.find_element_by_link_text('Serie histórica completa (desde 27/11/1991)').click()

            #Paso 2: Esperar máximo 30 segundos a que termine de descargar el archivo antes de usarlo
            while True:
                time.sleep(1)
                if os.path.exists("1.1.1.TCM_Serie histórica IQY.xlsx")==True:
                    break
    
        except TimeoutException:
            driver.close()
            driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions)
            continue

        break
        
    #Paso 3: Importar la base de datos de Excel como un data frame y limpiarla para calcular el promedio MAT
    trm = skip_rows(pd.read_excel("1.1.1.TCM_Serie histórica IQY.xlsx")).reset_index(drop = True).dropna() 
    trm["Fecha (dd/mm/aaaa)"] = trm["Fecha (dd/mm/aaaa)"].dt.date
    #Eliminar la data que está por fuera de la ventana de tiempo de interés
    trm = trm.iloc[trm[trm["Fecha (dd/mm/aaaa)"]==last_year].index.values[0]:trm[trm["Fecha (dd/mm/aaaa)"]==today].index.values[0]+1,:]

    #Paso 4: Anexarl el promedio como una fila adicional al final
    trm = trm.append({"Fecha (dd/mm/aaaa)":"Promedio", "Tasa de cambio representativa del mercado (TRM)":trm["Tasa de cambio representativa del mercado (TRM)"].mean(axis = 0)} , ignore_index = True)

#----------------------------------------------------------------------TASAS DE CAMBIO-----------------------------------------------------------------------------------------------------------

    while True:
        try:
            #Paso 1:Ingresar a la página del Banco de la República que contiene la info. histórica de las divisas extranjeras 
            driver.get("https://totoro.banrep.gov.co/analytics/saw.dll?Go&Action=prompt&path=%2Fshared%2fSeries%20Estad%c3%adsticas_T%2F1.%20Monedas%20disponibles%2F1.2.TCM_Serie%20para%20un%20rango%20de%20fechas%20dado&Options=rdf&lang=es&NQUser=publico&NQPassword=publico123")

            #Paso 2: Seleccionar las fechas
            WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//input[starts-with(@name, 'saw_')]")))
            fecha_inicial = driver.find_element_by_xpath("//input[starts-with(@name, 'saw_')]")
            fecha_inicial.clear()
            fecha_inicial.send_keys(last_year_str)

            #Siguiente
            driver.find_element_by_id("next").click()

            #Paso 3: Seleccionar las divisas de interés
            #Cada checkbox tiene un id. descargamos sólamente los necesarios para a agilizar el proceso
            id = [6,8,9,15,22,25,28,29,31,32,35] #ID de las monedas respecitvamente [NOK, AUD, CAD, EUR, GBP, PEN, ARS, CLP, MXN, UYU, BRL]

            #Seleccionar monedas:
            for i in id: 
                i = str(i)
                WebDriverWait(driver, 40).until(EC.element_to_be_clickable((By.XPATH, "//input[contains(@id, '_o" + i + "')]")))
                driver.find_element_by_xpath("//input[contains(@id, '_o" + i + "')]").click()
    
            #Siguiente
            driver.find_element_by_id("gobtn").click()

            #Esperar 15 seg. a que cargue la página
            WebDriverWait(driver, 40).until(EC.visibility_of_element_located((By.XPATH, "//select[contains(@id, '_6_1')]")))

            #Paso 4: Exportar la tabla con las tasas de venta a un data frame
            #A veces BanRep se demora en cargar la tasas con la fecha actual y por ende solo está disponible la info hasta el día anterior
            try:
                fx_venta = tasas_de_cambio(source = driver.page_source, t0 = last_year_str, t1 = today_str)
            except:
                fx_venta = tasas_de_cambio(source = driver.page_source, t0 = last_year_str, t1 = yesterday_str)
    
            #Paso 5: Cambiar a las tasas de compra
            tipo_de_tasa = driver.find_element_by_xpath("//select[contains(@id, '_6_1')]") 
            Select(tipo_de_tasa).select_by_visible_text("Compra")
            time.sleep(5) #Esperar a que carguen los resultados
    
            #Paso 6: Exportar la tabla con las tasas de compra a un data frame
            #A veces BanRep se demora en cargar la tasas con la fecha actual y por ende solo está disponible la info hasta el día anterior
            try:
                fx_compra = tasas_de_cambio(source = driver.page_source, t0 = last_year_str, t1 = today_str)
            except:
                fx_compra = tasas_de_cambio(source = driver.page_source, t0 = last_year_str, t1 = yesterday_str)
    
            #Paso 7: Combinar ambos data frame (compra y venta) y calcular la tasa media o punto intermedio

            #Unir la data de compra y venta
            fx = fx_venta.append(fx_compra).reset_index(drop = True)

            #Agrupar la data por fecha calculando el promedio 
            fx_media = fx.groupby(['FECHA'])[monedas].mean().reset_index()

            #Paso 8: #Anexar el promedio de cada divisa como una fila adicional

            #Calcular el promedio de cada columna
            promedios = fx_media[monedas].mean(axis = 0).reset_index()

            #Trasponer, limpiar y anexar los promedio como una fila adicional
            promedios = promedios.transpose()
            promedios  = promedios.iloc[1:].rename(columns = promedios.iloc[0])
            promedios["FECHA"] = "Promedio"

            fx_media = fx_media.append(promedios).reset_index(drop = True)
    
        except TimeoutException:
            driver.close()
            driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=chromeOptions)
            continue

        break

#---------------------------------------------------------------------Finalizar la consulta----------------------------------------------------------------------------------------------------

driver.close() #Cerrar el navegador de Chrome después de haber hecho todas las consultas
borrar_empty() #Borrar todos los archivos vacíos después de haber hecho todas las consultas

##############################################################################################################################################################################################

# ------------------------------------------ Limpiar cada Data Frame para calcular métricas de interés --------------------------------------------------------------------------------------

###########################################################################################################################################################################################

#------------------------------------------------------------------------PANAMÁ----------------------------------------------------------------------------------------------------------------------

############################################################################################################################################################################################

if len(pan.index)>0:
    pan["PA"] = pan["Medicamento"].replace(traductor_esp) #Crear la columna PA
    pan = limpiar_df(df=pan, precio = "Precio Referencia", ff = "Descripción") #Limpiar el df
    #Asigna valores a la columna con la FF de acuerdo a las palabras definidas
    pan.loc[(pan["U. Medida"].str.contains("Comprimido",flags=re.IGNORECASE, regex=True)) & (pan["FF"]=="") ,"FF"] = "TAB"
    pan.loc[(pan["U. Medida"].str.contains("Ampolla|Vial",flags=re.IGNORECASE, regex=True)) & (pan["FF"]=="") ,"FF"] = "INJ"
    
    pan["Cantidad"] = pan["Cantidad"].astype(str).str.replace(",","").astype(int) #Ajustar la columna Cantidad
    pan["Precio Unitario"] = pan["Precio Referencia"] / pan["Cantidad"] #Calcular el precio unitario
    
    for ind in pan.index: #Iterar sobre el df
        if re.search("[\d.]+\s*(MG|MCG)", pan["Descripción"][ind], flags = re.IGNORECASE) is not None: #llenar la columna con las unidades mínimas de concentración  
            pan["UMC (mg)"][ind] = re.search("[\d.]+", re.search("[\d.]+\s*(MG|MCG)", pan["Descripción"][ind],flags = re.IGNORECASE).group()).group()
    
        #Medicamentos con XXMG/ML con ML totales en la columna Descripción
        if (re.search("/\s*ML", pan["Descripción"][ind], flags = re.IGNORECASE) is not None) and (re.search("[\d.]+\s*ML", pan["Descripción"][ind], flags = re.IGNORECASE) is not None):
            pan["Quantity"][ind] = re.search("[\d.]+", re.search("[\d.]+\s*ML", pan["Descripción"][ind], flags = re.IGNORECASE).group()).group()
    
        elif pan["PA"][ind] == "DARATUMUMAB": #Darzalex tiene un error no reporta ML totales, pero de acuerdo al precio parece ser un vial de 20 ML
            pan["Quantity"][ind] = "20"
            
        else: #Todos los demás medicamentos ya están en unidades de dispensación
            pan["Quantity"][ind] = "1"
            
        #Realizar la conversión de unidades de MCG a MG
        if re.search("[\d.]+\s*MCG", pan["Descripción"][ind],flags=re.IGNORECASE) is not None:
            pan["UMC (mg)"][ind] = float(pan["UMC (mg)"][ind]) / 1000

    ajustar_columnas(df=pan, columnas = ["UMC (mg)", "Quantity"]) #Ajustar las columnas            
    finalPan = final(df= pan, precio = "Precio Unitario", bd = "PANAMA COMPRA (PAN)") #Crear el df final

else:
    finalPan = pd.DataFrame(columns = ["PA","PANAMA COMPRA (PAN)"])

###########################################################################################################################################################################################

#------------------------------------------------------------------------CHILE----------------------------------------------------------------------------------------------------------------------

############################################################################################################################################################################################

if len(chi.index)>0: #Revisar que el df no este vacío
    #La búsqueda arrastra muchos medicamentos que nada que ver, utiilzamos la siguiente línea para guardar sólo los medicamentos que hayan coincidido con nuestra búsqueda
    chi = chi[chi['Esp. Comprador'].str.contains("|".join(chi['PA']))]
    chi['Esp. Proveedor'] = chi['Esp. Proveedor'].apply(lambda x: x + " ") #Creamos un espacio extra al final, para mejorar la clasificación de las FF
    chi.reset_index(inplace=True, drop = True)

    chi = limpiar_df(df = chi, precio = "Precio Unit.", ff = "Esp. Proveedor")

    #Asigna valores a la columna con la FF de acuerdo a las palabras definidas
    chi.loc[((chi["Medida"].str.contains("Comprimido",flags=re.IGNORECASE, regex=True))| (chi["Esp. Comprador"].str.contains("Comprimido",flags=re.IGNORECASE, regex=True))) & (chi["FF"]=="") ,"FF"] = "TAB"
    chi.loc[(chi["Medida"].str.contains("Ampolla",flags=re.IGNORECASE, regex=True)) & (chi["FF"]=="") ,"FF"] = "INJ"
    chi.loc[(chi["Esp. Comprador"].str.contains("INY",flags=re.IGNORECASE, regex=True)) & (chi["FF"]=="") ,"FF"] = "INJ"

    for ind in chi.index:
        #UMC (mg)
        if re.search("[\d,.]+\s*(MG|MCG)" , chi["Esp. Comprador"][ind], flags = re.IGNORECASE) is not None: 
            chi["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*(MG|MCG)" , chi["Esp. Comprador"][ind], flags = re.IGNORECASE).group()).group()
        
        #Quantity
        if re.search("[\d,.]+\s*ML" , chi["Esp. Comprador"][ind], flags = re.IGNORECASE) and re.search("/\s*ML" , chi["Esp. Comprador"][ind], flags = re.IGNORECASE) is not None: 
            chi["Quantity"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*ML" , chi["Esp. Comprador"][ind], flags = re.IGNORECASE).group()).group()
    
        else: #De lo contrario llenar cantidades con un 1
            chi["Quantity"][ind] = 1
            
        if re.search("MCG", chi["Esp. Comprador"][ind], flags = re.IGNORECASE) is not None: #Realizar la conversión de MCG a MG
            chi["UMC (mg)"][ind] = float(chi["UMC (mg)"][ind].replace(",","."))/1000
       
    ajustar_columnas(columnas=["UMC (mg)","Quantity"], df = chi)

    #Crear data frame final
    finalChi = final(df=chi, precio="Precio Unit.", bd = "CHILE COMPRA (CHI)")

else: #Si el data frame esta vacío crear el df frame final vacío
    finalChi = pd.DataFrame(columns = ["PA", "CHILE COMPRA (CHI)"])
    
###########################################################################################################################################################################################

#------------------------------------------------------------------------MEXICO----------------------------------------------------------------------------------------------------------------------

############################################################################################################################################################################################

if len(mex)>0: #verificar que la consulta haya tenido resultados
    mex["PA"] = mex["Molécula"].replace(traductor_esp) #Crear la columna PA

    for ind in mex.index: #Los medicamentos que buscabamos con dos prinicipios activos pueden quedar con un sólo PA, estos los borramos
        if mex["PA"][ind].find("/")!=-1 and len(re.findall("[\d,.]+\s*MG", mex["Producto"][ind], flags = re.IGNORECASE))!=2:
            mex.drop(ind, inplace = True)
        
        elif mex["PA"][ind].find("/")==-1 and len(re.findall("[\d,.]+\s*MG", mex["Producto"][ind], flags = re.IGNORECASE))>1:
            mex.drop(ind, inplace = True)
    
    
    mex = limpiar_df(df=mex, precio = "Precio", ff = "Producto") #Limpiar el df

    for ind in mex.index:
        if mex["PA"][ind].find("/")!=-1: #Buscar la máxima concentración de los medicamentos con dos principios activos
            mex["UMC (mg)"][ind] = max([float(y) for y in [re.search("[\d,.]+", x).group() for x in re.findall("[\d,.]+\s*MG", mex["Producto"][ind], flags = re.IGNORECASE)]])
            
        elif re.search("[\d,.]+\s*MG", mex["Producto"][ind], flags = re.IGNORECASE) is not None: #Buscar la concentración de los medicamentos con un principio activo
            mex["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*MG", mex["Producto"][ind]).group()).group()
    
        if re.search("CON\s*[\d,.]+", mex["Producto"][ind], flags = re.IGNORECASE) is not None: #llenar la columna "Quantity", siempre la cantidad está después de la palabra "CON"
            mex["Quantity"][ind] = re.search("[\d,.]+", re.search("CON\s*[\d,.]+", mex["Producto"][ind], flags = re.IGNORECASE).group()).group()
    
        else: #De lo contrario llenar con un valor de 1
            mex["Quantity"][ind] = "1"

    ajustar_columnas(columnas=["UMC (mg)","Quantity"], df = mex) #Ajustar las columnas
    finalMex = final(df = mex, precio = "Precio", bd = "IMSS (MEX)") #Crear el df final

else: #Si el data frame esta vacío crear el df frame final vacío
    finalMex = pd.DataFrame(columns = ["PA", "IMSS (MEX)"])

###########################################################################################################################################################################################

#--------------------------------------------------------------------PORTUGAL----------------------------------------------------------------------------------------------------------------------

############################################################################################################################################################################################

if len(por.index)>0: #Revisar que el df no este vacío
    #Crear la columna PA 
    por["PA"] = por["Substância Ativa/DCI"].str.upper().replace(traductor_por)

    #Limpiar el df
    por = limpiar_df(df=por, precio = "Preco" , ff = "Forma Farmacêutica")

    for ind in por.index:
        #Si el medicamento tiene dos principios activos, quedarse con el mayor
        if por["PA"][ind].find("/") !=-1:
            por["UMC (mg)"][ind] = max(float(re.search("[\d,.]+",k).group()) for k in re.findall("[\d,.]+\s*µg|[\d,.]+\s*mg", por["Dosagem"][ind]))
    
        #Obtener UMC (mg) de la columna Dosagem    
        elif re.search("[\d,.]+\s*mg|[\d,.]+\s*µg", por["Dosagem"][ind]) is not None:
            por["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*mg|[\d,.]+\s*µg" , por["Dosagem"][ind]).group()).group()
    
        #Obtener Quantity de la columna Presentacion
        if re.search("\d+\s*unidade", por["Presentacion"][ind]):
            por["Quantity"][ind] = re.search("\d+", re.search("\d+\s*unidade", por["Presentacion"][ind]).group()).group()
        
        #Medicamentos XXMG/ML con cantidades ML en la columa Presentacion
        if re.search("/\s*ml", por["Dosagem"][ind], flags = re.IGNORECASE) and (re.search("[\d,.]+\s*ml", por["Presentacion"][ind], flags = re.IGNORECASE)):
            por["Quantity"][ind] = float(por["Quantity"][ind]) * float(re.search("[\d,.]+", re.search("[\d,.]+\s*ml", por["Presentacion"][ind]).group()).group()) 
        
        #Medicamentos XXMG/DOSE con cantidades de dosis (dose) en la columna Presentacion
        elif re.search("/\s*dose", por["Dosagem"][ind], flags = re.IGNORECASE) and (re.search("\d+\s*dose", por["Presentacion"][ind], flags = re.IGNORECASE)):
            por["Quantity"][ind] = float(por["Quantity"][ind]) * float(re.search("[\d,.]+", re.search("\d+\s*dose", por["Presentacion"][ind]).group()).group()) 
    
        #Realizar la conversión de unidades de MCG a MG
        if re.search("[\d,.]+\s*µg", por["Dosagem"][ind]) is not None:
            por["UMC (mg)"][ind] = float(por["UMC (mg)"][ind]) / 1000

        #Borrar las filas que tengan presentaciones no comercializadas
        if re.search("Não Comercializado" , por["Presentacion"][ind], flags = re.IGNORECASE) is not None: 
            por.drop(ind, inplace = True)
    
    ajustar_columnas(columnas=["UMC (mg)","Quantity"], df = por)

    #Crear data frame final
    finalPor = final(df=por, precio="Preco", bd = "INFARMED (POR)")

else: #Si el df está vacío crear el df final vacío
    finalPor = pd.DataFrame(columns = ["PA", "INFARMED (POR)"])

###########################################################################################################################################################################################

#--------------------------------------------------------------------FRANCIA----------------------------------------------------------------------------------------------------------------------

############################################################################################################################################################################################

if len(fra.index)>0: #Revisar si el df está vacío
    #Borrar las observaciones precios vacíos
    fra.drop(fra[(fra["Prix HT"] == "-") & (fra["Prix TTC"] == "-" )].index, inplace=True)
    #Limpiar el df
    fra = limpiar_df(df=fra, precio = "Prix HT" , ff = "Conditionnement")

    #Agregar un valor arbitrario para los medicamentos que no tienen ninguna descripción acerca de su FF
    fra.loc[(fra["Désignation"].str.contains("ml",flags=re.IGNORECASE, regex=True)) & (fra["FF"]=="") ,"FF"] = "INJ" 
       
    for ind in fra.index:
        
        #################################################################################################################################################################################################################################################################
        #------------------------------------------------------------------  UMC (mg) -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        #################################################################################################################################################################################################################################################################
        
        #UMC (mg) - Medicamentos con info. en la columna Conditionnement
        if re.search("[\d,.]+\s*(MG|MIC|MCG)", fra["Conditionnement"][ind], flags = re.IGNORECASE) is not None:
            fra["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*(MG|MIC|MCG)", fra["Conditionnement"][ind], flags = re.IGNORECASE).group()).group()
        
        #UMC (mg) - Medciamentos con info. en la columna Désgination
        elif re.search("[\d,.]+\s*(MG|MIC|MCG)", fra["Désignation"][ind], flags = re.IGNORECASE) is not None:
            fra["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*(MG|MIC|MCG)", fra["Désignation"][ind], flags = re.IGNORECASE).group()).group()
    
        #Medicamentos sin cantidades de MG o MCG asignar un valor aribitrario
        else:
            fra["UMC (mg)"][ind] = 0.000001
                
        #################################################################################################################################################################################################################################################################
        #------------------------------------------------------------------  Quantity -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        #################################################################################################################################################################################################################################################################        
        
        #Quantity - Medicamentos con info. de las cantidades en caja (boîte) o estuche (étui)
        if re.search("\d*[\s,]*((é|e)tui|bo(î|i)te)\s(de)*[\s,]*\d+,*", fra["Conditionnement"][ind], flags = re.IGNORECASE) is not None:
            if len(re.findall("\d+", re.search("\d*[\s,]*((é|e)tui|bo(î|i)te)\s(de)*[\s,]*\d+,*", fra["Conditionnement"][ind], flags = re.IGNORECASE).group())) == 2:
                fra["Quantity"][ind] = float(re.search("\d+", re.findall("\d+,*", fra["Conditionnement"][ind])[0]).group()) * float(re.search("\d+", re.findall("\d+,*", fra["Conditionnement"][ind])[1]).group()) 
            
            elif len(re.findall("\d+", re.search("\d*[\s,]*((é|e)tui|bo(î|i)te)\s(de)*[\s,]*\d+,*", fra["Conditionnement"][ind], flags = re.IGNORECASE).group())) == 1:
                fra["Quantity"][ind] = float(re.search("\d+" , re.search("((é|e)tui|bo(î|i)te)\s(de)*[\s,]*\d+,*", fra["Conditionnement"][ind], flags = re.IGNORECASE).group()).group())
               
            #Si el medicamento esta en XXMG/ML multiplicar el número anterior por los ML en la columna Conditionnement
            if (re.search("/\s*ML", fra["Désignation"][ind], flags = re.IGNORECASE) is not None) and (re.search("[\d,.]+\s*ML", fra["Conditionnement"][ind], flags = re.IGNORECASE) is not None):
                fra["Quantity"][ind] = fra["Quantity"][ind] * float(re.search("[\d,.]+" , re.search("[\d,.]+\s*ML", fra["Conditionnement"][ind], flags = re.IGNORECASE).group()).group())
    
        #Quantity - Medicamento en XXMG/ML con info. de las cantidades de ampollas (ampoules) o botellas (flacon) seguido por la canidad de ML
        elif (re.search("/\s*ML", fra["Désignation"][ind], flags = re.IGNORECASE) is not None) and re.search("\d+[\s,]*(flacon|ampoules*)\sde\s[\d,.]+\s*ml", fra["Conditionnement"][ind], flags = re.IGNORECASE) is not None:
            fra["Quantity"][ind] = float(re.findall("[\d,.]+",re.search("\d+[\s,]*(flacon|ampoules*)\sde\s[\d,.]+\s*ml", fra["Conditionnement"][ind], flags = re.IGNORECASE).group())[0]) * float(re.findall("[\d,.]+",re.search("\d+[\s,]*(flacon|ampoules*)\sde\s[\d,.]+\s*ml", fra["Conditionnement"][ind], flags = re.IGNORECASE).group())[1])
        
        #Quantity - Medicamento en XXMG/ML con cantidad de ML en la columna Conditionnement
        elif (re.search("/\s*ML", fra["Désignation"][ind], flags = re.IGNORECASE) is not None) and re.search("[\d,.]+\s*ml", fra["Conditionnement"][ind], flags = re.IGNORECASE) is not None:
            fra["Quantity"][ind] = re.search("[\d,.]+" , re.search("[\d,.]+\s*ml", fra["Conditionnement"][ind], flags = re.IGNORECASE).group()).group() 
        
        #Quantity - Medicamento en XXMG y cantidad total de jeringas 
        elif re.search("\d+\s(FLACONS|SERINGUES|STYLOS)", fra["Conditionnement"][ind], flags = re.IGNORECASE) is not None:
           fra["Quantity"][ind] = re.search("\d+", re.search("\d+\s(FLACONS|SERINGUES|STYLOS)", fra["Conditionnement"][ind], flags = re.IGNORECASE).group()).group() 
            
        #Para todos los demás asignar un valor de 1
        else: 
            fra["Quantity"][ind] = 1
    
        #Realizar la conversión de unidades de MCG a MG
        if (re.search("[\d,.]+\s*(MIC|MCG)", fra["Désignation"][ind], flags = re.IGNORECASE) is not None) or (re.search("[\d,.]+\s*(MIC|MCG)", fra["Conditionnement"][ind], flags = re.IGNORECASE) is not None):
            fra["UMC (mg)"][ind] = float(fra["UMC (mg)"][ind]) / 1000 
    
    ajustar_columnas(df=fra, columnas = ["UMC (mg)", "Quantity"])

    #Crear data frame final
    finalFra = final(df=fra, precio="Prix HT", bd = "L'AM (FRA)")
  
else: #Si el df está vacío crear el df final vacío
    finalFra = pd.DataFrame(columns = ["PA", "L'AM (FRA)"])

###########################################################################################################################################################################################

#--------------------------------------------------------------------PERÚ----------------------------------------------------------------------------------------------------------------------

############################################################################################################################################################################################

if len(per.index)>0: #Revisar si el df está vacío
    #Borrar las columnas que no son de interés
    per.drop(['Fecha Actualizac.', 'Farmacia/Botica','Telefono','indice'], axis=1, inplace = True)

    #Reemplazar valores vacíos por NaN
    per["Cantidades"].replace(r'^\s*$', np.nan, regex=True, inplace = True)
    #Llenar cantidades NaN usando ffill (forward fill)
    per["Cantidades"].ffill(axis = 0 , inplace = True) 
    
    #Ajustar la columna del principio activo
    per["PA"] = per["PA"].str.replace("+","/")
    per["PA"] = per["PA"].replace(traductor)
    
    #Conservar los medicamentos de Janssen            
    per = limpiar_df(df=per, precio = "Precio Unit" , ff = "Nombre de Producto")
    per["Quantity"] = per["Cantidades"]

    for ind in per.index:
        
        #Medicamentos con dos principios activos
        if (per["PA"][ind].find("/") != -1) and (re.search("mg", per["Nombre de Producto"][ind], flags = re.IGNORECASE) is not None):
            per["UMC (mg)"][ind] = max([float(y.replace(",",".")) for y in [re.search("[\d,.]+" , x).group() for x in re.findall("[\d,.]+\s*mg",per["Nombre de Producto"][ind], flags = re.IGNORECASE)]])
    
        #Medicamentos con un principio activo
        elif re.search("[\d,.]+", per["Nombre de Producto"][ind]) is not None:
            per["UMC (mg)"][ind] = re.search("[\d,.]+", per["Nombre de Producto"][ind]).group()
    
        #Hay que llenar la información de algunos productos a mano
        if ("EVRA" in per["Nombre de Producto"][ind])==True:
            per["UMC (mg)"][ind] = 6
        
        if per["Nombre de Producto"][ind].find("µg") != -1: #Convertir microgramos a mg
           per["UMC (mg)"][ind] = float(per["UMC (mg)"][ind])/1000
            
        #Imbruvica tiene un error (reporta un precio muy bajo y no tiene sentido) por eso lo borraremos
        if float (per["Precio Unit"][ind])<2 and (per["PA"][ind]=="IBRUTINIB"): 
            per.drop(ind, inplace = True)
            
        
    ajustar_columnas(df=per, columnas = ["UMC (mg)", "Quantity"])

    #Crear data frame final
    finalPer = final(df=per, precio="Precio Unit", bd = "DIGEMID (PER)")

else: #Si el df está vacío crear el df final vacío
    finalPer = pd.DataFrame(columns = ["PA", "DIGEMID (PER)"])

###########################################################################################################################################################################################

#----------------------------------------------------------------ARGENTINA----------------------------------------------------------------------------------------------------------------------

############################################################################################################################################################################################

if len(arg.index)>0: #Revisar si el df está vacío
    arg.fillna("" , inplace = True) #Reemplazar los NaN por vacío para evitar errores
    #Asignarle valores a la columna "PA" y limpiar texto de la columna "Precio Venta al Público"
    arg["PA"]=""
    
    for ind in arg.index:
        #Medicamentos con dos principios activos
        if (arg["Genérico"][ind].count("+")==1):
            arg["PA"][ind] =  re.split("[+]", arg["Genérico"][ind])[0][:re.search("\s\d", re.split("[+]", arg["Genérico"][ind])[0]).start()] + " / " + re.split("[+]", arg["Genérico"][ind])[1][1:re.search("\s\d", re.split("[+]", arg["Genérico"][ind])[1]).start()] 
        
        #Todos los demás medicamentos
        elif (arg["Genérico"][ind].count("+")==0):
            arg["PA"][ind] = arg["Genérico"][ind][:re.search("\s\d",arg["Genérico"][ind]).start()]

        #Medicamentos con un "("
        if arg["PA"][ind].find(" (")!=-1:
            arg["PA"][ind] = arg["PA"][ind][:arg["PA"][ind].find(" (")]  
    
        #Eliminar medicamentos sin información de precios
        if (re.search("\d", arg["Precio Venta al Público"][ind]) is None):
            arg.drop(ind, inplace = True)

        #Limpiar información de precios para quitar carácteres de texto
        elif re.search("\s*[(]", arg["Precio Venta al Público"][ind]) is not None:
            arg["Precio Venta al Público"][ind] = arg["Precio Venta al Público"][ind][:re.search("\s*[(]", arg["Precio Venta al Público"][ind]).start()]

    #Limpiar el data frame            
    arg = limpiar_df(df=arg, precio = "Precio Venta al Público" , ff = "Forma Farmacéutica")

    for ind in arg.index:
        #LLenar las cantidades con la información en la columna presentación
        if re.search("\d+\s*UNIDADES", arg["Presentación"][ind], flags = re.IGNORECASE) is not None:
            try:
                arg["Quantity"][ind] = float(arg["Presentación"][ind][:arg["Presentación"][ind].find(" ")])*float(re.search("\d+", re.search("\d+\s*UNIDADES", arg["Presentación"][ind]).group()).group())
            except:
                arg["Quantity"][ind] = re.search("\d+", re.search("\d+\s*UNIDADES", arg["Presentación"][ind]).group()).group()
        
        elif re.search("\A[\d,.]+", arg["Presentación"][ind]) is not None:
            arg["Quantity"][ind] = re.search("\A[\d,.]+", arg["Presentación"][ind]).group()
    
        else:
            arg["Quantity"][ind] = 1
        
        #Medicamentos con dos principios activos
        if arg["PA"][ind].find("/")!=-1 and arg["Genérico"][ind].count("MG")==2:
            arg["UMC (mg)"][ind] = max([float(x) for x in re.findall("[\d,.]+", arg["Genérico"][ind])])
    
        #Medicamentos con UMC (mg) en la columna "Presentación"
        elif re.search("[\d,.]+\s*MG|[\d,.]+\s*MCG", arg["Presentación"][ind]) is not None:
            arg["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("([\d,.]+\s*MG)|([\d,.]+\s*MCG)", arg["Presentación"][ind]).group()).group()
         
            #Medicamentos con cantidades XX (MG|MCG|G) / XX ML en la columna "Genérico"
        elif (re.search("[\d,.]+\s*(G|MG|MCG)\s*/\s*[\d,.]+\s*ML", arg["Genérico"][ind]) is not None) and (re.search("[\d,.]+\s*ML", arg["Presentación"][ind])) :
            arg["UMC (mg)"][ind] = float(re.search("[\d,.]+", re.search("[\d,.]+\s*(G|MG|MCG)", arg["Genérico"][ind]).group()).group().replace(",",".")) / float(re.search("[\d,.]+", re.search("[\d,.]+\s*ML", arg["Genérico"][ind]).group()).group().replace(",","."))
            arg["Quantity"][ind] = float(re.search("[\d,.]+", re.search("[\d,.]+\s*ML", arg["Presentación"][ind]).group()).group().replace(",",".")) * float(arg["Quantity"][ind].replace(",","."))
    
        #Medicamentos con cantidades XX (MG|MCG|G) / ML en la columna "Genérico"
        elif (re.search("[\d,.]+\s*(G|MG|MCG)\s*/\s*ML", arg["Genérico"][ind]) is not None) and (re.search("[\d,.]+\s*ML", arg["Presentación"][ind])) :
            arg["UMC (mg)"][ind] = float(re.search("[\d,.]+", re.search("[\d,.]+\s*(G|MG|MCG)", arg["Genérico"][ind]).group()).group().replace(",",".")) 
            arg["Quantity"][ind] = float(re.search("[\d,.]+", re.search("[\d,.]+\s*ML", arg["Presentación"][ind]).group()).group().replace(",",".")) * float(arg["Quantity"][ind].replace(",","."))
    
        #Medicamentos con UMC en la columna "Genérico"
        elif re.search("[\d,.]+\s*(G|MG|MCG)", arg["Genérico"][ind]) is not None:
            arg["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*(G|MG|MCG)", arg["Genérico"][ind]).group()).group()
    
        #Realizar la conversión de unidades de G a MG y de MCG a MG     
        if re.search("[\d,.]+\s*G", arg["Genérico"][ind]) is not None:
            arg["UMC (mg)"][ind] = float(arg["UMC (mg)"][ind]) * 1000 
        
        elif re.search("[\d,.]+\s*MCG", arg["Genérico"][ind]) is not None:
            arg["UMC (mg)"][ind] = float(arg["UMC (mg)"][ind]) / 1000 
        
    #Convertir las columnas Quantity  y UMC (g) a número para poder hacer cálculos
    ajustar_columnas(df=arg, columnas=["UMC (mg)","Quantity"])

    #Crear data frame final
    finalArg = final(df=arg, precio="Precio Venta al Público", bd = "ANMAT (ARG)")
    
else: #Si el df está vacío crear el df final vacío
    finalArg = pd.DataFrame(columns = ["PA", "ANMAT (ARG)"])
     
###########################################################################################################################################################################################

#----------------------------------------------------------------EEUU----------------------------------------------------------------------------------------------------------------------

############################################################################################################################################################################################

#Conservar las columnas relevantes
eeuu = eeuu.filter(["VendorName","Generic","TradeName","Price","PA","UMC (mg)","PackageDescription","PriceType"])

#Asignarle valores a la columna "PA"
eeuu["PA"]=""
for ind in eeuu.index:
    
    #Medicamentos con dos principios activos
    if (eeuu["Generic"][ind].count("MCG")==2 or eeuu["Generic"][ind].count("MG")==2) and (re.search("\s\d", eeuu["Generic"][ind]) is not None and re.search("/\s*\w+", eeuu["Generic"][ind]) is not None):
        eeuu["PA"][ind] = eeuu["Generic"][ind][:re.search("\s\d", eeuu["Generic"][ind]).start()] + " / " + re.search("\w+",re.search("/\s*\w+", eeuu["Generic"][ind]).group()).group()    
    
    #Todos los demás medicamentos
    else:
        eeuu["PA"][ind] = eeuu["Generic"][ind].split(" ")[0]
        
#Conservar los medicamentos de Janssen            
eeuu = limpiar_df(df=eeuu, precio = "Price" , ff = "Generic")

#Covertir TODA la columna de package description a texto para poder manipularla
eeuu['PackageDescription'] = eeuu['PackageDescription'].astype(str)

#Crear una columnas auxiliares para facilitar los cálculos
eeuu["mg"]=""
eeuu["ml"]=""

#Llenar la columnas "UMC (mg)" y "Quantity"  con este bucle y convertir los medcamentos en microgramos a mg
for ind in eeuu.index:
    
    #Medicamentos con doble principio activo
    if eeuu["PA"][ind].find("/")!=-1:
        eeuu["UMC (mg)"][ind] = str(max([float(x) for x in [re.search("[\d.]+", y).group() for y in [z.replace(",",".") for z in re.findall("[\d,.]+\s*MG|[\d,.]+\s*MCG", eeuu["Generic"][ind])]]]))
        
    #Medicamentos con valores de XXMG/XXML en la columna "Generic"
    elif (re.search("[\d.]+[\s]*(MG|MCG)/[\d.]+[\s]*ML", eeuu["Generic"][ind]) is not None) and (re.search("[\d.]+ML", eeuu["PackageDescription"][ind]) is not None):
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+[\s]*(MG|MCG)/[\d.]+[\s]*ML", eeuu["Generic"][ind]).group()
        eeuu["mg"][ind] = eeuu["UMC (mg)"][ind][:re.search("\s|(MG|MCG)",eeuu["UMC (mg)"][ind]).start()]
        eeuu["ml"][ind] = eeuu["UMC (mg)"][ind][re.search("[\d.]+[\s]*ML",eeuu["UMC (mg)"][ind]).start():re.search("[\s]*ML",eeuu["UMC (mg)"][ind]).start()]
        eeuu["UMC (mg)"][ind] = float(eeuu["mg"][ind])/float(eeuu["ml"][ind])
        eeuu["Quantity"][ind] = eeuu["PackageDescription"][ind][re.search("[\d.]+ML",eeuu["PackageDescription"][ind]).start():re.search("ML",eeuu["PackageDescription"][ind]).start()]  
        
    #Medicamentos con valores de XXMG/ML en la columna "Generic" 
    elif (re.search("[\d.]+[\s]*(MG|MCG)/[\s]*ML", eeuu["Generic"][ind]) is not None) and (re.search("[\d.]+ML", eeuu["PackageDescription"][ind]) is not None):
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+[\s]*(MG|MCG)/[\s]*ML", eeuu["Generic"][ind]).group()
        eeuu["UMC (mg)"][ind] = eeuu["UMC (mg)"][ind][:re.search("\s|(MG|MCG)",eeuu["UMC (mg)"][ind]).start()]
        eeuu["Quantity"][ind] = eeuu["PackageDescription"][ind][re.search("[\d.]+ML",eeuu["PackageDescription"][ind]).start():re.search("ML",eeuu["PackageDescription"][ind]).start()]  
    
    #Medicamentos con valores de XXMG/XXML en la columna "Trade Name"
    elif (re.search("[\d.]+[\s]*(MG|MCG)/[\d.]+[\s]*ML", eeuu["TradeName"][ind]) is not None) and (re.search("[\d.]+ML", eeuu["PackageDescription"][ind]) is not None):
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+[\s]*(MG|MCG)/[\d.]+[\s]*ML", eeuu["TradeName"][ind]).group()
        eeuu["mg"][ind] = eeuu["UMC (mg)"][ind][:re.search("\s|(MG|MCG)",eeuu["UMC (mg)"][ind]).start()]
        eeuu["ml"][ind] = eeuu["UMC (mg)"][ind][re.search("[\d.]+[\s]*ML",eeuu["UMC (mg)"][ind]).start():re.search("[\s]*ML",eeuu["UMC (mg)"][ind]).start()]
        eeuu["UMC (mg)"][ind] = float(eeuu["mg"][ind])/float(eeuu["ml"][ind])
        eeuu["Quantity"][ind] = eeuu["PackageDescription"][ind][re.search("[\d.]+ML",eeuu["PackageDescription"][ind]).start():re.search("ML",eeuu["PackageDescription"][ind]).start()]  
        
    #Medicamentos con valores de XXMG/ML en la columna "Trade Name"
    elif (re.search("[\d.]+[\s]*(MG|MCG)/[\s]*ML", eeuu["TradeName"][ind]) is not None) and (re.search("[\d.]+ML", eeuu["PackageDescription"][ind]) is not None):
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+[\s]*(MG|MCG)/[\s]*ML", eeuu["TradeName"][ind]).group()
        eeuu["UMC (mg)"][ind] = eeuu["UMC (mg)"][ind][:re.search("\s|(MG|MCG)",eeuu["UMC (mg)"][ind]).start()]
        eeuu["Quantity"][ind] = eeuu["PackageDescription"][ind][re.search("[\d.]+ML",eeuu["PackageDescription"][ind]).start():re.search("ML",eeuu["PackageDescription"][ind]).start()]  
        
    #Medicamentos con valores de MG o MCG en la columna "Trade Name" 
    elif (re.search("[\d.]+[\s]*(MG|MCG|G)", eeuu["TradeName"][ind]) is not None):
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+[\s]*(MG|MCG|G)", eeuu["TradeName"][ind]).group()
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+", eeuu["TradeName"][ind]).group()
        
    #Medicamentos con valores de MG o MCG en la columna "Generic", realizar la conversión de unidades
    elif (re.search("[\d.]+[\s]*(MG|MCG|G)", eeuu["Generic"][ind]) is not None):
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+[\s]*(MG|MCG|G)", eeuu["Generic"][ind]).group()
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+", eeuu["Generic"][ind]).group()
        
    #Medicamentos con valores de MG o MCG en la columna "Trade Name" que no están indexados con MG o MCG 
    elif (re.search("[\d.]+", eeuu["TradeName"][ind]) is not None):    
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+", eeuu["TradeName"][ind]).group()
    
    #Medicamentos sin valores de MG o MCG en las columnas "Trade Name" o "Generic" pero hay alguna información está en "Package Description"    
    elif (re.search("[\d.]+", eeuu["PackageDescription"][ind]) is not None):    
        eeuu["UMC (mg)"][ind] = re.search("[\d.]+", eeuu["PackageDescription"][ind]).group()
        eeuu["Quantity"][ind] = "1"
        
    #Medicamentos (TAB, PATCH, LOZENGE) con las cantidades reportadas con "X" en la columna "Package Description"    
    if (re.search("[\d.]+X[\d.]+", eeuu["PackageDescription"][ind], flags = re.IGNORECASE)  is not None) and (re.search("TAB|LOZENGE|PATCH|SUBLINGUAL SPRAY",eeuu["FF"][ind]) is not None) and (eeuu["Quantity"][ind]==""):
        eeuu["Quantity"][ind] = float(eeuu["PackageDescription"][ind][:re.search("X", eeuu["PackageDescription"][ind] , flags = re.IGNORECASE).start()])*float(eeuu["PackageDescription"][ind][re.search("X",eeuu["PackageDescription"][ind],flags=re.IGNORECASE).start()+1:])  
    
    #Medicamentos (TAB, PATCH, LOZENGE) con las cantidades reportadas con números que pueden estar seguidos de letras en la columna "Package Description"    
    elif (re.search("[\d.]+[a-zA-Z]*",eeuu["PackageDescription"][ind]) is not None) and (re.search("TAB|LOZENGE|PATCH|SUBLINGUAL SPRAY",eeuu["FF"][ind]) is not None) and (eeuu["Quantity"][ind]==""):
        eeuu["Quantity"][ind] = re.search("[\d.]+", eeuu["PackageDescription"][ind]).group()
    
    #Para todos los demás medicamentos la cantidad es igual a "1"    
    elif eeuu["Quantity"][ind]=="":
        eeuu["Quantity"][ind] = "1"
        
    #Medicamentos inyectables cuyo paquete viene con 1 o más jeringas
    if (re.search("[\d]+X", eeuu["PackageDescription"][ind], flags=re.IGNORECASE) is not None) and (eeuu["FF"][ind]=="INJ"):
        eeuu["Quantity"][ind] = float(eeuu["PackageDescription"][ind][:re.search("X", eeuu["PackageDescription"][ind], flags=re.IGNORECASE).start()])*float(eeuu["Quantity"][ind])
    
    #Realizar la conversión de MCG a MG 
    if (re.search("MCG",eeuu["Generic"][ind]) is not None) or (re.search("MCG",eeuu["TradeName"][ind]) is not None):
        eeuu["UMC (mg)"][ind] = float(eeuu["UMC (mg)"][ind])/1000
    
#Borrar las columnas auxiliares        
eeuu.drop(columns=['mg', 'ml'], inplace=True)

#Convertir las columnas Quantity  y UMC (g) a número para poder hacer cálculos       
ajustar_columnas(df=eeuu,columnas=["UMC (mg)","Quantity"])

#Crear columna con precio / mg (hay que hacerlo con este condicional pues pueden haber medicamentos cuyo precio reportado sea por unidad de dispensación)
#Utilizar este condicional pues puden haber medicamentos cuyo precio está reportado por unidad de dispensación
for ind in eeuu.index:
    if eeuu["Price"][ind]>1:
        eeuu["Precio UMC (mg)"][ind] = (eeuu["Price"][ind])/(eeuu["UMC (mg)"][ind]*eeuu["Quantity"][ind])
    else:
        eeuu["Precio UMC (mg)"][ind] = (eeuu["Price"][ind])/(eeuu["UMC (mg)"][ind])

#Agrupar los medicamentos
finalEEUU = eeuu.groupby(['PA', "FF"])["Precio UMC (mg)"].min()
finalEEUU=finalEEUU.reset_index()
finalEEUU["PA"] = finalEEUU["PA"] + " - " + finalEEUU["FF"]
finalEEUU.drop(columns=['FF'],inplace=True)
finalEEUU.rename(columns={'Precio UMC (mg)':'FSS (EEUU)'},inplace=True)

##################################################################################################################################################################################

#---------------------------------------------------------NORUEGA-----------------------------------------------------------------------------------------------------------------

##################################################################################################################################################################################

#Crear data frame de Noruega y lista de medicmamentos de Janssen regulados por PRI
nor=nor.filter(["Product name","MA-holder","Packages","Active substance","Pharmaceutical form","Strength","Package type","Packsize","Unit","PPP"])

#Llenar valores vacíos con el fin de evitar errores
nor[["Packsize","Packages"]] = nor[["Packsize","Packages"]].fillna(1)
nor.fillna("N/A",inplace=True)

#Asignarle valores a la columna "PA"
nor["PA"]=""

for ind in nor.index:
    
    #Medicamentos con dos principios activos
    if ((nor["Strength"][ind].count("mikrog")==2) or (nor["Strength"][ind].count("mg")==2)) and (re.search("\s*,\s*", nor["Active substance"][ind]) is not None):
        nor["PA"][ind] = nor["Active substance"][ind][:re.search("\s*,", nor["Active substance"][ind]).start()] + " / " + re.search("\w+", re.search(",\s*\w+", nor["Active substance"][ind]).group()).group()     
        
    #Todos los demás medicamentos
    else:
        nor["PA"][ind] = nor["Active substance"][ind]

#Limpiar df
nor = limpiar_df(df=nor, precio ="PPP", ff="Pharmaceutical form")

#Asignarle valores a las columnas de Unidades Mínimas de Concentración "UMC" y cantidades "Quantity"  
for ind in nor.index:
    
    #Medicamentos con dos principios activos
    if nor["PA"][ind].find("/") != -1:
        nor["UMC (mg)"][ind] = str(max([float(w) for w in [re.search("[\d,.]+", x).group() for x in [y.replace(",",".") for y in [z.encode('ascii','ignore').decode("utf-8") for z in re.findall("[\d,.\s]+\s*mg|[\d,.\s]+\s*mikrog", nor["Strength"][ind])]]]]))
        nor["Quantity"][ind] = nor["Packsize"][ind]
    
    #Medicamentos inyectables cuya concentración total está reportada en la columna "Packsize" y por ende su cantidad es "1"
    elif (re.search("(mg|mikrog)", nor["Strength"][ind]) is not None) and (re.search("(mg|mikrog)", nor["Unit"][ind]) is not None):
        nor["UMC (mg)"][ind] = nor["Packsize"][ind]
        nor["Quantity"][ind] = 1
        
    #Medicamentos (ml) cuya concentración total está reportada en la columna "Strength" y por ende su cantidad es "1"
    elif (re.search("(mg|mikrog)/ml", nor["Strength"][ind]) is None) and (nor["Unit"][ind]=="ml"):
        nor["UMC (mg)"][ind] = re.sub("\s","",re.search("[\d,.\s]+", nor["Strength"][ind]).group())
        nor["Quantity"][ind] = 1
     
    #Medicamentos cuya concentración está en "Strength" y cantidades están en la columa "Packsize"
    else: 
        nor["UMC (mg)"][ind] = re.sub("\s","",re.search("[\d,.\s]+", nor["Strength"][ind]).group())        
        nor["Quantity"][ind] = nor["Packsize"][ind]
    
    #Los medicamentos que no sean parche y estén en microgramos serán convertidos mg
    if nor["Strength"][ind].find("mikrog")!=-1:          
            nor["UMC (mg)"][ind] = float(nor["UMC (mg)"][ind].replace(",","."))/1000
    
    #Multiplicar la columna "packages" por "quantity" para aquellos medicamentos que vienen en mas de una unidad (ej: mas de una jeringa) o tabletas con información faltante en la columna "Packsize"
    nor["Quantity"][ind] = float(nor["Quantity"][ind])*float(nor["Packages"][ind])      


#Convertir las columnas Quantity  y UMC (g) a número para poder hacer cálculos
ajustar_columnas(df=nor,columnas=["UMC (mg)","Quantity"])

#Crear data frame final
finalNor = final(df=nor, precio="PPP", bd = "NOMA (NOR)")

######################################################################################################################################################################

#---------------------------------------------------------------AUSTRALIA----------------------------------------------------------------------------------------------------

#######################################################################################################################################################################

#Crear data frame de Australia al unir las dos bases de datos de Australia
aus = aus.append(aus1, ignore_index = True)

#Conservar las columnas relevantes
aus = aus.filter(["Legal Instrument Drug","Legal Instrument Form","Legal Instrument Moa","Brand Name","Pack Quantity","AEMP","AMT Trade Product Pack"])

#Asignarle valores a la columna "PA"
aus["PA"]=""

for ind in aus.index:
    
    #Medicamentos con dos principios activos
    if ((aus["Legal Instrument Form"][ind].count("micorgram")==2) or (aus["Legal Instrument Form"][ind].count("mg")==2)) and (re.search("with",aus["Legal Instrument Drug"][ind], flags = re.IGNORECASE) is not None):
        aus["PA"][ind] = re.findall("\w+", aus["Legal Instrument Drug"][ind])[0] + " / " + re.findall("\w+", aus["Legal Instrument Drug"][ind])[2]     
        
    #Todos los demás medicamentos
    else:
        aus["PA"][ind] = aus["Legal Instrument Drug"][ind]

#Limpiar el df
aus=limpiar_df(df=aus,precio="AEMP", ff = "Legal Instrument Form")

#Asignarle valores a las columna de UMC (mg) y cantidades "Quantity"
for ind in aus.index: 
    
    #Medicamentos con dos principios activos
    if aus["PA"][ind].find("/") != -1:
        aus["UMC (mg)"][ind] = str(max([float(x) for x in [re.search("[\d.]+", y).group() for y in [z.replace(",",".") for z in re.findall("[\d,.]+\s*mg|[\d,.]+\s*micrograms*", aus["Legal Instrument Form"][ind])]]])) 
        aus["Quantity"][ind] = aus["Pack Quantity"][ind]
    
    #Medicamentos con valores XXMG/ML y ML totales en la columna "Legal Instrument Form"
    elif (re.search("[\d.]+\smg\sper\sm(l|L)", aus["Legal Instrument Form"][ind]) is not None) and (re.search("[\d.]+[\s]m(l|L)",aus["Legal Instrument Form"][ind]) is not None):
        aus["UMC (mg)"][ind] = re.search("[\d.]+", re.search("[\d.]+\smg\sper\sm(l|L)", aus["Legal Instrument Form"][ind]).group()).group()
        aus["Quantity"][ind] = re.search("[\d.]+", re.search("[\d.]+[\s]m[l|L]", aus["Legal Instrument Form"][ind]).group()).group()
        
    #Medicamentos con UMC (mg) en la columna Legal Instrument Form y Cantidades en la columna "Quantity"
    elif (re.search("[\d.]+\s(mg|microgram[s]*)", aus["Legal Instrument Form"][ind]) is not None): 
        aus["UMC (mg)"][ind] = re.search("[\d.]+",re.search("[\d.]+\s(mg|microgram[s]*)", aus["Legal Instrument Form"][ind]).group()).group()
        aus["Quantity"][ind] = aus["Pack Quantity"][ind]
    
    #Realizar la conversión de unidades de MCG a MG
    if re.search("microgram[s]*",aus["Legal Instrument Form"][ind]) is not None:
            aus["UMC (mg)"][ind] = float(aus["UMC (mg)"][ind])/1000 
            
#Convertir las columnas Quantity  y UMC (g) a número para poder hacer cálculos
ajustar_columnas(df=aus,columnas=["UMC (mg)","Quantity"])

#Crear el data frame final
finalAus = final(df = aus, precio = "AEMP", bd = "PBS (AUS)")

####################################################################################################################################################################

#---------------------------------------------------------------------------ESPAÑA---------------------------------------------------------------------------------------------------

###################################################################################################################################################################

#Crear data frame de España al unir las dos bases de datos
esp = esp.append(esp1, ignore_index = True)

#Conservar las columnas relevantes
esp=esp.filter(["Article","Manufacturer","ATC Description","Active Ingredients","Public Price"])

#Asignarle valores a la columna "PA"
esp["PA"]=""

for ind in esp.index:
    
    #Medicamentos con dos principios activos
    if (esp["Article"][ind].count("+")==1) and (re.search("/",esp["Active Ingredients"][ind]) is not None):
        esp["PA"][ind] = re.split("/", esp["Active Ingredients"][ind])[0] + " / " + re.split("/", esp["Active Ingredients"][ind])[1]    
        
    #Todos los demás medicamentos
    else:
        esp["PA"][ind] = esp["ATC Description"][ind]

#Conservar los medicamentos de Janssen            
esp = limpiar_df(df=esp,precio="Public Price", ff = "Article")
#Hay medicamentos inyectables que no están identificados con ninguna palabra clave, por ende los llenamos arbitrariamente si su FF se encuentra vacía
esp.loc[esp["FF"]=="","FF"]="INJ"

#Llenar las columnas con un bucle
for ind in esp.index:

    #Agregar valores a "Quantity"
    
    #TABLETAS (TAB) & PARCHES (PATCH)
    if (re.search("[\d,]+\s*(CPR|CPS|COMPRIMIDO[S]*|SUPP|BUST|CER)", esp["Article"][ind]) is not None):
        esp["Quantity"][ind] = re.search("[\d,]+",re.search("[\d,]+\s*(CPR|CPS|COMPRIMIDO[S]*|SUPP|BUST|CER)", esp["Article"][ind]).group()).group()
        
    #SOLUCIONES ORALES E INYECTABLES (INJ)    
    elif (re.search("[\d,]+MG/ML", esp["Article"][ind]) is not None) and (re.search("[\d,]+ML", esp["Article"][ind]) is not None):
        esp["Quantity"][ind] = re.search("[\d,]+", re.search("[\d,]+ML", esp["Article"][ind]).group()).group()           
    
    else:    
        esp["Quantity"][ind]="1"
    
    #Medicamentos cuyo paquete puede tener más de una jeringa
    if re.search("\d+\s*(FL|SIR|PEN)", esp["Article"][ind]) is not None:
        esp["Quantity"][ind] = float(esp["Quantity"][ind].replace(",","."))*float(re.search("\d+", re.search("\d+\s*(FL|SIR|PEN)", esp["Article"][ind]).group()).group())
        
    #Agregar valores a "UMC (mg)"        
    
    #Medicamentos con dos principios activos
    if esp["PA"][ind].find("/") != -1:
        esp["UMC (mg)"][ind] = [float(x) for x in [re.search("[\d,.]+", y).group() for y in [z.replace(",",".") for z in re.split("[+]", re.search("[\d,.]+\s*\w*\s*[+]\s*[\d,.]+\s*\w*", esp["Article"][ind]).group())]]]
        lista = [1000 if x == "MCG" else 1 for x in re.findall("\d[+]|MG|MCG",esp["Article"][ind])]
        esp["UMC (mg)"][ind] = str(max([i / j for i, j in zip(esp["UMC (mg)"][ind],lista)]))
        
        
    elif (re.search("(\s|[a-zA-Z])[\d,]+(MG|MCG)", esp["Article"][ind]) is not None): 
         esp["UMC (mg)"][ind] = re.search("[\d,]+" , re.search("(\s|[a-zA-Z])[\d,]+(MG|MCG)",esp['Article'][ind]).group()).group()
            
    elif re.search("[\d,]+",esp["Article"][ind]) is not None: 
        esp["UMC (mg)"][ind] = re.search("[\d,]+",esp["Article"][ind]).group() 
        
    else:
        #Medicamentos que tienen información faltante llenamos con un valor arbitrario que no cause errores
        esp["UMC (mg)"][ind]="9999999"

    #Realizar las la conversión de MCG a MG
    if (re.search("MCG", esp["Article"][ind]) is not None) and (esp["PA"][ind].find("/")==-1):
        esp["UMC (mg)"][ind]= float(esp["UMC (mg)"][ind].replace(",","."))/1000

#Convertir las columnas Quantity  y UMC (g) a número para poder hacer cálculos
ajustar_columnas(df=esp,columnas=["UMC (mg)","Quantity"])

#Crear el data frame final
finalEsp = final(df=esp, precio = "Public Price", bd="PETRONE (ESP)")

######################################################################################################################################################################################

#--------------------------------------------------------------BRASIL (ANVISA) -----------------------------------------------------------------------------------------------------------------

########################################################################################################################################################################################

#Crear data frame de Brasil al unir las dos bases de datos
bra = bra.append(bra1, ignore_index = True)

#Conservar y crear las columnas relevantes
bra=bra.filter(["APRESENTAÇÃO", "LABORATÓRIO", "PF 0%", "PF Sem Impostos","PMVG 0%","PMVG Sem Impostos", "PRINCÍPIO ATIVO","PRODUTO","SUBSTÂNCIA"])

#Crear una columna única que recoja el principio activo "PA": Llenamos los valores vacíos de cada columna y luego los sumamos para tener una columna única 
bra["PRINCÍPIO ATIVO"] = bra["PRINCÍPIO ATIVO"].fillna(value="")
bra["SUBSTÂNCIA"] = bra["SUBSTÂNCIA"].fillna(value="")
bra["SUBSTÂNCIA"] = bra['SUBSTÂNCIA'] + bra['PRINCÍPIO ATIVO']

#Borramos la columna que ya no vamos a usar
bra.drop(columns=['PRINCÍPIO ATIVO'],inplace=True)

#Consolidamos una sola columna con el precio "Price" que recoja el mínimo precio de todos los disponibles en la base de datos 
bra[["PF 0%","PF Sem Impostos","PMVG 0%","PMVG Sem Impostos"]] = bra[["PF 0%","PF Sem Impostos","PMVG 0%","PMVG Sem Impostos"]].fillna("99999999").applymap(lambda x: x.replace(",",".")).astype(float)
bra["Price"] = bra[["PF 0%", "PF Sem Impostos","PMVG 0%","PMVG Sem Impostos"]].min(axis=1, skipna=True)

#Asignarle valores a la columna "PA"
bra["PA"]=""

for ind in bra.index:
    
    #Medicamentos con dos principios activos
    if (bra["SUBSTÂNCIA"][ind].count(";")==1) and ((bra["APRESENTAÇÃO"][ind].count("MG")==2) or (bra["APRESENTAÇÃO"][ind].count("MCG")==2)):
        bra["PA"][ind] = re.split(";", bra["SUBSTÂNCIA"][ind])[0] + " / " + re.split(";", bra["SUBSTÂNCIA"][ind])[1]    
        
    #Todos los demás medicamentos
    else:
        bra["PA"][ind] = bra["SUBSTÂNCIA"][ind]

#Limpiar el data frame
bra = limpiar_df(df=bra, precio = "Price", ff = "APRESENTAÇÃO")

#Llenar las columnas con las UMC (mg) y cantidades "Quantity"
for ind in bra.index:

    #Medicamentos con dos principios activos
    if bra["PA"][ind].find("/") != -1:
        bra["UMC (mg)"][ind] = str(max([float(x) for x in [re.search("[\d.]+", y).group() for y in [z.replace(",",".") for z in re.findall("[\d,.]+\s*MG|[\d,.]+\s*MCG", bra["APRESENTAÇÃO"][ind])]]])) 
        
    #Unidades mínimas de concentración "UMC (mg)"
    elif re.search("\s*(MG|MCG)", bra["APRESENTAÇÃO"][ind]) is not None:
        bra["UMC (mg)"][ind] = bra["APRESENTAÇÃO"][ind][:re.search("\s*(MG|MCG)", bra["APRESENTAÇÃO"][ind]).start()]
    
    #Realizar la conversión de MCG a MG
    if re.search("MCG" , bra["APRESENTAÇÃO"][ind]) is not None:
        bra["UMC (mg)"][ind] = float(bra["UMC (mg)"][ind].replace(",","."))/1000 
    
    #Quantity: TABLETAS (TAB) y PARCHES (PATCH)
    if  ((bra["FF"][ind]!="INJ") and (bra["FF"][ind]!="SOL ORAL")) and (re.search("X\s\d+", bra["APRESENTAÇÃO"][ind]) is not None):
        bra["Quantity"][ind] = bra["APRESENTAÇÃO"][ind][re.search("X\s\d+", bra["APRESENTAÇÃO"][ind]).start()+2:re.search("X\s\d+", bra["APRESENTAÇÃO"][ind]).span()[1]]
    
    #Quantity: SOLUCIONES ORALES E INYECTABLES (INJ)    
    elif (re.search("(MG|MCG)/\s*ML", bra["APRESENTAÇÃO"][ind]) is not None) and (re.search("X[\s]*[\d,.]+[\s]*ML", bra["APRESENTAÇÃO"][ind]) is not None):
        bra["Quantity"][ind] = re.search("[\d,.]+", re.search("X[\s]*[\d,.]+[\s]*ML", bra["APRESENTAÇÃO"][ind]).group()).group()           
    
    else:
        bra["Quantity"][ind]="1"

    #Medicamentos cuya caja puede incluir más de una ampolla
    if re.search("C(T|X)\s\d+", bra["APRESENTAÇÃO"][ind]) is not None:
       bra["Quantity"][ind] = float(bra["Quantity"][ind].replace(",","."))*float(re.search("\d+",re.search("C(T|X)\s\d+", bra["APRESENTAÇÃO"][ind]).group()).group())


#Convertir las columnas Quantity  y UMC (g) a número para poder hacer cálculos
ajustar_columnas(df=bra,columnas=["UMC (mg)","Quantity"])

#Crear columna con precio / mg
finalBra = final(df=bra,precio="Price", bd = "ANVISA (BRA)")

######################################################################################################################################################################################

#--------------------------------------------------------------BRASIL (BPS) -----------------------------------------------------------------------------------------------------------------

########################################################################################################################################################################################

#Si la búsqueda arrojo más de un resultado, limpiar el df
if len(bps)>0:
    bps.rename(columns=lambda x: x.replace(u'\xa0', " "), inplace = True) #Remplazar estos carácteres especiales por un espacio normal
    bps = bps.filter(["Descrição CATMAT"," Unidade de Fornecimento ", "Fabricante", "Preço Unitário", "Base"]) #Conservar únicamente las columnas de interés
    bps["PA"] = bps["Descrição CATMAT"].apply(lambda x: x[re.search("[A-Z]",x).start():x.find(",")].replace("  "," ")) #Extraer el principio activo de la columna Descripción
    bps = limpiar_df(df = bps, precio = "Preço Unitário" , ff = " Unidade de Fornecimento ") #Limipiar el df

    #Hay FF que están la columna de descripción, por ende creamos estas líneas de código auxiliares para llenar los valores faltantes
    bps.loc[(bps["Descrição CATMAT"].str.contains("SUSPENSÃO ORAL|SOLUÇÃO ORAL",flags=re.IGNORECASE, regex=True)) & (bps["FF"]==""),"FF"] = "SOL ORAL"
    bps.loc[(bps["Descrição CATMAT"].str.contains("inj",flags=re.IGNORECASE, regex=True)) & (bps["FF"]==""),"FF"] = "INJ"
    bps.loc[(bps["Descrição CATMAT"].str.contains("ADESIVO TRANSDÉRMICO",flags=re.IGNORECASE, regex=True)) & (bps["FF"]==""),"FF"] = "PATCH"
    bps.loc[(bps[" Unidade de Fornecimento "].str.contains("ML",flags=re.IGNORECASE, regex=True)) & (bps["FF"]==""),"FF"] = "SOL ORAL"
    bps = bps[(~bps["Descrição CATMAT"].str.contains("G/MOL"))] #Borramos medicamentos en g/mol, pues la información esta incompleta

    for ind in bps.index:
        #Asignamos un valor por defecto a las cantidades de 1, pues los precios ya están en unidades de dispensación 
        bps["Quantity"][ind] = 1
        
        if bps["PA"][ind].find("/") != -1: #Si el medicamento tiene más de un principio activo
            bps["UMC (mg)"][ind] = max([float(y.replace(",",".")) for y in [re.search("[\d,.]+", x).group() for x in re.findall("[\d,.]+\s*MG", bps["Descrição CATMAT"][ind] , flags = re.IGNORECASE)]])
        
        #Medicamentos con XXMG/ML y ML totales en la columna Unidade de Fornecimento
        elif (re.search("[\d.,]+\s*(MG|MCG)", bps["Descrição CATMAT"][ind], flags = re.IGNORECASE) is not None) and (re.search("[\d.,]+\s*ML", bps[" Unidade de Fornecimento "][ind], flags = re.IGNORECASE) is not None):
            bps["UMC (mg)"][ind] = re.search("[\d.,]+", re.search("[\d.,]+\s*(MG|MCG)", bps["Descrição CATMAT"][ind], flags = re.IGNORECASE).group()).group()
            bps["Quantity"][ind] = re.search("[\d.,]+", re.search("[\d.,]+\s*ML", bps[" Unidade de Fornecimento "][ind] , flags = re.IGNORECASE).group()).group()
            
        #Llenar la columna de UMC (mg)        
        elif re.search("[\d.,]+\s*(MG|MCG)", bps["Descrição CATMAT"][ind], flags = re.IGNORECASE) is not None:
            bps["UMC (mg)"][ind] = re.search("[\d.,]+", re.search("[\d.,]+\s*(MG|MCG)", bps["Descrição CATMAT"][ind], flags = re.IGNORECASE).group()).group()
        
        #Hacer la conversión de MCG a MG
        if re.search("MCG", bps["Descrição CATMAT"][ind], flags = re.IGNORECASE) is not None:
            bps["UMC (mg)"][ind] = float(bps["UMC (mg)"][ind].replace(",","."))/1000        
        
    ajustar_columnas(df=bps, columnas = ["UMC (mg)", "Quantity"]) #Ajustar la columnas UMC (mg) y Quantity
    finalBps = final(df=bps,precio="Preço Unitário", bd = "BPS (BRA)") #Crear el df final con los precios mínimos por mg

else: #Si la búsqueda no arrojo resultados, crear un df vacío
    finalBps = pd.DataFrame(columns = ["PA", "BPS (BRA)"])
    
###########################################################################################################################################################################

#---------------------------------------------------------- ECUADOR (SISTEMA DE COMPRAS PÚBLICAS)--------------------------------------------------------------------------

############################################################################################################################################################################

if len(ecu_sp) >0: #Si el df no está vacío correr el siguiente código 
    ecu_sp["PA"] = ecu_sp["Medicamento"] #Crear la columna "PA" usando la columna "Medicamento"
    ecu_sp["PA"] = ecu_sp["PA"].replace(traductor_esp) #Traducir el nombre en español al inglés
    ecu_sp = limpiar_df(df=ecu_sp,ff="Descripción",precio="Precio Ref. Unitario") #Limpiar el df
    ecu_sp.loc[ecu_sp["FF"]=="", "FF"] = "INJ" #Si la forma famacéutica queda vacía, asignar un "INJ" por defecto
    
    for ind in ecu_sp.index:
        ecu_sp["Quantity"][ind] = "1" #Los medicamentos reportados ya están en unidades de dispensación, por ende asignamos un valor default de 1
        
        #---------------------------- Llenar la columna UMC (mg) y ajustar "Quantity" en caso de que sea necesario --------------------------------------#
        
        #Medicamentos con XX MG / ML y en la columna Descripción y ML totales en la columna Descripción o Bien / Servicio, es necesario ajustar cantidades
        if re.search("[\d,.]+\s*MG\s*/\s*1*\s*ML", ecu_sp["Descripción"][ind] ,flags = re.IGNORECASE) is not None:
                ecu_sp["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*MG\s*/\s*1*\s*ML", ecu_sp["Descripción"][ind] ,flags = re.IGNORECASE).group()).group()    
                #Para los ML cogemos la mayor cantidad de ML disponible que será la correcta. Intentar por la columna Descripción y después por la columna Bien / Servicio
                if re.search("[\d,.]+\s*ML", ecu_sp["Descripción"][ind] ,flags = re.IGNORECASE) is not None:
                    ecu_sp["Quantity"][ind] = max([float(z) for z in [re.search("[\d,.]+",x).group().replace(",",".") for x in re.findall("[\d,.]+\s*ML", ecu_sp["Descripción"][ind] ,flags = re.IGNORECASE)]])                     
                elif re.search("[\d,.]+\s*ML", ecu_sp["Bien / Servicio"][ind] ,flags = re.IGNORECASE) is not None:
                    ecu_sp["Quantity"][ind] = max([float (z) for z in [re.search("[\d,.]+",x).group().replace(",",".") for x in re.findall("[\d,.]+\s*ML", ecu_sp["Bien / Servicio"][ind] ,flags = re.IGNORECASE)]])                     
                   
        #Medicamentos con XX MG / ML y en la columna Bien / Servicio y ML totales en la columna Descripción o Bien / Servicio, es necesario ajustar cantidades
        elif re.search("[\d,.]+\s*MG\s*/\s*1*\s*ML", ecu_sp["Bien / Servicio"][ind] ,flags = re.IGNORECASE) is not None:
                ecu_sp["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*MG\s*/\s*1*\s*ML", ecu_sp["Bien / Servicio"][ind] ,flags = re.IGNORECASE).group()).group()    
                #Para los ML cogemos la mayor cantidad de ML disponible que será la correcta. Intentar por la columna Descripción y después por la columna Bien / Servicio
                if re.search("[\d,.]+\s*ML", ecu_sp["Descripción"][ind] ,flags = re.IGNORECASE) is not None:
                    ecu_sp["Quantity"][ind] = max([float(z) for z in [re.search("[\d,.]+",x).group().replace(",",".") for x in re.findall("[\d,.]+\s*ML", ecu_sp["Descripción"][ind] ,flags = re.IGNORECASE)]])                     
                elif re.search("[\d,.]+\s*ML", ecu_sp["Bien / Servicio"][ind] ,flags = re.IGNORECASE) is not None:
                    ecu_sp["Quantity"][ind] = max([float (z) for z in [re.search("[\d,.]+",x).group().replace(",",".") for x in re.findall("[\d,.]+\s*ML", ecu_sp["Bien / Servicio"][ind] ,flags = re.IGNORECASE)]])                     
        
        #Para todos los demás medicamentos buscar el MG máximo en la fila. Intentar por la columna Descripción y después por la columna Bien / Servicio        
        elif re.search("[\d,.]+\s*MG", ecu_sp["Descripción"][ind] ,flags = re.IGNORECASE) is not None: #Descricpión
            ecu_sp["UMC (mg)"][ind] = max(float(y) for y in [re.search("[\d,.]+", x).group().replace(",",".") for x in re.findall("[\d,.]+\s*MG", ecu_sp["Descripción"][ind] ,flags = re.IGNORECASE)]) 
            
        elif re.search("[\d,.]+\s*MG", ecu_sp["Bien / Servicio"][ind] ,flags = re.IGNORECASE) is not None: #Bien / Servicio
            ecu_sp["UMC (mg)"][ind] =  max(float(y) for y in [re.search("[\d,.]+", x).group().replace(",",".") for x in re.findall("[\d,.]+\s*MG", ecu_sp["Bien / Servicio"][ind] ,flags = re.IGNORECASE)])
    
    ajustar_columnas(df=ecu_sp , columnas =["UMC (mg)", "Quantity"]) #Ajustar las columna UMC (mg) y "Quantity"
    finalEcuSp = final(df=ecu_sp,precio="Precio Ref. Unitario", bd ="SIST. COMPRAS PÚBLICAS (ECU)") #Crear el data frame final
        
else: #Si el la consulta no arrojó resultados, crear un df vacío
    finalEcuSp = pd.DataFrame(columns = ["PA", "SIST. COMPRAS PÚBLICAS (ECU)"])
    
###########################################################################################################################################################################

#-------------------------------------------------- ECUADOR (CONSEJO NACIONAL DE FIJACIÓN DE PRECIOS DE MEDICAMENTOS) ------------------------------------------------------

############################################################################################################################################################################

#Crear data frame de Ecuador
ecu["Precio"] = ecu.filter(regex=("Precio")) 

#Conservar las columnas relevantes
ecu.columns = map(str.upper, ecu.columns)
ecu=ecu.filter(["PRINCIPIO ACTIVO","PRIMER NIVEL DE DESAGREGACIÓN","FORMA FAMRACÉUTICA", "CONCENTRACIÓN","PRESENTACIÓN COMERCIAL","PRECIO"]) 

#Borrar las filas que no tienen información de precios
ecu.dropna(subset = ["PRECIO"], inplace=True)
ecu.drop(ecu[ecu["PRECIO"]=="-"].index,axis=0, inplace=True)

#Convertir TODA la columna de concentración a cadena de texto
ecu["CONCENTRACIÓN"] = ecu["CONCENTRACIÓN"].astype(str)

#Asignarle valores a la columna "PA"
ecu["PA"]=""

for ind in ecu.index:
    
    #Medicamentos con dos principios activos
    if (ecu["PRINCIPIO ACTIVO"][ind].count("+")==1) and (ecu["CONCENTRACIÓN"][ind].count("mg")==2):
        ecu["PA"][ind] = re.split("[+]", ecu["PRINCIPIO ACTIVO"][ind])[0] + "/" + re.split("[+]", ecu["PRINCIPIO ACTIVO"][ind])[1]    
        
    #Todos los demás medicamentos
    else:
        ecu["PA"][ind] = ecu["PRINCIPIO ACTIVO"][ind]

#Limpiar data frame
ecu = limpiar_df(df=ecu,precio="PRECIO", ff = "PRIMER NIVEL DE DESAGREGACIÓN")

#Llenar las columnas con las UMC (mg) y cantidades "Quantity"
for ind in ecu.index:

    #Medicamentos con dos principios activos
    if ecu["PA"][ind].find("/") != -1:
        ecu["UMC (mg)"][ind] = str(max([float(x) for x in [re.search("[\d.]+", y).group() for y in [z.replace(",",".") for z in re.findall("[\d,.]+\s*mg|[\d,.]+\s*mcg", ecu["CONCENTRACIÓN"][ind])]]])) 
        ecu["Quantity"][ind]="1"

    #Medicamentos XXMG/XXML
    elif (re.search("[\d,.]+\s*mg\s*/\s*[\d,.]+\s*ml", ecu["CONCENTRACIÓN"][ind]) is not None) and (re.search("[\d,.]+\s*ml", ecu["PRESENTACIÓN COMERCIAL"][ind]) is not None):
        ecu["UMC (mg)"][ind] = float(re.search("[\d,.]+" , re.search("[\d,.]+\s*mg", ecu["CONCENTRACIÓN"][ind]).group()).group().replace(",",".")) / float(re.search("[\d,.]+" , re.search("[\d,.]+\s*ml", ecu["CONCENTRACIÓN"][ind]).group()).group().replace(",","."))
        ecu["Quantity"][ind] = re.search("[\d,.]+" , re.search("[\d,.]+\s*ml", ecu["PRESENTACIÓN COMERCIAL"][ind]).group()).group() 
    
    #Medicamentos XXMG/ML
    elif (re.search("[\d,.]+\s*mg\s*/\s*ml", ecu["CONCENTRACIÓN"][ind]) is not None) and (re.search("[\d,.]+\s*ml", ecu["PRESENTACIÓN COMERCIAL"][ind]) is not None):
        ecu["UMC (mg)"][ind] = float(re.search("[\d,.]+" , re.search("[\d,.]+\s*mg", ecu["CONCENTRACIÓN"][ind]).group()).group().replace(",",".")) 
        ecu["Quantity"][ind] = re.search("[\d,.]+" , re.search("[\d,.]+\s*ml", ecu["PRESENTACIÓN COMERCIAL"][ind]).group()).group() 
    
    else:
        ecu["UMC (mg)"][ind] = ecu["CONCENTRACIÓN"][ind].split(' ')[0]
        ecu["Quantity"][ind]="1"

#Convertir las columnas Quantity  y UMC (g) a número para poder hacer cálculos
ajustar_columnas(df=ecu,columnas=["UMC (mg)","Quantity"])

#Crear el df final para ecuador
finalEcu = final(df=ecu,precio="PRECIO", bd ="CONSEJO NACIONAL DE FIJACIÓN Y REVSIÓN DE PRECIOS DE MEDICAMENTOS (ECU)")

###########################################################################################################################################

#----------------------------------------------------------UNITED KINGDOM (eMIT)-----------------------------------------------------------

###########################################################################################################################################

#Conservar y crear las columnas relevantes
uk = uk.filter(["Name & PackSize","Weighted Average Price"])

#Asignarle valores a la columna "PA"
uk["PA"]=""

for ind in uk.index:
    
    #Medicamentos con dos principios activos
    if (uk["Name & PackSize"][ind].count("micrograms")==2 or uk["Name & PackSize"][ind].count("mg")==2) and (uk["Name & PackSize"][ind].count("ml")==0) and (uk["Name & PackSize"][ind].count("/")>1): 
        uk["PA"][ind] = uk["Name & PackSize"][ind][:re.search("\s\d", uk["Name & PackSize"][ind]).start()] + " / " + re.search("\w+",re.search("/\s*\w+", uk["Name & PackSize"][ind]).group()).group()    
            
    #Todos los demás medicamentos
    else:
        uk["PA"][ind] = uk['Name & PackSize'][ind].split(' ')[0]
        
#Crear columna con principio activo "PA"
uk = limpiar_df(df=uk, precio="Weighted Average Price" , ff = "Name & PackSize")

#Crear las columnas auxiliares para facilitar los cálculos  
uk["mg"] = "" 
uk["ml"] = ""

for ind in uk.index:
    
    if re.search("Packsize\s*\d+", uk["Name & PackSize"][ind], flags = re.IGNORECASE) is not None:
        uk["Quantity"][ind] = re.search("\d+", re.search("Packsize\s*\d+", uk["Name & PackSize"][ind], flags = re.IGNORECASE).group()).group()
        
    else:
        uk["Quantity"][ind]="1"
        
    #Medicamentos con dos principios activos
    if uk["PA"][ind].find("/") != -1:
        uk["UMC (mg)"][ind] = str(max([float(x) for x in [re.search("[\d.]+", y).group() for y in [z.replace(",",".") for z in re.findall("[\d,.]+\s*mg|[\d,.]+\s*micrograms*", uk["Name & PackSize"][ind])]]])) 
        
    #Medicamentos XXMG/XXML
    elif (re.search("[\d.,]+(mg|microgram[s]*)/[\d.]+ml",uk["Name & PackSize"][ind]) is not None) and (re.search("[a-zA-Z]\s[\d.,]+[\s]*ml",uk["Name & PackSize"][ind]) is not None):
        uk["mg"][ind] = re.search("[\d,.]+", re.search("[\d.,]+(mg|microgram[s]*)/[\d.,]+ml",uk["Name & PackSize"][ind]).group()).group()
        uk["ml"][ind] = re.search("[\d,.]+", re.search("/\s*[\d,.]+\s*ml", uk["Name & PackSize"][ind]).group()).group()
        uk["UMC (mg)"][ind] = float(uk["mg"][ind])/float(uk["ml"][ind])
        uk["Quantity"][ind] = float(re.search("[\d.,]+", re.search("[a-zA-Z]\s[\d.,]+[\s]*ml",uk["Name & PackSize"][ind]).group()).group().replace(",","."))*float(uk["Quantity"][ind])

    #Medicamentos XXMG/ML
    elif (re.search("[\d.,]+(mg|microgram[s]*)/ml",uk["Name & PackSize"][ind]) is not None) and (re.search("[a-zA-Z]\s[\d.,]+[\s]*ml",uk["Name & PackSize"][ind]) is not None):
        uk["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d.,]+(mg|microgram[s]*)/ml",uk["Name & PackSize"][ind]).group()).group()
        uk["Quantity"][ind] = float(re.search("[\d.,]+", re.search("[a-zA-Z]\s[\d.,]+[\s]*ml",uk["Name & PackSize"][ind]).group()).group().replace(",","."))*float(uk["Quantity"][ind])

    #Medicamentos con un solo principio activo
    elif re.search("[\d.,]+\s*(mg|microgram)", uk["Name & PackSize"][ind]) is not None:
        uk["UMC (mg)"][ind] = re.search("[\d.,]+", re.search("[\d.,]+[\s]*(mg|microgram)",uk["Name & PackSize"][ind]).group()).group()
        
    #Realizar la conversión de unidades de microgramos a mg
    if re.search("microgram",uk["Name & PackSize"][ind]) is not None:
            uk["UMC (mg)"][ind] = float(uk["UMC (mg)"][ind])/1000
    
#Borrar la columnas auxiliares
uk.drop(columns=['mg', 'ml'],inplace=True)     

#Convertir las columnas Quantity  y UMC (g) a número para poder hacer cálculos
ajustar_columnas(df=uk,columnas=["UMC (mg)","Quantity"])

#Crear columna con precio / mg
finalUk = final(df=uk,precio="Weighted Average Price", bd ="eMIT (UK)")

###############################################################################################################################################

#--------------------------------------------------------------UNITED KINGDOM (NICE)-----------------------------------------------------------

##############################################################################################################################################

if len(nice)>0: #Corremos el siguiente código si la búsqueda arrojó al menos un resultado
    nice.replace({'‑': '-'}, regex=True , inplace = True) #Reemplazar este guión raro por un guión normal por facilidad
    nice["UMC (mg)"] = "" #Crear la columna con unidades mínimas de concentración (UMC)
    nice["Pharmaceutical Form"] = "" #Crear la columna con la forma farmacéutica original del texto
    nice["FF"] = "" #Crear la columna con la forma farmacéutica en el formato que usamos siempre "FF"
    nice["Quantity"] = "" #Crear la columna con cantidades "Quantity"
    nice["Price"] = "" #Crear una columna con el precio "Price"
    nice["Precio UMC (mg)"] = "" #Crear la columna con el precio por UMC (mg), en este caso, de una vez será con el precio mínimo

    for ind in nice.index: #Iterar sobre las filas del df
    
        #Llenar la fila con un valor "default" de precio y cantidad (manejaremos de ahora en adelante todo como listas, pues en cada fila puede haber más de un medicamento)
        if re.search("\s£\s*[\d,.]+", nice["Medicamento"][ind]) is not None:
            nice["Price"][ind] = [float(y) for y in [x.replace("£","").replace(",","") for x in re.findall("\s£\s*[\d,.]+", nice["Medicamento"][ind])]]
        
        elif re.search("\A£\s*[\d,.]+", nice["Medicamento"][ind]) is not None:
            nice["Price"][ind] = [float(y) for y in [x.replace("£","").replace(",","") for x in re.findall("\A£\s*[\d,.]+", nice["Medicamento"][ind])]]
        
        nice["Quantity"][ind] = [1] #Por defecto asignamos un valor de uno (este puede ser cambiado dependiendo del caso)
    
        try: #Llenar la columna "Pharmaceutical form" con las palabras clave: syringe, vial, injection, tablet, capsule, oral solution
            nice["Pharmaceutical Form"][ind] = re.search("syringe|vial|injection|tablet|capsule|oral solution", nice["Medicamento"][ind], flags = re.IGNORECASE).group()
        
        except AttributeError: #Si no encuentra la FF, concatenar todas las oraciones anteriores que estaban antes del precio y volver a intentar
            nice["Medicamento"][ind] = nice["Resultado"][ind][:nice["Resultado"][ind].find(nice["Medicamento"][ind])] + nice["Medicamento"][ind]
            nice["Pharmaceutical Form"][ind] = re.search("syringe|vial|injection|tablet|capsule|oral solution", nice["Medicamento"][ind], flags = re.IGNORECASE).group()
               
        #Llenar la columna "FF" con el formato estándar que hemos definido para todos los df: TAB, INJ, SOL ORAL
        if re.search("tablet|capsule", nice["Pharmaceutical Form"][ind], flags=re.IGNORECASE) != None: nice["FF"][ind] = "TAB"
        if re.search("syringe|vial|injection", nice["Pharmaceutical Form"][ind], flags=re.IGNORECASE) != None: nice["FF"][ind] = "INJ"
        if re.search("oral solution", nice["Pharmaceutical Form"][ind], flags=re.IGNORECASE) != None: nice["FF"][ind] = "SOL ORAL"
        
        #Llenar la columna con las UMC (mg)
        if re.search("\d+\s*×\s*[\d,.]+.mg|[\d,.]+[\s-]*mg|[\d,.]+[\s-]*microgram|[\d,.]+[\s-]*mcg", nice["Medicamento"][ind], flags = re.IGNORECASE) is not None:
            #Buscar XX mg o XX microgram y eliminar comas y espacios
            nice["UMC (mg)"][ind] = [x.replace(" ", "").replace(",","") for x in re.findall("\d+\s*×\s*[\d,.]+.mg|[\d,.]+[\s-]*mg|[\d,.]+[\s-]*microgram|[\d,.]+[\s-]*mcg", nice["Medicamento"][ind], flags = re.IGNORECASE)]
            #Hacer las multplicaciones correspondientes (cuando hay un "×")
            nice["UMC (mg)"][ind] = [float(y[:y.find("×")])*float(y[y.find("×")+1:re.search("-|m", y).start()]) if y.find("×") != -1 else float(re.search("[\d.]+", y).group()) for y in nice["UMC (mg)"][ind]]
            #Convertir de microgramos a miligramos
            unidades = [1000 if re.search("micro|mcg", z) is not None else 1 for z in re.findall("mg|microgram|mcg",nice["Medicamento"][ind])]
            nice["UMC (mg)"][ind] = [i/j for i,j in zip(nice["UMC (mg)"][ind],unidades)]
            
        elif nice["PA"][ind]=="GUSELKUMAB": #Tremfya no tiene UMC, entonces las llenamos a mano
            nice["UMC (mg)"][ind] = [100] 
    
        elif nice["PA"][ind]=="ABIRATERONE": #Zytiga no tiene UMC, entonces las llenamos a mano
            if re.search("\s120\stab", nice["Medicamento"][ind]) is not None:
                nice["UMC (mg)"][ind] = [250] #250 mg si la cantidad de tabletas es 120
            elif re.search("\s60\stab", nice["Medicamento"][ind]) is not None:
                nice["UMC (mg)"][ind] = [500] #500 mg si la cantidad de tabletas es 120
    
        #Para todos los demás medicamentos (no inyectables) buscamos la cantidad de tabletas y asignamos este valor a la columna "Quantity"    
        if (re.search("\d+.tab|\d+.cap|\d+.tab|\d+.pack", nice["Medicamento"][ind]) is not None) and (nice["FF"][ind] != "INJ"):       
            nice["Quantity"][ind] = [re.search("[\d,.]+", x).group() for x in re.findall("\d+.tab|\d+.cap|\d+.tab|\d+.pack",nice["Medicamento"][ind], flags = re.IGNORECASE)]  
    
        #Medicamentos en XXMG/ML, llenamos la columna "Quantity" utilizando la cantidad de mililitros 
        elif (re.search("/\s*ml", nice["Medicamento"][ind], flags = re.IGNORECASE) is not None) and (re.search("[\d,.]+\s*ml", nice["Medicamento"][ind], flags = re.IGNORECASE) is not None):       
            nice["Quantity"][ind] = [re.sub("\sml", "", x, flags = re.IGNORECASE) for x in re.findall("[\d,.]+\s*ml", nice["Medicamento"][ind], flags = re.IGNORECASE)]
        
        #Si el tamaño de la lista UMC (mg) y la lista "Price" no coinciden, igualar los tamaños para poder realizar operaraciones matemáticas entre ellas
        if len(nice["UMC (mg)"][ind])>len(nice["Price"][ind]):
            nice["Price"][ind] = nice["Price"][ind]*len(nice["UMC (mg)"][ind])

        #Si el tamaño de la lista UMC (mg) y la lista "Quantity" no coinciden, igualar los tamaños para poder realizar operaraciones matemáticas entre ellas
        if len(nice["UMC (mg)"][ind])> len(nice["Quantity"][ind]):
            nice["Quantity"][ind] = nice["Quantity"][ind]*len(nice["UMC (mg)"][ind])

        #Multiplicar los elementos de las listas "Quantity" y "UMC (mg)"
        nice["Precio UMC (mg)"][ind] = [float(x)*float(y) for x,y in zip(nice["Quantity"][ind], nice["UMC (mg)"][ind])]
        #Dividir los elementos de las lista "Price" entre la mutliplicación que realizamos en la línea anterior y que había quedado guardada en "Price UMC (mg)"
        nice["Precio UMC (mg)"][ind] = min([float(i)/float(j) for i,j in zip(nice["Price"][ind], nice["Precio UMC (mg)"][ind])]) #Utilizamos el mínimo para quedarnos de una vez con el valor mínimo de la lista
    
    #Creamos el df final: finalNice
    finalNice = nice.groupby(['PA', "FF"])["Precio UMC (mg)"].min().reset_index().rename(columns={"Precio UMC (mg)": "NICE (UK)"})
    finalNice["PA"] = finalNice["PA"] + " - " + finalNice["FF"]
    finalNice.drop(columns=['FF'],inplace=True)

else: #Si el df está vacío crear el df final vacío
    finalNice = pd.DataFrame(columns = ["PA", "NICE (UK)"])

###################################################################################################################################################

#----------------------------------------------------------UNITED KINGDOM (NHS)--------------------------------------------------------------------

###################################################################################################################################################

#-----------EL PAQUETE TABULA NO LEE LAS TABLAS CORRECTAMENTE!!----------------

nhs_txt = [] #Después de convertir el .pdf a .txt, en esta lista guardaremos todas la líneas de texto del pdf. en el intervalo de interes 
iniciar = False #Con este booleano, guardarmeos sólo las lineas a partir de las cuales aparecen medicamentos
medicamentos_nhs = [] #En esta lista guardermos todas la líneas de texto con precios

# 1) Cargar el PDF
with open(carpeta + "nhs.pdf", "rb") as f:
    pdf = pdftotext.PDF(f)
 
# 2) Guardar todo el texto en un archivo .txt llamado nhs.txt
with open(carpeta + 'nhs.txt',"w", encoding="utf-8") as f:
    f.write("\n\n".join(pdf))

# 3) leemos el archivo .txt y guardamos las lineas a partir de las cuales aparecen los medicamentos
with open ('nhs.txt', 'rt',encoding="utf-8") as myfile: 
    for line in myfile: #Iterar sobre las líneas del.txt
        if all([x in line for x in ["Drug", "Quantity","Basic Price", "Category"]])==True:
            iniciar = True #Si todas las palabras anteriores se encuentran, iniciar = True 
        
        if line.find("Part VIIIB") != -1 and iniciar == True: #Si ya se llega a la otra sección, parar de concatenar líneas
            break
        
        #Concatenar línea por línea, pero evitando ciertas líneas que generan ruido
        if iniciar == True and len(line.replace(" ","").replace("\n",""))!=3 and re.search("(Part|Drug)", line, flags=re.IGNORECASE) is None and line != "\n": 
            nhs_txt.append(line)

#Llenar la lista de precios de acuerdo a la siguiente expresión regular
for i in range(0,len(nhs_txt)):
    if re.search("\d+\s+(L|K|A|M|C)", nhs_txt[i]) is not None:
        medicamentos_nhs.append(nhs_txt[i][re.search("[^\s]", nhs_txt[i]).start():])
        
#Crear el df prinicipal con la misma cantidad de filas que la lista medicamentos_nhs        
nhs = pd.DataFrame(index=np.arange(len(medicamentos_nhs)), columns = ["Drug", "Basic Price", "PA", "Quantity", "Price", "UMC (mg)"])
for ind in nhs.index: #Iterar sobre las filas del df
    #Limpiar la línea con el nombres del medicamentos
    nhs["Drug"][ind] = medicamentos_nhs[ind][:re.search("\s\s\s", medicamentos_nhs[ind]).start()].replace(")","").replace("(","")
    #Limpiar la línea con el precio del medicamento
    nhs["Basic Price"][ind] = medicamentos_nhs[ind][re.search("\s\s\s\w", medicamentos_nhs[ind]).start():]
    if re.search("[A-Z]\w+" , nhs["Basic Price"][ind]) is not None: #Volver a limpiar con el fin de quitar los datos del fabricante
        nhs["Basic Price"][ind] = nhs["Basic Price"][ind][:re.search("(L|K|A|M|C)", nhs["Basic Price"][ind]).start()]
    
    iniciar = False #Con este booleano sabremos cuando empezar a iterar
    for line in nhs_txt: #Completar los datos de los medicamentos que quedaron truncados
        if medicamentos_nhs[ind] == line[re.search("[^\s]", line).start():]: #Si la línea coincide empezar a concatenar
            iniciar = True
            continue
        
        line = line.replace(")","").replace("(","") #Eliminar paréntesis, ya que la expresión regular arroja error
        if iniciar == True and re.search("\d+\s+(L|K|A|M|C)", line) is None: #Concatenar la línea al medicamento que apareció truncado
           nhs["Drug"][ind] = nhs["Drug"][ind] + " " + line[re.search("[^\s]", line).start():]
        
        if iniciar == True and re.search("\d+\s+(L|K|A|M|C)", line) is not None: #Si ya encuentra el precio del siguiente medicamento para de iterar
            break

    if re.search("\s\s\s\s", nhs["Drug"][ind]) is not None: #Eliminar espacios excesivos en la fila
        nhs["Drug"][ind] = nhs["Drug"][ind][:re.search("\s\s\s\s", nhs["Drug"][ind]).start()]
        
    if len(re.findall("\d+" , nhs["Basic Price"][ind]))==1: #Si quedo el sólo el precio, hay información de las concentraciones en la columna Basic Price
        nhs["Drug"][ind] = re.sub("\s[A-Z]\w+","", nhs["Drug"][ind]) #Eliminar ruido de la fila
        nhs["Basic Price"][ind] = nhs["Drug"][ind] + nhs["Basic Price"][ind] #Concatenar la información faltante desde la columna "Drug"
        nhs["Drug"][ind] = "" #Asignar un valor vacío, ya que después haremos un ffill caserito
        
    elif len(nhs["Drug"][ind]) < 6: #Si la fila tiene muy pocos caracteres esto es un error
        nhs["Drug"][ind] = "" #Asignar un valor vacío, ya que después haremos un ffill caserito
        
    if nhs["Drug"][ind] == "": #Hacer el ffill caserito
        nhs["Drug"][ind] = nhs["Drug"][ind-1]
        
    nhs["PA"][ind] = re.findall("[A-Z]\w+.[a-z]*", nhs["Drug"][ind]) #Asignar valores a la columna "PA" de acuerdo a una expresión regular
    nhs["PA"][ind] = [w[:-1] if w[-1] == " " else w for w in nhs["PA"][ind]] #Eliminar espacios vacíos que quedaron al final de cada palabra
    if len(re.findall("[\d,.]+\s*(g|mg|micro)", nhs["Drug"][ind]))==2 and len(nhs["PA"][ind]) == 2: #Si el medicamento tiene dos principos activos, concatenar con un "/" en el medio
        nhs["PA"][ind] = nhs["PA"][ind][0] + " / " + nhs["PA"][ind][1]
        
    else: #Si sólo tiene un PA, quedarse con el primer elemento de la lista
        nhs["PA"][ind] = nhs["PA"][ind][0]
 
nhs = limpiar_df(df=nhs, precio = "Price", ff = "Drug") #Limpiar el df para convservar sólo los medicamentos de interés
       
for ind in nhs.index: #Iterar nuevamente para halla UMC (mg) y Quantity   
    nhs["Quantity"][ind] = re.findall("[\d,.]+", nhs["Basic Price"][ind])[0] #La cantidad es el primer elemento de la la columna Basic Price
    nhs["Price"][ind] = float(re.findall("[\d,.]+", nhs["Basic Price"][ind])[-1])/100 #El precio es el último elemento de la columna Basic Price / 100 (de acuerdo a la guía)
    
    if nhs["PA"][ind].find("/") != -1: #Si el medicamento tiene doble principio activo 
        nhs["UMC (mg)"][ind] = re.findall("[\d,.]+\s*g|[\d,.]+\s*mg|[\d,.]+\s*micro", nhs["Drug"][ind]) #Hallar todas las UMC existentes
        unidades = [re.search("[a-z]+",x).group() for x in nhs["UMC (mg)"][ind]] #Guardar la unidades de UMC (mg,g microgramo)
        nhs["UMC (mg)"][ind] = [float(y) for y in [re.search("[\d,.]+", x).group() for x in nhs["UMC (mg)"][ind]]] #Convertir las UMC a flotante
        for i in range(0, len(unidades)): #Realizar la conversión de unidades de microgramos a mg o de g a mg
            if unidades[i] == "micro": #MCG
                nhs["UMC (mg)"][ind][i] = nhs["UMC (mg)"][ind][i] / 1000
            elif unidades[i] == "g": #G
                nhs["UMC (mg)"][ind][i] = nhs["UMC (mg)"][ind][i] * 1000
               
        nhs["UMC (mg)"][ind] = max(nhs["UMC (mg)"][ind]) #Conservar el valor máximo de las UMC disponibles         
               
    elif re.search("[\d,.]+\s*mg|[\d,.]+\s*micro|[\d,.]+\s*g|[\d,.]+\s*%", nhs["Drug"][ind]) is not None: #Si el medicamento sólo tiene un "PA"
        nhs["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*mg|[\d,.]+\s*micro|[\d,.]+\s*g|[\d,.]+\s*%", nhs["Drug"][ind]).group()).group() #Buscar las UMC
        if nhs["Drug"][ind].find("micro") != -1: #Si el medicamento tiene microgramos, realizar la conversión de unidades
            nhs["UMC (mg)"][ind] = float(nhs["UMC (mg)"][ind])/1000
        
        elif re.search("[\d,.]+\s*g", nhs["Drug"][ind]) is not None: #Si el medicamento tiene gramos, realizar la conversión de unidades
            nhs["UMC (mg)"][ind] = float(nhs["UMC (mg)"][ind])*1000
    
    #Si el medicamento no tiene UMC en la columna Drug, es posible que la tenga en la columna Basic Price
    elif re.search("[\d,.]+\s*mg|[\d,.]+\s*micro|[\d,.]+\s*g|[\d,.]+\s*%", nhs["Basic Price"][ind]) is not None:
        nhs["UMC (mg)"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*mg|[\d,.]+\s*micro|[\d,.]+\s*g|[\d,.]+\s*%", nhs["Basic Price"][ind]).group()).group()
     
    #Si el medicamento está en XXMG/XXML dividir las cantidades de la columna UMC (mg) entre los ML totales que se encuentran en la columna Basic price    
    if (re.search("[\d.,]+\s*(mg|micro|g)\s*/\s*[\d,.]+\s*ml", nhs["Drug"][ind]) is not None) and (re.search("[\d,.]+\s*ml", nhs["Basic Price"][ind]) is not None):    
       nhs["UMC (mg)"][ind] = float(nhs["UMC (mg)"][ind]) / float(re.search("[\d,.]+", re.search("[\d,.]+\s*ml", nhs["Drug"][ind]).group()).group()) 
       

#Ajustar las colummnas para poder hacer cálculos   
ajustar_columnas(df=nhs, columnas=["UMC (mg)","Quantity"])

#Crear df final
finalNHS = final(df=nhs, precio ="Price", bd= "NHS (UK)")

       
############################################################################################################################################################################

#-----------------------------------------------------------------CANADA-----------------------------------------------------------------------------------------------------------

#############################################################################################################################################################################

#-------------------------------------------Definir los patrones de texto y variables que usaremos para leer el archivo .txt-------------------------------------------------------------

#ESTE PDF ESTA ENCRIPTADO POR ENDE PAQUETES COMO PYPDF NO SIRVEN / EL PAQUETE TABULA TAMPOCO LEE LAS TABLAS CORRECTAMENTE

#1) El fránces tiene tres tipos de acento la vocales (grave, agudo y circunflejo) que son importantes para las letras minúsculas y mayúsculas
acentos_may = "ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛ"
acentos_min = "áéíóúàèìòùâêîôû"

#2) TODAS las líneas con medicamentos tienen el mismo patrón: Ej: 10 ml     2000.00   EL PRECIO SIEMPRE TIENE DOS DECIMALES. Este patrón es nuestro punto de partida para los otros dos. 
medicamento_can = re.compile("[\d]+\s*[áéíóúàèìòùâêîôûa-zA-ZÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛ]*[\s]+[\d]+(,|[.])[\d]{2}") 

#3) TODAS las líneas con el nombre de las moléculas son palabra(s) siempre en mayúsculas.    
moleculas_can = re.compile("\A.[ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛA-Z][ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛA-Z]\w+")

#4) TODAS las líneas con las concentraciones de las moléculas son palabra(s) que comienzan por mayúsculas y luego minúsculas.    
concentracion_can = re.compile("\A[ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛA-Z][áéíóúàèìòùâêîôûa-z][.\w]*|\A\s[ÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛA-Z][áéíóúàèìòùâêîôûa-z][.\w]*")

#5) Quitamos estos patrones que están atravesados y estorban.
errores = re.compile("\AAnnexe|\APage|ANNEXE|Légende|Symboles")

#6) Definimos las siguientes listas, donde guardaremos la info. de: concentraciones, medicamentos y moléculas (respectivamente). La variable contador almacena la línea de texto del archivo .txt
canada_txt = []
lista_concentracion = []
lista_medicamentos = []
lista_moleculas = []                            
contador = 0

#--------------------------------------------------------------------Cargar y leer el archivo .txt-------------------------------------------------------------
# 1) Cargar el PDF
with open(carpeta + "can.pdf", "rb") as f:
    pdf = pdftotext.PDF(f)
 
# 2) Guardar todo el texto en un archivo .txt llamado canada.txt
with open(carpeta + 'canada.txt',"w", encoding="utf-8") as f:
    f.write("\n\n".join(pdf))

# 3) leemos el archivo .txt y guardamos los medicamentos en la lista usando nuestro patrón. Este será nuestro punto de partida porque este patrón es ÚNICO en todo el archivo
with open ('canada.txt', 'rt',encoding="utf-8") as myfile: 
    for line in myfile:
        if (medicamento_can.search(line) != None):
               lista_medicamentos.append([contador,line.rstrip('\n')])
        
        canada_txt.append(line)       
        contador = contador + 1    

# 4) leemos el archivo .txt nuevamente y guardamos las concentraciones y moléculas usando nuestro patrón JUSTO ANTES de la línea donde aparece el primer medicamento.
#El nombre de la primera molécula está (4) líneas atrás del primer medicamento y la concentración (2) líneas atrás del primer medicamento. 
contador = 0        
with open ('canada.txt', 'rt',encoding="utf-8") as myfile:
    for line in myfile:
        if (moleculas_can.search(line)!=None) and (errores.search(line)==None) and (contador>=lista_medicamentos[0][0]-4):
            lista_moleculas.append([contador,line.rstrip('\n')])
        
        if (concentracion_can.search(line)!=None) and (errores.search(line)==None) and (contador>=lista_medicamentos[0][0]-2):
            lista_concentracion.append([contador,line.rstrip('\n')])
        
        contador = contador + 1
#--------------------------------------------------------------------Construir el Data Frame para Cánada-------------------------------------------------------------

#Convertir las listas en Data Frames para que sea más fácil acceder a estos a la par que llenamos el df de Cánada
lista_moleculas = pd.DataFrame(lista_moleculas, columns = ["Indice","Moleculas"] ).set_index('Indice')        
lista_concentracion = pd.DataFrame(lista_concentracion, columns = ["Indice","Concentracion"] ).set_index('Indice')        

#Establecemos la forma y columnas del data frame de Cánada, sabemos que existen N medicamentos de acuerdo al tamaño de lista medicamentos         
can = pd.DataFrame(index=np.arange(len(lista_medicamentos)), columns=["Line","Code","Médicament","Trade Name","Format","Price","Pack Size","Concentration","PA","FF","UMC (mg)","Quantity","Precio UMC (mg)"])

#Iniciamos un bucle de tal manera que vamos llenado cada observación con la info. de nuestras listas.
for ind in can.index:
    
    #La línea con respecto al archivo .txt original
    can["Line"][ind] = lista_medicamentos[ind][0]
    
    #Código
    if re.search("\d{8}", lista_medicamentos[ind][1]) is not None:
        occ = re.search("\d{8}", lista_medicamentos[ind][1]).span()[0]
        lista_medicamentos[ind][1] = lista_medicamentos[ind][1].replace(" ","", occ) 
        can["Code"][ind] = re.search("\d{8}", lista_medicamentos[ind][1]).group()
        codigo = can["Code"][ind]
        start = re.search("\d{8}", lista_medicamentos[ind][1]).span()[1]
    
    else:
        can["Code"][ind] = codigo
        start = 0
        
    #Pack Size & Quantity
    if re.search("\s\s\d+\s\s", lista_medicamentos[ind][1]) is not None:
        can["Pack Size"][ind] = re.search("\s\s\d+\s\s", lista_medicamentos[ind][1]).group().replace(" ","")
        end = re.search("\s\s\d+\s\s", lista_medicamentos[ind][1]).span()[0]
    
    elif re.search("\s\s\d+(,\d)*\s(ml|mg|mcg|g)", lista_medicamentos[ind][1], flags = re.IGNORECASE) is not None:
        can["Pack Size"][ind] =  re.search("\s\s\d+(,\d)*\s(ml|mg|mcg|g)",lista_medicamentos[ind][1], flags = re.IGNORECASE).group().replace(" ", "",2)
        end = re.search("\s\s\d+(,\d)*\s(ml|mg|mcg|g)",lista_medicamentos[ind][1], flags = re.IGNORECASE).span()[0]
    
    #Trade Name
    if start !=0: 
        #can["Trade Name"][ind] = lista_medicamentos[ind][1][start:end]
        can["Trade Name"][ind] = re.sub("(\s\s)+","",can["Trade Name"][ind])
        nombre = can["Trade Name"][ind]
        
    else:
        can["Trade Name"][ind]= nombre
    
    #Precio
    if re.search("[\d]+(,|[.])+[\d]{2}", lista_medicamentos[ind][1]) is not None:
        can["Price"][ind] = re.search("[\d]+(,|[.])+[\d]{2}", lista_medicamentos[ind][1]).group().replace(",",".")        
    
    #Médicament y principio activo "PA"
    try:
        can["Médicament"][ind] = lista_moleculas["Moleculas"][lista_medicamentos[ind][0]-4]
        if re.search("\s[A-Z]|\s:", can["Médicament"][ind][-2:]) is not None:
            can["Médicament"][ind] = can["Médicament"][ind][:-2]
                
        if re.search("\s[(]", can["Médicament"][ind]) is not None:
            can["PA"][ind] = can["Médicament"][ind][:re.search("\s[(]", can["Médicament"][ind]).start()]
        
        elif (can['Médicament'][ind].count("/") == 1):
            can["PA"][ind] = re.split("/",can["Médicament"][ind])[0] + " /" + re.split("/",can["Médicament"][ind])[1]
        
        else:
            can["PA"][ind] = can['Médicament'][ind]
        
        molecula = can["Médicament"][ind]
        principio_activo = can["PA"][ind]
    
    except KeyError:
        can["Médicament"][ind] = molecula
        can["PA"][ind] = principio_activo
    
    #Formato "Format" y Concentración "Concentration"    
    try:
        can["Format"][ind] = lista_concentracion["Concentracion"][lista_medicamentos[ind][0]-2][:lista_concentracion["Concentracion"][lista_medicamentos[ind][0]-2].find("  ")].replace("  ","")
        can["Concentration"][ind] = lista_concentracion["Concentracion"][lista_medicamentos[ind][0]-2][lista_concentracion["Concentracion"][lista_medicamentos[ind][0]-2].find("  ")+1:].replace("  ","")
        formato = can["Format"][ind]
        concentracion = can["Concentration"][ind]
                        
    except KeyError:
        can["Format"][ind] = formato
        can["Concentration"][ind] = concentracion
    
#----------------------Una vez llena la info. original del texto, es necesario llenar las columnas que necesitamos: "UMC (mg)","FF","Quantity"-------------------------------------------------- 
    
    #Asignarle valores  a la columna "FF"
    if re.search("Co.|Caps", can["Format"][ind], flags=re.IGNORECASE) != None: can["FF"][ind] = "TAB"
    if re.search("inj|perf|sir.", can["Format"][ind], flags=re.IGNORECASE) != None: can["FF"][ind] = "INJ"
    if re.search("timbre", can["Format"][ind], flags=re.IGNORECASE) != None: can["FF"][ind] = "PATCH"
    if re.search("(Susp|Sol|Pd). Orale", can["Format"][ind], flags=re.IGNORECASE) != None: can["FF"][ind] = "SOL ORAL"
    
    #Medicamentos con dos principios activos
    if can["PA"][ind].find("/")!=-1:
        #Si el medicamento tiene XX MG / XX ML con ML totales en la columna "Pack Size"
        if (re.search("[\d,.]+\s*mg\s*/\s*[\d,.]+\s*ml", can["Concentration"][ind] , flags = re.IGNORECASE) is not None) and (re.search("[\d,.]+\s*ml", can["Pack Size"][ind], flags = re.IGNORECASE) is not None):
            umc_1 = (float(re.search("[\d,.]+", re.search("[\d,.]+\s*mg\s*/\s*[\d,.]+\s*ml", can["Concentration"][ind] , flags = re.IGNORECASE).group()).group().replace(",",".")) / float(re.search("[\d,.]+", re.search("/\s*[\d,.]+\s*ml", can["Concentration"][ind], flags = re.IGNORECASE).group()).group().replace(",","."))) * float(re.search("[\d,.]+", re.search("[\d,.]+\s*ml", can["Pack Size"][ind], flags = re.IGNORECASE).group()).group().replace(",","."))
            umc_2 = float(re.search("[\d,.]+", re.search("[\d,.]+\s*mg|[\d,.]+\s*mcg|[\d,.]+\s*g|[\d,.]+\s*%", can["Concentration"][ind]).group()).group().replace(",","."))
            can["UMC (mg)"][ind] = max(umc_1, umc_2)
            can["Quantity"][ind] = 1
        
        else:
            can["UMC (mg)"][ind] = str(max([float(x) for x in [re.search("[\d.]+", y).group() for y in [z.replace(",",".") for z in re.findall("[\d,.]+\s*mg|[\d,.]+\s*mcg|[\d,.]+\s*g|[\d,.]+\s*%", can["Concentration"][ind])]]])) 
            can["Quantity"][ind] = can["Pack Size"][ind]
    
    #Medicamentos con unidades XXMG/ML y cantidades totales ml en la columna Pack Size
    elif (re.search("[\d,.]+\s*(mg|mcg)/\s*ml", can["Concentration"][ind], flags = re.IGNORECASE) is not None) and (re.search("[\d,.]+\s*ml", can["Pack Size"][ind], flags = re.IGNORECASE) is not None):
        can["UMC (mg)"][ind] = re.search("[\d,.]+", can["Concentration"][ind]).group()
        can["Quantity"][ind] = re.search("[\d,.]+", can["Pack Size"][ind]).group()
        
    #Medicamentos con unidades XXMG/ML y cantidades totales ml en la columna Concentration. El número de jeringas está en la columa "Pack Size"
    elif (re.search("[\d,.]+\s*(mg|mcg)/\s*ml", can["Concentration"][ind], flags = re.IGNORECASE) is not None) and (re.search("[\d,.]+\s*ml", can["Concentration"][ind], flags = re.IGNORECASE) is not None):
        can["UMC (mg)"][ind] = re.search("[\d,.]+", can["Concentration"][ind]).group()
        can["Quantity"][ind] = float(re.search("[\d,.]+",re.search("[\d,.]+\s*ml", can["Concentration"][ind], flags = re.IGNORECASE).group()).group().replace(",","."))*float(can["Pack Size"][ind])
        
    #Medicamentos con unidades XXMG/XXML y cantidades totales ml en la columna Pack Size
    elif (re.search("[\d,.]+\s*(mg|mcg)/\s*[\d,.]+\s*ml", can["Concentration"][ind], flags = re.IGNORECASE) is not None) and (re.search("[\d,.]+\s*ml", can["Pack Size"][ind], flags = re.IGNORECASE) is not None):
        can["UMC (mg)"][ind] = (float(re.search("[\d,.]+", can["Concentration"][ind]).group().replace(",",".")))/(float((re.search("[\d,.]+", re.search("[\d,.]+\s*m(l|L)",can["Concentration"][ind]).group()).group().replace(",","."))))
        can["Quantity"][ind] = re.search("[\d,.]+", re.search("[\d,.]+\s*ml", can["Pack Size"][ind], flags = re.IGNORECASE).group()).group()
              
    #Medicamentos con unidades XXMG/XXML, cuyas cantidades totales ya están epxresadas en XXMG 
    elif (re.search("[\d,.]+\s*(mg|mcg)/\s*[\d,.]+ml", can["Concentration"][ind], flags = re.IGNORECASE) is not None) and (re.search("[\d,.]+\s*ml", can["Pack Size"][ind], flags = re.IGNORECASE) is None):
        can["UMC (mg)"][ind] = re.search("[\d,.]+", can["Concentration"][ind]).group()
        can["Quantity"][ind] = can["Pack Size"][ind]
        
    #Medicamentos con unidades mg|mcg (tabletas, parches, etc) y cantidades totales en la columna Packsize
    elif re.search("[\d,.]+\s*(mg|mcg|g)", can["Concentration"][ind], flags = re.IGNORECASE) is not None:
        can["UMC (mg)"][ind] = re.search("[\d,.]+", can["Concentration"][ind]).group()
        can["Quantity"][ind] = can["Pack Size"][ind]
        
    #Convertir las unidades de los medicamentos en mcg o gramos a mg
    if re.search("mcg", can["Concentration"][ind]) is not None:
            can["UMC (mg)"][ind] = float(can["UMC (mg)"][ind].replace(",","."))/1000
    
    elif re.search("(\d|\s)g", can["Concentration"][ind]) is not None:
            can["UMC (mg)"][ind] = float(can["UMC (mg)"][ind].replace(",","."))*1000

    #Algunos medicamentos pueden tener cantidades adicionales con un número entre paréntesis en la columna "Format"
    if re.search("[(]\d+[)]", can["Format"][ind]) is not None:
        can["Quantity"][ind] = float(can["Quantity"][ind])*float(re.search("\d+", re.search("[(]\d+[)]", can["Format"][ind]).group()).group())
    
    #Las pastillas anticonceptivas tienen la información de cantidades en la columna "Trade Name" y pueden ser 21 o 28 pastillas
    elif re.search("[^\d]21[^\d]|[^\d]28[^\d]", can["Trade Name"][ind]) is not None:
        can["Quantity"][ind] = float(can["Quantity"][ind])*float(re.search("21|28", re.search("[^\d]21[^\d]|[^\d]28[^\d]", can["Trade Name"][ind]).group()).group())
        
#Conservar los medicamentos de Janssen
can.fillna("",inplace=True)            
can=limpiar_df(df=can,precio="Price", ff="FF")

#Ajustar las colummnas para poder hacer cálculos   
ajustar_columnas(df=can, columnas=["UMC (mg)","Quantity","Price"])

#Crear df final
finalCan = final(df=can, precio ="Price", bd= "RAMQ (CAN)")

############################################################################################################################################################################

#-----------------------------------------------------------------ALEMANIA-----------------------------------------------------------------------------------------------------------

#############################################################################################################################################################################

#Leer todas las tablas del archivo .pdf
ger_df = tabula.read_pdf("ger.pdf", pages='all')
ger = pd.DataFrame() #Crear un df vacío principal

for df in ger_df: #Iterar sobre la lista de df
    df.columns = [*range(0, len(df.columns))] #Asignar números a las columnas  
    df = df.dropna(subset=[0]) #Borrar filas vacías
    df = df[~df.iloc[:,0].str.contains("[a-z]", regex = True)] #Eliminar filas incorrectas
    df.dropna(axis=1, how='all', inplace = True) #Eliminar columna con todos los valores en NaN 
    ger = ger.append(df) #Concatenar cada df axuliar al df principal

#Agregar las etiquetas a las columnas
ger.columns = ["Descripción", "Código", "Paquete", "Precio_1" , "Precio_2", "Diferencia",  "Conc.1", "Conc.2", "Presentación", "Grupo","Nível"]
ger.reset_index(inplace = True, drop = True) #Resetear el índice del df

#Eliminar información innecesaria de la columna Grupo
ger["Grupo"] = ger["Grupo"].apply(lambda x: x[: x.find(", ")] if x.find(", ") !=-1 else x)

#Convertir la columna la columna Paquete a string
ger["Paquete"] = ger["Paquete"].astype(str) 

#Arreglar las columnas con el precio y el paquete pues pueden tener errores
ger["Precio_1"] = ger.apply(lambda x: x["Paquete"][x["Paquete"].find(" ")+1:] if x["Paquete"].find(" ") !=-1 else x["Precio_1"], axis = 1)
ger["Paquete"] = ger["Paquete"].apply(lambda x: x[:x.find(" ")] if x.find(" ") !=-1 else x)

#Conservar el mínimo precio reportado
ger["Precio_1"] = ger["Precio_1"].str.replace(",",".").astype(float) 
ger["Precio_2"] = ger["Precio_2"].str.replace(",",".").astype(float) 
ger['Precio'] = ger[['Precio_1','Precio_2']].min(axis=1)

#Agregar un espacio al final de la columna Presentación
ger["Presentación"] = ger["Presentación"].apply(lambda x: x + " ")

#Crear dos df auxiliares cada uno guardará una columna de principio actvo diferentes
ger1 = ger.copy()
ger2 = ger.copy()

#Crear la columna con el principio activo PA para ger1 y ger2
ger1["PA"] = "" #Ger1
for ind in ger1.index: #Llenar los valores con el PA
    if re.search("\s\d",ger1["Descripción"][ind]) is not None:
        ger1["PA"][ind] = ger1["Descripción"][ind][:re.search("\s\d", ger1["Descripción"][ind]).start()]
        
    else:
        ger1["PA"][ind] = ger1["Descripción"][ind]
        
ger2["PA"] = ger2["Grupo"] #Ger2 
        
#Conservar los medicamentos de interés
ger1 = limpiar_df(df=ger1,precio="Precio", ff = "Presentación")
ger2 = limpiar_df(df=ger2,precio="Precio", ff = "Presentación")

ger = ger1.append(ger2).reset_index(drop = True) #Fundir los dos df auxiliares en uno nuevo y resetear el índice

#Llenar las columnas de UMC (mg) y Quantity
for ind in ger.index:
    ger["Quantity"][ind] = ger["Paquete"][ind]
    
    if re.search("[\d,.]+\s*MG", ger["Descripción"][ind]) is not None:
        ger["UMC (mg)"][ind] = re.search("[\d,.]+" , re.search("[\d,.]+\s*MG", ger["Descripción"][ind]).group()).group()
        
    else:
        ger["UMC (mg)"][ind] = ger["Conc.1"][ind]
        
#Ajustar las colummnas para poder hacer cálculos   
ajustar_columnas(df=ger, columnas=["UMC (mg)","Quantity","Precio"])

#Crear df final
finalGer = final(df=ger, precio ="Precio", bd= "DIMDI (GER)")

################################################################################################################################################################################################################################################################################################################################################################################

############################################ Juntar todos los data frame finales y exportar los resultados #####################################################################################################################################################################################################################################################################

################################################################################################################################################################################################################################################################################################################################################################################

finales = [finalEEUU,finalNor,finalAus,finalEsp,finalBra,finalBps,finalEcu,finalEcuSp,finalUk, finalNice, finalNHS, finalCan, finalArg, finalPer, finalFra, finalPor, finalChi, finalMex, finalPan, finalGer] 
final = reduce(lambda left,right: pd.merge(left,right,on=['PA'],how='outer'), finales).fillna("").sort_values('PA')

#Borrar datos anteriores: Pestañas con los soportes de cada país, TRM y FX en Anexos.xlsx
wb = openpyxl.load_workbook("Anexos.xlsx", read_only = False)
for hojas in ['Resultados','FX','TRM', 'FSS (EEUU)','NOMA (NOR)','PBS (AUS)','PETRONE (ESP)','ANVISA (BRA)','BPS (BRA)','CONSJ. MEDICAMENTOS (ECU)','SIST. COMPRAS PÚBLICAS (ECU)','eMIT (UK)','NICE (UK)',"NHS (UK)",'RAMQ (CAN)', 'ANMAT (ARG)', "DIGEMID (PER)", "L'AM (FRA)", "INFARMED (POR)", "CHILE COMPRA (CHI)", "IMSS (MEX)","PANAMA COMPRA (PAN)","DIMDI (GER)"]:
    ws = wb[hojas]
    ws.delete_cols(1, 35)

wb.save("Anexos.xlsx")
wb.close()

#Exportar los resultados al archivo "Anexos.xlsx"
writer = pd.ExcelWriter("Anexos.xlsx", engine='openpyxl')
writer.book = openpyxl.load_workbook("Anexos.xlsx", read_only = False)
writer.sheets = {ws.title: ws for ws in writer.book.worksheets}
final.to_excel(writer, sheet_name = "Resultados" , index = False)                      #RESULTADOS
trm.to_excel(writer, sheet_name = "TRM", index = False)                                #TRM
fx_media.to_excel(writer, sheet_name = "FX", index = False)                            #MONEDAS (FX)
eeuu.to_excel(writer, sheet_name='FSS (EEUU)',index=False)                             #EEUU
nor.to_excel(writer, sheet_name='NOMA (NOR)',index=False)                              #NORUEGA
aus.to_excel(writer, sheet_name='PBS (AUS)',index=False)                               #AUSTRALIA
esp.to_excel(writer, sheet_name='PETRONE (ESP)',index=False)                           #ESPAÑA
bra.to_excel(writer, sheet_name='ANVISA (BRA)',index=False)                            #BRASIL (ANVISA)
bps.to_excel(writer, sheet_name='BPS (BRA)',index=False)                               #BRASIL (BPS)
ecu.to_excel(writer, sheet_name='CONSJ. MEDICAMENTOS (ECU)',index=False)               #ECUADOR (CONSJ. MEDICAMENTOS)
ecu_sp.to_excel(writer, sheet_name = "SIST. COMPRAS PÚBLICAS (ECU)", index = False)    #ECUADOR (SIST. COMPRAS PÚBLICAS)
uk.to_excel(writer, sheet_name='eMIT (UK)',index=False)                                #UK (EMIT)
nice.to_excel(writer, sheet_name='NICE (UK)',index=False)                              #UK (NICE)
nhs.to_excel(writer, sheet_name='NHS (UK)',index=False)                                #UK (NHS)
can.to_excel(writer, sheet_name='RAMQ (CAN)',index=False)                              #CANADA
arg.to_excel(writer, sheet_name='ANMAT (ARG)',index=False)                             #ARGENTINA
per.to_excel(writer, sheet_name='DIGEMID (PER)',index=False)                           #PERU
fra.to_excel(writer, sheet_name="L'AM (FRA)",index=False)                              #FRANCIA
por.to_excel(writer, sheet_name='INFARMED (POR)',index=False)                          #PORTUGAL
chi.to_excel(writer, sheet_name='CHILE COMPRA (CHI)',index=False)                      #CHILE
mex.to_excel(writer, sheet_name='IMSS (MEX)',index=False)                              #MÉXICO
pan.to_excel(writer, sheet_name='PANAMA COMPRA (PAN)',index=False)                     #PÁNAMA
ger.to_excel(writer, sheet_name='DIMDI (GER)',index=False)                             #ALEMANIA

workbook = writer.book
workbook.filename =  "Anexos.xlsx"
writer.save()
writer.close()

#Medir cuánto tiempo se demoró en ejecutar el sript
print("La calculadora se demoró: " + str(round((time.time()-start_time)/60,2)) + " minutos :'D")