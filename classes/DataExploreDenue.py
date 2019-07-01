from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as f

import parameters.Globals as G
import functions.Utilities as U
from functions.FooPlot import create_dict_colors

from functions.FooPySpark import *
import pandas as pd

import requests
import json
import time


class Denue:
    """
        Loads INEGI Denue data to DataFrames
        from downloaded  *.csv
        from INEGI API
    """

    # Class constructor
    def __init__(self, host_):
        #  check all options at :
        # 'https://spark.apache.org/docs/latest/configuration.html'
        self.host = host_
        conf = SparkConf().setAll([('spark.master', self.host),
                                   ('spark.executor.memory', '6g'),
                                   ('spark.app.name', 'denue mining App'),
                                   ('spark.executor.cores', '3'),
                                   ('spark.cores.max', '3'),
                                   ('spark.driver.memory', '4g')])

        self.sc = SparkContext(conf=conf)
        self.sql = SQLContext(self.sc)

    # Load activity class id map
    def load_denue_raw(self, Path_, FileName_) -> object:
        # load csv data
        df = self.sql.read.format("com.databricks.spark.csv") \
            .options(delimiter=',')\
            .options(header='true', inferSchema='true') \
            .load(Path_+FileName_)

        return df

    # Load from CSV file
    def load_denue_csv(self, Path_, FileName_) -> object:
        # Load Postal Codes of CDMX
        FileName = ''
        if '.csv' in str(FileName_):
            FileName = FileName_
        else:
            FileName = str(FileName_)+'.csv'

        df = self.sql.read.format("com.databricks.spark.csv") \
            .options(delimiter=',')\
            .options(header='true', inferSchema='true') \
            .load(Path_+FileName)

        df = df.select(
            f.col('id'),
            f.col('nom_estab'),
            f.col('raz_social'),
            f.col('nombre_act'),
            f.col('codigo_act'),
            f.col('nom_vial'),
            f.col('nom_v_e_1'),
            f.col('nom_v_e_2'),
            f.col('nom_v_e_3'),
            f.col('numero_ext'),
            f.col('tipo_asent'),
            f.col('nomb_asent'),
            f.col('cod_postal'),
            f.col('entidad'),
            f.col('municipio'),
            f.col('localidad'),
            f.col('www'),
            f.col('tipoUniEco'),
            f.col('latitud'),
            f.col('longitud'),
            f.col('fecha_alta')
        )

        return df

    # Maps data to geojson
    def maps_DF_to_geojson(self, df_B):
        print('Mapeando datos a formato GeoJson')
        try:
            geo_model = df_B.rdd.map(
                lambda x: {"type": "Feature",
                           "properties": {
                               "id": U.cleanChars(str(x["id"])),
                               "nom_estab": U.cleanChars(x['nom_estab']),
                               "raz_social": U.cleanChars(x['raz_social']),
                               "nombre_act": U.cleanChars(x['nombre_act']),
                               "categoria": U.cleanChars(x['category']),
                               "color_cat": x['color_cat'],
                               "direccion": [
                                   ' '.join(['Calle:', U.cleanChars(x['nom_vial']), U.cleanChars(x['numero_ext'])]),
                                   'Entre : ' + U.cleanChars(x['nom_v_e_1']) + ' y '+ U.cleanChars(x['nom_v_e_2']),
                                   U.cleanChars(x['tipo_asent']) + ': ' + U.cleanChars(x['nomb_asent']),
                                   'CP: ' + U.cleanChars(x['cod_postal']),
                                   'Municipio: ' + U.cleanChars(x['municipio']),
                                   'Entidad: ' + U.cleanChars(x['entidad'])
                               ],
                               'sitio_web': U.cleanChars(x['www']),
                               'fecha_alta': U.cleanChars(x['fecha_alta']),
                           },
                           "geometry": {
                               "type": "Point",
                               "coordinates": U.verify_geojson_coords(x['longitud'],
                                                                      x['latitud'])
                           }
                           })
            print('data maped to geojson ...')
            geojson = {"type": "FeatureCollection", "features": geo_model.collect()}
        except Exception as e:
            geojson = {"type": "FeatureCollection", "features": []}
            print('Error mapping Economic Units : \n', str(e))

        return geojson

    # Function filter by street name and <Entidad>
    def filter_by_address_and_federal_entity(self, df_B: object, Keywords):
        DataColumns = ['nom_vial', 'entidad']
        df_B = df_B.filter(contains_keyword_and_entity_udf(Keywords)(
            f.struct(DataColumns)))\
            .select('*')

        return df_B

    # Filter by keywords on any valid input field
    def filter_keyword_over_columnField(self, df_B, Keywords, ColField):
        df_B = df_B.filter(filter_column_udf(Keywords)(f.struct(ColField)))
        return df_B

    # Match "id_act" VS activity from SCIAN catalog
    def match_id_activity_catalog(self, df_B) -> object:
        # load SCIAN catalog
        df_cat = self.load_denue_raw('./data_sample/',
                                             'denue_ramas.csv')
        df_cat = df_cat.drop_duplicates(subset=['id_act'])
        # Cross <codigo_act> V.C <adtividad>
        a = df_B.alias('a')
        b = df_cat.alias('b')
        df_B = a.join(b, f.col('a.codigo_act') == f.col('b.id_act'), 'left')\
            .select([f.col('a.'+xx) for xx in a.columns] +
                    [f.col('b.actividad').alias('category')])

        return df_B

    # Broups and counts bussiness by category
    # Prints out colors by category
    def group_by_categories_colored(self, df_B, MaxCatDisplay, KeysCategory):
        CountDict = {}
        df = df_B.groupby('category').count().sort(f.col('count').desc())
        #df = self.reduce_category_name(df)
        #
        if len(KeysCategory) > 0:
            df = df.filter(f.col('category').isin(KeysCategory))
            df_B = df_B.filter(f.col('category').isin(KeysCategory))

        for item in df.collect():
            CountDict[item['category']] = item['count']

        if len(CountDict) >= MaxCatDisplay:
            KeysCategory = list(CountDict.keys())[:MaxCatDisplay]
        else:
            KeysCategory = list(CountDict.keys())

        CountOther = 0
        for Key in CountDict.keys():
            if Key not in KeysCategory:
                CountOther += CountDict[Key]

        CountDict = dict([(i, CountDict[i]) for i in KeysCategory])
        CountDict.update([('Other', CountOther)])

        ColorDict = create_dict_colors(KeysCategory)
        df_B = df_B.withColumn('color_cat',
                               color_category_udf(ColorDict)(f.col('category')))

        return df_B, CountDict, ColorDict

    # Reduce Category name with dictionary
    def reduce_category_name(self, df_B):
        ReductDict = {
                      "Comercio al por menor de frutas y verduras frescas": "frutasVerduras",
                      "Comercio al por menor de carnes rojas": "carnesRojas",
                      "Comercio al por menor en minisupers": "minisupers",
                      "Comercio al por menor de carne de aves": "carneAves",
                      "Comercio al por menor de dulces y materias primas para repostería": "dulcesReposteria",
                      "Comercio al por menor de cerveza": "cerveza",
                      "Comercio al por menor de otros alimentos": "alimentos",
                      "Comercio al por menor de bebidas no alcohólicas y hielo": "bebidasNoAlcoholicas",
                      "Comercio al por menor de leche": "lacteos",
                      "Comercio al por menor de artículos de mercería y bonetería": "merceriaBoneteria",
                      "Comercio al por menor de paletas de hielo y helados": "helados",
                      "Comercio al por menor de semillas y granos alimenticios": "semillasGranos",
                      "Comercio al por menor de vinos y licores": "vinosLicores",
                      "Comercio al por menor de pescados y mariscos": "pescadosMariscos",
                      "Comercio al por menor en supermercados": "supermercados",
                      "Comercio al por menor de blancos": "blancos",
                      "Comercio al por menor de telas": "telas",
                      "Comercio al por menor en tiendas departamentales": "tiendasDepartamentales",
                      "Comercio al por menor de cigarros": "cigarros",
                      "Comercio al por menor en tiendas de abarrotes": "abarrotes",
                      "Centros de acondicionamiento físico del sector privado": "gimnacioPrivado",
                      "Casas de juegos electrónicos": "juegosElectronicos",
                      "Venta de billetes de lotería": "billetesLoteria",
                      "Cantantes y grupos musicales del sector privado": "gruposMusicalesPrivado",
                      "Centros de acondicionamiento físico del sector público": "gimnacioPublico",
                      "Promotores del sector público de espectáculos artísticos": "promotorEspectaculosPublico",
                      "Otros servicios recreativos prestados por el sector privado": "serviciosRecreativosPrivado",
                      "Otros servicios recreativos prestados por el sector público": "serviciosRecreativosPublico",
                      "Parques acuáticos y balnearios del sector privado": "parquesAcuaticosPrivado",
                      "Parques acuáticos y balnearios del sector público": "parquesAcuaticosPublico",
                      "Promotores del sector privado de espectáculos artísticos": "promotorEspectaculosPrivado",
                      "Promotores de espectáculos artísticos": "promotorEspectaculosArtisticos",
                      "Museos del sector público": "museosPublico",
                      "Museos del sector privado": "museosPrivado",
                      "Clubes o ligas de aficionados": "clubesAficionados",
                      "Clubes deportivos del sector privado": "clubesDeportivosPrivado",
                      "Clubes deportivos del sector público": "clubesDeportivosPublico",
                      "Parques de diversiones y temáticos del sector público": "parqueDiversionesPublico",
                      "Parques de diversiones y temáticos del sector privado": "parqueDiversionesPrivado",
                      "Otros juegos de azar": "juegosAzar",
                      "Campos de golf": "camposGolf",
                      "Agentes y representantes de artistas": "agentesArtistas",
                      "Equipos deportivos profesionales": "equiposDeportivosProfesionales",
                      "Boliches": "boliches",
                      "Artistas": "artistas",
                      "Otras compañías y grupos de espectáculos artísticos del sector privado": "espectaculosArtisticosPrivado",
                      "Marinas turísticas": "marinasTuristicas",
                      "Jardines botánicos y zoológicos del sector privado": "jardinesBotanicos&ZologicosPrivado",
                      "Jardines botánicos y zoológicos del sector público": "jardinesBotanicos&ZologicosPublico",
                      "Compañías de teatro del sector privado": "teatroPrivado",
                      "Compañías de danza del sector privado": "danzaPrivado",
                      "Compañías de teatro del sector público": "teatroPublico",
                      "Grutas": "grutas",
                      "Deportistas profesionales": "deportistasProfesionales",
                      "Sitios históricos": "sitiosHistoricos",
                      "Compañías de danza del sector público": "danzaPublico"}

        df_B = df_B.select('*',
                           reduce_name_of_category_udf(ReductDict)(f.struct('category'))
                           .alias('reduct_cat_name'))
        return df_B

    # Alternative to reduce name at the end of excecution
    def reduce_category_name_legend(self, CatDict):
        ReductDict = {"Comercio al por menor": "C_aP_menor",
                  "Comercio al por mayor": "C_aP_mayor",
                  "Restaurantes con servicio de preparación": "RcSP",
                  "Servicios de preparación de otros alimentos para consumo inmediato": "SP_OA_CI",
                  "Restaurantes que preparan otro tipo": "RqP_OT",
                  "Departamentos y casas amueblados": "D_CA"}

        NewDict = {}
        for CatKey in CatDict.keys():
            NewKey = CatKey
            for RKey in ReductDict.keys():
                if str(RKey) in str(CatKey):
                    NewKey = str(CatKey).replace(RKey, ReductDict[RKey])
                    break
            NewDict[NewKey] = CatDict[CatKey]

        return NewDict

    # Maps data model coming from searchExplore extraction
    @staticmethod
    def map_dataframe_to_OCB_model(df: object, Categoria, Tipo) -> list:
        print('Mapeando datos a formato OCB')
        try:
            OCB_model = df.rdd.map(lambda x: {
                "id": "denue-" + str(x["id"]),
                "type": Tipo,
                "address": {
                    "type": "StructuredValue",
                    "value": {
                        "addressCountry": U.cleanChars(x["entidad"]),
                        "addressRegion": U.cleanChars(x["municipio"]),
                        "postalCode": U.cleanChars(x["cod_postal"]),
                        "streetAddress": U.cleanChars(x["nom_vial"]) + "  " +
                                         U.cleanChars(x["numero_ext"]) + ", " +
                                         U.cleanChars(x["tipo_asent"]) + ", " + U.cleanChars(x["nomb_asent"])
                    }
                },
                "category": {
                    "type": "Text",
                    "value": str(Categoria)
                },
                "subCategory": {
                    "type": "Text",
                    "value": U.cleanChars(x["reduct_cat_name"])
                },
                "dateCreated": {
                    "type": "DateTime",
                    "value": str(x["fecha_alta"]) + "-01T12:00:00.00Z"
                },
                "description": {
                    "type": "Text",
                    "value": U.cleanChars(x["nombre_act"])
                },
                "location": {
                    "type": "geo:json",
                    "value": {
                        "type": "Point",
                        "coordinates": U.verify_geojson_coords(
                            x["longitud"],
                            x["latitud"]
                        )
                    }
                },
                "name": {
                    "type": "Text",
                    "value": U.cleanChars(x["nom_estab"])
                },
                "alternateName": {
                    "type": "Text",
                    "value": U.cleanChars(x["raz_social"])
                },
                "dataProvider": {
                    "type": "Text",
                    "value": x["www"]
                },
                "economyUnit": {
                    "type": "Text",
                    "value": U.cleanChars(x["tipoUniEco"])
                }

            })
            print("mapeo exitoso  ...")
            out = OCB_model.collect()
            print(type(out))
        except Exception as e:
            print('Error en mapeo a OCB : ', str(e))
            out = []

        return out

    # Sends data model to Orion Context Broker
    def send_entities_to_orionCB(self, Data_):
        k = 0
        for item in Data_:
            try:
                resp_orion = requests.post(G.ORION_URL2,
                                           data=json.dumps(item),
                                           headers=G.HEADERS,
                                           timeout=5)
                print('request[%s] : code = %s ' % (k, resp_orion.status_code))
                if resp_orion.status_code == 201:
                    print('Data sent to ODB : Success! code[%s]' % resp_orion.status_code)
            except requests.exceptions.Timeout as t:
                print('Error', t)
                print('Message : ', resp_orion.text)

            # time.sleep(1)
            k += 1

    # Sentds data model to Orion Context Broker , batch mode
    @staticmethod
    def send_entities_to_orionCB_batch(Data) -> None:
        k = 0
        for batch in U.split(G.BATCH_SIZE, Data):
            try:
                resp_orion = requests.post(G.ORION_URL3,
                                           data=json.dumps({"actionType": "APPEND",
                                                            "entities": batch}),
                                           headers=G.HEADERS,
                                           timeout=30)
                #print('type :', type(batch))
                #print('len', len(batch))
                #print(batch[0])
                print('batch request[%s] : code = %s' % (k, resp_orion.status_code))
                if resp_orion.status_code == 201:
                    N = len(batch)
                    print('Data items[%s] sent to OCB : Success! code[%s]' % (N, resp_orion.status_code))
            except requests.exceptions.Timeout as t:
                print('Error,  timeout : ', str(t))

            except Exception as e:
                print("Error sending data to Orion Context Broker :\n", str(e))

            k += 1
