from classes.DataExploreDenue import Denue
from classes.DataModel import ViewData, GeoJson
from pyspark.sql import functions as f
import parameters.Globals as G
import csv

from classes.DataModel import GeoJson
import sys
from classes import DataExplore
import functions.Utilities as utls


if __name__ == '__main__':
    # Load "comercio al por mayor"
    # Load "comercio al por menor"
    View = ViewData()
    spark = Denue('local[*]')
    path_porMayor = G.INPUT_SOURCE_PATH + '/inegi/denue_00_43_csv/conjunto_de_datos/'
    file_porMayor = 'denue_inegi_43_.csv'
    path_porMenor_1 = G.INPUT_SOURCE_PATH + '/inegi/denue_com_pormenor_1/conjunto_de_datos/'
    file_porMenor_1 = 'denue_inegi_46111_.csv'
    path_porMenor_2 = G.INPUT_SOURCE_PATH + '/inegi/denue_com_pormenor_2/conjunto_de_datos/'
    file_porMenor_2 = 'denue_inegi_46112-46311_.csv'
    path_porMenor_3 = G.INPUT_SOURCE_PATH + '/inegi/denue_com_pormenor_3/conjunto_de_datos/'
    file_porMenor_3 = 'denue_inegi_46321-46531_.csv'
    path_porMenor_4 = G.INPUT_SOURCE_PATH + '/inegi/denue_com_pormenor_4/conjunto_de_datos/'
    file_porMenor_4 = 'denue_inegi_46591-46911_.csv'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_com_pormenor_2/conjunto_de_datos/'
    File = 'denue_inegi_46112-46311_.csv'
    Categoria = 'comercioPorMenor'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_esparcimiento_cult_dep_recreat/conjunto_de_datos/'
    File = 'denue_inegi_71_.csv'
    Categoria = 'culturaYRecreacion'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_prep_alim_bebidas/conjunto_de_datos/'
    File = 'denue_inegi_72_.csv'
    Categoria = 'preparacionAlimentos'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_inmov_alquiler_bienes_mueb/conjunto_de_datos/'
    File = 'denue_inegi_53_.csv'
    Categoria = 'rentaInmuebles'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_prof_cientf_tec/conjunto_de_datos/'
    File = 'denue_inegi_54_.csv'
    Categoria = 'serviciosProfesionales'

    """
    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_educativos/conjunto_de_datos/'
    File = 'denue_inegi_61_.csv'
    Categoria = 'serviciosEducativos'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_salud_y_asistencia_social/conjunto_de_datos/'
    File = 'denue_inegi_62_.csv'
    Categoria = 'saludYAsistenciaSocial'
    """

    # df0 = spark.load_denue_csv(path_porMayor, file_porMayor)
    # df_all = spark.load_denue_csv(path_porMenor_2, file_porMenor_2)
    df_all = spark.load_denue_csv(Path, File)
    # df3 = spark.load_denue_csv(path_porMenor_3, file_porMenor_3)
    # df4 = spark.load_denue_csv(path_porMenor_4, file_porMenor_4)

    # df_all = df1.unionAll(df2)
    # df_all = df_all.unionAll(df3)
    # df_all = df_all.unionAll(df4)
    # df_all.printSchema()
    print('count: ', df_all.count())
    df_all = df_all.drop_duplicates(subset=['id'])
    print('after drop duplicates id: ', df_all.count())

    Keywords = ['insurgentes', 'ciudad de méxico']
    Keywords = []
    df_all = spark.filter_by_address_and_federal_entity(df_all, Keywords)
    print('after filter by address : ', df_all.count())

    df_all = spark.match_id_activity_catalog(df_all)

    #Reduce category -> subCategory
    df_all = spark.reduce_category_name(df_all)
    N = df_all.count()
    N_out = 'Total : ' + str(N)

    # Denue Count categories
    df_ck = df_all.groupby('reduct_cat_name').count().sort(f.col('count').desc())
    out = df_ck.collect()
    SaveFile = "/home/milo/Develope/output/denue_count.csv"
    SaveFile = G.OUTPUT_PATH + "/denue/denue_count.csv"
    with open(SaveFile, mode="w", encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|',
                            quoting=csv.QUOTE_MINIMAL,
                            lineterminator='\n')
        writer.writerows(out)
        writer.writerow({N_out})

    # sys.exit(0)

    # Filter by <nombre_act> and <raz_social>
    FilterKeys = ['comput', 'cómput', 'dato', 'data', 'informat', 'informát']
    # FilterKeys = ['jardin', 'grutas', 'musica', 'hist', 'danza', 'teatro', 'museo']
    # FilterKeys = ['bares', 'pizzas', 'tortas', 'tacos']
    # FilterKeys = []
    ColFileds = ['raz_social', 'nombre_act']
    df_all = spark.filter_keyword_over_columnField(df_all, FilterKeys, ColFileds)
    print('after filter by kewords over: raz_social, nombre_act : ', df_all.count())

    # sys.exit(0)
    # Convert DataFrame to Context Broker datamodel
    OCB_model = spark.map_dataframe_to_OCB_model(df_all,
                                                 Categoria,
                                                 'PointOfInterest')

    View.printListFormat_sample(OCB_model[:10])
    file = 'comercio_por_menor.json'
    # GeoJson.save_geojson(OCB_model[:300], G.OUTPUT_PATH + '/geojson/', file)

    #print('------------------')

    # sys.exit(0)
    # Send data to greenroute map Context Broker URL_1
    # spark.send_entities_to_orionCB(OCB_model, G.ORION_URL)

    # Send data to Orion Context Broker URL_2
    # spark.send_entities_to_orionCB(OCB_model[:50], G.ORION_URL2)

    # Send data to Orion Batch Mode
    # spark.send_entities_to_orionCB_batch(OCB_model)
    """

    # Send data to Orion Context Broker

    OCB_model = spark.map_dataframe_to_OCB_model(df_all, 'comercioPorMenor', 'unidadEconomica')

    #View.printListFormat_sample(OCB_model[:20])

    #print('------------------')
    print(OCB_model[:1])
    #print('------------------')
    file = 'comercio_por_menor.json'
    GeoJson.save_geojson(OCB_model[:300], G.OUTPUT_PATH + '/geojson/', file)
    #spark.send_entities_to_orionCB(OCB_model)

    # Send data to Orion Batch Mode
    spark.send_entities_to_orionCB_batch(OCB_model[213850:])
    PointsDenue = spark.maps_DF_to_geojson(df_all)

    # Save file
    File_1 = 'denue_comercio_PMenor_1'
    GeoJson.save_geojson(PointsDenue,
                         '/var/www/html/map_osm/data/geojson/',
                         File_1)

    KW = ['A', 'B', 'C']
    for i in KW[:-1]:
        print(i)

    """