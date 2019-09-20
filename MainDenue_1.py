from classes import DataExplore
from classes.DataExploreDenue import Denue
from classes.DataModel import ViewData, GeoJson
from pyspark.sql import functions as f
import functions.Utilities as utls
import parameters.Globals as G
import csv

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

    #df0 = spark.load_denue_csv(path_porMayor, file_porMayor)
    #df1 = spark.load_denue_csv(path_porMenor_1, file_porMenor_1)
    df_all = spark.load_denue_csv(path_porMenor_2, file_porMenor_2)
    #df3 = spark.load_denue_csv(path_porMenor_3, file_porMenor_3)
    #df4 = spark.load_denue_csv(path_porMenor_4, file_porMenor_4)

    # df_cat = spark.load_denue_categories('./data_sample/', 'denue_ramas.csv')

    # df_all = df1.unionAll(df2)
    #df_all = df_all.unionAll(df3)
    #df_all = df_all.unionAll(df4)
    # df_all.printSchema()
    print('count: ', df_all.count())
    df_all = df_all.drop_duplicates(subset=['id'])
    print('after drop duplicates id: ', df_all.count())

    Keywords = ['insurgentes', 'ciudad de méxico']
    df_all = spark.filter_by_address_and_federal_entity(df_all, Keywords)
    print('after filter by address : ', df_all.count())

    df_all = spark.match_id_activity_catalog(df_all)

    #Reduce category -> subCategory
    df_all = spark.reduce_category_name(df_all)

    df_all.select('*').show()
    # Denue Count categories
    df_ck = df_all.groupby('category').count().sort(f.col('count').desc())
    #N = df_ck.count()
    #print('Total Categorias : ', N)
    #df_ck.filter(f.col('category').isNull()).show()
    out = df_ck.collect()
    SaveFile = G.OUTPUT_PATH + "/denue/denue_count.csv"
    with open(SaveFile, mode="w", encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|',
                            quoting=csv.QUOTE_MINIMAL,
                            lineterminator='\n')
        writer.writerows(out)

    # Send data to Orion Context Broker

    OCB_model = spark.map_dataframe_to_OCB_model(df_all, 'comercioPorMenor', 'unidadEconomica')

    #View.printListFormat_sample(OCB_model[:20])

    #print('------------------')
    print(OCB_model[:1])
    #print('------------------')
    file = 'comercio_por_menor.json'
    GeoJson.save_geojson(OCB_model[:300], G.OUTPUT_PATH + '/geojson/', file)
    #spark.send_entities_to_orionCB(OCB_model)

    """
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