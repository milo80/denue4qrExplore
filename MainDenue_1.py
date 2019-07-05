from classes.DataExploreDenue import Denue
from classes.DataModel import ViewData
from pyspark.sql import functions as f
import parameters.Globals as G
import csv
from classes.DataModel import GeoJson
import sys


if __name__ == '__main__':
    # Load "comercio al por mayor"
    # Load "comercio al por menor"
    View = ViewData()
    spark = Denue('local[*]')
    path_porMayor = '/home/milo/Develope/input/inegi/denue_00_43_csv/conjunto_de_datos/'
    file_porMayor = 'denue_inegi_43_.csv'
    path_porMenor_1 = '/home/milo/Develope/input/inegi/denue_com_pormenor_1/conjunto_de_datos/'
    file_porMenor_1 = 'denue_inegi_46111_.csv'
    path_porMenor_2 = '/home/milo/Develope/input/inegi/denue_com_pormenor_2/conjunto_de_datos/'
    file_porMenor_2 = 'denue_inegi_46112-46311_.csv'
    path_porMenor_3 = '/home/milo/Develope/input/inegi/denue_com_pormenor_3/conjunto_de_datos/'
    file_porMenor_3 = 'denue_inegi_46321-46531_.csv'
    path_porMenor_4 = '/home/milo/Develope/input/inegi/denue_com_pormenor_4/conjunto_de_datos/'
    file_porMenor_4 = 'denue_inegi_46591-46911_.csv'

    Path = '/home/milo/Develope/input/inegi/denue_com_pormenor_2/conjunto_de_datos/'
    File = 'denue_inegi_46112-46311_.csv'
    Categoria = 'comercioPorMenor'

    Path = '/home/milo/Develope/input/inegi/denue_serv_esparcimiento_cult_dep_recreat/conjunto_de_datos/'
    File = 'denue_inegi_71_.csv'
    Categoria = 'culturaYRecreacion'

    Path = '/home/milo/Develope/input/inegi/denue_serv_prep_alim_bebidas/conjunto_de_datos/'
    File = 'denue_inegi_72_.csv'
    Categoria = 'preparacionAlimentos'

    Path = '/home/milo/Develope/input/inegi/denue_serv_inmov_alquiler_bienes_mueb/conjunto_de_datos/'
    File = 'denue_inegi_53_.csv'
    Categoria = 'rentaInmuebles'

    Path = '/home/milo/Develope/input/inegi/denue_serv_prof_cientf_tec/conjunto_de_datos/'
    File = 'denue_inegi_54_.csv'
    Categoria = 'serviciosProfesionales'

    Path = '/home/milo/Develope/input/inegi/denue_serv_educativos/conjunto_de_datos/'
    File = 'denue_inegi_61_.csv'
    Categoria = 'serviciosEducativos'
    """
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

    Keywords = ['', 'ciudad de méxico']
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
    with open(SaveFile, mode="w", encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|',
                            quoting=csv.QUOTE_MINIMAL,
                            lineterminator='\n')
        writer.writerows(out)
        writer.writerow({N_out})

    # sys.exit(0)

    # Filter by <nombre_act> and <raz_social>
    FilterKeys = ['comput', 'cómput', 'dato', 'data', 'informat', 'informát']
    FilterKeys = ['jardin', 'grutas', 'musica', 'hist', 'danza', 'teatro', 'museo']
    FilterKeys = ['bares', 'pizzas', 'tortas', 'tacos']
    FilterKeys = []
    ColFileds = ['raz_social', 'nombre_act']
    df_all = spark.filter_keyword_over_columnField(df_all, FilterKeys, ColFileds)
    print('after filter by kewords over: raz_social, nombre_act : ', df_all.count())

    # sys.exit(0)
    # Convert DataFrame to Context Broker datamodel
    OCB_model = spark.map_dataframe_to_OCB_model(df_all,
                                                 Categoria,
                                                 'PointOfInterest')

    # View.printListFormat_sample(OCB_model[:10])
    #GeoJson.save_geojson(OCB_model[:10],
    #                     '/home/milo/Develope/output/',
    #                     'testJSON')
    #print('------------------')

    # sys.exit(0)
    # Send data to greenroute map Context Broker URL_1
    # spark.send_entities_to_orionCB(OCB_model, G.ORION_URL)

    # Send data to Orion Context Broker URL_2
    # spark.send_entities_to_orionCB(OCB_model, G.ORION_URL2)

    # Send data to Orion Batch Mode
    spark.send_entities_to_orionCB_batch(OCB_model)
    """

    """