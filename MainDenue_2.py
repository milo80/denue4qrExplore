from classes import DataExplore
from classes.DataExploreDenue import Denue
from classes.DataModel import ViewData, GeoJson
import parameters.Globals as G
from pyspark.sql import functions as f
import functions.Utilities as utls
import csv

if __name__ == '__main__':

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_com_pormenor_2/conjunto_de_datos/'
    File = 'denue_inegi_46112-46311_.csv'
    CategoryName = 'comercios'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_esparcimiento_cult_dep_recreat/conjunto_de_datos/'
    File = 'denue_inegi_71_.csv'
    CategoryName = 'cultYRecrea'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_prep_alim_bebidas/conjunto_de_datos/'
    File = 'denue_inegi_72_.csv'
    CategoryName = 'alimentos'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_inmov_alquiler_bienes_mueb/conjunto_de_datos/'
    File = 'denue_inegi_53_.csv'
    CategoryName = 'rentaInmu'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_prof_cientf_tec/conjunto_de_datos/'
    File = 'denue_inegi_54_.csv'
    CategoryName = 'servProf'

    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_educativos/conjunto_de_datos/'
    File = 'denue_inegi_61_.csv'
    CategoryName = 'servEduc'

    """
    Path = G.INPUT_SOURCE_PATH + '/inegi/denue_salud_y_asistencia_social/conjunto_de_datos/'
    File = 'denue_inegi_62_.csv'
    CategoryName = 'saludYSS'
    """

    spark = Denue('local[*]')
    df_all = spark.load_denue_csv(Path, File)
    # N = df_all.count()
    # print('count: ', N)
    df_all = df_all.drop_duplicates(subset=['id'])
    print('after drop duplicates id: ', df_all.count())

    Keywords = ['', '']

    df_all = spark.filter_by_address_and_federal_entity(df_all, Keywords)
    print('after filter by address : ', df_all.count())

    df_all = spark.match_id_activity_catalog(df_all)

    #Reduce category -> subCategory
    df_all = spark.reduce_category_name(df_all)

    # Filter by <nombre_act> and <raz_social>
    FilterKeys = ['jardin', 'grutas', 'musica', 'hist', 'danza', 'teatro', 'museo']
    FilterKeys = ['bares', 'pizzas', 'tortas', 'tacos']
    FilterKeys = []
    FilterKeys = ['comput', 'cómput', 'dato', 'data', 'informat', 'informát']
    ColFileds = ['raz_social', 'nombre_act']
    df_all = spark.filter_keyword_over_columnField(df_all, FilterKeys, ColFileds)
    print('after filter by kewords over: raz_social, nombre_act : ', df_all.count())

    # Filter by category name
    KeysCategory = []
    df_all, CntDict, ColorDict = spark.group_by_categories_colored(df_all,
                                                                   35,
                                                                   KeysCategory,
                                                                   'reduct_cat_name')

    #print('Dict 1 \n', CntDict)
    #print('Dict 2 \n', ColorDict)

    # df_all.select('category').show()

    ColorDict = GeoJson.add_count_to_category_legend(ColorDict,
                                                     CntDict)

    # ColorDict = spark.reduce_category_name_legend(ColorDict)

    legend_name = 'legend_' + CategoryName + '.json'
    GeoJson.save_legend_map_display(ColorDict,
                                    G.OUTPUT_PATH + '/legend/',
                                    legend_name)
    # df_all.show()
    PointsDenue = spark.maps_DF_to_geojson(df_all)

    # Save file
    GeoJson.save_geojson(PointsDenue,
                         G.OUTPUT_PATH + '/geojson/',
                         CategoryName)

    """
    SaveFile = "./data_sample/denue_count.csv"
    with open(SaveFile, mode="w", encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='|',
                            quoting=csv.QUOTE_MINIMAL,
                            lineterminator='\n')
        writer.writerows(out)
    """