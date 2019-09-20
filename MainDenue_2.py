from classes import DataExplore
from classes.DataExploreDenue import Denue
from classes.DataModel import ViewData, GeoJson
from pyspark.sql import functions as f
import functions.Utilities as utls
import parameters.Globals as G
import csv

if __name__ == '__main__':
    # Load "servicios temporaldes de alimentos y bebidas"
    Path_1 = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_prep_alim_bebidas/conjunto_de_datos/'
    File_1 = 'denue_inegi_72_.csv'

    Path_2 = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_esparcimiento_cult_dep_recreat/conjunto_de_datos/'
    File_2 = 'denue_inegi_71_.csv'

    Path_3 = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_inmov_alquiler_bienes_mueb/conjunto_de_datos/'
    File_3 = 'denue_inegi_53_.csv'

    Path_4 = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_prof_cientf_tec/conjunto_de_datos/'
    File_4 = 'denue_inegi_54_.csv'

    Path_5 = G.INPUT_SOURCE_PATH + '/inegi/denue_serv_educativos/conjunto_de_datos/'
    File_5 = 'denue_inegi_61_.csv'

    Path_6 = G.INPUT_SOURCE_PATH + '/inegi/denue_com_pormenor_1/conjunto_de_datos/'
    File_6 = 'denue_inegi_46111_.csv'

    spark = Denue('local[*]')

    CategoryName = 'alimentos-bebidas'
    df = spark.load_denue_csv(Path_1, File_1)

    print('count: ', df.count())
    df_all = df.drop_duplicates(subset=['id'])
    print('after drop duplicates id: ', df_all.count())

    Keywords = ['', 'ciudad de méxico']
    df_all = spark.filter_by_address_and_federal_entity(df_all, Keywords)
    print('after filter by address : ', df_all.count())

    df_all = spark.match_id_activity_catalog(df_all)

    FilterKeys = ['comput', 'cómput', 'dato', 'data', 'informat', 'informát']
    ColFileds = ['raz_social', 'nombre_act']
    # df_all = spark.filter_keyword_over_columnField(df_all, FilterKeys, ColFileds)

    # TODO: change to match keyword category NOT exact category name
    KeysCategory = []

    df_all, CntDict, ColorDict = spark.group_by_categories_colored(df_all, 35, KeysCategory)

    print('Dict 1 \n', CntDict)
    print('Dict 2 \n', ColorDict)
    # df_all.select('category').show()

    ColorDict = GeoJson.add_count_to_category_legend(ColorDict,
                                                     CntDict)

    ColorDict = spark.reduce_category_name_legend(ColorDict)
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