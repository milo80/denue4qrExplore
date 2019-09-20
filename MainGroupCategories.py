from classes import DataExplore
from classes.DataModel import GeoJson
import parameters.Globals as G

from classes import MongoDB

if __name__ == '__main__':

    spark = DataExplore.Category(G.MDB_USER_1, G.MDB_PASS_USER_1, '127.0.0.1:27017')
    #
    # df = spark.load_mongoDB('foursquare', 'searchVeanues')
    #
    #filter_cat = []
    # print('before filter size : ', df.count(), )
    #df_2 = spark.filter_categories(df, filter_cat, False)
    #
    #print(' after filter by categories \n %s count : %s'
    #       %(filter_cat, df_2.count()))
    #
    # df_3 = spark.validate_postalCode_municipio(df_2)
    # df_3.printSchema()

    # Poblar nueva colleccion de datos
    #spark.write_mongoDB(df_3, 'foursquare', 'veanuesCleanCategory')

    df_3 = spark.load_full_collection_mongoDB('foursquare', 'veanuesCleanCategory')
    df_3.printSchema()
    # spark.plot_pie(df_3)

    # Desplega resultados en mapa
    # data_OCB = spark.maps_to_OCB_dataModel(df_3)
    # GeoJson.map_points(data_OCB, G.OUTPUT_PATH + '/geojson/')
