from classes import DataExplore
from classes.DataModel import GeoJson
import parameters.Globals as G

from functions.FooPlot import *

if __name__ == '__main__':

    # Create class Object
    spark = DataExplore.Category(G.MDB_USER_1,
                                        G.MDB_PASS_USER_1,
                                        '127.0.0.1:27017')
    #
    df = spark.load_mongoDB('foursquare', 'searchVeanues')
    #

    keys_cat = ['Taco Place',
        'Pharmacy',
        'Medical Center',
        'Fast Food Restaurant',
        'Church',
        'Gas Station',
        'Internet Cafe',
        'Gym'
    ]
    print('before filter size : ', df.count())

    # Clean categories: repeted, empty category field
    df = spark.clean_categories(df)

    df_2, colors_dict = spark.filter_categories_colored(
        df, keys_cat, True)
    #
    print(' after filter by categories \n %s count : %s'
          %(keys_cat, df_2.count()))
    #
    df_3 = spark.validate_postalCode_municipio(df_2)
    # Desplega resultados en mapa
    data_OCB = spark.maps_to_OCB_dataModel(df_3)
    GeoJson.map_points(data_OCB,
                       G.OUTPUT_PATH + '/geojson/',
                       'venues_OCB_categories')
    GeoJson.save_legend_map_display(colors_dict,
                                    G.OUTPUT_PATH + '/legend/',
                                    'legend_venues_OCB_categories')
    """
    Color = create_color_palette(keys_cat)

    print('test :', Color[keys_cat[1]])
    """
