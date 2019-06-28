from classes import DataExplore
from classes.DataModel import GeoJson, FoursquareToOCB
import parameters.Globals as G


if __name__ == '__main__':

    # Create class Object
    spark = DataExplore.Category(G.MDB_USER_1,
                                 G.MDB_PASS_USER_1,
                                 '127.0.0.1:27017')
    # (2) Load raw 4sqr data from mongo
    df = spark.load_mongoDB('foursquare', 'searchVeanues')

    # (3) Filter settings
    Keywords = ['']
    KeysCategory = ['Taco Place', 'Bar']
    # KeysCategory = ['Office', 'Building', 'Mexican Restaurant',
    #                'Taco Place', 'Bank', 'Coffee Shop',"Doctor's Office",
    #                'Government Building', 'Tech Startup', 'Bar', 'Pharmacy',
    #                'Residential Building (Apartment / Condo)', 'Business Center',
    #                'Automotive Shop', 'University', 'Medical Center']

    if len(KeysCategory) == 0:
        ApplyCategoryFilter = False
    else:
        ApplyCategoryFilter = True

    # (4) Clean categories: repeted, empty category field
    df = spark.clean_categories(df)

    # (7) Match valid Postal Code from INEGI database
    df = spark.validate_postalCode_municipio(df)

    # (5) Filter by keywords on address
    df_2 = spark.filter_by_keyword_address(df, Keywords)

    # (5.1) Counts Venues by Category
    count_dict = spark \
        .group_by_categories_dict(df_2,
                                  KeysCategory,
                                  ApplyCategoryFilter)

    # (5.2) Display color bar with first max N counts
    MaxCategoryDisplay = 25
    if len(count_dict) >= MaxCategoryDisplay:
        KeysCategory = list(count_dict.keys())[:MaxCategoryDisplay]
    else:
        KeysCategory = list(count_dict.keys())

    CntOther = spark.count_others(KeysCategory, count_dict)
    count_dict = dict([(i, count_dict[i]) for i in KeysCategory])
    count_dict.update([('Other', CntOther)])


    # (6) Apply color to categories and filter categories
    df_3, colors_dict = spark.filter_categories_colored(
        df_2, KeysCategory, ApplyCategoryFilter)

    print("count_dict : ", count_dict)
    print("colors_dict: ", colors_dict)

    # TODO : add count in javaScript , temporal fix
    colors_dict = GeoJson.add_count_to_category_legend(colors_dict,
                                                       count_dict)
    # print(colors_dict)

    # (8) Set Data Model format to Orion Context Broker
    data_OCB = spark.maps_to_OCB_dataModel(df_3)

    # Display Data Model
    # FoursquareToOCB.printListFormat_sample(data_OCB[:2])

    # (9) Display results on http://localhost/map_osp
    GeoJson.map_points(data_OCB,
                       '/var/www/html/map_osm/data/geojson/')
    GeoJson.save_legend_map_display(colors_dict,
                               '/var/www/html/map_osm/data/',
                               'legend_colors.json')

    #GeoJson.save_legend_map_display(count_dict,
    #                           '/var/www/html/map_osm/data/',
    #                           'legend_count.json')

    print('Total Number of categories : ', len(count_dict))
    print('Total veanues sites : ', df_3.count())

