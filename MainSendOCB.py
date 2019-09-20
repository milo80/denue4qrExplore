from classes import DataExplore, FourSquareRequest
import parameters.Globals as G
from classes.DataModel import FoursquareToOCB, GeoJson

if __name__ == '__main__':

    FQR = FourSquareRequest.EndPointsAccess()
    # TODO: source mongoDB
    spark = DataExplore.GroupByCategory(G.MDB_USER_1,
                                        G.MDB_PASS_USER_1,
                                        '127.0.0.1:27017')
    #
    # Filtra categorias con datos incompletos y codigos postales
    # no registrados en CDMX
    #filter_1 = []
    #venues = spark.filter_categories(df, filter_1, False)
    #print('Total venues : ', venues.count())

    # Carga datos depurados por categorias
    df = spark.load_full_collection_mongoDB('foursquare',
                                            'veanuesCleanCategory')

    #venues.show(20)
    data_OCB = spark.maps_to_OCB_dataModel(df)

    FoursquareToOCB.printListFormat_sample(data_OCB[:3])
    #FQR.sendDataToOCB(data_OCB)

