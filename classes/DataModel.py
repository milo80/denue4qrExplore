import pprint
import json
import datetime
import requests
import parameters.Globals as G
from functions import Utilities as U


class FoursquareToOCB:
    """
        Maps json format from FourSquare
        to Orion Context Brocker data model
    """

    listDict = []

    def __init__(self):
        self.pp = pprint.PrettyPrinter(indent=2)
        self.SEARCH_DATE = U.getTodayFormatSearch(G.DELAY_DAYS)

    # Maps data model coming from searchExplore extraction
    def map4SqrToOCB_formatSearchExplore(self, data):
        print("Ejecutando map4sqrToOCD  simple : ")
        self.listDict = []
        for item in data['response']['venues']:
            aux_id = '4sqr-'+str(item['id'])
            D = datetime.datetime.today()
            aux_date = str(D.date())+'T'+str(D.time())+'Z'
            if len(item['categories']) > 0:
                aux_categories = U.cleanChars(item['categories'][0]['name'])
            else:
                aux_categories = U.cleanChars(item['name'])

            self.listDict.append({"id": aux_id,
                             "type": "PointOfInterest",
                             "address": {
                                 "type": "StructuredValue",
                                 "value": {
                                     "addressCountry": U.cleanChars(item['location'].get('state')),
                                     "addressLocality": U.cleanChars(item['location'].get('city'))
                                 }
                             },
                             "category": {
                                 "type": "Text",
                                 "value": [
                                     "1",
                                     aux_categories
                                 ]
                             },
                             "dateCreated": {
                                 "type": "DateTime",
                                 "value": aux_date
                             },
                             "description": {
                                 "type": "Text",
                                 "value": aux_categories
                             },
                             "location": {
                                 "type": "geo:json",
                                 "value": {
                                     "type": "Point",
                                     "coordinates": [
                                         item['location'].get("lng"),
                                         item['location'].get("lat"),
                                     ]
                                 }
                             },
                             "name": {
                                 "type": "Text",
                                 "value": U.cleanChars(item['name'])
                             },
                             "url": {
                                 "type": "Text",
                                 "value": "no url"
                             }
                             })

        return self.listDict

    # Maps to OCB data model adding photos URL
    def map4SqrToOCB_formatSearchExplore_photos(self, data):
        print("Ejecutando map4sqrToOCD : ")

        listDict = []
        k = 0
        for item in data['response']['venues']:
            aux_id = '4sqr-'+str(item['id'])
            D = datetime.datetime.today()
            aux_date = str(D.date())+'T'+str(D.time())+'Z'
            if len(item['categories']) > 0:
                aux_categories = U.cleanChars(item['categories'][0]['name'])
            else:
                aux_categories = U.cleanChars(item['name'])

            DataOut, r = self.requestPhotoURL(item['id'])
            print(type(DataOut))
            if DataOut['response'].get('photos') is None:
                print('no <photos> key : code = ', r.status_code)
                photo_url = '_'
            elif len(DataOut['response']['photos']['items']) == 0:
                print('no items : code = ', r.status_code)
                photo_url = '__'
            else:
                print('photo request found : ', r.status_code)
                photo_url = DataOut['response']['photos']['items'][0].get('prefix') \
                            + DataOut['response']['photos']['items'][0]['user'].get('id') \
                            + DataOut['response']['photos']['items'][0].get('suffix')

            listDict.append({"id": aux_id,
                             "type": "PointOfInterest",
                             "address": {
                                 "type": "StructuredValue",
                                 "value": {
                                     "addressCountry": U.cleanChars(item['location'].get('state')),
                                     "addressLocality": U.cleanChars(item['location'].get('city'))
                                 }
                             },
                             "category": {
                                 "type": "Text",
                                 "value": [
                                     "1",
                                     aux_categories
                                 ]
                             },
                             "dateCreated": {
                                 "type": "DateTime",
                                 "value": aux_date
                             },
                             "description": {
                                 "type": "Text",
                                 "value": aux_categories
                             },
                             "location": {
                                 "type": "geo:json",
                                 "value": {
                                     "type": "Point",
                                     "coordinates": [
                                         item['location'].get("lng"),
                                         item['location'].get("lat"),
                                     ]
                                 }
                             },
                             "name": {
                                 "type": "Text",
                                 "value": U.cleanChars(item['name'])
                             },
                             "url": {
                                 "type": "Text",
                                 "value": photo_url
                             },
                             "photo_url": {
                                 "type": "Text",
                                 "value": r.status_code
                             }
                             })
            k += 1

        return listDict

    # Completes photos url address execution additional request
    def requestPhotoURL(self, ID):
        url = 'https://api.foursquare.com/v2/venues/'+str(ID)+'/photos'
        params = dict(
            client_id=G.CLIENT_ID,
            client_secret=G.CLIENT_SECRET,
            v=self.SEARCH_DATE,
        )
        try:
            resp = requests.get(url=url, params=params)
            PhotoMetaData = json.loads(resp.text, encoding="utf-8")
            # print("<photo requests> exitoso")
        except requests.exceptions.RequestException as e:
            print("Photo request rror <>: ", e)

        return PhotoMetaData, resp

    # Prints list fist item and counts elements
    @staticmethod
    def printListFormat_sample(Data: list) -> object:
        print('Total Items : %s' % (len(Data)))
        print(type(Data))
        pp = pprint
        pp.pprint(Data)

    # Prints venues search data model
    def print4SqrFormat_sample(self, Data):
        print('Total Items : %s' % (len(Data)))
        print(type(Data))
        self.pp.pprint(Data)
        extraction = Data['response']['venues']
        print(type(extraction))

    # Auxiliary printing not defined
    def print4SqrSample_categories(self, Data_) -> None:
        print('Total Categories : %s' % (len(Data_)))
        print(len(Data_['response']['venues']))
        for item in Data_['response']['venues']:
            for i in range(0, len(item['categories'])):
                print(item['categories'][i]['name'])

    # Save list of dictionaries as json file
    # data_ : list / dic ; data to save
    # file_name : string
    def saveToJSON(self, data_, file_name: str):
        with open(G.OUTPUT_PATH + '/foursquare' + file_name, mode="w", encoding='utf-8') as SaveFile1:
            json.dump(data_, SaveFile1)


class GeoJson:
    """
        Maps json format from FourSquare
        to Orion Context Brocker data model
    """
    def __init__(self):
        self.pp = pprint.PrettyPrinter(indent=2)
        self.SEARCH_DATE = U.getTodayFormatSearch(G.DELAY_DAYS)

    # saves geojson format file
    @staticmethod

    def map_points(data_list, path, file_name) -> None:
        out = []
        for item in data_list:
            if item['location']['value']['coordinates']:
                out.append({
                    "type": "Feature",
                    "properties": {
                        "name": item['id'],
                        "category": item['category']['value'][1],
                        "color_cat": item['category']['value'][2],
                        "address": item['address']['value']['addressLocality']
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": item['location']['value']['coordinates']
                    }

                })

            else:
                print('Incomplete data to generate geojson file')
                continue

        geoJson = {"type": "FeatureCollection", "features": out}
        #
        with open(path + file_name + '.json', mode="w", encoding='utf-8') as SaveFile1:
            json.dump(geoJson, SaveFile1)

        return None

    @staticmethod
    def add_count_to_category_legend(Color, Cnt):
        NewDict = {}
        for i in Cnt:
            NewKey = i + '[' + str(Cnt[i]) + ']'
            #print('( %s  : %s ) \n'%(NewKey, Color[i]))
            NewDict[NewKey] = Color[i]

        return NewDict

    @staticmethod
    def save_legend_map_display(Data, Path, FileName) -> None:
        if '.json' not in str(FileName):
            FileName = str(FileName) + '.json'

        with open(Path + FileName, mode="w", encoding='utf-8') as SaveFile1:
            json.dump(Data, SaveFile1)

    @staticmethod
    def save_geojson(Data, Path_, FileName) -> None:
        if '.json' not in str(FileName):
            FileName = str(FileName)+'.json'

        with open(Path_ + FileName, mode="w", encoding='utf-8') as SaveFile1:
            json.dump(Data, SaveFile1)


class ViewData:
    """
        Auxiliar class to print json datamodel
    """
    def __init__(self):
        self.pp = pprint.PrettyPrinter(indent=2)

    # Prints list fist item and counts elements
    def printListFormat_sample(self, Data: list) -> object:
        print('Total Items : %s' % (len(Data)))
        print(type(Data))
        self.pp = pprint
        self.pp.pprint(Data)
