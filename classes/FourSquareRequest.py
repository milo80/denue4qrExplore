import parameters.Globals as G
import functions.Utilities as U
from classes import DataModel as DM

import requests
import json
import datetime
import time


class EndPointsAccess:
    """
        Execute requests to Foursquare endpoints
        request options are setup as methods
        Note: temporally has metods to send data to
            Orion Context Broker
    """
    N = 0
    url = ' '
    params = dict()

    def __init__(self):
        self.AllData = []
        self.SEARCH_DATE = U.getTodayFormatSearch(G.DELAY_DAYS)
        self.dm = DM.FoursquareToOCB()

    #@classmethod
    #def withDelayDay(self, days_delay):
    #    self.params = dict()
    #    self.dm = DM.FoursquareToOCB()

    # access to "explore" endpoint, with query option
    def exploreAndQuery(self, LatLng_, radius_,
                        query_, limit_):
        print('Ejecutando Explore')
        self.url = 'https://api.foursquare.com/v2/venues/explore'
        self.params = dict(
            client_id=G.CLIENT_ID,
            client_secret=G.CLIENT_SECRET,
            ll=LatLng_,
            radius=radius_,
            v=self.SEARCH_DATE,
            query=query_,
            limit=limit_
        )

    # access to "explore" endpoint, simple
    def explore(self, LatLng_, radius_, limit_):
        print('Ejecutando Explore')
        self.url = 'https://api.foursquare.com/v2/venues/explore'
        self.params = dict(
            client_id=G.CLIENT_ID,
            client_secret=G.CLIENT_SECRET,
            ll=LatLng_,
            radius=radius_,
            v=self.SEARCH_DATE,
            limit=limit_
        )

    #  access to "search" endpoint , with browse intent
    def searchBrowse(self,  str_SW, str_NE, limit_):
        print("Foursquare request : intent = browse")
        self.url = 'https://api.foursquare.com/v2/venues/search'
        self.params = dict(
            sw=str_SW,
            ne=str_NE,
            intent='browse',
            client_id=G.CLIENT_ID,
            client_secret=G.CLIENT_SECRET,
            v=self.SEARCH_DATE,
            limit=limit_,
        )

    # access to "search" endpoint, by category
    def searchBrowseByCategory(self, str_SW, str_NE, categoryId_, limit_):
        print("Foursquare request by category : intent = browse")
        self.url = 'https://api.foursquare.com/v2/venues/search'
        self.params = dict(
            sw=str_SW,
            ne=str_NE,
            categoryId=categoryId_,
            intent='browse',
            client_id=G.CLIENT_ID,
            client_secret=G.CLIENT_SECRET,
            v=self.SEARCH_DATE,
            limit=limit_,
        )

    # Generic execution of request, prints status of executions
    # Format : boolean; True -> Returns OCB format
    #                   False-> Returns 4sqr-venue format
    def request(self, Format) -> object:
        resp = requests.get(url=self.url, params=self.params)
        try:
            print('rate_limit : ', resp.headers['X-RateLimit-Limit'])
            print('rate_remaining : ', resp.headers['X-RateLimit-Remaining'])
        except requests.exceptions.RequestException as e:
            print('Request to 4sqr : uncertain result : error : ', e)

        if resp.status_code == 200:
            data = json.loads(resp.text, encoding="utf-8")
            print('Download 4sqr data Ok : %s' % resp.status_code)
            if Format:
                data2 = self.dm.map4SqrToOCB_formatSearchExplore(data)
                return data2, resp
            else:
                return data, resp
        else:

            print("Request 4sqr error <code>: ", resp.status_code)
            return [], resp

    # Same as self.request :  saving data to file,
    # 4square format saved
    # OCB format saved
    def requestLogData(self, PATH_):
        resp = requests.get(url=self.url, params=self.params)
        try:
            print('rate_limit : ', resp.headers['X-RateLimit-Limit'])
            print('rate_remaining : ', resp.headers['X-RateLimit-Remaining'])
        except requests.exceptions.RequestException as e:
            print('Request to 4sqr : uncertain result : error : ', e)

        if resp.status_code == 200:
            data = json.loads(resp.text, encoding="utf-8")
            d = datetime.datetime.today()
            d = str(d).split('.')[0].replace(' ', '_')
            file_name = '4sqr_browse_'+d+'.json'
            with open(PATH_+file_name, "w") as SaveFile1:
                json.dump(data, SaveFile1)

            data2 = self.dm.map4SqrToOCB_formatSearchExplore(data)
            file_name = 'fsqr_OCB_format_'+d+'.json'
            with open(PATH_+file_name, "w") as SaveFile2:
                json.dump(data2, SaveFile2)

            print('request 4sqr OK !! : %s' % resp.status_code)
            return data2, resp
        else:

            print("Request 4sqr error <code>: ", resp.status_code)
            return [], resp

    # TODO:  build own class to populate data to OCB
    # Sends data model to Orion Context Broker
    def sendDataToOCB(self, Data_):
        k = self.N
        for item in Data_:
            try:
                resp_orion = requests.post(G.ORION_URL, data=json.dumps(item), headers=G.HEADERS, timeout=5)
                print('request[%s] : code = %s ' % (k, resp_orion.status_code))
                if resp_orion.status_code == 201:
                    print('Data sent to ODB : Success! code[%s]' % resp_orion.status_code)
            except requests.exceptions.Timeout as t:
                print('Error', t)
                print('Message : ', resp_orion.text)

            time.sleep(1)
            k += 1
