from classes import FourSquareRequest, DataModel
import functions.Utilities as U
import sys
import parameters.Globals as G
import random
import time

if __name__ == '__main__':

    FQR = FourSquareRequest.EndPointsAccess()
    dm = DataModel.FoursquareToOCB()
    SW, NE = U.meshByQuadrant([19.269080, -99.255295],
                              [19.562263, -99.014593], 60, 70)
    print(type(SW))
    DataCollect = []
    k = 0
    """
    for key, value in SW.items():
        k += 1
        # last [0 - 2220, 3144] ok
        if k < 3144:
            continue
        a = '[%s] %s : SW[ %s] : NE[%s]' % (k, key, value, NE[key])
        print(a)
        t = random.randint(1, 4)
        time.sleep(t)
        FQR.searchBrowse(SW[key], NE[key], 200)
        data, resp = FQR.request(False)  # False : No cambia formato a OCB
        if resp.status_code == 200:
            # Regresa diccionario de formato search venue
            for item in data['response']['venues']:
                DataCollect.append(item)
            # TODO : save in mongoDB
            dm.saveToJSON(DataCollect, 'collect_4sqr_23_mayo_2019_HH.json')
            print('[%s] data saved :' % len(DataCollect))
        else:
            print('Error code = %s' % resp.status_code)
            sys.exit()
    """
    # dm.printListFormat_sample(DataCollect)
