import datetime
import math as m
from datetime import timedelta
from itertools import islice


# Cleans invalid characters in Orion Context Broker
def cleanChars(S):
    chars = {"(", ")", ";", "=", "'", ">", "<"}
    if isinstance(S, list):
        for i in range(0, len(S)):
            for j in chars:
                S[i] = str(S[i]).replace(j, ' ')
        return S
    else:
        for i in chars:
            S = str(S).replace(i, ' ')
        return S


# Verify geojson coordinates format
def verify_geojson_coords(longitud, latitud):
    out = []
    try:
        lng = float(longitud)
        lat = float(latitud)
        out.append(lng)
        out.append(lat)
    except Exception as e:
        print('Coud not cast [%s , %s] ' % (longitud, latitud))
        print('Error : ', str(e))

    return out


# more economic verification
def verify_coords_vals(longitud, latitud):
    if (longitud is None) or (latitud is None):
        return []

    out = []
    Lng = longitud.replace('-', '').split('.')
    Lat = latitud.replace('-', '').split('.')
    if len(Lng) == 2 and Lng[0].isdigit() and Lng[1].isdigit():
        out.append(float(longitud))
    else:
        print('\n Can Not cast Lng: %s to float' % longitud)

    if len(Lat) == 2 and Lat[0].isdigit() and Lat[1].isdigit():
        out.append(float(latitud))
    else:
        print('\n Can Not cast Lat: %s to float' % latitud)

    if len(out) == 2:
        return out
    else:
        return []

# Returns date with parameters
# days_ : number of days shift
def getTodayFormatSearch(days_):
    days_b = datetime.date.today() - timedelta(days=days_)
    #today = datetime.date.today()
    return str(days_b).replace('-', '')


# Creates Sub-Quadrants : From given main Quadrant
# returns
# SW : dict : <key> : <string>(South-West coordinates)
# NE : dict : <key> : <string>(North.East coordinates)
def meshByQuadrant(SW, NE, X_pts, Y_pts):
    Cords_SW = {}
    Cords_NE = {}
    delta_X = (NE[1] - SW[1])/X_pts
    delta_Y = (NE[0] - SW[0])/Y_pts
    print('  Dimensiones de Quadrantes calculados :')
    print('dx[lng] = %s [m] : dy[lat] = %s [m]' % (delta_X*100000, delta_Y*100000))

    k = 0
    for j in range(0, Y_pts):
        y_sw = SW[0] + j*delta_Y
        y_ne = SW[0] + (j+1)*delta_Y # nueva esquna
        for i in range(0, X_pts):
            x_sw = SW[1] + i*delta_X
            x_ne = SW[1] + (i+1)*delta_X # nueva esquina
            key_sw = str(i)+'-'+str(j)
            key_ne = str(i)+'-'+str(j)
            Cords_SW[key_sw] = str(y_sw)+', '+str(x_sw)
            Cords_NE[key_ne] = str(y_ne)+', '+str(x_ne)
            k += 1

    print('Total de Cuadrantes : ', k)
    return Cords_SW, Cords_NE


# Auxiliary function to measure distance between two points
def verifyDistance(SW, NE):
    dist = m.sqrt((SW[0]-NE[0])**2 + (SW[1]-NE[1])**2)
    print('dist 2 pts = %s : [m]' % dist)


# Splits json list of entities, for batch upload to Orion CB
def split(n, iterable):
    i = iter(iterable)
    piece = list(islice(i, n))
    while piece:
        yield piece
        piece = list(islice(i, n))
