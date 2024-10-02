import math
import json
import requests
import datetime
import pytz
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from keycloak import KeycloakOpenID
from bearer_auth import BearerAuth

# FROST URLs
FROST_BASE_URL = os.environ.get('FROST_SERVER')
FROST_THINGS_WITH_DATASTREAMS = FROST_BASE_URL+"/Things?$expand=Datastreams,Locations"
FROST_OBSERVATIONS = FROST_BASE_URL+"/Datastreams(<DATASTREAM_ID>)/Observations?$filter=not phenomenonTime lt <STARTTIME>&$count=false"
POST_URL = FROST_BASE_URL+"/$batch"

FROST_USER = os.environ.get('FROST_USER')
FROST_PASS = os.environ.get('FROST_PASSWORD')
frost_auth=(FROST_USER,FROST_PASS)

CAMDATA_URL = os.environ.get('CAMDATA_URL')
CAMDATA_AUTH_URL = os.environ.get('CAMDATA_AUTH_URL')
CAMDATA_REALM = os.environ.get('CAMDATA_REALM')
CAMDATA_CLIENT_ID = os.environ.get('CAMDATA_CLIENT_ID')
CAMDATA_CLIENT_SECRET = os.environ.get('CAMDATA_CLIENT_SECRET')

API_URL = "http://20.218.113.185/api/thermicam?fromDay=<FROM>>&toDay=<TO>&fromHour=0&toHour=23&fromMinute=0&toMinute=59&ids=<CAM_ID>"
# Interval definitions
INTERVAL_5_MIN = "5-Min"
INTERVAL_1_HOUR = "1-Stunde"
INTERVAL_1_DAY = "1-Tag"
INTERVAL_1_WEEK = "1-Woche"
INTERVAL_1_MONTH = "1-Monat"
INTERVAL_1_YEAR = "1-Jahr"

INTERVAL_5_MIN_LABEL = "5 Minuten"
INTERVAL_1_HOUR_LABEL = "Stunde"
INTERVAL_1_DAY_LABEL = "Tag"
INTERVAL_1_WEEK_LABEL = "Woche"
INTERVAL_1_MONTH_LABEL = "Monat"
INTERVAL_1_YEAR_LABEL = "Jahr"

INTERVAL_5_MIN_DURATION = datetime.timedelta(minutes=5)
INTERVAL_1_HOUR_DURATION = datetime.timedelta(hours=1)
INTERVAL_1_DAY_DURATION = datetime.timedelta(days=1)
INTERVAL_1_WEEK_DURATION = datetime.timedelta(days=7)

TIMEZONE = pytz.timezone("Europe/Berlin")
UTC = pytz.utc

TIMEOUT = 180

mq_dummy_zone = {
    "zoneId" : "MQ",
    "lane" : "Messquerschnitt"
}

mot_label = {
    "ped": "Fußgänger",
    "bike": "Fahrrad",
    "Car": "PKW",
    "motorbike": "Motorrad",
    "van": "Lieferwagen",
    "smallTruck": "Kleinlaster",
    "largeTruck": "Großer LKW",
    "bus": "Bus"
}

mot_count = {
    "ped": "qPed",
    "bike": "qBike",
    "Car": "qCar",
    "motorbike": "qMotorbike",
    "van": "qVan",
    "smallTruck": "qSmallTruck",
    "largeTruck": "qLargeTruck",
    "bus": "qBus"
}

mot_speed = {
    "ped": "vPed",
    "bike": "vBike",
    "Car": "vCar",
    "motorbike": "vMotorbike",
    "van": "vVan",
    "smallTruck": "vSmallTruck",
    "largeTruck": "vLargeTruck",
    "bus": "vBus"
}

cams = None

observedPropertyCount = None
observedPropertySpeed = None
sensor = None
things = None

sched = BlockingScheduler()

# Configure client
keycloak_openid = KeycloakOpenID(server_url=CAMDATA_AUTH_URL,
                                 client_id=CAMDATA_CLIENT_ID,
                                 realm_name=CAMDATA_REALM,
                                 client_secret_key=CAMDATA_CLIENT_SECRET)


def getToken():
    token = keycloak_openid.token(grant_type='client_credentials')

    return BearerAuth(token['access_token'])

def load_master_data():
    global cams
    q_res = requests.get(CAMDATA_URL, timeout=TIMEOUT)
    if (q_res.status_code == 200):
        cams = q_res.json()
    else:
        print("Error "+str(q_res.status_code))
        print(q_res.text)
        raise Exception('Could not load Cam Data!')

def init():
    load_master_data()
    init_observedProperty()
    init_sensor()
    init_things()
    #load_hourly_data()

def init_observedProperty():
    global observedPropertyCount
    global observedPropertySpeed
    # load measurements
    observedPropertyCount = load_observedProperty('Verkehrsstärke')
    observedPropertySpeed = load_observedProperty('Geschwindigkeit')

    if observedPropertyCount is None:
        # create measurement
        observedPropertyCount = create_observedProperty('Verkehrsstärke')
    if observedPropertySpeed is None:
        # create measurement
        observedPropertySpeed = create_observedProperty('Geschwindigkeit')

def load_observedProperty(name):
    q_res = requests.get(FROST_BASE_URL+'/ObservedProperties', auth=frost_auth, timeout=TIMEOUT)
    if (q_res.status_code == 200):
        json_response = q_res.json()
        if 'value' in json_response:
            for property in  json_response['value']:
                if 'name' in property and property['name'] == name:
                    return property['@iot.id']
    else:
        print("Error "+str(q_res.status_code))
        print(q_res.text)
        raise Exception('Could not load ObservedProperty!')
    return None

def create_observedProperty(name):
    q_data = {
        "name": name,
        "description": "SenUMVK Definition",
        "definition": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
    }

    q_res = requests.post(FROST_BASE_URL+'/ObservedProperties', auth=frost_auth, json=q_data, timeout=TIMEOUT)
    if (q_res.status_code != 201):
        print("Could not create ObservedProperty for '"+name+"'")
    else:
        q_data = requests.get(q_res.headers['location'], timeout=TIMEOUT)
        print(name+'-id: ' + str(q_data.json()['@iot.id']))
        return q_data.json()['@iot.id']

def init_sensor():
    global sensor
    # load sensor
    sensor = load_sensor()

    if sensor is None:
        sensor = create_sensor()

def load_sensor():
    q_res = requests.get(FROST_BASE_URL+'/Sensors', auth=frost_auth, timeout=TIMEOUT)
    if (q_res.status_code == 200):
        json_response = q_res.json()
        if 'value' in json_response:
            for sensor in  json_response['value']:
                if 'name' in sensor and sensor['name'] == 'ThermiCam AI':
                    return sensor['@iot.id']
    else:
        print("Error "+str(q_res.status_code))
        print(q_res.text)
        raise Exception('Could not load Sensor!')
    return None

def create_sensor():
    sensor_data = {
        "name" : "ThermiCam AI",
        "description" : "Wärmebild-Verkehrssensor mit KI-Unterstützung",
        "encodingType" : "text/html",
        "metadata" : "https://www.flir.de/products/thermicam-ai/?model=10-7736"
    }

    q_res = requests.post(FROST_BASE_URL+'/Sensors', auth=frost_auth, json=sensor_data, timeout=TIMEOUT)
    if (q_res.status_code != 201):
        print("Could not create Sensor for 'ThermiCam AI'")
    else:
        q_data = requests.get(q_res.headers['location'], timeout=TIMEOUT)
        print('Sensor-id: ' + str(q_data.json()['@iot.id']))
        return q_data.json()['@iot.id']

def load_things():
    print(FROST_THINGS_WITH_DATASTREAMS)
    results = []
    try:
        r = requests.get(FROST_THINGS_WITH_DATASTREAMS, timeout=TIMEOUT)
    except:
        r = requests.get(FROST_THINGS_WITH_DATASTREAMS, timeout=TIMEOUT)
    if (r.status_code == 200):
        json_response = r.json()
        if 'value' in json_response:
            results += json_response['value']
        else:
            results.append(json_response)
        while '@iot.nextLink' in json_response:
            try:
                r = requests.get(json_response['@iot.nextLink'], timeout=TIMEOUT)
            except:
                r = requests.get(json_response['@iot.nextLink'], timeout=TIMEOUT)
            if r.status_code != 200:
                print(str(r.status_code)+": "+r.json()['message'])
                raise Exception("Could not load Data from Frost")
            json_response = r.json()
            results += json_response['value']
    else:
        print("Error "+str(r.status_code))
        print(r.text)
        raise Exception('Could not load Data from Frost!')
    for thing in results:
        if "Datastreams@iot.nextLink" in thing:
            nextLink = thing['Datastreams@iot.nextLink']
            while nextLink != None:
                try:
                    r = requests.get(nextLink, timeout=TIMEOUT)
                except:
                    r = requests.get(nextLink, timeout=TIMEOUT)
                if r.status_code != 200:
                    print(str(r.status_code)+": "+r.json()['message'])
                    raise Exception("Could not load Data from Frost")
                json_response = r.json()
                thing["Datastreams"] += json_response['value']
                nextLink = json_response['@iot.nextLink'] if '@iot.nextLink' in json_response else None

    return results

def init_things():
    global things, cams
    # load things
    things = load_things()
    # Update
    update_things(things, cams)
    # Reload changed things
    #things = load_things()

def update_things(things, cams):
    for cam in cams:
        thing = find_thing(things, cam)
        if thing is None:
            create_thing(cam)
        else:
            update_thing(thing, cam)

def find_thing(things, cam):
    for thing in things:
        if thing['properties']['cameraId'] == cam['cameraId']:
            return thing
    return None

def find_cam(cams, cameraId):
    for cam in cams:
        if cam['cameraId'] == cameraId:
            return cam
    return None


def create_thing(cam):
    description = cam['position'] + ' (' + cam['pos_detail'] + ')  - Richtung: ' + cam['direction']
    thing = {
        "name": cam['cameraId'],
        "description": description,
        "properties": {
            "cameraId": cam['cameraId'],
            "position": cam['position'],
            "position_detail": cam['pos_detail'],
            "plz": cam['plz'],
            "bezirk": cam['bezirk'],
            "ortsteil": cam['ortsteil'],
            "direction": cam['direction'],
            "lamppost": cam["lamppost"]
        },
        "Locations": [
            {
                "name": cam['position'] + ' (' + cam['pos_detail'] + ')',
                "description": description,
                "encodingType": "application/geo+json",
                "location": {
                    "type": "Point",
                    "coordinates": [cam['longitude'], cam['latitude']]
                }
            }
        ],
        "Datastreams": []
    }
    create_datastreams(thing, cam);

    # Store Thing in Frost-Server
    print(json.dumps(thing, indent=4, sort_keys=True))
    q_res = requests.post(FROST_BASE_URL + '/Things', auth=frost_auth, json=thing, timeout=TIMEOUT)
    if (q_res.status_code != 201):
        print("Could not create Thing " + thing['name'])
        print(q_res.text)
    else:
        print("Created Thing " + thing['name'])

def create_datastreams(thing, cam):
    for mot in mot_label.keys():
        thing['Datastreams'].append(create_datastreamCount(cam, mq_dummy_zone, mot, INTERVAL_5_MIN, INTERVAL_5_MIN_LABEL))
        thing['Datastreams'].append(create_datastreamCount(cam, mq_dummy_zone, mot, INTERVAL_1_HOUR, INTERVAL_1_HOUR_LABEL))
        thing['Datastreams'].append(create_datastreamCount(cam, mq_dummy_zone, mot, INTERVAL_1_DAY, INTERVAL_1_DAY_LABEL))
        thing['Datastreams'].append(create_datastreamCount(cam, mq_dummy_zone, mot, INTERVAL_1_WEEK, INTERVAL_1_WEEK_LABEL))
        thing['Datastreams'].append(create_datastreamCount(cam, mq_dummy_zone, mot, INTERVAL_1_MONTH, INTERVAL_1_MONTH_LABEL))
        thing['Datastreams'].append(create_datastreamCount(cam, mq_dummy_zone, mot, INTERVAL_1_YEAR, INTERVAL_1_YEAR_LABEL))

        thing['Datastreams'].append(create_datastreamSpeed(cam, mq_dummy_zone, mot, INTERVAL_5_MIN, INTERVAL_5_MIN_LABEL))
        thing['Datastreams'].append(create_datastreamSpeed(cam, mq_dummy_zone, mot, INTERVAL_1_HOUR, INTERVAL_1_HOUR_LABEL))
        thing['Datastreams'].append(create_datastreamSpeed(cam, mq_dummy_zone, mot, INTERVAL_1_DAY, INTERVAL_1_DAY_LABEL))
        thing['Datastreams'].append(create_datastreamSpeed(cam, mq_dummy_zone, mot, INTERVAL_1_WEEK, INTERVAL_1_WEEK_LABEL))
        thing['Datastreams'].append(create_datastreamSpeed(cam, mq_dummy_zone, mot, INTERVAL_1_MONTH, INTERVAL_1_MONTH_LABEL))
        thing['Datastreams'].append(create_datastreamSpeed(cam, mq_dummy_zone, mot, INTERVAL_1_YEAR, INTERVAL_1_YEAR_LABEL))
    for zone in cam['zones']:
        for mot in mot_label.keys():
            thing['Datastreams'].append(create_datastreamCount(cam, zone, mot, INTERVAL_5_MIN, INTERVAL_5_MIN_LABEL))
            thing['Datastreams'].append(create_datastreamCount(cam, zone, mot, INTERVAL_1_HOUR, INTERVAL_1_HOUR_LABEL))
            thing['Datastreams'].append(create_datastreamCount(cam, zone, mot, INTERVAL_1_DAY, INTERVAL_1_DAY_LABEL))
            thing['Datastreams'].append(create_datastreamCount(cam, zone, mot, INTERVAL_1_WEEK, INTERVAL_1_WEEK_LABEL))
            thing['Datastreams'].append(create_datastreamCount(cam, zone, mot, INTERVAL_1_MONTH, INTERVAL_1_MONTH_LABEL))
            thing['Datastreams'].append(create_datastreamCount(cam, zone, mot, INTERVAL_1_YEAR, INTERVAL_1_YEAR_LABEL))

            thing['Datastreams'].append(create_datastreamSpeed(cam, zone, mot, INTERVAL_5_MIN, INTERVAL_5_MIN_LABEL))
            thing['Datastreams'].append(create_datastreamSpeed(cam, zone, mot, INTERVAL_1_HOUR, INTERVAL_1_HOUR_LABEL))
            thing['Datastreams'].append(create_datastreamSpeed(cam, zone, mot, INTERVAL_1_DAY, INTERVAL_1_DAY_LABEL))
            thing['Datastreams'].append(create_datastreamSpeed(cam, zone, mot, INTERVAL_1_WEEK, INTERVAL_1_WEEK_LABEL))
            thing['Datastreams'].append(create_datastreamSpeed(cam, zone, mot, INTERVAL_1_MONTH, INTERVAL_1_MONTH_LABEL))
            thing['Datastreams'].append(create_datastreamSpeed(cam, zone, mot, INTERVAL_1_YEAR, INTERVAL_1_YEAR_LABEL))

def create_datastreamCount(cam, zone, mot, step_name_part, step_label):
    datastream =  {
        "name": "Anzahl " + mot_label[mot] +" " + step_label + " -  " + zone['lane'],
        "description" : "Anzahl " + mot_label[mot] +" pro " + step_label +" für " + str(cam['position']) + " (" + str(cam['pos_detail']) + ") - " + zone['lane'],
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "Sensor": {"@iot.id": sensor},
        "unitOfMeasurement": {
            "name": "Verkehrsstärke",
            "symbol": mot_label[mot]+"/"+step_label
        },
        "ObservedProperty": {"@iot.id": observedPropertyCount},
        "properties": {
            "layerName": "Anzahl_"+mot+"_Zaehlstelle_"+step_name_part,
            "periodLength": step_name_part,
            "periodLengthLabel": step_label,
            "lane": zone["zoneId"],
            "laneLabel": zone["lane"],
            "vehicle": mot,
            "vehicleLabel": mot_label[mot],
            "measurement": "Anzahl"
        }
    }
    return datastream

def create_datastreamSpeed(cam, zone, mot, step_name_part, step_label):
    datastream = {
        "name": "Geschwindigkeit "+mot_label[mot]+" "+step_label + " -  "+zone['lane'],
        "description" : "Geschwindigkeit " + mot_label[mot] +" pro " + step_label +" für " + str(cam['position']) + " (" + str(cam['pos_detail']) + ") - " + zone['lane'],
        "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "Sensor": {"@iot.id": sensor},
        "unitOfMeasurement": {
            "name": "Geschwindigkeit",
            "symbol": "km/h"
        },
        "ObservedProperty": {"@iot.id": observedPropertySpeed},
        "properties": {
            "layerName": "Geschwindigkeit_"+mot_label[mot]+"_Zaehlstelle_"+step_name_part,
            "periodLength": step_name_part,
            "periodLengthLabel": step_label,
            "lane": zone["zoneId"],
            "laneLabel": zone["lane"],
            "vehicle": mot,
            "vehicleLabel": mot_label[mot],
            "measurement": "Geschwindigkeit"
        }
    }
    return datastream

def update_thing(thing, cam):
    updatedThing = {'properties':thing['properties']}
    changed = False

    description = cam['position'] + ' (' + cam['pos_detail'] + ')  - Richtung: ' + cam['direction']
    if thing['description'] != description:
        updatedThing['description'] = description
        changed = True
    if thing['properties']['position'] != cam['position']:
        updatedThing['properties']['position'] = cam['position']
        changed = True
    if 'position_detail' in thing['properties'] and thing['properties']['position_detail'] != cam['pos_detail']:
        updatedThing['properties']['position_detail'] = cam['pos_detail']
        changed = True
    if thing['properties']['plz'] != cam['plz']:
        updatedThing['properties']['plz'] = cam['plz']
        changed = True
    if thing['properties']['bezirk'] != cam['bezirk']:
        updatedThing['properties']['bezirk'] = cam['bezirk']
        changed = True
    if thing['properties']['bezirk'] != cam['bezirk']:
        updatedThing['properties']['bezirk'] = cam['bezirk']
        changed = True
    if thing['properties']['ortsteil'] != cam['ortsteil']:
        updatedThing['properties']['ortsteil'] = cam['ortsteil']
        changed = True
    if thing['properties']['direction'] != cam['direction']:
        updatedThing['properties']['direction'] = cam['direction']
        changed = True
    if thing['properties']['lamppost'] != cam['lamppost']:
        updatedThing['properties']['lamppost'] = cam['lamppost']
        changed = True

    location_name = cam['position'] + ' (' + cam['pos_detail'] + ')'
    if (thing['Locations'][0]["name"] != location_name) or (thing['Locations'][0]['location']['coordinates'][0] != cam['longitude']) or (thing['Locations'][0]['location']['coordinates'][1] != cam['latitude']):
        updatedThing['Locations'] = thing['Locations']
        updatedThing['Locations'][0]["name"] = location_name
        updatedThing['Locations'][0]['location']['coordinates'] = [cam['longitude'], cam['latitude']]
        changed = True

    if changed:
        # Update Thing in Frost-Server
        q_res = requests.patch(FROST_BASE_URL+'/Things('+str(thing['@iot.id'])+')', auth=frost_auth, json=updatedThing, timeout=TIMEOUT)
        if (q_res.status_code != 200):
            print(json.dumps(updatedThing, indent=4, sort_keys=True))
            print("Could not update Thing "+thing['name']+'('+str(thing['@iot.id'])+')')
            print(q_res.text)
        else:
            print("Updated Thing "+thing['name']+'('+str(thing['@iot.id'])+')')
            # Update Datastreams

def load_observations(datastream, starttime):
    url = FROST_OBSERVATIONS.replace('<DATASTREAM_ID>', str(datastream['@iot.id'])).replace('<STARTTIME>', starttime.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    results = []
    r = requests.get(url, timeout=TIMEOUT)
    if (r.status_code == 200):
        json_response = r.json()
        if 'value' in json_response:
            results += json_response['value']
        else:
            results.append(json_response)
        while '@iot.nextLink' in json_response:
            r = requests.get(json_response['@iot.nextLink'], timeout=TIMEOUT)
            if r.status_code != 200:
                print(r.text)
                print(str(r.status_code)+": "+r.json()['message'])
                raise Exception("Could not load Data from Frost")
            json_response = r.json()
            results += json_response['value']
    else:
        print('Could not load Observations - '+str(r.status_code))
    observations = {}

    for result in results:
        phenomenonTime = result['phenomenonTime']
        phenomenonTimeSplit = phenomenonTime.split('/')
        phenomenonTimeStart = UTC.localize(datetime.datetime.strptime(phenomenonTimeSplit[0], "%Y-%m-%dT%H:%M:%SZ"))
        phenomenonTimeEnd = UTC.localize(datetime.datetime.strptime(phenomenonTimeSplit[1], "%Y-%m-%dT%H:%M:%SZ"))
        phenomenonTime = phenomenonTimeStart.strftime("%Y-%m-%dT%H:%M:%SZ")+'/'+phenomenonTimeEnd.strftime("%Y-%m-%dT%H:%M:%SZ")
        observations[phenomenonTime] = result;
    return observations

def updateThingStatus(thing, status):
    dummy = None
    updatedThing = {'properties':thing['properties']}
    updatedThing['properties']['status'] = status

    # Update Thing in Frost-Server
    q_res = requests.patch(FROST_BASE_URL+'/Things('+str(thing['@iot.id'])+')', auth=frost_auth, json=updatedThing, timeout=TIMEOUT)
    if (q_res.status_code != 200):
        print(json.dumps(updatedThing, indent=4, sort_keys=True))
        print("Could not update Thing "+thing['name']+'('+str(thing['@iot.id'])+')')
        print(q_res.text)
    else:
        print("Updated Thing Status "+thing['name']+'('+str(thing['@iot.id'])+')')

def updateStatus():
    start = TIMEZONE.localize(datetime.datetime.now() - datetime.timedelta(days=2))
    end = TIMEZONE.localize(datetime.datetime.now())
    data = load_api_data(start, end)
    for thing in things:
        status = "inactive"
        for dataset in data:
            if thing["properties"]["cameraId"] == dataset["cameraId"]:
                status = "active"
        if not "status" in thing["properties"] or thing["properties"]["status"] != status:
            updateThingStatus(thing, status)
            thing["properties"]["status"] = status



def import_observations(start, intervals):
    start = UTC.localize(start.replace(hour=0, minute=0, second = 0, microsecond = 0, tzinfo=None))
    end = TIMEZONE.localize(datetime.datetime.now())
    observations = []
    for thing in things:
        print(thing["properties"]["cameraId"])
        data = load_api_data(start, end, thing["properties"]["cameraId"])
        print(len(data))
        for datastream in thing["Datastreams"]:
            #print("Datastream: "+str(datastream['@iot.id']))
            if(datastream['properties']["periodLength"] in intervals):
                observations += createAndUpdateObservations(thing, datastream, data, start, end)
            if len(observations) >= 1000:
                post_observations(observations)
                observations = []
    post_observations(observations)

def createAndUpdateObservations(thing, datastream, data, start, end):
    if datastream['properties']["measurement"] == "Anzahl":
        return createAndUpdateObservationsCount(thing, datastream, data, start, end)
    else:
        return createAndUpdateObservationsSpeed(thing, datastream, data, start, end)

def createAndUpdateObservationsCount(thing, datastream, data, start, end):
    mot = datastream['properties']["vehicle"]
    zone = datastream['properties']['lane']
    interval = datastream['properties']["periodLength"]
    begin = startOfStep(start, interval)
    existingObservations = load_observations(datastream, begin)

    results = {}
    for dataset in data:
        if thing["properties"]["cameraId"] == dataset["cameraId"] and (datastream["properties"]["lane"] == dataset["zoneName"] or datastream["properties"]["lane"]  == "MQ"):
            phenomenonTimeStart = startOfStep(UTC.localize(datetime.datetime.strptime(dataset["utc"], "%Y-%m-%dT%H:%M:%S.%fZ")), interval)
            phenomenonTimeEnd = getEndTime(phenomenonTimeStart, interval)
            if not phenomenonTimeStart.isoformat() in results:
                results[phenomenonTimeStart.isoformat()] = {
                    "phenomenonTimeStart": phenomenonTimeStart,
                    "phenomenonTimeEnd": phenomenonTimeEnd,
                    "value": 0
                }
            results[phenomenonTimeStart.isoformat()]["value"] += dataset[mot_count[mot]]


    observations = []
    for result in results.values():
        observation = create_or_update_observation(result, datastream, existingObservations)
        if not observation is None:
            observations.append(observation)
    return observations




def createAndUpdateObservationsSpeed(thing, datastream, data, start, end):
    mot = datastream['properties']["vehicle"]
    zone = datastream['properties']['lane']
    interval = datastream['properties']["periodLength"]
    begin = startOfStep(start, interval)
    existingObservations = load_observations(datastream, begin)

    results = {}
    for dataset in data:
        if thing["properties"]["cameraId"] == dataset["cameraId"] and (datastream["properties"]["lane"] == dataset["zoneName"] or datastream["properties"]["lane"]  == "MQ"):
            speedValue = dataset[mot_speed[mot]]
            countValue = dataset[mot_count[mot]]
            if speedValue > -1:
                phenomenonTimeStart = startOfStep(UTC.localize(datetime.datetime.strptime(dataset["utc"], "%Y-%m-%dT%H:%M:%S.%fZ")), interval)
                phenomenonTimeEnd = getEndTime(phenomenonTimeStart, interval)
                if not phenomenonTimeStart.isoformat() in results:
                    results[phenomenonTimeStart.isoformat()] = {
                        "phenomenonTimeStart": phenomenonTimeStart,
                        "phenomenonTimeEnd": phenomenonTimeEnd,
                        "value": 0,
                        "countSum": 0,
                        "speedSum": 0
                    }
                results[phenomenonTimeStart.isoformat()]["countSum"] += dataset[mot_count[mot]]
                results[phenomenonTimeStart.isoformat()]["speedSum"] += dataset[mot_speed[mot]] * dataset[mot_count[mot]]
                if results[phenomenonTimeStart.isoformat()]["countSum"] > 0:
                    results[phenomenonTimeStart.isoformat()]["value"] = round(results[phenomenonTimeStart.isoformat()]["speedSum"] / results[phenomenonTimeStart.isoformat()]["countSum"],2)


    observations = []
    for result in results.values():
        observation = create_or_update_observation(result, datastream, existingObservations)
        if not observation is None:
            observations.append(observation)
    return observations

def startOfStep(time, step):
    time = time.replace(second = 0, microsecond = 0)
    if step == INTERVAL_5_MIN:
        return time.replace(minute=math.floor(time.minute/5)*5)
    if step == INTERVAL_1_HOUR:
        return time.replace(minute=0)
    if step == INTERVAL_1_DAY:
        return TIMEZONE.localize(time.replace(hour=0, minute=0, tzinfo=None))
    if step == INTERVAL_1_WEEK:
        return TIMEZONE.localize((time - datetime.timedelta(days=time.weekday())).replace(hour=0, minute=0, tzinfo=None))
    if step == INTERVAL_1_MONTH:
        return TIMEZONE.localize(time.replace(day = 1, hour=0, minute=0, tzinfo=None))
    if step == INTERVAL_1_YEAR:
        return TIMEZONE.localize(time.replace(month = 1, day = 1, hour=0, minute=0, tzinfo=None))
    return None

def getEndTime(start, step):
    if step == INTERVAL_5_MIN:
        return start + INTERVAL_5_MIN_DURATION
    if step == INTERVAL_1_HOUR:
        return start + INTERVAL_1_HOUR_DURATION
    if step == INTERVAL_1_DAY:
        return start + INTERVAL_1_DAY_DURATION
    if step == INTERVAL_1_WEEK:
        return start + INTERVAL_1_WEEK_DURATION
    if step == INTERVAL_1_MONTH:
        if start.month < 12:
            return start.replace(month=start.month+1)
        else:
            return start.replace(year=start.year+1, month=1)
    if step == INTERVAL_1_YEAR:
        return start.replace(year=start.year+1)
    return None

def load_api_data(start, end, cam = ""):
    url = API_URL.replace('<FROM>', start.astimezone(UTC).strftime("%Y-%m-%d")).replace('<TO>', end.astimezone(UTC).strftime("%Y-%m-%d")).replace('<CAM_ID>', cam)
    results = []
    r = requests.get(url, auth=getToken(), timeout=TIMEOUT)
    if (r.status_code == 200):
        return r.json()
    else:
        print('Could not load Data - '+str(r.status_code))


def post_observations(observations):
    if len(observations) > 500:
        post_observations(observations[:len(observations)-500])
        observations = observations[len(observations)-500:]
    print('Observations: '+str(len(observations)))
    if len(observations) > 0:
        r = requests.post(url=POST_URL, auth=frost_auth, json={"requests": observations}, headers={"Content-Type": "application/json;charset=UTF-8"}, timeout=TIMEOUT)
        #print(str(r.status_code)+": "+r.text)
        if (r.status_code != 200):
            print("Could not save Observations")
            print(str(r.status_code)+": "+r.text)
        else:
            json_response = r.json()
            if 'responses' in json_response:
                print(str(len(json_response['responses'])) + ' Responses')
                for response in json_response['responses']:
                    if response['status'] != 200 and response['status'] != 201:
                        print(str(response['id']) + " (" + str(response['status']) + "): ") + response['body']

def update_obersvation(observation):
    q_res = requests.patch(FROST_BASE_URL+'/Observations('+str(observation['@iot.id'])+')', auth=frost_auth, json=observation, timeout=TIMEOUT)
    if (q_res.status_code != 200):
        print("Could not update Observation "+observation['phenomenonTime']+'('+str(observation['@iot.id'])+')')
        print(q_res.text)
    else:
        print("Updated Observation "+observation['phenomenonTime']+'('+str(observation['@iot.id'])+')')


def create_or_update_observation(result, datastream, observations):
    isoDateStart = result["phenomenonTimeStart"]
    isoDateEnd = result["phenomenonTimeEnd"]
    phenomenonTime = isoDateStart.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")+'/'+isoDateEnd.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    if phenomenonTime in observations:
        observation = observations[phenomenonTime]
        if not observation['result'] == result['value']:
            #print(str(observation['result']) + ' != ' + str(result['value']))
            observation = {
                "@iot.id" : observation['@iot.id'],
                "phenomenonTime": observation['phenomenonTime'],
                "resultTime": datetime.datetime.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "result": result['value']
            }
            #update_obersvation(observation)
            return {
                "id": str(observation['@iot.id']),
                "method": "patch",
                "url": 'Observations('+str(observation['@iot.id'])+')',
                "body": observation
            }
        return None
    else:
        #print('/Datastreams('+str(datastream['@iot.id'])+')/Observations')
        return {
            "id": str(datastream['@iot.id'])+'_'+isoDateStart.isoformat(),
            "method": "post",
            "url": 'Datastreams('+str(datastream['@iot.id'])+')/Observations',
            "body": {
                "phenomenonTime": phenomenonTime,
                "resultTime": datetime.datetime.now().astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "result": result['value']
            }
        }


@sched.scheduled_job('cron',minute="*/5")
def run_import():
    init_things()
    import_observations(datetime.datetime.now()-datetime.timedelta(hours=2), [INTERVAL_5_MIN, INTERVAL_1_HOUR])
    import_observations(datetime.datetime.now()-datetime.timedelta(days=2), [INTERVAL_1_DAY])
    updateStatus()

@sched.scheduled_job('cron',hour="0", minute="32")
def run_import_long():
    import_observations(datetime.datetime(year=2023, month=12, day=30), [INTERVAL_1_WEEK, INTERVAL_1_MONTH, INTERVAL_1_YEAR])


def import_archive():
    init_things()
    import_observations(datetime.datetime(year=2023, month=12, day=20), [INTERVAL_5_MIN, INTERVAL_1_HOUR, INTERVAL_1_DAY, INTERVAL_1_WEEK, INTERVAL_1_MONTH, INTERVAL_1_YEAR])
    updateStatus()

init()

#import_archive()
#run_import()
#run_import_long()

print("Starting Scheduler")
sched.start()
print("End")
