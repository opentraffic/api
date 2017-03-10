#!/usr/bin/env python

'''
If you're running this from this directory you can start the server with the following command:
./query.py localhost:8004

sample url looks like this:
http://localhost:8004/query?segment_ids=19320,67128184
http://localhost:8005/query?segment_ids=19320,67128184,156531727209&hours=11,12,3&dow=0&include_geometry=true
http://localhost:8005/query?segment_ids=19320,67128184,156531727209&hours=11,12,3&dow=0&include_geometry=false
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00
http://localhost:8004/query?segment_ids=19320,67128184&dow=0
http://localhost:8004/query?segment_ids=19320,67128184&hours=11,12,3
http://localhost:8004/query?segment_ids=19320,67128184&hours=11,12,3&dow=0,1,2,3,4,5,6
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00&hours=11,12,3,0
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00&dow=0,1,2,3,4,5,6
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00&hours=11,12,3,0&dow=0,1,2,3,4,5,6
http://localhost:8004/query?segment_ids=19320,67128184&hours=11,12,3&dow=0&boundingbox=120.885,14.327,121.2,14.9&include_geometry=false
http://localhost:8004/query?segment_ids=19320,67128184&hours=11,12,3&dow=0&boundingbox=120.885,14.327,121.2,14.9&include_geometry=true
http://localhost:8004/query?segment_ids=19320,67128184&start_date_time=2017-01-02T00:00:00&end_date_time=2017-03-07T16:00:00&hours=11,12,3,0&dow=0,1,2,3,4,5,6&include_geometry=true&boundingbox=120.885,14.327,121.2,14.9
'''

import sys
import json
import multiprocessing
import threading
from Queue import Queue
import socket
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from cgi import urlparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import time
import calendar
import datetime
import urllib
import math
from distutils.util import strtobool
import rtree
from shapely.geometry import shape
from shapely.geometry import box
from bitstring import BitArray

actions = set(['query'])
tile_hierarchy = None
# tile ids for tiles found on disk
tile_ids = {}
# rtree index
index = rtree.index.Index()
# tiles we have loaded into index
cached_tiles = set()

# this is where thread local storage lives
thread_local = threading.local()

#world bb
minx_ = -180
miny_ = -90
maxx_ = 180
maxy_ = 90

class BoundingBox(object):

  def __init__(self, min_x, min_y, max_x, max_y):
     self.minx = min_x
     self.miny = min_y
     self.maxx = max_x
     self.maxy = max_y

  #The bounding boxes do NOT intersect if the other bounding box is
  #entirely LEFT, BELOW, RIGHT, or ABOVE bounding box.
  def Intersects(self, bbox):
    if ((bbox.minx < self.minx and bbox.maxx < self.minx) or
        (bbox.miny < self.miny and bbox.maxy < self.miny) or
        (bbox.minx > self.maxx and bbox.maxx > self.maxx) or
        (bbox.miny > self.maxy and bbox.maxy > self.maxy)):
      return False
    return True

class TileHierarchy(object):

  def __init__(self):
    self.levels = {}
    # local -- no local tiles at this time
    # self.levels[2] = Tiles(BoundingBox(minx_,miny_,maxx_,maxy_),.25)
    # arterial
    self.levels[1] = Tiles(BoundingBox(minx_,miny_,maxx_,maxy_),1)
    # highway
    self.levels[0] = Tiles(BoundingBox(minx_,miny_,maxx_,maxy_),4)

class Tiles(object):

  def __init__(self, bbox, size):
     self.bbox = bbox
     self.tilesize = size

     self.ncolumns = int(math.ceil((self.bbox.maxx - self.bbox.minx) / self.tilesize))
     self.nrows = int(math.ceil((self.bbox.maxy - self.bbox.miny) / self.tilesize))
     self.max_tile_id = ((self.ncolumns * self.nrows) - 1)

  def TileCount(self):
    return self.ncolumns * self.nrows

  def Row(self, y):
    #Return -1 if outside the tile system bounds
    if (y < self.bbox.miny or y > self.bbox.maxy):
      return -1

    #If equal to the max y return the largest row
    if (y == self.bbox.maxy):
      return nrows - 1
    else:
      return int((y - self.bbox.miny) / self.tilesize)

  def Col(self, x):
    #Return -1 if outside the tile system bounds
    if (x < self.bbox.minx or x > self.bbox.maxx):
      return -1

    #If equal to the max x return the largest column
    if (x == self.bbox.maxx):
      return self.ncolumns - 1
    else:
      col = (x - self.bbox.minx) / self.tilesize
      return int(col) if (col >= 0.0) else int(col - 1)

  def Digits(self, number):
    digits = 1 if (number < 0) else 0
    while long(number):
       number /= 10
       digits += 1
    return long(digits)

  def TileExists(self, row, col, level, directory):

    #get the tile id
    tile_id = (row * self.ncolumns) + col

    max_length = self.Digits(self.max_tile_id)

    remainder = max_length % 3
    if remainder:
      max_length += 3 - remainder

    #if it starts with a zero the pow trick doesn't work
    if level == 0:
       file_suffix = '{:,}'.format(int(pow(10, max_length)) + tile_id).replace(',', '/')
       file_suffix += ".json"
       file_suffix = "0" + file_suffix[1:]
       file = directory + '/' + file_suffix

       if (os.path.isfile(file)):
         return tile_id
       return None

    #it was something else
    file_suffix = '{:,}'.format(level * int(pow(10, max_length)) + tile_id).replace(',', '/')
    file_suffix += ".json"
    file = directory + '/' + file_suffix

    if (os.path.isfile(file)):
      return tile_id
    return None

  # get the file name based on tile_id and level
  def GetFilename(self, tile_id, level, directory):

    max_length = self.Digits(self.max_tile_id)

    remainder = max_length % 3
    if remainder:
       max_length += 3 - remainder

    #if it starts with a zero the pow trick doesn't work
    if level == 0:
      file_suffix = '{:,}'.format(int(pow(10, max_length)) + tile_id).replace(',', '/')
      file_suffix += ".json"
      file_suffix = "0" + file_suffix[1:]
      file = directory + '/' + file_suffix

      if (os.path.isfile(file)):
        return file
      return None

    #it was something else
    file_suffix = '{:,}'.format(level * int(pow(10, max_length)) + tile_id).replace(',', '/')
    file_suffix += ".json"
    file = directory + '/' + file_suffix

    if (os.path.isfile(file)):
      return file
    return None

  def TileId(self, y, x):
    if (y < self.bbox.miny or x < self.bbox.minx or
        y > self.bbox.maxy or x > self.bbox.maxx):
      return -1

    #Find the tileid by finding the latitude row and longitude column
    return (self.Row(y) * self.ncolumns) + self.Col(x)

  # Get the bounding box of the specified tile.
  def TileBounds(self, tileid):
    row = tileid / self.ncolumns
    col = tileid - (row * self.ncolumns)

    x = self.bbox.minx + (col * self.tilesize)
    y = self.bbox.miny + (row * self.tilesize)
    return BoundingBox(x, y, x + self.tilesize, y + self.tilesize)

  # Get the neighboring tileid to the right/east.
  def RightNeighbor(self, tileid):
    row = tileid / self.ncolumns
    col = tileid - (row * self.ncolumns)

    return (tileid + 1) if (col < self.ncolumns - 1) else (tileid - self.ncolumns + 1)

  # Get the neighboring tileid to the left/west.
  def LeftNeighbor(self, tileid):
    row = tileid / self.ncolumns
    col = tileid - (row * self.ncolumns)
    return (tileid - 1) if (col > 0) else (tileid + self.ncolumns - 1)

  # Get the neighboring tileid above or north.
  def TopNeighbor(self, tileid):
    return (tileid + self.ncolumns) if (tileid < int(self.TileCount() - self.ncolumns)) else tileid

  # Get the neighboring tileid below or south.
  def BottomNeighbor(self, tileid):
    return tileid if (tileid < self.ncolumns) else (tileid - self.ncolumns)

  # Get the list of tiles that lie within the specified bounding box.
  # The method finds the center tile and spirals out by finding neighbors
  # and recursively checking if tile is inside and checking/adding
  # neighboring tiles
  def TileList(self, bbox, ids):
    # Get tile at the center of the bounding box. Return -1 if the center
    # of the bounding box is not within the tiling system bounding box.

    tilelist = []
    # Get the center of the BB to get the tile id
    tileid = self.TileId(((bbox.miny + bbox.maxy) * 0.5), ((bbox.minx + bbox.maxx) * 0.5))

    if (tileid == -1):
      return tilelist

    # List of tiles to check if in view. Use a list: push new entries on the
    # back and pop off the front. The tile search tends to spiral out from
    # the center.
    checklist = []

    # Visited tiles
    visited_tiles = set()

    # Set this tile in the checklist and it to the list of visited tiles.
    checklist.append(tileid)
    visited_tiles.add(tileid)

    # Get neighboring tiles in bounding box until NextTile returns -1
    # or the maximum number specified is reached
    while (len(checklist) != 0):
      #Get the element off the front of the list and add it to the tile list.
      tileid = checklist.pop(0)
      # only add tile ids that have been found in the geojson directory.
      if tileid in ids:
        tilelist.append(tileid)

      # Check neighbors
      neighbor = self.LeftNeighbor(tileid)
      if (neighbor not in visited_tiles and
          bbox.Intersects(self.TileBounds(neighbor))):
        checklist.append(neighbor)
        visited_tiles.add(neighbor)

      neighbor = self.RightNeighbor(tileid)
      if (neighbor not in visited_tiles and
          bbox.Intersects(self.TileBounds(neighbor))):
        checklist.append(neighbor)
        visited_tiles.add(neighbor)

      neighbor = self.TopNeighbor(tileid)
      if (neighbor not in visited_tiles and
          bbox.Intersects(self.TileBounds(neighbor))):
        checklist.append(neighbor)
        visited_tiles.add(neighbor)

      neighbor = self.BottomNeighbor(tileid)
      if (neighbor not in visited_tiles and
          bbox.Intersects(self.TileBounds(neighbor))):
        checklist.append(neighbor)
        visited_tiles.add(neighbor)

    return tilelist

#use a thread pool instead of just frittering off new threads for every request
class ThreadPoolMixIn(ThreadingMixIn):
  allow_reuse_address = True  # seems to fix socket.error on server restart

  def serve_forever(self):
    # set up the threadpool
    self.requests = Queue(multiprocessing.cpu_count())
    for x in range(multiprocessing.cpu_count()):
      t = threading.Thread(target = self.process_request_thread)
      t.setDaemon(1)
      t.start()
    # server main loop
    while True:
      self.handle_request()
    self.server_close()

  def make_thread_locals(self):

    # connect to the db
    try:
      self.credentials = (os.environ['POSTGRES_DB'], os.environ['POSTGRES_USER'], os.environ['POSTGRES_HOST'],
                          os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_PORT'])
      sql_conn = psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'" % self.credentials)
      sql_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
      setattr(thread_local, 'sql_conn', sql_conn)
      cursor = sql_conn.cursor()
    except Exception as e:
      raise Exception('Failed to connect to database: ' + repr(e))

    #show some info
    try:
        cursor.execute('select pg_backend_pid();')
        sys.stdout.write('init: %s(%d) connected has connection %s(%d)' % (threading.current_thread().getName(), id(threading.current_thread()), cursor.fetchone()[0], id(sql_conn)) + os.linesep)
        sys.stdout.flush()
    except Exception as e:
      raise Exception('Failed to identify database connection: ' + repr(e))

    # id only query
    try:
      prepare_statement = "PREPARE q_ids AS SELECT segment_id, avg(speed) as average_speed FROM " \
	                        " segments where segment_id = ANY ($1) group by segment_id;"
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepare statement: ' + repr(e))

    # id, date, hours, and dow query
    try:
      prepare_statement = "PREPARE q_ids_date_hours_dow AS SELECT segment_id, avg(speed) as average_speed, " \
                          "start_time_dow as dow, start_time_hour as hour, count(segment_id) as observation_count " \
                          "FROM segments where segment_id = ANY ($1) and " \
                          "((start_time >= $2 and start_time <= $3) and (end_time >= $2 and end_time <= $3)) and " \
                          "(start_time_hour = ANY ($4) and end_time_hour = ANY ($4)) and " \
                          "(start_time_dow = ANY ($5) and end_time_dow = ANY ($5)) " \
                          "group by segment_id, start_time_dow, start_time_hour;"
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepare statement: ' + repr(e))

    # id, date, and hours query
    try:
      prepare_statement = "PREPARE q_ids_date_hours AS SELECT segment_id, avg(speed) as average_speed, " \
                          "start_time_hour as hour FROM segments where " \
                          "segment_id = ANY ($1) and " \
                          "((start_time >= $2 and start_time <= $3) and (end_time >= $2 and end_time <= $3)) and " \
                          "(start_time_hour = ANY ($4) and end_time_hour = ANY ($4)) " \
                          "group by segment_id, start_time_hour;"
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepare statement: ' + repr(e))

    # id, date, and dow query
    try:
      prepare_statement = "PREPARE q_ids_date_dow AS SELECT segment_id, avg(speed) as average_speed, " \
                          "start_time_dow as dow FROM segments where " \
                          "segment_id = ANY ($1) and " \
                          "((start_time >= $2 and start_time <= $3) and (end_time >= $2 and end_time <= $3)) and " \
                          "(start_time_dow = ANY ($4) and end_time_dow = ANY ($4)) " \
                          "group by segment_id, start_time_dow;"
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepare statement: ' + repr(e))

    # id, hours, and dow query
    try:
      prepare_statement = "PREPARE q_ids_hours_dow AS SELECT segment_id, avg(speed) as average_speed, " \
                          "start_time_dow as dow, start_time_hour as hour, count(segment_id) as observation_count " \
                          "FROM segments where segment_id = ANY ($1) and " \
                          "(start_time_hour = ANY ($2) and end_time_hour = ANY ($2)) and " \
                          "(start_time_dow = ANY ($3) and end_time_dow = ANY ($3)) " \
                          "group by segment_id, start_time_dow, start_time_hour;"
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepare statement: ' + repr(e))

    # id and date
    try:
      prepare_statement = "PREPARE q_ids_date AS SELECT segment_id, avg(speed) as average_speed " \
                          "FROM segments where " \
                          "segment_id = ANY ($1) and " \
                          "((start_time >= $2 and start_time <= $3) and (end_time >= $2 and end_time <= $3)) " \
                          "group by segment_id;"
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepare statement: ' + repr(e))

    # id and hours query
    try:
      prepare_statement = "PREPARE q_ids_hours AS SELECT segment_id, avg(speed) as average_speed, " \
                          "start_time_hour as hour FROM segments where " \
                          "segment_id = ANY ($1) and " \
                          "(start_time_hour = ANY ($2) and end_time_hour = ANY ($2)) " \
                          "group by segment_id, start_time_hour;"
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepare statement: ' + repr(e))

    # id and dow query
    try:
      prepare_statement = "PREPARE q_ids_dow AS SELECT segment_id, avg(speed) as average_speed, " \
                          "start_time_dow as dow FROM segments where " \
                          "segment_id = ANY ($1) and " \
                          "(start_time_dow = ANY ($2) and end_time_dow = ANY ($2)) " \
                          "group by segment_id, start_time_dow;"
      cursor.execute(prepare_statement)
    except Exception as e:
      raise Exception('Could not create prepare statement: ' + repr(e))

    sys.stdout.write("Created prepare statements.\n")
    sys.stdout.flush()

  def process_request_thread(self):
    self.make_thread_locals()
    while True:
      request, client_address = self.requests.get()
      ThreadingMixIn.process_request_thread(self, request, client_address)

  def handle_request(self):
    try:
      request, client_address = self.get_request()
    except socket.error:
      return
    if self.verify_request(request, client_address):
      self.requests.put((request, client_address))

#enable threaded server
class ThreadedHTTPServer(ThreadPoolMixIn, HTTPServer):
  pass

#custom handler for getting routes
class QueryHandler(BaseHTTPRequestHandler):

  #boiler plate parsing
  def parse_url(self, post):
    #split the query from the path
    try:
      split = urlparse.urlsplit(self.path)
    except:
      raise Exception('Try a url that looks like /action?query_string')
    #path has the action in it
    try:
      if split.path.split('/')[-1] not in actions:
        raise
    except:
      raise Exception('Try a valid action: ' + str([k for k in actions]))
    #handle POST
    if post:
      body = self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')
      params = urlparse.parse_qs(body)
      return params
    #handle GET
    else:
      params = urlparse.parse_qs(split.query)
      return params

  # loads a tile into the rtree index if tile is not in the cache.
  def load_into_index(self, t, level, tile_dir):
    file_name = tile_hierarchy.levels[level].GetFilename(t, level, os.environ['TILE_DIR'])
    # if the file has not be cached, we must load it up into the index.
    if t not in cached_tiles:

      with open(file_name) as f:
        geojson = json.load(f)

      for feature in geojson['features']:
        geom = shape(feature['geometry'])
        osmlr_id = long(feature['properties']['osmlr_id'])
        index.insert(osmlr_id, geom.bounds)

      # this is our set of tiles that have been loaded into the index, only load each tile once.
      cached_tiles.add(t)

  #parse the request because we dont get this for free!
  def handle_request(self, post):
    #get the query data
    params = self.parse_url(post)

    try:   
      # get the kvs
      boundingbox = params['boundingbox'] if 'boundingbox' in params else None
      ids = s_date_time = e_date_time = hours = dow = None
      bbox = minx = miny = maxx = maxy = None
      list_of_ids = params['segment_ids'] if 'segment_ids' in params else None
      list_of_dow = params['dow'] if 'dow' in params else None
      list_of_hours = params['hours'] if 'hours' in params else None
      start_date_time = params.get('start_date_time', None)
      end_date_time = params.get('end_date_time', None)
      include_observation_counts = params.get('include_observation_counts', None)
      include_geometry = params.get('include_geometry', None)
      cursor = thread_local.sql_conn.cursor()
      features_index = {}
      osmlr_ids = set()
      feature_collection = {'features':[]}

      #include observation counts? this will be for authorized users.
      try:
        if include_observation_counts:
          include_observation_counts = bool(strtobool(str(include_observation_counts[0])))
        else:
          include_observation_counts = False
      #invalid value entered.
      except:
        include_observation_counts = False

      #include the geom?
      try:
        if include_geometry:
          include_geometry = bool(strtobool(str(include_geometry[0])))
        else:
          include_geometry = True
      #invalid value entered.
      except:
        include_geometry = True

      if include_geometry == True:
        results = {"type":"FeatureCollection",'features':[]}
      else: results = {}

      #ids will come in as csv string.  we must split and cast to list
      #so that the cursor can bind the list.
      if list_of_ids:
        ids = [ long(i) for i in list_of_ids[0].split(',')]

      if boundingbox and include_geometry == True:
        bbox = [ float(i) for i in boundingbox[0].split(',')]
        b_box = BoundingBox(bbox[0], bbox[1], bbox[2], bbox[3])
        # we need to check the cache first i.e., make sure the tiles we are
        # intersecting are loaded into the cache
        for level, t_ids in tile_ids.items():
          # only get the tiles that intersect the bounding box and have a
          # geojson file as well.
          tiles = tile_hierarchy.levels[level].TileList(b_box,t_ids)
          for t in tiles:
            self.load_into_index(t, level, os.environ['TILE_DIR'])

        # cache is all set.
        # intersect the bb
        osmlr_ids = set(index.intersection((bbox[0], bbox[1], bbox[2], bbox[3])))

        # ids were sent in.  Only grab data for those ids.
        if ids:
          osmlr_ids = set(ids).intersection(osmlr_ids)
        else:
          ids = list(osmlr_ids)

        feature_index = 0
        for level, t_ids in tile_ids.items():
          # only get the tiles that intersect the bounding box and have a
          # geojson file as well.
          tiles = tile_hierarchy.levels[level].TileList(b_box,t_ids)
          for t in tiles:
            file_name = tile_hierarchy.levels[level].GetFilename(t, level, os.environ['TILE_DIR'])
            with open(file_name) as f:
              geojson = json.load(f)

            for feature in geojson['features']:
              osmlr_id = long(feature['properties']['osmlr_id'])
              if osmlr_id in osmlr_ids:
                feature_collection['features'].append(feature)
                features_index[osmlr_id] = feature_index
                feature_index += 1
      else:
        # ids only no BB.
        # need to obtain the list of tiles from the segment IDs
        if ids:
          if include_geometry == True:
            feature_index = 0
            for i in ids:
              graphid = BitArray(uint=i, length=64)

              # last 3 bits is our level
              level = graphid[-3:].uint
              # these 22 bits equal our tile id
              tileid = graphid[-25:-3].uint

              self.load_into_index(tileid, level, os.environ['TILE_DIR'])

              file_name = tile_hierarchy.levels[level].GetFilename(tileid, level, os.environ['TILE_DIR'])
              with open(file_name) as f:
                geojson = json.load(f)

              for feature in geojson['features']:
                osmlr_id = long(feature['properties']['osmlr_id'])
                if osmlr_id in ids:
                  feature_collection['features'].append(feature)
                  features_index[osmlr_id] = feature_index
                  feature_index += 1
        else:
          return 400, "Please provide a bounding box or array of IDs."

      #hand it back -- empty results
      if not ids:
        return 200, results

      if start_date_time and not end_date_time:
        return 400, "Please provide an end_date_time."
      elif end_date_time and not start_date_time:
        return 400, "Please provide a start_date_time."

      if start_date_time:
        s_date_time = calendar.timegm(time.strptime(start_date_time[0],"%Y-%m-%dT%H:%M:%S"))

      if end_date_time:
        e_date_time = calendar.timegm(time.strptime(end_date_time[0],"%Y-%m-%dT%H:%M:%S"))

      if list_of_dow:
        dow = [ int(i) for i in list_of_dow[0].split(',')]

      if list_of_hours:
        hours = [ int(i) for i in list_of_hours[0].split(',')]

      columns = ['segment_id', 'average_speed']
      # id only query
      if all(parameters is None for parameters in (s_date_time, e_date_time, hours, dow)):
        cursor.execute("execute q_ids (%s)",(ids,))

      # id, date, hours, and dow query
      elif all(parameters is not None for parameters in (s_date_time, e_date_time, hours, dow)):
        cursor.execute("execute q_ids_date_hours_dow (%s, %s, %s, %s, %s)",
                      ((ids,),s_date_time,e_date_time,(hours,),(dow,)))
        if include_observation_counts == True:
          columns = ['segment_id', 'average_speed', 'dow', 'hour', 'observation_count']
        else:
          columns = ['segment_id', 'average_speed', 'dow', 'hour']

      # id, date, and hours query
      elif all(parameters is not None for parameters in (s_date_time, e_date_time, hours)):
        cursor.execute("execute q_ids_date_hours (%s, %s, %s, %s)",
                      ((ids,),s_date_time,e_date_time,(hours,)))
        columns = ['segment_id', 'average_speed', 'hour']

      # id, date, and dow query
      elif all(parameters is not None for parameters in (s_date_time, e_date_time, dow)):
        cursor.execute("execute q_ids_date_dow (%s, %s, %s, %s)",
                      ((ids,),s_date_time,e_date_time,(dow,)))
        columns = ['segment_id', 'average_speed', 'dow']

      # id, hours, and dow query
      elif all(parameters is not None for parameters in (hours, dow)):
        cursor.execute("execute q_ids_hours_dow (%s, %s, %s)",
                      ((ids,),(hours,),(dow,)))
        if include_observation_counts == True:
          columns = ['segment_id', 'average_speed', 'dow', 'hour', 'observation_count']
        else:
          columns = ['segment_id', 'average_speed', 'dow', 'hour']

      # id and date query
      elif all(parameters is not None for parameters in (s_date_time, e_date_time)):
        cursor.execute("execute q_ids_date (%s, %s, %s)",
                      ((ids,),s_date_time,e_date_time))

      # id and hours query
      elif hours is not None:
        cursor.execute("execute q_ids_hours (%s, %s)",((ids,),(hours,)))
        columns = ['segment_id', 'average_speed', 'hour']

      # id and dow query
      elif dow is not None:
        cursor.execute("execute q_ids_dow (%s, %s)",((ids,),(dow,)))
        columns = ['segment_id', 'average_speed', 'dow']

      rows = cursor.fetchall()
      current_id = feature = None
      # get the db results
      # add the speeds to the original feature.
      # can be multiple speeds based on query.
      if include_geometry == True:
        for row in rows:
          speed = dict(zip(columns, row))
          if current_id != speed['segment_id']:
            if current_id != None:
              results['features'].append(feature)
            current_id = speed['segment_id']
            feature_index = features_index[current_id]
            feature = feature_collection['features'][feature_index]
            feature['properties']['speeds'] = []

          speed.pop('segment_id')
          feature['properties']['speeds'].append(speed)

        if current_id != None:
          results['features'].append(feature)
      # no geom requested.
      else:
        speeds = None
        for row in rows:
          speed = dict(zip(columns, row))
          if current_id != speed['segment_id']:
            if current_id != None:
              results[current_id] = speeds
            speeds = {'speeds':[]}
            current_id = speed['segment_id']

          speed.pop('segment_id')
          speeds['speeds'].append(speed)
        if current_id != None:
          results[current_id] = speeds

    except Exception as e:
      return 400, str(e)

    #hand it back
    return 200, results

  #send an answer
  def answer(self, code, body):

    response = json.dumps(body, separators=(',', ':')) if type(body) == dict else json.dumps({'response': body})

    self.send_response(code)

    #set some basic info
    self.send_header('Access-Control-Allow-Origin','*')
    self.send_header('Content-type', 'application/json;charset=utf-8')
    self.send_header('Content-length', len(response))
    self.end_headers()

    #hand it back
    self.wfile.write(response)

  #handle the request
  def do(self, post):
    try:
      code, body = self.handle_request(post)
      self.answer(code, body)
    except Exception as e:
      self.answer(400, str(e))

  def do_GET(self):
    self.do(False)
  def do_POST(self):
    self.do(True)

def check_db():
  #try to connect forever...
  credentials = (os.environ['POSTGRES_DB'], os.environ['POSTGRES_USER'], os.environ['POSTGRES_HOST'], 
                 os.environ['POSTGRES_PASSWORD'], os.environ['POSTGRES_PORT'])
  while True:
    try:
      with psycopg2.connect("dbname='%s' user='%s' host='%s' password='%s' port='%s'" % credentials) as sql_conn:
        # check and see if db exists.
        # NOTE: this will have to change for redshift.
        cursor = sql_conn.cursor()
        cursor.execute("select exists(select relname from pg_class where relname = 'segments' and relkind='r');")
        if cursor.fetchone()[0] == False:
          sys.stdout.write("No tables exist!\n".format(e))
          sys.stdout.flush()
          sys.exit(1)
        break
    except Exception as e:
      sys.stdout.write('Could not find table: ' + repr(e) + os.linesep)
      sys.stdout.flush()
      time.sleep(5)

#program entry point
if __name__ == '__main__':

  #parse out the address to bind on
  try:
    address = sys.argv[1].split('/')[-1].split(':')
    address[1] = int(address[1])
    address = tuple(address)
    os.environ['POSTGRES_DB']
    os.environ['POSTGRES_USER']
    os.environ['POSTGRES_HOST']
    os.environ['POSTGRES_PASSWORD']
    os.environ['POSTGRES_PORT']
    os.environ['TILE_DIR']

    sys.stdout.write("Creating the list of geojson tiles.\n")
    sys.stdout.flush()

    tile_hierarchy = TileHierarchy()
    for level, tiles in tile_hierarchy.levels.items():
      tile_ids[level] = set()
      for row in xrange(0, tiles.nrows):
        for col in xrange(0, tiles.ncolumns):
          tile_id = tiles.TileExists(row, col, level, os.environ['TILE_DIR'])
          if tile_id:
            tile_ids[level].add(tile_id)

    sys.stdout.write("Finished.\n")
    sys.stdout.flush()

  except Exception as e:
    sys.stderr.write('Bad address or environment: {0}\n'.format(e))
    sys.stderr.flush()
    sys.exit(1)

  #check the db for the default tables.
  check_db()
  
  #setup the server
  QueryHandler.protocol_version = 'HTTP/1.0'
  httpd = ThreadedHTTPServer(address, QueryHandler)

  #wait until interrupt
  try:
    httpd.serve_forever()
  except KeyboardInterrupt:
    httpd.server_close()
