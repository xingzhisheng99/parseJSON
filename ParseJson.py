###############################################################
######### Team: Yishuo Tang and Kaitlyn Mitchell ##############
######### SER325 DATABASE Final Project Millstone1 ############
######### Parsing JSON data and converting to DB ##############
###############################################################


import urllib2
import json
import sqlite3

showsList = []
episodesList = []
networksList = []
webChannelsList = []
countriesList = []
genresList = []
genre_showList = []

def parseData(url):

    respond = urllib2.urlopen(url);
    json_file = respond.read();
    # print(json);
    info = json.loads(json_file);

    shows = {}
    episodes = {}
    networks = {}
    webChannels = {}
    countries = {}
    genres = {}
    genre_show = {}

    # parse general data for the show
    shows['id'] = info['id']
    shows['url'] = info['url']
    shows['name'] = info['name']
    shows['type'] = info['type']
    shows['language'] = info['language']
    shows['status'] = info['status']
    shows['runtime'] = info['runtime']
    shows['premiered'] = info['premiered']
    shows['officialSite'] = info['officialSite']
    for item in info['schedule']['days']:
        shows['schedule_day'] = item
    shows['rating'] = info['rating']['average']
    try:
        shows['network_id'] = info['network']['id']
    except:
        shows['network_id'] = None

    try:
        shows['webChannel_id'] = info['webChannel']['id']
    except:
        shows['webChannel_id'] = None

    # parse network + country data
    try:
        networks['id'] = info['network']['id']
        networks['name'] = info['network']['name']
        networks['country_code'] = info['network']['country']['code']
        countries['name'] = info['network']['country']['name']
        countries['code'] = info['network']['country']['code']
        countries['timezone'] = info['network']['country']['timezone']
    except:
        networks['id'] = None
        networks['name'] = None
        networks['country_code'] = None
        countries['name'] = None
        countries['code'] = None
        countries['timezone'] = None

    # parse web channel + country data
    try:
        webChannels['id'] = info['webChannel']['id'] # FK
        webChannels['name'] = info['webChannel']['name']
        print info['webChannel']['id'], info['webChannel']['name']
    except:
        webChannels['id'] = None
        webChannels['name'] = None


    try:
        webChannels['country_code'] = info['webChannel']['country']['code']  # FK
        countries['name'] = info['webChannel']['country']['name']
        countries['code'] = info['webChannel']['country']['code']
        countries['timezone'] = info['webChannel']['country']['timezone']
        print info['webChannel']['country']['code'], info['webChannel']['country']['name'], info['webChannel']['country']['timezone']
    except:
        webChannels['country_code'] = None
        countries['name'] = None
        countries['code'] = None
        countries['timezone'] = None

    # parse genres data
    # print len(info['genres'])
    for item in info['genres']:
        genres['genre_name'] = item
        genre_show['genre_name'] = item
        genre_show['show_id'] = info['id']
        genresList.append(genres.copy())
        genre_showList.append(genre_show.copy())

    # parse episodes data
    for item in info['_embedded']['episodes']:
        episodes['show_id'] = info['id'] # FK
        episodes['id'] = item['id'] # PK
        episodes['url'] = item['url']
        episodes['name'] = item['name']
        episodes['season'] = item['season']
        episodes['number'] = item['number']
        episodes['runtime'] = item['runtime']
        try:
            episodes['image'] = item['image']['original']
        except:
            episodes['image'] = None
        try:
            episodes['summary'] = item['summary']
        except:
            episodes['summary'] = None
        episodesList.append(episodes.copy())

    showsList.append(shows.copy())
    networksList.append(networks.copy())
    webChannelsList.append(webChannels.copy())
    countriesList.append(countries.copy())


    allTables = [showsList, networksList, webChannelsList, countriesList, genresList, episodesList, genre_showList]

    return allTables

# parsing data for each show and append new information in corresponding lists
parseData("http://api.tvmaze.com/singlesearch/shows?q=Homeland&embed=episodes")
parseData("http://api.tvmaze.com/singlesearch/shows?q=silicon-valley&embed=episodes")
parseData("http://api.tvmaze.com/singlesearch/shows?q=south-park&embed=episodes")
parseData("http://api.tvmaze.com/singlesearch/shows?q=house-of-cards&embed=episodes")
parseData("http://api.tvmaze.com/singlesearch/shows?q=westworld&embed=episodes")
parseData("http://api.tvmaze.com/singlesearch/shows?q=game-of-thrones&embed=episodes")
parseData("http://api.tvmaze.com/singlesearch/shows?q=mr-robot&embed=episodes")
parseData("http://api.tvmaze.com/singlesearch/shows?q=big-bang-theory&embed=episodes")

print webChannelsList

# filter the genre list
filteredGenresList = []
counter = 1 # id for genres
for i in range(0, len(genresList)):
    if genresList[i] not in genresList[i+1:]:
        genresList[i]['id'] = counter
        filteredGenresList.append(genresList[i])
        counter = counter + 1

# genre id's
#"1"	"Espionage"
#"2"	"Science-Fiction"
#"3"	"Western"
#"4"	"Adventure"
#"5"	"Fantasy"
#"6"	"Drama"
#"7"	"Crime"
#"8"	"Thriller"
#"9"	"Comedy"

# connect genre id's with show id's
for item in genre_showList:
    switch = item['genre_name']
    if switch == 'Espionage':
        item['genre_id'] = 1
    elif switch == 'Science-Fiction':
        item['genre_id'] = 2
    elif switch == 'Western':
        item['genre_id'] = 3
    elif switch == 'Adventure':
        item['genre_id'] = 4
    elif switch == 'Fantasy':
        item['genre_id'] = 5
    elif switch == 'Drama':
        item['genre_id'] = 6
    elif switch == 'Crime':
        item['genre_id'] = 7
    elif switch == 'Thriller':
        item['genre_id'] = 8
    elif switch == 'Comedy':
        item['genre_id'] = 9

print genre_showList

# DATABASE
conn = sqlite3.connect('tvshows.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Shows;
DROP TABLE IF EXISTS Episodes;
DROP TABLE IF EXISTS WebChannels;
DROP TABLE IF EXISTS Networks;
DROP TABLE IF EXISTS Genres;
DROP TABLE IF EXISTS Countries;
DROP TABLE IF EXISTS genre_show;

CREATE TABLE `Episodes` (
	`id`	INTEGER NOT NULL,
	`url`	TEXT,
	`name`	TEXT,
	`season`	INTEGER,
	`number`	INTEGER,
	`runtime`	INTEGER,
	`image`	TEXT,
	`summary`	TEXT,
	`show_id`	INTEGER,
	PRIMARY KEY(`id`),
	FOREIGN KEY(show_id) REFERENCES shows(id)
);

CREATE TABLE `Countries` (
	`code`	TEXT,
	`name`	TEXT,
	`timezone`	TEXT,
	PRIMARY KEY(`code`)
);

CREATE TABLE `WebChannels` (
	`id`	INTEGER,
	`name`	TEXT,
	`country_code`	TEXT,
	PRIMARY KEY(`id`),
	FOREIGN KEY(country_code) REFERENCES countries(code)
);

CREATE TABLE `Networks` (
	`id`	INTEGER,
	`name`	TEXT,
	`country_code`	TEXT,
	PRIMARY KEY(`id`),
	FOREIGN KEY(country_code) REFERENCES countries(code)
);

CREATE TABLE `Genres` (
	`id`	INTEGER NOT NULL PRIMARY KEY,
	`genre_name`	TEXT
);

CREATE TABLE `Shows` (
	`id`	INTEGER NOT NULL,
	`url`	TEXT,
	`name`	TEXT NOT NULL,
	`type`	TEXT,
	`language`	TEXT,
	`status`	TEXT,
	`runtime`	INTEGER,
	`premiered`	DATE,
	`officialSite`	TEXT,
	`schedule_day`	TEXT,
	`rating`	NUMERIC,
	`webChannel_id`	INTEGER,
	`network_id`	INTEGER,
	PRIMARY KEY(`id`),
	FOREIGN KEY(webChannel_id) REFERENCES WebChannels(id),
	FOREIGN KEY(network_id) REFERENCES Networks(id)
);

CREATE TABLE `Genre_Show` (
	`genre_id`	INTEGER NOT NULL,
	`show_id`	INTEGER NOT NULL,
	PRIMARY KEY(genre_id, show_id),
	FOREIGN KEY(genre_id) REFERENCES Genres(id),
	FOREIGN KEY(show_id) REFERENCES Shows(id)	
);
''')

# populate TABLE COUNTRIES
columns = 'code, name, timezone'
query = 'INSERT OR IGNORE INTO countries (' + columns + ') VALUES (?, ?, ?)'
print query

for item in countriesList:
    cur.execute(query, (item['code'], item['name'], item['timezone']))
    conn.commit()

print "countries DONE"

# populate TABLE WEBCHANNELS
columns = 'id, name, country_code'
query = 'INSERT OR IGNORE INTO WebChannels (' + columns + ') VALUES (?, ?, ?)'
print query

for item in webChannelsList:
    cur.execute(query, (item['id'], item['name'], item['country_code']))
    conn.commit()

print "webChannels DONE"

# populate TABLE NETWORKS
columns = 'id, name, country_code'
query = 'INSERT OR IGNORE INTO Networks (' + columns + ') VALUES (?, ?, ?)'
print query

for item in networksList:
    cur.execute(query, (item['id'], item['name'], item['country_code']))
    conn.commit()

print "networks DONE"

# populate TABLE SHOWS
columns = 'id, url, name, type, language, status, runtime, premiered, officialSite, schedule_day, rating, webChannel_id, network_id'
query = 'INSERT OR IGNORE INTO shows (' + columns + ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
print query

for item in showsList:
    cur.execute(query, (item['id'], item['url'], item['name'], item['type'], item['language'], item['status'],
                        item['runtime'], item['premiered'], item['officialSite'], item['schedule_day'], item['rating'],
                        item['webChannel_id'], item['network_id']))
    conn.commit()

print "shows DONE"

# populate TABLE EPISODES
columns = 'id, url, name, season, number, runtime, image, summary, show_id'
query = 'INSERT OR IGNORE INTO Episodes (' + columns + ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'
print query

for item in episodesList:
    cur.execute(query, (item['id'], item['url'], item['name'], item['season'], item['number'], item['runtime'],
                        item['image'], item['summary'], item['show_id']))
    conn.commit()

print "episodes DONE"

# populate TABLE GENRES
columns = 'id, genre_name'
query = 'INSERT OR IGNORE INTO Genres (' + columns + ') VALUES (?, ?)'
print query

for item in filteredGenresList:
    cur.execute(query, (item['id'], item['genre_name']))
    conn.commit()

print "genres DONE"

# populate TABLE GENRE_SHOW
columns = 'genre_id, show_id'
query = 'INSERT OR IGNORE INTO Genre_Show (' + columns + ') VALUES (?, ?)'
print query

for item in genre_showList:
    cur.execute(query, (item['genre_id'], item['show_id']))
    conn.commit()

print "genre_show DONE"
