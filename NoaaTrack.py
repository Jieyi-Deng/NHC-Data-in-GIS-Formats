"""
Date:09/10/2020
Author: Jieyi Deng
Class NoaaTrack is designed to pull the historical hurricane tracks into pandas.DataFrame 
including the best track and the advisory track
"""

# load packages
import pandas as pd
import numpy as np
import geopandas as gpd
from bs4 import BeautifulSoup
import requests
import re
import shapefile
from shapely.geometry import shape
from zipfile import ZipFile
import io
import csv
from time import sleep
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import warnings
warnings.simplefilter("ignore")

class NoaaTrack:
    """
    The object NoaaTrack is designed to download the storm  pts data from NOAA website.
    Followings are the functions for different GIS Data:
        readZip: extrack track data from zip file, return data in DataFrame
        readZip_psurge: extract surge data from zip file, return data in DataFrame
        hurricane_advisory_url: pull advisory track download url, return in dictionary {storm:url} 
        hurricane_best_track_url: pull best track download url, return in dictionary {storm:url}
        surge_rul: pull surge download url by feet value, return in dictionary {storm:url}
        download_advisory_track: download advisory PTS track, return data in DataFrame
        download_best_track: download best PTS track, return data in DataFrame
        download_surge: download probalistic surge data, return data in DataFrame
    """
    
    def __init__(self):
        pass
        
    def readZip(self, add, name_dbf, name_prj, name_shp, name_shx):
        """
        return storm data in DataFrame
        extract zip files from url
        the head is: https://www.nhc.noaa.gov/gis/
        input: add, file link
               name_dbf, file name extension, .dbf
               name_prj, file name extension, .prj
               name_shp, file name extension, .shp
               name_shx, file name extension, .shx
        output: geopandas of indivisual storm best track
        update: extract single dbf, prj, shp, shx file from zip file
        """
        # setting:
        proxies = {"http":"http://username:password@proxy_ip:proxy_port"}   

        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        county_file_url = 'https://www.nhc.noaa.gov/gis/' + add
        zipfile = ZipFile(io.BytesIO(session.get(county_file_url).content))
        namelist = []
        sleep(5)

        for ele in sorted(zipfile.namelist()):
            if ('position') in ele:
                namelist.append(ele)

            if ('pts') in ele:
                namelist.append(ele)

        end_dic = [name_dbf, name_prj, name_shp, name_shx]
        filenames = [y for y in sorted(namelist) for ending in end_dic if y.endswith(ending)] 
        # update: using set to remove duplicated files
        selected_filenames = sorted(filenames)[0:4]
        dbf, prj, shp, shx = [io.BytesIO(zipfile.read(filename)) for filename in selected_filenames]


        r = shapefile.Reader(shp=shp, shx=shx, dbf=dbf)
        attributes, geometry = [], []
        field_names = [field[0] for field in r.fields[1:]]

        for row in r.shapeRecords():  
            geometry.append(shape(row.shape.__geo_interface__))  
            attributes.append(dict(zip(field_names, row.record)))

        gdf = gpd.GeoDataFrame(data = attributes, geometry = geometry)

        return gdf
        
    def readZip_psurge(self, add, name_dbf, name_prj, name_shp, name_shx):

        """
        return surge data in DataFrame
        extract zip files from url
        the head is: https://www.nhc.noaa.gov/gis/
        input: add, file link
               name_dbf, file name extension, .dbf
               name_prj, file name extension, .prj
               name_shp, file name extension, .shp
               name_shx, file name extension, .shx
        output: geopandas of indivisual storm best track
        """
        proxies = {"http":"http://username:password@proxy_ip:proxy_port"}
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        county_file_url = 'https://www.nhc.noaa.gov/gis/' + add
        zipfile = ZipFile(io.BytesIO(session.get(county_file_url).content))
        sleep(5)
        
        end_dic = [name_dbf, name_prj, name_shp, name_shx]
        filenames = [y for y in sorted(zipfile.namelist()) for ending in end_dic if y.endswith(ending)] 
        if filenames:
            dbf, prj, shp, shx = [io.BytesIO(zipfile.read(filename)) for filename in filenames]
        else:
            return None
        r = shapefile.Reader(shp=shp, shx=shx, dbf=dbf)
        attributes, geometry = [], []
        field_names = [field[0] for field in r.fields[1:]]
        for row in r.shapeRecords():  
            geometry.append(shape(row.shape.__geo_interface__))  
            attributes.append(dict(zip(field_names, row.record)))
            
        gdf = gpd.GeoDataFrame(data = attributes, geometry = geometry)
        gdf['timestamp'] = add.split('_')[3][0:10]
        
        return gdf

    def hurricane_advisory_url(self, hurricane_dict):
        """
        return advisory tracks as of today in dictionary 
        hurricane_dict: a dictionary for hurricane and year {year:[hurricane]}
        """
        links = set()
        selected_links = []
        # save zip files' link in dictionary 'file'
        for year in hurricane_dict.keys():
            head = f"https://www.nhc.noaa.gov/gis/archive_forecast.php?year={year}"
            proxies = {"http":"http://username:password@proxy_ip:proxy_port"}
            # url for selected hurricanes in NOAA
            r = requests.get(head, proxies=proxies)
            soup = BeautifulSoup(r.content)
            # obtain all the zip file name from the download add:
            for link in soup.findAll('a', attrs={'href': re.compile("name=Hurricane")}):
                links.add(link.get('href'))
        
        for link in links:
            for year in hurricane_dict.keys():
                if any(storm in link.split()[1] for storm in hurricane_dict[year]):
                    selected_links.append(link)
                    
        file = {}
        for link in selected_links:
            header = 'https://www.nhc.noaa.gov'
            r = requests.get(header + link, proxies=proxies)
            soup = BeautifulSoup(r.content)
            if link not in file.keys():
                file[link] = set()
                # obtain all the zip file name from the download add:
                for ele in soup.findAll('a', attrs={'href': re.compile(".zip")}):
                    file[link].add(ele.get('href'))
            else:
                for ele in soup.findAll('a', attrs={'href': re.compile(".zip")}):
                    file[link].add(ele.get('href'))
        return file
        
    def hurricane_best_track_url(self, hurricane_dict):
        """
        return best track url in dictionary: {hurricane: urls}
        hurricane_dict: a dictionary for hurricane and year {year:[hurricane]}
        """
        
        links = set()
        selected_links = []
        # save zip files' link in dictionary 'file'
        for year in hurricane_dict.keys():
            head = f"https://www.nhc.noaa.gov/gis/archive_besttrack.php?year={year}"
            proxies = {"http":"http://username:password@proxy_ip:proxy_port"}
            # url for selected hurricanes in NOAA
            r = requests.get(head, proxies=proxies)
            soup = BeautifulSoup(r.content)
            # obtain all the zip file name from the download add:
            for link in soup.findAll('a', attrs={'href': re.compile("name=Hurricane")}):
                links.add(link.get('href'))
        
        for link in links:
            for year in hurricane_dict.keys():
                if any(storm in link.split()[1] for storm in hurricane_dict[year]):
                    selected_links.append(link)
                    
        file = {}
        for link in selected_links:
            header = 'https://www.nhc.noaa.gov'
            r = requests.get(header + link, proxies=proxies)
            soup = BeautifulSoup(r.content)
            if link not in file.keys():
                file[link] = set()
                # obtain all the zip file name from the download add:
                for ele in soup.findAll('a', attrs={'href': re.compile(".zip")}):
                    file[link].add(ele.get('href'))
            else:
                for ele in soup.findAll('a', attrs={'href': re.compile(".zip")}):
                    file[link].add(ele.get('href'))
        return file

    def surge_url(self, surge_feet, hurricane_dict):
        """
        return probabilistic surge url in dictionary {hurricane:urls}
        surge_feet: numeric value of surge height, default is 5 (feet)
        hurricane_dict: a dictionary for hurricane and year {year:[hurricane]}
        """
        
        feet = str(surge_feet)
        links = set()
        selected_links = []
        for year in hurricane_dict.keys():
            head = f"https://www.nhc.noaa.gov/gis/archive_psurge.php?&year={year}"
            # setting:
            proxies = {"http":"http://username:password@proxy_ip:proxy_port"}
            # url for Michael in NOAA
            r = requests.get(head, proxies=proxies)
            soup = BeautifulSoup(r.content)
            # obtain all the zip file name from the download add:
            for link in soup.findAll('a', attrs={'href': re.compile("name=Hurricane")}):
                links.add(link.get('href'))

        # prepare the links for historical storm:
        for link in links:
            for year in hurricane_dict.keys():
                if any(storm in link.split()[1] for storm in hurricane_dict[year]):
                    selected_links.append(link)

        psurge = {}

        # setting:
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)

        for link in selected_links:
            header = 'https://www.nhc.noaa.gov'
            r = requests.get(header + link, proxies=proxies)
            soup = BeautifulSoup(r.content)
            name = link.split(' ')[-1]
            if name not in psurge.keys():
                psurge[name] = set()
                # obtain all the zip file name from the download add:
                
                for ele in soup.findAll('a', attrs={'href': re.compile(r'_psurge'+feet+'_.*zip')}):
                    psurge[name].add(ele.get('href'))
            else:
                for ele in soup.findAll('a', attrs={'href': re.compile(r'_psurge'+feet+'_.*zip')}):
                    psurge[name].add(ele.get('href'))

        return psurge

    def download_advisory_track(self, catcode, url):
        """
        return selected advisory track pts data in DataFrame with CATCODE
        catcode: a dictionary for hurricane and CAT code {hurricane: code}
        url: a dictionary for hurricanes' url {hurricane: url}
        """
        # check the link in the list name:
        name = list(url.keys())
        advisory = pd.DataFrame()
        for link in name:
            for loc,add in enumerate(url[link]):
                if loc<(len(url[link])-1) and add is not None:
                    gdf = self.readZip(add, '.dbf', '.prj', '.shp', '.shx')
                    gdf['CATCODE'] = catcode[link.split()[1]]
                    advisory = pd.concat([advisory, gdf.head(1)], axis=0)
            gdf = self.readZip(add, '.dbf', '.prj', '.shp', '.shx')
            gdf['CATCODE'] = catcode[link.split()[1]]
            advisory = pd.concat([advisory, gdf], axis=0)
        return advisory

    def download_best_track(self, catcode, url):
        """
        return selected best track pts data in DataFrame with CATCODE
        catcode: a dictionary for hurricane and CAT code {hurricane: code}
        url: a dictionary for hurricanes' url {hurricane: url}
        """         
         # check the link in the list name:
        name = list(url.keys())
        best_track = pd.DataFrame()
        for link in name:
            for loc,add in enumerate(url[link]):
                if loc<len(url[link])-1:
                    gdf = self.readZip(add, '.dbf', '.prj', '.shp', '.shx')
                    gdf['CATCODE'] = catcode[link.split()[1]]
                    best_track = pd.concat([best_track, gdf.head(1)], axis=0)
                else:
                    gdf = self.readZip(add, '.dbf', '.prj', '.shp', '.shx')
                    gdf['CATCODE'] = catcode[link.split()[1]]
                    best_track = pd.concat([best_track, gdf], axis=0)
        return best_track

    def download_surge(self, catcode, url):
        """
        return selected hurricanes' surge data in DataFrame with CATCODE
        catcode: a dictionary for hurricane and CAT code {hurricane: code}
        url: a dictionary for hurricanes' url {hurricane: url}
        """
        surge_df = pd.DataFrame()

        for link in url.keys():
            for add in url[link]:
                if add is not None:
                    gdf = self.readZip_psurge(add, '.dbf', '.prj', '.shp', '.shx')
                    if gdf is not None:
                        key_ = link.split(' ')[-1]
                        gdf['CATCODE'] = catcode[key_]
                    surge_df = pd.concat([surge_df, gdf], axis=0, sort = True, ignore_index = True)
        return surge_df



   