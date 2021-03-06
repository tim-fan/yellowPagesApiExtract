# -*- coding: utf-8 -*-
"""
Python interface to the Sensis API
Created on Thu Mar 17 20:10:40 2016

@author: tim
"""

import json
import pandas as pd
import requests
import time
import re
import os


class SensisInterface(object):
    def __init__(self, apiKey):
        self.apiUrl = 'http://api.sensis.com.au/v1/test/search'
        
        #store options for running queries. 'rows' option will default to 50 - 
        #this is the maximum number of rows that can be requested per page
        self.queryOptions = {'rows' : '50',
                             'key'  : apiKey}
        
    def setQuery(self, query):
        self.queryOptions['query'] = query
        
    def setState(self, state):
        self.queryOptions['state'] = state

    def setPage(self, pageNum):
        self.queryOptions['page'] = str(pageNum)
        
    def setLocation(self, location):
        self.queryOptions['location'] = location

    def getQueryUrl(self):
        #return the url that would be queried given the current options
        buildOptsUrl = lambda options : ('?' + '&'.join([key + '=' + value for (key, value) in options.items()])).replace(' ', '+')
        url = self.apiUrl + buildOptsUrl(self.queryOptions)   
        return url
    
    def runQuery(self):
        print("QUERY: " + self.getQueryUrl())
        response = self.__awaitQuerySuccess()
        
    #    #Save query in cache to avoid sending the same api query twice
    #    queryFile = open('cache/'+buildOptsUrl(queryOptions), 'w')
    #    queryFile.write(json.dumps(response))
    #    queryFile.close()
        return response
    
    def parseResponse(self, responseJson):
        #get data frame from respose:
        #one row per listing, one column for each field
        #as selected in 'parseListing'
        
        allListings = pd.concat([self.__parseListing(listing) for listing in responseJson['results']])
        return allListings
        
    def queryAllPages(self):
        #a query function for querying multiple pages
        
        def queryPage(pageNumber = 1):
            print("Extracting page: " + str(pageNumber))
            self.setPage(pageNumber)
            return self.runQuery()
            
        #we're set up to query the api.
        #first lets find how many pages are available to query:
        firstQuery = queryPage()
        if not 'totalPages' in firstQuery.keys():
            #TODO: management of api response code 418 vs http response codes
            return None
    
        numPages = firstQuery['totalPages']
        print("Number of pages to query: " + str(numPages))
        
        if numPages == 0:
            allResults = None        
    
        else:
            #now we know how many queries to run. Run them all:
            allResults = pd.concat([self.parseResponse(firstQuery)] + [self.parseResponse(queryPage(pageNum)) for pageNum in range(2,numPages+1)])
        return allResults
        
    
    ## Private methods:
    
    def __queryOnce(self):
        #Run a query once, return the response
        return requests.get(self.getQueryUrl())
        
    def __awaitQuerySuccess(self):
        #keeps running query until either a
        #response code of 200 or an unhandled 
        #reponse code is received
        
        querySuccess = False
        retryTime = 2
        while not querySuccess:
            response = self.__queryOnce()
            
            responseCode = response.status_code
            if responseCode == 200 :
                querySuccess = True
            elif responseCode == 418:
                print('API error: ' + response.json()['message'])
            elif responseCode == 403 :
                print('Hit API limit: ' + str(response.status_code))
                time.sleep(retryTime) #hit the API limit. Wait a bit and try again 
                retryTime *= 1.5
                print("Waiting " + "{0:.2f}".format(retryTime/60) + " minutes before retry")
            else:
                raise RuntimeError('Unhandled API response code ' + str(responseCode))
        return response.json()

    def __parseListing(self, listing):
        #convert from dictionary representation of a listing to a single-row data frame of the chosen fields
        getEmail  = lambda contacts : [contact['value'] for contact in contacts if contact['type'] == 'EMAIL'][0]
        getUrl    = lambda contacts : [contact['value'] for contact in contacts if contact['type'] == 'URL'][0]
    
        #test: pull email by regex:
        extractMatch = lambda match : match.group(0) if match is not None else ''
        searchForEmail = lambda listing : extractMatch(re.search(r'[\w\.-]+@[\w\.-]+', str(listing)))
    
        extractionFunctions =     {
                        'Name'          : lambda : [listing['name']],
                        'State'         : lambda : [listing['primaryAddress']['state']],
                        'Suburb'        : lambda : [listing['primaryAddress']['suburb']],
                        'Street.Address': lambda : [listing['primaryAddress']['addressLine']],
                        'Postcode'      : lambda : [listing['primaryAddress']['postcode']],
                        'Email'         : lambda : getEmail(listing['primaryContacts']),
                        'SearchedEmail' : lambda : searchForEmail(listing),
                        'Url'           : lambda : getUrl(listing['primaryContacts'])
                    }
    
        def extractSafely(extractionFunction):
            #wrap the given function with
            #a catch for key/index errors
            #(these are thrown when the values
            #are are looking for aren't present
            try:
                return extractionFunction()
            except (KeyError, IndexError):
                return ''
    
        parsedListingDict = {field : extractSafely(extractionFunction) for field, extractionFunction in extractionFunctions.items()}
        parsedListing = pd.DataFrame(parsedListingDict)
        return parsedListing
        