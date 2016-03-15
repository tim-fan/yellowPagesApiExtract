#yellowPagesApiExtract: pulls data from yellow pages api
# usage: set query parameters below, then run:
#          python3 yellowPagesApiExtract.py

import json
import pandas as pd
import requests
import time
import re
import os

### CONFIG ###

apiUrl = 'http://api.sensis.com.au/v1/test/search'

queryOptions = {
        'query' : 'electrical contractors',
        #'state' : 'SA',
        'rows'  : '50' #API allows at most 50 rows per request
}

### HELPER FUNCTIONS ###

def awaitQuerySuccess(queryFn):
	#keeps running query until either a
	#response code of 200 or an unhandled 
	#reponse code is received
	querySuccess = False
	retryTime = 2
	while not querySuccess:
		response = queryFn()
		
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
			raise RuntimeError('Unhandled API response code ' + str(responseCode) + ', with message: ' + responseMsg)
	return response.json()
	
def runQuery(apiUrl, options):
	buildOptsUrl = lambda options : ('?' + '&'.join([key + '=' + value for (key, value) in options.items()])).replace(' ', '+')
	url = apiUrl + buildOptsUrl(queryOptions)
	print("QUERY: " + url)
	response = awaitQuerySuccess(lambda: requests.get(url))
	return response

def makeQueryPage(apiUrl, options):
	#returns a function which can be used to loop through pages of query results
	def queryPage(pageNumber = 1):
		print("Extracting page: " + str(pageNumber))
		options['page'] = str(pageNumber)
		return runQuery(apiUrl, options)
	return queryPage

def parseListing(listing):
	#convert from dictionary representation of a listing to a single-row data frame of the chosen fields
	getEmail  = lambda contacts : [contact['value'] for contact in contacts if contact['type'] == 'EMAIL'][0]
	getUrl    = lambda contacts : [contact['value'] for contact in contacts if contact['type'] == 'URL'][0]

	#test: pull email by regex:
	extractMatch = lambda match : match.group(0) if match is not None else ''
	searchForEmail = lambda listing : extractMatch(re.search(r'[\w\.-]+@[\w\.-]+', str(listing)))

	extractionFunctions = 	{
					'Name'          :	lambda : [listing['name']],
					'State'		:	lambda : [listing['primaryAddress']['state']],
                                	'Suburb'        :       lambda : [listing['primaryAddress']['suburb']],
                                	'Street.Address':	lambda : [listing['primaryAddress']['addressLine']],
                                	'Postcode'	:	lambda : [listing['primaryAddress']['postcode']],
					'Email'         :       lambda : getEmail(listing['primaryContacts']),
					'SearchedEmail'	:	lambda : searchForEmail(listing),

					'Url'	        :	lambda : getUrl(listing['primaryContacts'])
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

def parseResponse(responseJson):
	#get data frame from respose:
	#one row per listing, one column for each field
	#as selected in 'parseListing'
	
	allListings = pd.concat([parseListing(listing) for listing in responseJson['results']])
	return allListings

#prepare a query function for querying multiple pages
def queryAllPages(apiUrl, queryOptions):
	queryPage = makeQueryPage(apiUrl, queryOptions)

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
		allResults = pd.concat([parseResponse(firstQuery)] + [parseResponse(queryPage(pageNum)) for pageNum in range(2,numPages+1)])
	
	return allResults

def setPostcodeOption(postcode, queryOptions):
	queryOptions['location'] = postcode
	return queryOptions

### QUERY SCRIPT ###

#add api key to query options
queryOptions['key'] = os.environ['SENSIS_API_KEY']

#loop through all post codes
postcodeData = pd.read_csv('data/Australian_Post_Codes_Lat_Lon.csv')
postcodes = [str(pc) for pc in set(postcodeData.postcode.values)]

def queryPostcodeWithOptions(postcode, apiUrl, queryOptions):
	queryOptions = setPostcodeOption(postcode, queryOptions)
	return queryAllPages(apiUrl, queryOptions)

queryPostcode = lambda postcode : queryPostcodeWithOptions(postcode, apiUrl, queryOptions)

allPcResults = queryPostcode(postcodes[0])
csvName = (queryOptions['query'] + '_' + 'allPostcodes.csv').replace(' ','_')

for postcode in postcodes[1:]:
	print("Querying for post code: " + str(postcode))
	pcResults = queryPostcode(postcode)
	if pcResults is not None:
		allPcResults = pd.concat([allPcResults, pcResults])

		#in case of crash: save intermediate results
		allPcResults.to_csv(csvName, index = False)

#finally, save to a .csv file
print("Done! Saving to file: " + csvName)
allPcResults.to_csv(csvName, index = False)

#Done!
