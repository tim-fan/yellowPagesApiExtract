#yellowPagesApiExtract: pulls data from yellow pages api
# usage: set query parameters below, then run:
#          python3 yellowPagesApiExtract.py

import json
import pandas as pd
import requests
import time

### CONFIG ###

apiUrl = 'http://api.sensis.com.au/v1/test/search'

queryOptions = {
        'query' : 'electrical contractors',
        'state' : 'NSW',
        'rows'  : '50' #API allows at most 50 rows per request
}

### HELPER FUNCTIONS ###

def awaitQuerySuccess(queryFn):
	#keeps running query until either a
	#response code of 200 or an unhandled 
	#reponse code is received
	querySuccess = False
	while not querySuccess:
		response = queryFn()
		responseCode = response['code']
		responseMsg = response['message']
		if responseCode == 200 :
			querySuccess = True
		elif responseCode == 403 :
			print('Hit API limit: ' + responseMsg)
			time.sleep(10) #hit the API limit. Wait a bit and try again 
		else:
			raise RuntimeError('Unhandled API response code ' + responseCode + ', with message: ' + responseMsg)
	return response
	
def runQuery(apiUrl, options):
	buildOptsUrl = lambda options : ('?' + '&'.join([key + '=' + value for (key, value) in options.items()])).replace(' ', '+')
	url = apiUrl + buildOptsUrl(queryOptions)
	response = awaitQuerySuccess(lambda: requests.get(url).json())
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
	extractionFunctions = 	{
					'Name'          :	lambda : [listing['name']],
                                	'Suburb'        :       lambda : [listing['primaryAddress']['suburb']],
                                	'Street.Address':	lambda : [listing['primaryAddress']['addressLine']],
                                	'Email'         :       lambda : getEmail(listing['primaryContacts']),
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


### QUERY SCRIPT ###

#add api key to query options
queryOptions['key'] = input("Enter your sensis api key: ")

#prepare a query function for querying multiple pages
queryPage = makeQueryPage(apiUrl, queryOptions)

#we're set up to query the api.
#first lets find how many pages are available to query:
firstQuery = queryPage()
numPages = firstQuery['totalPages']
print("Number of pages to query: " + str(numPages))

#now we know how many queries to run. Run them all:
allResults = pd.concat([parseResponse(queryPage(pageNum)) for pageNum in range(1,numPages+1)])

#finally, save to a .csv file
csvName = (queryOptions['query'] + '_' + queryOptions['state'] + '.csv').replace(' ','_')
print("Done! Saving to file: " + csvName)
allResults.to_csv(csvName, index = False)

#Done!
