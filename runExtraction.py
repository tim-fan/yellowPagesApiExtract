# -*- coding: utf-8 -*-
"""
Script to run an extraction from the Sensis API, using a SensisInterface object
Created on Thu Mar 17 20:10:40 2016

@author: tim
"""

from SensisApiInterface import SensisInterface
import os

apiKey = os.environ['SENSIS_API_KEY']
sensis = SensisInterface(apiKey)
sensis.setQuery('Electrical Contractors')
sensis.setState('VIC')

queryResults = sensis.queryAllPages()

#post process
queryResults.drop_duplicates(cols = ['Name'])
queryResults.drop_duplicates(cols = ['Email'])
queryResults.dropna(how = 'any', subset = ['Name', 'Email'], inplace = True)

#save results
queryResults.to_csv('electrical_contractors_VIC.csv')



## Initial attempt: looping through postcodes:

##loop through all post codes
#postcodeData = pd.read_csv('data/Australian_Post_Codes_Lat_Lon.csv')
#postcodes = [str(pc) for pc in set(postcodeData.postcode.values)]
#
#def queryPostcodeWithSensis(postcode, sensis):
#    sensis.setLocation(postcode)
#    return sensis.queryAllPages()
#
#queryPostcode = lambda postcode : queryPostcodeWithSensis(postcode, sensis)
#
#allPcResults = queryPostcode(postcodes[0])
#csvName = (queryOptions['query'] + '_' + 'allPostcodes.csv').replace(' ','_')
#
#for postcode in postcodes[1:]:
#    print("Querying for post code: " + str(postcode))
#    pcResults = queryPostcode(postcode)
#    if pcResults is not None:
#        allPcResults = pd.concat([allPcResults, pcResults])
#
#        #in case of crash: save intermediate results
#        allPcResults.to_csv(csvName, index = False)
#
##finally, save to a .csv file
#print("Done! Saving to file: " + csvName)
#allPcResults.to_csv(csvName, index = False)

#Done!
