# -*- coding: utf-8 -*-
"""
Script to run an extraction from the Sensis API, using a SensisInterface object
Created on Thu Mar 17 20:10:40 2016

@author: tim
"""

from SensisApiInterface import SensisInterface
import os
import argparse

parser = argparse.ArgumentParser(description='Run a sensis API query.')
parser.add_argument('query',
                   help='string to use in api query. E.g. "Electrical Contractors"')
parser.add_argument('state',
                   help='state to run query for')
parser.add_argument('outputFilename',
                   help='name for query result file')

args = parser.parse_args()
apiKey = os.environ['SENSIS_API_KEY']

print('Sensis config:')
print('Key:        ' + apiKey)
print('Query:      ' + args.query)
print('State:      ' + args.state)
print('OutputFile: ' + args.outputFilename)


sensis = SensisInterface(apiKey)
sensis.setQuery(args.query)
sensis.setState(args.state)

#queryResults = sensis.queryAllPages()
import pandas as pd
queryResults = pd.DataFrame.from_csv('electrical_contractors_QLD.csv')

#post process
queryResults = queryResults.drop_duplicates(cols = ['Name'])
queryResults = queryResults.drop_duplicates(cols = ['Email'])

#TODO not sure if this is working...
queryResults = queryResults.dropna(how = 'any', subset = ['Name', 'Email'])

#save results
queryResults.to_csv(args.outputFilename, index = False)
