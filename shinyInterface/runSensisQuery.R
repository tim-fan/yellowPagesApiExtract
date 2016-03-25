# R function to invoke python command line util for making an api query.
#Loads the output .csv file as a dataFrame

runSensisQuery <- function(query, state)
{
  saveFile <- tempfile(pattern = "sensisApiQueryResult", tmpdir = tempdir())
  query <- paste0('"', query, '"') #wrap query with quotes in case of spaces
  systemCall = paste('python3',
                     '../extractionUtil.py',
                     query,
                     state,
                     saveFile)
  
  #run the query
  system(systemCall)
  
  if (!file.exists(saveFile))
  {
    #failed to write results - assume query failed
    results = NULL;
  }
  else
  {
    results <- read.csv(saveFile)
  }
  
  results
}