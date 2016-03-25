
# This is the server logic for a Shiny web application.
# You can find out more about building applications with Shiny here:
#
# http://shiny.rstudio.com
#

library(shiny)
library("xlsx")

source('runSensisQuery.R')

shinyServer(function(input, output) {
  
  output$keyCheck <- renderText({
    #returns no text if the api key is found, otherwise
    #prints the assertion error message
    stopifnot(Sys.getenv('SENSIS_API_KEY') != "")
    ""
  })
  
  
  currentResults <- reactive({
    if (input$runButton == 0)
    {
      results <- NULL
    }
    else
    {
      selectedQuery <- isolate(input$selectQuery)
      selectedState <- isolate(input$selectState)
      results <-  withProgress(message = 'Running API query', value = 0.2, 
      {
        runSensisQuery(selectedQuery, selectedState)
      })
      
    }
    results
  })
  
  output$resultsTable <- renderDataTable(currentResults())
  
  output$downloadData <- downloadHandler(
    
    # This function returns a string which tells the client
    # browser what name to use when saving the file.
    filename = function() {
      if (is.null(currentResults()))
      {
        stop('No results to save')
      }
      
      fname <- paste("queryResults", input$selectQuery, input$selectState, ".xlsx", sep = "_")
      fname <- gsub(" ", "_", fname)
      fname
    },
    
    # This function should write data to a file given to it by
    # the argument 'file'.
    content = function(file) {
      write.xlsx(currentResults(), file)
    }
  )
  
})
