
# This is the user-interface definition of a Shiny web application.
# You can find out more about building applications with Shiny here:
#
# http://shiny.rstudio.com
#

library(shiny)

australianStates <- list("ACT", "NSW", "NT", "QLD", "SA", "TAS", "VIC", "WA")

shinyUI(fluidPage(

  # Application title
  titlePanel("Sensis API Query Dashboard"),

  # Sidebar with a slider input for number of bins
  sidebarLayout(
    sidebarPanel(
#       selectInput("selectQuery", label = h3("Search for:"), 
#                   choices = list("Electrical Contractors"), 
#                   selected = 1),
      textInput("selectQuery", label = h3("Search for:"), value = "Enter query (e.g. electrical contractors)..."),
      selectInput("selectState", label = h3("In state:"), 
                  choices = australianStates, 
                  selected = 1),
      actionButton("runButton", "Run Query"),
      downloadButton('downloadData', 'Download Results'),
      textOutput('keyCheck')
    ),
    
    # Show a plot of the generated distribution
    mainPanel(
      dataTableOutput("resultsTable")
    )
  )
))
