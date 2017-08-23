#' Queries available forecasts
#'
#' Queries TVA's Characteristics of Power Resources (CPR) database for available forecast types, 
#' including fuel prices and asset characteristics.
#' 
#' This function creates a connection to the CPR database, which is maintained by Resource Planning and
#' Strategy, and contains the forecast data used to feed long-term generation planning models. 
#' 
#' @param username character for connecting to the database. Leave username and/or password as NULL to use
#' Windows Authentication via a trusted connection
#' @param password character for connecting to the database
#' @param driver.name character for the database driver to be used
#' @importFrom magrittr "%>%"
#' @return A three column data frame, including ForecastID, TemplateID, and Name for each forecast

queryCPRForecasts = function(username = NULL, password = NULL, driver.name = 'SQL Server') {

  # build connection string
  if (is.null(username) | is.null(password)) {
    
    conn.str = paste0('driver=', driver.name, ';',
                      'server=sqlgpprod14db1.main.tva.gov;',
                      'database=CPR;',
                      'trusted_connection=yes')
    
  } else {
    
    conn.str = paste0('driver=', driver.name, ';',
                      'server=sqlgpprod14db1.main.tva.gov;',
                      'database=CPR;',
                      'uid=', username, ';pwd=', password)
    
  }
  
  # create database connection
  conn = RODBC::odbcDriverConnect(conn.str)

  # forecasts
  forecasts = RODBC::sqlQuery(conn, 'SELECT ForecastID, TemplateID, Name FROM CPR.dbo.Forecasts')

  # close connection
  RODBC::odbcClose(conn)

  return(forecasts)

}
