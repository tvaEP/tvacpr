#' Queries available gen plan vintages
#'
#' Queries TVA's Characteristics of Power Resources (CPR) database for available gen plans, 
#' including monthly gen plans, and strategic and budget power supply plans, for a given forecast type.
#' 
#' @param forecast.type character indicating the desired forecast. Users can generate a table of available
#' options using \code{queryCPRForecasts()}.
#' @param username character for connecting to the database. Leave username and/or password as NULL to use
#' Windows Authentication via a trusted connection
#' @param password character for connecting to the database
#' @param driver.name character for the database driver to be used
#' @importFrom magrittr "%>%"
#' @return A two column data frame, including ForecastTitle and UploadDate
#' @examples 
#' # query list of forecasts
#' forecasts = queryCPRForecasts()
#' 
#' # obtain gen plans for a given forecast type, listed in \code{forecasts}
#' gen.plans = queryCPRGenPlans(forecast.type = 'Henry Hub Natural Gas')


queryCPRGenPlans = function(forecast.type, username = NULL, password = NULL, driver.name = 'SQL Server') {

  # build connection string
  if (is.null(username) | is.null(password)) {
    
    # windows authentication via trusted_connection
    conn.str = paste0('driver=', driver.name, ';',
                      'server=sqlgpprod14db1.main.tva.gov;',
                      'database=CPR;',
                      'trusted_connection=yes')
    
  } else {
    
    # SQL server authentication
    conn.str = paste0('driver=', driver.name, ';',
                      'server=sqlgpprod14db1.main.tva.gov;',
                      'database=CPR;',
                      'uid=', username, ';pwd=', password)
    
  }
  
  # create database connection
  conn = RODBC::odbcDriverConnect(conn.str)

  # forecasts
  forecasts = RODBC::sqlQuery(conn, 'SELECT ForecastID, TemplateID, Name FROM CPR.dbo.Forecasts')

  # filter to desired forecast
  forecast.i  = dplyr::filter(forecasts, Name == forecast.type)
  forecast.id = forecast.i$ForecastID

  # create list of available forecasts, both current and archive
  query = paste0("SELECT ForecastDataID, ForecastID, UploadDate, ForecastTitle, MetaDataCase ",
                 "FROM CPR.dbo.ForecastData WHERE MetaDataCase='Base' AND ForecastID=", forecast.id)
  data.current = RODBC::sqlQuery(conn, query)

  query = paste0("SELECT ForecastArchiveID, ForecastID, UploadDate, ForecastTitle, MetaDataCase ",
                 "FROM CPR.dbo.ForecastArchive WHERE MetaDataCase='Base' AND ForecastID=", forecast.id)
  data.archive = RODBC::sqlQuery(conn, query)

  # combine into single table and identify most recent base version for each gen plan
  data.out = data.current %>%
    dplyr::mutate(ForecastArchiveID = NA) %>%
    rbind(data.archive %>% dplyr::mutate(ForecastDataID = NA)) %>%
    dplyr::mutate(DateTime = as.POSIXct(UploadDate, format = '%b %d %Y %I:%M %p')) %>%
    dplyr::group_by(ForecastTitle) %>%
    dplyr::filter(DateTime == max(DateTime)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(dplyr::desc(DateTime)) %>%
    dplyr::select(ForecastTitle, UploadDate)

  # close connection
  RODBC::odbcClose(conn)
  
  # return output
  return(data.out)

}
