#' Queries CPR data
#'
#' Queries TVA's Characteristics of Power Resources (CPR) database for planning assumptions, 
#' including fuel prices and generation unit characteristics.
#' 
#' Resource Planning and Strategy maintains
#' 
#' @param forecast.type character indicating the desired forecast. Users can generate a table of available
#' options using \code{queryCPRForecasts()}.
#' @param gen.plan character indicating the desired forecast vintage. Users can generate a table of available
#' options for a given forecast using \code{queryCPRGenPlans()}, or leave as NULL to select most recent vintage.
#' @param username character for connecting to the database. Leave username and/or password as NULL to use
#' Windows Authentication via a trusted connection
#' @param password character for connecting to the database
#' @param driver.name character for the database driver to be used
#' @return A data frame, with a variable number of columns corresponding to the layout of the underlying CPR XML files.
#' @importFrom magrittr "%>%"
#' @examples 
#' # query list of forecasts
#' forecasts = queryCPRForecasts()
#' 
#' # query data for the most recent forecast vintage
#' gas.price = queryCPRData(forecast.type = 'Henry Hub Natural Gas')
#' 
#' # obtain gen plans for a given forecast type
#' gen.plans = queryCPRGenPlans(forecast.type = 'Henry Hub Natural Gas')
#' 
#' # query data for a given forecast and gen plan
#' gas.price.spsp = queryCPRData(forecast.type = 'Henry Hub Natural Gas', 
#'                               gen.plan = 'FY18 Strategic PSP')


queryCPRData = function(forecast.type, gen.plan = NULL, username = NULL, password = NULL, driver.name = 'SQL Server') {


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

  # templates
  templates = RODBC::sqlQuery(conn, 'SELECT * FROM CPR.dbo.Templates')

  # merge forecasts and templates
  forecasts.2 = forecasts %>%
    merge(y = templates %>% dplyr::select(TemplateID, TemplateString, SheetCount), by = 'TemplateID')

  # filter to desired forecast
  forecast.i = forecasts.2 %>% dplyr::filter(Name == forecast.type)

  forecast.id = forecast.i$ForecastID
  headers = forecast.i$TemplateString %>%
    stringr::str_split(pattern = '::\\|', simplify = TRUE) %>%
    gsub(':', '', .)

  # create list of available forecasts, both current and archive
  query = paste0("SELECT ForecastDataID, ForecastID, UploadDate, ForecastTitle, MetaDataCase ",
                 "FROM CPR.dbo.ForecastData WHERE MetaDataCase='Base' AND ForecastID=", forecast.id)
  data.current = RODBC::sqlQuery(conn, query)

  query = paste0("SELECT ForecastArchiveID, ForecastID, UploadDate, ForecastTitle, MetaDataCase ",
                 "FROM CPR.dbo.ForecastArchive WHERE MetaDataCase='Base' AND ForecastID=", forecast.id)
  data.archive = RODBC::sqlQuery(conn, query)

  # combine into single table and identify most recent base version for each gen plan
  data.combined = data.current %>%
    dplyr::mutate(ForecastArchiveID = NA) %>%
    rbind(data.archive %>% dplyr::mutate(ForecastDataID = NA)) %>%
    dplyr::mutate(DateTime = as.POSIXct(UploadDate, format = '%b %d %Y %I:%M %p')) %>%
    dplyr::group_by(ForecastTitle) %>%
    dplyr::filter(DateTime == max(DateTime)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(dplyr::desc(DateTime))

  # look up most recent title
  if (is.null(gen.plan)) {
    gen.plan = data.combined$ForecastTitle[1]
  }

  row = data.combined %>%
    dplyr::filter(ForecastTitle == gen.plan)

  # check for unmatched gen.plan
  if (nrow(row) == 0) {
    print('Could not match provided gen.plan. Please run queryCPRGenPlans to view available forecast vintages.')
    return(NULL)
  }

  # identify table to query
  if (is.na(row$ForecastArchiveID)) {
    table.name = 'ForecastData'
  } else {
    table.name = 'ForecastArchive'
  }

  # identify fields for each forecast.type
  xml.query = paste(paste0("c.value('c[@l=\"", headers, "\"][1]', 'varchar(max)') as [", make.names(headers), "]"), collapse = ', ')

  # build query
  query = paste("SELECT",
                xml.query,
                paste0("FROM CPR.[dbo].[", table.name, "] CROSS APPLY ForecastData.nodes('import/dataset/r') as m(c)"),
                paste0("WHERE MetaDataCase='Base' AND ForecastId=", forecast.id, " AND ForecastTitle='", row$ForecastTitle, "'"))

  data.out = RODBC::sqlQuery(conn, query)

  # close connection
  RODBC::odbcClose(conn)

  # return results
  return(data.out)

}
