# From cohort

library(yaml)


aggregate_cohort <- function(cohortConnectionDetails,  cohort_id, cdmConnectionDetails,
                             getQuery, calculateFunction, aggregateFunction, gisConnectionDetails,
                             destinationTable, destinationName){
  # Get subject_id (person_id) list of the cohort
  conn <- connect(cohortConnectionDetails)
  sql <- "SELECT subject_id FROM COHORT WHERE cohort_definition_id= @cohort_identifier"
  query <- renderSql(sql,
                     cohort_identifier= cohort_id)$sql
  query <- translateSql(query, targetDialect = "postgresql")$sql

  tmp <- querySql(conn, query)
  dbDisconnect(conn)

  # Add location data
  tmp <- add_geo_identifier(tmp, cdmConnectionDetails)

  # Add value
  tmp <- get_value(tmp, cdmConnectionDetails, getQuery)

  # Perform calculation on values
  tmp$value <- calculateFunction(tmp$value)

  x <- aggregateFunction(tmp)

  z <- append_to_data(x, gisConnectionDetails, destinationTable, destinationName)

  z

}


add_geo_identifier <- function(df, cdmConnectionDetails){
  # add location information to each member of the cohort

  conn <- connect(cdmConnectionDetails)
  sql <- "SELECT person_id, location_id from PERSON;"
  person <- querySql(conn, sql)
  sql <- "SELECT location_id, @geo_identifier from LOCATION;"
  query <- renderSql(sql, geo_identifier = "geoid")$sql
  location <- querySql(conn, query)
  dbDisconnect(conn)

  # Merge geoid from location table
  person$geoid <- with(location, location$GEOID[match(person$LOCATION_ID, location$LOCATION_ID)])
  # Merge geoid from person table
  df$geoid <- with(person, person$geoid[match(df$SUBJECT_ID, person$PERSON_ID)])
  df$geoid <- as.character(df$geoid)


  df[!is.na(df$geoid),]

}


get_value <- function(df, cdmConnectionDetails, query){
  conn <- connect(cdmConnectionDetails)

  df$value <- ""

  for(i in 1:nrow(df)){
    sql <- renderSql(query, person_id=df$SUBJECT_ID[i])$sql
    df$value[i] <- querySql(conn, sql)
  }

  dbDisconnect(conn)

  df$value <- unlist(df$value)

  df

}

append_to_data <- function(df, gisConnectionDetails, destinationTable, destinationName){

  conn <- connect(gisConnectionDetails)

  sql <- "SELECT * FROM @destTable"
  query <- renderSql(sql, destTable = destinationTable)$sql
  dest <- querySql(conn, query)

  dest[[destinationName]] <- with(df, df$x[match(dest$GEOID, df$geoid)])


  #----- Different approach needed here
  # sql <- "DROP TABLE @destTable"
  # query <- renderSql(sql, destTable = destinationTable)$sql
  # tmp <- dbGetQuery(conn, query)
  # # write table to database
  # dbWriteTable(conn, c("region","county"), dest,  row.names=FALSE)



  dbDisconnect(conn)

  dest
}

# Function to return a single connection
get_connection_details <- function(configPath, obj){
  config = yaml.load_file(configPath)
  x <- config[[obj]]

  z <- createConnectionDetails(
                          dbms= x$dbms,
                          user= x$user,
                          password=x$password,
                          server=x$server,
                          schema=x$schema)
  z
}

# Function that returns an object containing all connections
get_all_connections <- function(configPath){
  config = yaml.load_file(configPath)

  connList <- {}

  for(i in 1:length(config)){
    x <- config[[i]]
    tempCon <-   createConnectionDetails(
      dbms= x$dbms,
      user= x$user,
      password=x$password,
      server=x$server,
      schema=x$schema)

    connList[[names(config[i])]] <-  tempCon

  }


}





