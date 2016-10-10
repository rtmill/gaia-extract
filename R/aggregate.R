# playground


library(DatabaseConnector)
library(SqlRender)



# Returns array of each region id in the mapping polygon table
# Further calculations use this array as an indexing column
get_region_ids <- function(connectionDetails, geoTable, idColumn){

  conn <- connect(connectionDetails)
  # query <- paste("SELECT", idColumn, "FROM", geoTable)
  tmp <- querySql(conn, "SELECT GEOID FROM cb_2015_us_county_20m")

  dbDisconnect(conn)

  # if(is(tmp, "list")){
  #   tmp <- unlist(tmp)
  # }


  # TODO:  REMOVE
  # Placeholder to subset by Maine... Test data set too huge
  check <- startsWith(tmp$GEOID, "23")




  k <- tmp[check,]
}



# Returns subset data.frame of Location table (LOCATION_ID, idColumn) that matches the prefix
get_locs_by_region <- function(connectionDetails, idColumn, idValue){

  if(is.null(idColumn)){
    idColumn <- "GEOID"
  }


  # To allow for easy addition of other identification standards
  # in the case of GEOID, the idValue is used as a prefix
  if(idColumn == "GEOID"){
    conn <- connect(connectionDetails)
    query <- paste("SELECT LOCATION_ID,", idColumn, "FROM LOCATION")
    tmp <- querySql(conn, query)

    tmp <- tmp[!is.na(tmp[[idColumn]]),]
    prefixSubset <- startsWith(as.character(tmp[[idColumn]]), idValue)

    dbDisconnect(conn)

    tmp[prefixSubset,]
  }
}


# Returns subset data.frame of Person table that resides at the given location
# TODO: Incorporate date?
get_persons_by_loc <- function(connectionDetails, location_id){

  conn <- connect(connectionDetails)

  query <- paste("SELECT * FROM PERSON WHERE LOCATION_ID =", location_id)
  tmp <- querySql(conn, query)
  dbDisconnect(conn)

  tmp
}

# Method that performs the user-defined analysis on the person
# TODO: Decide how to tackle this
analyze_patient <- function(connectionDetails, query, calcFunction){

  # Placeholder function to allow further development
  conn <- connect(connectionDetails)

  # query <- paste("SELECT", variable, "FROM", domain, "WHERE PERSON_ID =", person_id,";")
  tmp <- querySql(conn, query)
  dbDisconnect(conn)


  # Allows user to not specify a calculation function
  # Useful if they just want to return the value
  if(missing(calcFunction)){

  } else{
    tmp <- calcFunction(tmp)
  }

  tmp
}

# Goes through each person designated to this location and :
# - performs the desired analysis/retrieval
# - combines the values into a single Location value
parse_location <- function(connectionDetails, queryFunction, personFrame, calcFunction, combineFunction){

  res <- vector('numeric')

  for(i in 1:nrow(personFrame)){
    x <- analyze_patient(connectionDetails,
                         queryFunction(personFrame$PERSON_ID[i]),
                         calcFunction
    )
    res[i] <- x
  }
  res <- unlist(res)

  combineFunction(res)
}



# Test function
# Basically combines them all into one routine.
# TODO: Worth keeping?

create_table <- function (newColumnName,
                          cdmConnectionDetails,
                          gisConnectionDetails,
                          geoTable,
                          geoIdColumn,
                          queryFunction,
                          calcFunction,
                          combineFunction){


  regionIds <- get_region_ids(gisConnectionDetails, geoTable, geoIdColumn)

  df <- data.frame(regionIds, row.names = NULL)
  df$regionIds <- as.character(df$regionIds)
  df$numLocs <- 0
  df[[newColumnName]] <- 0

  #TODO : CHANGE BACK TO 1:nrow(df)
  # For each region
  for(i in 1:nrow(df)){
    currentLocations <- get_locs_by_region(cdmConnectionDetails,
                                           geoIdColumn,
                                           df$regionIds[i])
    # testing placeholder
    df$numLocs[i] <- nrow(currentLocations)

    res <- vector('double')
    # For each location in the region
    tmpMin <- min(nrow(currentLocations), 20)
    # For each location in the region
    for(j in 1:tmpMin){
      persons <- get_persons_by_loc(cdmConnectionDetails, currentLocations$LOCATION_ID[j])

      #res[j] <- NA

      if(nrow(persons) > 0){
        parsed <- parse_location(cdmConnectionDetails, queryFunction, persons, calcFunction, combineFunction)
        res[j] <- parsed
      }

    }

   # res <- res[!is.na(res)]

    df[[newColumnName]][i] <- combineFunction(res)

  }

  df

}
