install.packages("CoordinateCleaner")
library(CoordinateCleaner)
library(utils)
library(data.table)


#?cc_cap
df_old <- fread("9.csv")
View(df_old)

df_old = fread("test-occurrence.csv")

#newdata <- df_old[,c(4,5,6,7,8,9,10,13,16,22,23,38)]
#View(newdata)
#colnames(newdata)<-c("latitude","longitude","countryCode","countryName","species")
View(newdata)
####Removes or flags records within a certain radius around country capitals
df <- cc_cap(
  df_old,
  lon = "longitude",
  lat = "latitude",
  species = "scientific_name",
  buffer = 10000,
  geod = TRUE,
  ref = NULL,
  verify = FALSE,
  value = "clean",
  verbose = TRUE
)
?cc_coun
###Removes or flags mismatches between geographic coordinates and additional country information
df1 <- cc_coun(
  df,
  lon = "longitude",
  lat = "latitude",
  iso3 = "countryCode",
  value = "clean",
  ref = NULL,
  ref_col = "iso_a3",
  verbose = TRUE
)
#?cc_cen
###Removes or flags records within a radius around the geographic centroids of political countries and provinces
df2 <- cc_cen(
  df1,
  lon = "longitude",
  lat = "latitude",
  species = "scientific_name",
  buffer = 1000,
  geod = TRUE,
  test = "both",
  ref = NULL,
  verify = FALSE,
  value = "clean",
  verbose = TRUE
)
#?cc_inst
###Removes or flags records assigned to the location of zoos, botanical gardens, herbaria, universities 
###and museums, based on a global database of ~10,000 such biodiversity institutions
df3 <- cc_inst(
  df2,
  lon = "longitude",
  lat = "latitude",
  species = "scientific_name",
  buffer = 1000,
  geod = TRUE,
  ref = NULL,
  verify = FALSE,
  verify_mltpl = 10,
  value = "clean",
  verbose = TRUE
)
#?institutions###what means can be change?
#?cc_val
###Removes or flags non-numeric and not available coordinates as well as lat >90, la <-90, lon > 180 and lon < -180 are flagged
df4 <- cc_val(
  df3,
  lon = "longitude",
  lat = "latitude",
  value = "clean",
  verbose = TRUE
)
#?cc_zero
###Removes or flags records with either zero longitude or latitude and a radius around the point at zero longitude and zero latitude
df5 <- cc_zero(
  df4,
  lon = "longitude",
  lat = "latitude",
  buffer = 0.5,
  value = "clean",
  verbose = TRUE
)
fwrite(df5,"wash-occurrence.csv")
##cc_sea
df_bien_sea <- cc_sea(
  df_bien,
  lon = "longitude",
  lat = "latitude",
  ref = NULL,
  scale = 110,
  value = "clean",
  speedup = TRUE,
  verbose = TRUE
)
