setwd("D:\\Public\\Zhanglihao\\washdata")
df_country <- read.table("code.csv", header = T, sep=",")
# rownames(df_country) <- df_country[,1]  # Error
# df_country2 <- na.omit(df_country)
# rownames(df_country2) <- df_country2[,1]
# head(df_country2)
# code2 <- "AD"
# df_country2[code2, 2]
# code2 <- "AR"
# df_country2[code2, 2]

df_country[138,1] <- "NA"  # replace <NA>
rownames(df_country) <- df_country[,1]
head(df_country)
# code2 <- "AD"
# df_country[code2, 2]
# code2 <- "AR"
# df_country[code2, 2]


library(data.table)
# occ <- fread("Saxifragales.csv")
occ = fread("test-occurrence.csv")

head(occ$countryCode)
countryCode <- occ$countryCode
newCountryCode <- c()
n <- length(countryCode)
pb <- txtProgressBar(style = 3)


t1 = Sys.time()
for (i in 1:n){
  code2 <- countryCode[i]
  code3 <- df_country[code2, 2]  # 15.817 secs
  newCountryCode[i] <- code3  # 16.087 secs
  setTxtProgressBar(pb, i/n)
}
t2 = Sys.time()
print(t2-t1)
close(pb)


head(newCountryCode)
tail(newCountryCode)
occ$newCountryCode <- newCountryCode
library(utils)
library(data.table)
library(CoordinateCleaner)
View(occ)

fwrite(occ,"test-occurrence-1.csv")
