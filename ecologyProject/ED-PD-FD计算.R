library(readxl)
##df = read.excel("a所有物种ID.xlsx")
library(dplyr)
library(xlsx)
df= read.csv("ID-物种加补充-2.csv")
library(picante)
#将物种和location输出
species =df$species
location =df$Id
unique_df <- distinct(df)
#去重
all_location= unique(location)
all_species = unique(species)
##导入树
tree <- read.tree("reroot_2.tree")
##根据树来计算ed
ed = evol.distinct(tree,type = "equal.splits")
write.xlsx(ed, file = "ed.xlsx", sheetName = "Sheet1", row.names = FALSE)

##用excel加一下空格
ed1=read.xlsx("ed.xlsx", sheetName = "Sheet1")

#创建物种出现矩阵
tree_species = ed1$Species
presence_matrix = array(NA,dim = c(length(tree_species),length(all_location)),dimnames = list(tree_species,all_location))
for (i in seq_len(length(location))) {
  if(as.character(df[[2]][i]) %in% tree_species)
  {
    presence_matrix[as.character(df[[2]][i]),as.character(df[[1]][i])]=1
  }
}
presence_matrix[is.na(presence_matrix)] <- 0
library(ape)


ed_matrix=presence_matrix
##my_matrix <- matrix(1:12, nrow = 3, ncol = 4)

##得到物种ed分布矩阵
ed_matrix=presence_matrix
for (i in seq_len(nrow(ed1))) {
      ed_matrix[ed1[[1]][i],]=ed_matrix[ed1[[1]][i],]*ed1[[2]][i]
}
write.csv(ed_matrix, file = "ed_ID.csv")

transposed_presence_matrix <- t(presence_matrix)
write.csv(transposed_presence_matrix,file ="presence_matrix_T.csv")
##名称不一致，一定记得把名称用excel转换一下

exist_matrix = read.csv("presence_matrix_name.csv",row.names = 1)


PD_tree = read.tree("reroot_2_name.tree")
species_pd = pd(exist_matrix,tree,include.root = TRUE)

write.csv(species_pd,"PD.csv")


for (i in seq_len(nrow(species_pd))) {
  pd_matrix[species_pd[[1]][i],]=pd_matrix[species_pd[[1]][i],]*species_pd[[2]][i]
}

##测试
for (i in seq_len(nrow(ed1))) {
  print(ed1[[1]][i])
}

install.packages("mFD")
###############
##计算FD
#########
library("FD")
df= read.csv("ID-物种加补充-3.csv")
species =df$species
location =df$Id
unique_df <- distinct(df)
#去重
all_location= unique(location)
all_species = unique(species)
traits = read.csv("木兰性状花色单赋值表_插补后.csv")
traits_species = traits$X
traits_species = unique(traits_species)
distribution_matrix = array(NA,dim = c(length(traits_species),length(all_location)),dimnames = list(traits_species,all_location))
for (i in seq_len(length(location))) {
  if(as.character(df[[2]][i]) %in% traits_species)
  {
    distribution_matrix[as.character(df[[2]][i]),as.character(df[[1]][i])]=1
  }
}
distribution_matrix[is.na(distribution_matrix)] <- 0
write.csv(distribution_matrix,"distribution_matrix.csv")
#用excel将物种的数量和顺序对齐
traits_1 =read.csv("木兰性状花色多赋值表_插补后.csv",row.names=1)
traits_2 =read.csv("木兰性状花色单赋值表_插补后.csv",row.names=1)
distribution = read.csv("distribution_matrix.csv",row.names=1)


new_distribution = distribution[,!(distribution[314,]==0)]
write.csv(new_distribution,"distribution_del.csv")

###发现ID的表和trait表里的物种各不相同而不是包含关系，同时有物种不存在于任何分布，
#将三表中异常物种删除，删成了275种
traits_1 =read.csv("木兰性状花色多赋值表_插补后_del.csv",row.names=1)
traits_2 =read.csv("木兰性状花色单赋值表_插补后_del.csv",row.names=1)
distribution_new=read.csv("distribution_del.csv",row.names = 1)
distribution_t = t(distribution_new)

results = dbFD(traits_1,distribution_t,corr = "lingoes")
write.csv(results,"木兰性状花色多赋值表_插补后_FD.csv")
results = dbFD(traits_2,distribution_t,corr ="lingoes")
write.csv(results,"木兰性状花色单赋值表_插补后_FD.csv")
###测试
distribution = read.csv("distribution_matrix.csv")
traits_1 =read.csv("木兰性状花色多赋值表_插补后.csv")
species_1 =distribution$X
species_2 =traits_1$X
write.csv(species_1,"sp_d.scv")
write.csv(species_2,"sp_t.scv")