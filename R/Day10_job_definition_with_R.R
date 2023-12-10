library(SparkR)
sparkR.session(master = "", appName = "SparkR", sparkConfig = list())
df_iris <- read.df("abfss://1860beee-5b6a-48cc-92xxxxxe1@onelake.dfs.fabric.microsoft.com/a574d1a3xxxxxxx18c7128f/Tables/iris_data")
head(df_iris)

#every run we create a new dataframe
write.df(df_iris, 
         source = "delta", 
         path = "abfss://1860beee-5b6a-48cc-xxxxxx1@onelake.dfs.fabric.microsoft.com/a574d1a3-f10xxxxxxxxx18c7128f/Tables/iris_data", 
         mode = "append")