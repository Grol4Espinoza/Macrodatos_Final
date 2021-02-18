// consultas sql para datos csv
val df = spark.read.option("header",true).option("inferSchema",true).csv("dato.csv")   
import spark.implicits._  
//val tabla = df.select("Publisher").show()  
df.createOrReplaceTempView("tabladedatos")  

//Consultas
spark.sql("SELECT * FROM tabladedatos ").show 

//unidades vendidas por region y plataforma
spark.sql("SELECT Platform," +
  " sum(JP_Sales) as JP_millones_vendidos," +
  " sum(NA_Sales) as NA_millones_vendidos, " +
  "sum(EU_Sales) as EU_millones_vendidos " +
  "FROM tabladedatos GROUP BY Platform").show 
//promedio de unidades vendidas por categoria y region
spark.sql("SELECT Genre, " +
  "avg(JP_Sales) as JP_promedio, " +
  "avg(NA_Sales) as NA_promedio, " +
  "avg(EU_Sales) as EU_promedio  " +
  "FROM tabladedatos GROUP BY Genre").show 
//promedio de ventas de juegos con clasificacion excelente en japon por genero
spark.sql("SELECT Genre, " +
  "avg(JP_Sales) as JP_promedio_juegos_excelentes " +
  "FROM tabladedatos " +
  "WHERE Critic_Score_Class = 'Excelente' " +
  "GROUP BY Genre").show 
//promedio de ventas de juegos con clasificacion mala en japon por genero
spark.sql("SELECT Genre, " +
  "avg(JP_Sales) as JP_promedio_juegos_malos " +
  "FROM tabladedatos " +
  "WHERE Critic_Score_Class = 'Malo' " +
  "GROUP BY Genre").show 
//total de ventas de juegos por clasificacion y por publisher
spark.sql("SELECT Critic_Score_Class, " +
  "Publisher, sum(Global_Sales) as ventas_globales " +
  "FROM tabladedatos " +
  "GROUP BY Critic_Score_Class,Publisher").show 
//promedio de ventas de juegos EXCELENTES y MALOS por publisher
spark.sql("SELECT Critic_Score_Class, " +
  "Publisher, " +
  "avg(Global_Sales) as ventas_globales " +
  "FROM tabladedatos " +
  "WHERE Critic_Score_Class='Excelente' OR Critic_Score_Class='Malo' " +
  "GROUP BY Critic_Score_Class,Publisher ORDER BY Publisher ASC").show 
//total de ventas de juegos EXCELENTES y MALOS por GENERO DE NINTENDO
spark.sql("SELECT Publisher," +
  "Critic_Score_Class, " +
  "sum(Global_Sales) as ventas_globales " +
  "FROM tabladedatos " +
  "WHERE Publisher='Nintendo' " +
  "AND (Critic_Score_Class='Excelente' OR Critic_Score_Class='Malo') " +
  "GROUP BY Critic_Score_Class,Publisher " +
  "ORDER BY Publisher ASC").show 
//total de ventas de juegos EXCELENTES y MALOS por Plataforma
spark.sql("SELECT Platform," +
  "Critic_Score_Class, " +
  "sum(Global_Sales) as ventas_globales " +
  "FROM tabladedatos " +
  "WHERE Critic_Score_Class='Excelente' OR Critic_Score_Class='Malo' " +
  "GROUP BY Critic_Score_Class,Platform " +
  "ORDER BY Platform ASC").show 
//total de ventas de juegos EXCELENTES y MALOS por GENERO DE Microsoft Game Studios
spark.sql("SELECT Publisher," +
  "Critic_Score_Class, " +
  "sum(Global_Sales) as ventas_globales " +
  "FROM tabladedatos " +
  "WHERE Publisher='Microsoft Game Studios' " +
  "AND (Critic_Score_Class='Excelente' OR Critic_Score_Class='Malo') " +
  "GROUP BY Critic_Score_Class,Publisher " +
  "ORDER BY Publisher ASC").show 
//total de ventas de juegos EXCELENTES y MALOS por GENERO DE Activision
spark.sql("SELECT Publisher," +
  "Critic_Score_Class, " +
  "sum(Global_Sales) as ventas_globales " +
  "FROM tabladedatos " +
  "WHERE Publisher='Activision' " +
  "AND (Critic_Score_Class='Excelente' OR Critic_Score_Class='Malo') " +
  "GROUP BY Critic_Score_Class,Publisher " +
  "ORDER BY Publisher ASC").show 