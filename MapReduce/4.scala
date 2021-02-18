
//PARA CORRER ESTE SCRIPT ENTRE A SPARK-SHELL Y DENTRO DE SCALA TIPEE :load nombre.scala CON ESTO SE CORRERA EL SCRIPT
//leer archivo
val bdjuego = sc.textFile("dato.csv")
//quitar cabecera
val bd = bdjuego.mapPartitionsWithIndex{(idx,iter)=>if(idx==0) iter.drop(1) else iter}

val consulta = bd.map(s=>(s.split(",")(0))).map(word=>(word,1)).reduceByKey(_+_)
//MAYOR CANTIDAD DE PRODUCTO?
val mayor = consulta.map(item =>item.swap).sortByKey(false)
//MENOR CANTIDAD DE PRODUCTO?
val menor = consulta.map(item =>item.swap).sortByKey()

//imprimir el resultado
println("mayor cantidad de producto: ",mayor.first())
println("menor cantidad de producto: ",menor.first())