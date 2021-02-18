
//PARA CORRER ESTE SCRIPT ENTRE A SPARK-SHELL Y DENTRO DE SCALA TIPEE :load nombre.scala CON ESTO SE CORRERA EL SCRIPT
//leer archivo
val bdjuego = sc.textFile("dato.csv")
//quitar cabecera
val bd = bdjuego.mapPartitionsWithIndex{(idx,iter)=>if(idx==0) iter.drop(1) else iter}

val consulta = bd.map(s=>(s.split(",")(0),s.split(",")(7).toDouble)).reduceByKey(_+_)
//QUE COMPAÑIA VENDIO MAS?
val mayor = consulta.map(item =>item.swap).sortByKey(false)
//QUE COMPAÑIA VENDIO MENOS?
val menor = consulta.map(item =>item.swap).sortByKey()

//imprimir el resultado
println("mayor: ",mayor.first())
println("menor: ",menor.first())