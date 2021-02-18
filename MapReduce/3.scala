
//PARA CORRER ESTE SCRIPT ENTRE A SPARK-SHELL Y DENTRO DE SCALA TIPEE :load nombre.scala CON ESTO SE CORRERA EL SCRIPT
//leer archivo
val bdjuego = sc.textFile("dato.csv")
//quitar cabecera
val bd = bdjuego.mapPartitionsWithIndex{(idx,iter)=>if(idx==0) iter.drop(1) else iter}

val consulta = bd.map(s=>((s.split(",")(0),s.split(",")(1),s.split(",")(2),s.split(",")(8),s.split(",")(9)),s.split(",")(7).toDouble))
//QUE PRODUCTO VENDIO MAS?
val mayor = consulta.map(item =>item.swap).sortByKey(false)
//QUE PRODUCTO VENDIO MENOS?
val menor = consulta.map(item =>item.swap).sortByKey()

//imprimir el resultado
println("mayor venta: ",mayor.first())
println("menor venta: ",menor.first())