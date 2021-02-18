
//PARA CORRER ESTE SCRIPT ENTRE A SPARK-SHELL Y DENTRO DE SCALA TIPEE :load nombre.scala CON ESTO SE CORRERA EL SCRIPT
//leer archivo
val bdjuego = sc.textFile("dato.csv")
//quitar cabecera
val bd = bdjuego.mapPartitionsWithIndex{(idx,iter)=>if(idx==0) iter.drop(1) else iter}

val venta = bd.map(s=>(s.split(",")(2),(s.split(",")(7).toDouble,1))).reduceByKey {
  case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)
}

//PROMEDIO DE VENTAS DE LOS PUBLISHER
val promedio = venta.map({
    case(publisher,(venta,total)) => (venta/total,publisher)
}).sortByKey(false)

//
val showventa = venta.map(item =>item.swap).sortByKey(false)

//imprimir el resultado
//println("promedio de venta para Nintendo: ",mayor.first())
//println("venta")
//showventa.take(10).foreach(println)
println("PROMEDIO DE VENTAS DE LOS PUBLISHER")
promedio.collect().foreach(println)