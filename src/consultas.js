// Muestra los beneficios que obtiene la empresa por cada una de sus ventas.    

db.ventas.aggregate([
    {$project: 
        {beneficio:
            {$multiply:[{
                $subtract:["$pvpUnitario", "$pveUnitario"]},
                "$numeroUnidades"
            ]}
        }
    }
])

// Muestra el beneficio total de todas las ventas realizadas en el año 2015.

db.ventas.aggregate([
    {$match: 
        {fechaVenta: {$gte: new Date("2015,01,01"), $lte: new Date("2015,12,31")
    }}},     
    {$group: 
        {_id: "beneficio2015", 
        beneficio: 
            {$sum:
                {$multiply:[{
                    $subtract:["$pvpUnitario", "$pveUnitario"]},
                    "$numeroUnidades"
                ]}
            }
        }
    }
])
        
//Cuales han sido los beneficios que se ha generado a partir de 2020 en total.

db.ventas.aggregate([
    {$match:
        {$expr: {$eq: [{ $year: "$fechaVenta" }, 2019]}},
    },
    {$group:
        {_id: "beneficio2019", total: {$sum: {$multiply: ["$pvpUnitario","$pveUnitarios"]}}}
    }
])

// Muestra la cantidad de libros comprados por cada cliente, señalando los mejores.

db.ventas.aggregate([
    {$group:
        {_id: "$cliente", 
        librosVendidos: {$sum:"$numeroUnidades"}}
    }
])

//Cual es el valor total de cada articulo comprado por Universidad de Sevilla. Redondeamos a 1 cifra decimal.
 db.ventas.aggregate([
    {$match:
        {cliente: {$regex: /A.alde/i}}
    },
    {$group: 
        {_id: "$articuloVendido", total: {$sum: {$multiply: ["$pvpUnitario","$numeroUnidades"]}}}
    },  
    {$project: {total: {$round: ["$total", 2]}}}
])

// Muestra la cantidad de libros vendidos por la empresa por un trabajador concreto.

db.ventas.aggregate([
    {$match: 
        {vendedor:"Mirabal International"}
    },     
    {$group:
        {_id: "ventasMirabalInternational", 
        librosVendidos: {$sum:"$numeroUnidades"}}
    }
])

//Mostrar porcentaje de beneficio por cada articulo.

db.ventas.aggregate([
        { $project:
            {
                _id : 0 ,
                artículo: 1,
                porcentaje_beneficio:{
                    $multiply: [ {$divide: [ "$pveUnitarios", "$pvpUnitario" ]} , 100 ]   
                }
            } 
        }
])

// Muestra la cantidad de ventas realizadas por cada vendedor.

db.ventas.aggregate([
    {$group : 
        {_id:"$vendedor", 
        ventasRealizadas:{$sum:1}}
    }
])

// Muestra la cantidad de ventas realizadas cada mes.

db.ventas.aggregate([
    {$group:
        {_id: 
            {mes:{$month:"$fechaVenta"}, 
            año:{$year:"$fechaVenta"}},
        librosVendidos: {$sum:"$numeroUnidades"}
    }}
])

// Muestra la media de libros que vende un trabajador en concreto cada mes.

db.ventas.aggregate([
    {$match: 
        {vendedor:"The Book Zone"}
    }, 
    {$group:
        {_id:
            {mes:{$month:"$fechaVenta"}, 
            año:{$year:"$fechaVenta"}},
        mediaLibrosIñaki: {$avg:"$numeroUnidades"}}
    }
])

//Muestra los articulos que se vendieron en febrero del año 2021 y que su precio sea menor o igual a 18.95 euros

db.ventas.aggregate([
    {$match:
       {$and: [
            {$and: [
                {$expr: {$eq: [{ $year: "$fechaVenta" }, 2021]}},
                {$expr: {$eq: [{ $month: "$fechaVenta" }, 02]}},
            ]},
            {pvpUnitario: {$lte: 18.95}},
       ]}
    },
])

// Muestra el dinero obtenido por cada trabajador, permitiendo distinguir a los mejores.

db.ventas.aggregate([
    {$group: 
        {_id: "$vendedor", 
        ganancias: 
            {$sum:
                {$multiply:[{
                    $subtract: ["$pvpUnitario", "$pveUnitario"]},
                    "$numeroUnidades" 
                ]}    
            }         
        }
    }
])

// Muestra las mejores ventas realizadas por cada trabajador

db.ventas.aggregate([
    {$group: 
        {_id: "$vendedor", 
        cantidad: {$max:"$numeroUnidades"},
        beneficio: 
            {$max:
                {$sum:
                    {$multiply:[{
                        $subtract: ["$pvpUnitario", "$pveUnitario"]},
                        "$numeroUnidades" 
                    ]}
                }          
            }
        }
    }
])

// Muestra las mejores compras realizadas por cada cliente que han sido entregadas sin retraso.

db.ventas.aggregate([
    {$match:{retraso:false}},
    {$group: 
        {_id: "$cliente", 
        cantidad: {$max:"$numeroUnidades"},
        beneficio: 
            {$max:
                {$sum:
                    {$multiply:[{
                        $subtract: ["$pvpUnitario", "$pveUnitario"]},
                        "$numeroUnidades" 
                    ]}
                }          
            }
        }
    }
])

//El cliente Mirabal International paga 70 euros con un billete de 50 euros y otro de 20 euros. Calcular el cambio a dar.
db.ventas.aggregate([
    {$match:
        {$and: [
            {$expr: {$eq: [{ $year: "$fechaVenta" }, 2021]}},
            {cliente: /Mira.al/i},
        ]}},
    {$group:
        {_id: "CambioMirabalInternational2021", total: {$sum: {$multiply: ["$pvpUnitario","$pveUnitario"]}},
        }
    },
    {$project:
        {_id: "CambioMirabalInternational", resto: {$subtract:[70, "$total"]}}
    }
])

//Mostrar mejores clientes.
db.ventas.aggregate([
        { $group:
            {
                _id: "$cliente",
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                         {$add: [ {$multiply: ["$pvpUnitario", "$númeroUnidades" ]}]},
                         {$subtract: [ {$multiply: ["$pveUnitario", "$númeroUnidades" ]}] }
                        ]
                    }
                }
            }
        },{$sort: {beneficio_total:-1} }, {$limit:5} 
])

//Mostrar mejores vendedores.
db.ventas.aggregate([
        { $group:
            {
                _id: "$vendedor",
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                         {$add: [ {$multiply: ["$pvpUnitario", "$númeroUnidades" ]}]},
                         {$subtract: [ {$multiply: ["$pveUnitario", "$númeroUnidades" ]}] }
                        ]
                    }
                }
            }
        },{$sort: {beneficio_total:-1} }, {$limit:5} 
])

//Mostrar mejores articulos.
db.ventas.aggregate([
        { $group:
            {
                _id: "$articuloVendido",
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                            {$add: [ {$multiply: ["$pvpUnitario", "$numeroUnidades" ]}]},
                            {$subtract: [ {$multiply: ["$pveUnitario", "$numeroUnidades" ]}] }
                        ]
                    }
                }
            }
            
        },
        {$sort: {beneficio_total:-1} }, {$limit:5}
])
//Mostrar mejores meses.
db.ventas.aggregate([
        { $group:
            {
                _id: {$month:("$fechaVenta")} ,
                beneficio_total:
                {$sum: 
                    {$subtract:
                        [
                            {$add: [ {$multiply: ["$pvpUnitario", "$numeroUnidades" ]}]},
                            {$subtract: [ {$multiply: ["$pveUnitario", "$numeroUnidades" ]}] }
                        ]
                    }
                }
            }
        },
        {$sort: {beneficio_total:-1} }
    ]
)