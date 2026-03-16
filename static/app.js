function cambiarModo(){

    let modo = document.getElementById("modo").value

    document.getElementById("mesDiv").style.display = "none"
    document.getElementById("rangoDiv").style.display = "none"

    if(modo === "mes"){
        document.getElementById("mesDiv").style.display = "block"
    }

    if(modo === "rango"){
        document.getElementById("rangoDiv").style.display = "block"
    }

}

async function extraer(){

    let usuario = document.getElementById("usuario").value
    let password = document.getElementById("password").value
    let tabla = document.getElementById("tabla").value
    let anio = document.getElementById("anio").value
    let entorno = document.getElementById("entorno").value

    let modo = document.getElementById("modo").value

    let url = ""

    if(modo === "anio"){

        url = `/sap/historico?tabla=${tabla}&anio=${anio}&usuario=${usuario}&password=${password}&entorno=${entorno}`

    }

    if(modo === "mes"){

        let mes = document.getElementById("mes").value

        url = `/sap/historico-mes?tabla=${tabla}&anio=${anio}&mes=${mes}&usuario=${usuario}&password=${password}&entorno=${entorno}`

    }

    if(modo === "rango"){

        let mes_inicio = document.getElementById("mes_inicio").value
        let mes_fin = document.getElementById("mes_fin").value

        url = `/sap/historico-rango?tabla=${tabla}&anio=${anio}&mes_inicio=${mes_inicio}&mes_fin=${mes_fin}&usuario=${usuario}&password=${password}&entorno=${entorno}`

    }

    document.getElementById("resultado").innerText = "Extrayendo datos..."

    try{

        let res = await fetch(url)

        let data = await res.json()

        console.log("Respuesta API:", data)

        if(data.success){

            document.getElementById("resultado").innerText =
            "Registros insertados: " + data.registros_insertados

        }else{

            document.getElementById("resultado").innerText =
            "Error: " + data.error

        }

    }catch(error){

        document.getElementById("resultado").innerText =
        "Error conectando con el servidor"

    }

}