// Esse método de leitura tá funcionando. filename = "./nome_arquivo.csv"
function readCSV(filename){
    const fs = require("fs");
    const { parse } = require("csv-parse");

    fs.createReadStream(filename)
    .pipe(parse({ delimiter: ",", from_line: 2 }))
    .on("data", function (row) {
        console.log(row);
    })
    .on("end", function () {
        console.log("finished");
    })
    .on("error", function (error) {
        console.log(error.message);
    });
}



// o valor data tem que ter o formato {id: valor} para cada campo do csvWriter
// não testado ainda
function writeCSV(data){
    const createCsvWriter = require('csv-writer').createObjectCsvWriter;
    const csvWriter = createCsvWriter({
    path: 'saida.csv',
    header: [
        {id: 'nome_fantasia', title: 'Nome Fantasia'},
        {id: 'slug', title: 'Slug'},
        {id: 'inicio_atividades', title: 'Inicio Atividades'},
        {id: 'porte_empresa', title: 'Porte da Empresa'},
        {id: 'nome_cidade', title: 'Cidade'},
        {id: 'sigla_uf', title: 'UF'},
        {id: 'populacao_cidade', title: 'Populacao'},
        {id: 'latitude_cidade', title: 'Latitude'},
        {id: 'longitude_cidade', title: 'Longitude'},
    ]
    });

    csvWriter.writeRecords(data)
    .then(()=> console.log('The CSV file was written successfully'));
}

// não testado ainda
function writeCSVfast(data){
    const fastcsv = require('fast-csv');
    const fs = require('fs');
    const ws = fs.createWriteStream("out.csv");
    fastcsv
    .write(data, { headers: true })
    .pipe(ws);
}

module.exports = {
    readCSV,
    writeCSV,
    writeCSVfast
  }