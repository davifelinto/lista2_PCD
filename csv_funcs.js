// const csv = require('csv')

function readCSV(data){
    const csvParser = require('csv-parser')
    const fs = require('fs');

    fs.createReadStream(data)
        .pipe(csvParser())
        .on(data, (row) => {
            console.log(row);
        })
        .on('end', () => {
            console.log('CSV file successfully processed');
        });
}


// o valor data tem que ter o formato {id: valor} para cada campo do csvWriter

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