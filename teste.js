// AREA DE TESTES

// const read = require('readline-sync')
// const csvs = require('./csv_funcs');
// csvs.readCSV('cidade_populacao.csv');
// var data = 'cidade_populacao.csv'
// fs.createReadStream('cidade_populacao.csv')
//     .pipe(csvParser())
//     .on('cidade_populacao', (row) => {
//         console.log(row);
//     })
//     .on('end', () => {
//         console.log('CSV file successfully processed');
//     });

const fs = require('fs'); 
const {parse} = require('csv-parser');

// fs.createReadStream("./cidade_populacao.csv")
// .pipe(parser())
// .on('cidade_populacao', function(data){
//     console.log("passei pela funcao")

//     try {
//         console.log("Codigo ibge: "+data.cod_ibge);
//         console.log("Nome cidade: "+data.nome_cidade);
//         console.log("Nome uf: "+data.nome_uf);
//         console.log("Populacao: "+data.populacao);

//         //perform the operation
//     }
//     catch(err) {
//         console.log(err)
//         //error handler
//     }
// })
// .on('end',function(){
//     console.log("É o fim.")
//     //some final operation
// });

fs.createReadStream("./cidade_populacao.csv")
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    console.log(row);
  })