// AREA DE TESTES

// Step 1 — Reading CSV Files
// procedimento de leitura, usado posteriormente
const fs = require("fs");
const parser = require("csv-parser");
const files = ["./cidade_populacao.csv","./cidade_siafi.csv","./empresas_bahia.csv", "./uf.csv"]

files.forEach(function (item){
  fs.createReadStream(item)
  .pipe(parser({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    console.log(row);
  })
  .on("end", function () {
    console.log("finished");
  })
  .on("error", function (error) {
    console.log(error.message);
  });
});