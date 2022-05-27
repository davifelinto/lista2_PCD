// Step 1 — Reading CSV Files
// procedimento de leitura, usado posteriormente
const fs = require("fs");
const { parse } = require("csv-parse");

// files.forEach(function (item){
//   fs.createReadStream(item)
//   .pipe(parser({ delimiter: ",", from_line: 2 }))
//   .on("data", function (row) {
//     console.log(row);
//   })
//   .on("end", function () {
//     console.log("finished");
//   })
//   .on("error", function (error) {
//     console.log(error.message);
//   });
// });


// Step 2 — Inserting Data into the Database
const db = require("./db_old");
const files = ["./cidade_populacao.csv","./empresas_bahia.csv", "./uf.csv","./cidade_siafi.csv"]
const querys = ['INSERT INTO cidade VALUES (?, ?, ?, ?)',
                //uf_id == primeiros 2 digitos de cod_ibge,
                //nome, populacao, cod_ibge
                'INSERT INTO empresa VALUES (?, ?, ? , ?, ?, ?)',
                // slug, nome_fantasia, dt_inicioatividade, cnae_fiscal, cep, porte
                'INSERT INTO uf VALUES (?, ?)',
                //id, uf(sigla)
                'UPDATE uf SET nome_uf = ? WHERE id = ?;\
                UPDATE cidade SET latitude = ?, longitude = ?, cod_siafi = ? WHERE codigo_ibge = ?']
                // nome, codigo_uf
                // latitude, longitude, cod_siafi, cod_ibge

const params = ["row[0](2 digitos), row[1], row[3], row[0]",
                "slug.row[0],row[0],row[1],row[2],row[3],row[4],row[5]",
                "row[0],row[1]",
                "row[1], row[4], row[2], row[3], row[5], row[0]"]

// fazer funcao insert no arquivo db.js
// adaptar para o mysql
//looping para cada arquivo? -> acho que vai bugar, cada arquivo tem um formato diferente... fazer 4 variacoes?

files.forEach(function (item, i, querys, params, array){

  fs.createReadStream(item)
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    db.serialize(function () {
      db.run(querys[i],[params[i]],function (error) {
          if (error) {
            return console.log(error.message);
          }
          // console.log(`Inserted a row with the id: ${this.lastID}`);
        }
      );
    });
  });

});


// Step 3 — Writing CSV Files
//adaptar para mysql
// Adaptar para o modelo (tem algumas funcoes no arquivo csv_funcs.js)
const { stringify } = require("csv-stringify");
const filename = "saved_from_db.csv";
const writableStream = fs.createWriteStream(filename);
const columns = [
  "year_month",
  "month_of_release",
  "passenger_type",
  "direction",
  "sex",
  "age",
  "estimate",
];
const stringifier = stringify({ header: true, columns: columns });


db.each(`select * from migration`, (error, row) => {
  if (error) {
    return console.log(error.message);
  }
  stringifier.write(row);
});
stringifier.pipe(writableStream);
console.log("Finished writing data");