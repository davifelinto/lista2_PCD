// Pacotes necessarios:
// npm install mysql

const mysql = require('mysql');
const { VARCHAR, DATE, INT,  } = require('mysql/lib/protocol/constants/types');

const con = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "d14049703S",
  multipleStatements: true
});

// Conexao funcionando
con.connect(function(err) {
  if (err) throw err;
  console.log("Connected!");
});


// Ainda nao foi
con.connect(function(err) {
  var query = "CREATE DATABASE Lista2\
  GO CREATE DATABASE Lista2; CREATE TABLE IF NOT EXISTS uf (id INT NOT NULL AUTO_INCREMENT, sigla VARCHAR(2), nome_uf VARCHAR(50), primary key (id));\
  GO CREATE TABLE IF NOT EXISTS cidade (id INT NOT NULL AUTO_INCREMENT, uf_id INT, nome VARCHAR(255), populacao INT, latitude VARCHAR(255), longitude VARCHAR(255), cod_ibge VARCHAR(255), cod_siafi VARCHAR(255), primary key (id), foreign key (uf_id) references uf (id));\
  GO CREATE TABLE IF NOT EXISTS empresa (id INT NOT NULL AUTO_INCREMENT, cidade_id INT, slug VARCHAR(255), nome_fantasia VARCHAR(255), dt_inicioatividade DATE, cnae_fiscal VARCHAR(255), cep VARCHAR(255), porte INT, primary key (id), foreign key (cidade_id) references cidade (id));"
  con.query(query, function (err, result) {
    if (err) throw err;
    console.log("Database and tables created");
  });
});

//------------------------------------------------------------//

// db(sqlite)
const fs = require("fs");
const sqlite3 = require("sqlite3").verbose();
const filepath = "./population.db";

function connectToDatabase() {
  if (fs.existsSync(filepath)) {
    return new sqlite3.Database(filepath);
  } else {
    const db = new sqlite3.Database(filepath, (error) => {
      if (error) {
        return console.error(error.message);
      }
      createTable(db);
      console.log("Connected to the database successfully");
    });
    return db;
  }
}

function createTable(db) {
  db.exec(`
  CREATE TABLE migration
  ( year_month       VARCHAR(10),
    month_of_release VARCHAR(10),
    passenger_type   VARCHAR(50),
    direction        VARCHAR(20),
    sex              VARCHAR(10),
    age              VARCHAR(50),
    estimate         INT
  )`);
}

module.exports = connectToDatabase();

// Insert data(sqlite)
const fs = require("fs");
const { parse } = require("csv-parse");
const db = require("./db");

fs.createReadStream("./migration_data.csv")
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    db.serialize(function () {
      db.run(
        `INSERT INTO migration VALUES (?, ?, ? , ?, ?, ?, ?)`,
        [row[0], row[1], row[2], row[3], row[4], row[5], row[6]],
        function (error) {
          if (error) {
            return console.log(error.message);
          }
          console.log(`Inserted a row with the id: ${this.lastID}`);
        }
      );
    });
  });