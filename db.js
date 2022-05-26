// Pacotes necessarios:
// npm install mysql

const mysql = require('mysql');
const { VARCHAR, DATE, INT,  } = require('mysql/lib/protocol/constants/types');

const con = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "" // Colocar senha
});

// Conexao funcionando
con.connect(function(err) {
  if (err) throw err;
  console.log("Connected!");
});


// Ainda nao foi
con.connect(function(err) {
  var query = "CREATE DATABASE Lista2; CREATE TABLE IF NOT EXISTS uf (id INT, sigla VARCHAR(2), nome_uf VARCHAR(50), primary key (id)); CREATE TABLE IF NOT EXISTS cidade (id INT, uf_id INT, nome VARCHAR(255), populacao INT, latitude VARCHAR(255), longitude VARCHAR(255), cod_ibge VARCHAR(255), cod_siafi VARCHAR(255), primary key (id), foreign key (uf_id) references uf (id)); CREATE TABLE IF NOT EXISTS empresa (id INT, cidade_id INT, slug VARCHAR(255), nome_fantasia VARCHAR(255), dt_inicioatividade DATE, cnae_fiscal VARCHAR(255), cep VARCHAR(255), porte INT, primary key (id), foreign key (cidade_id) references cidade (id))"
  con.query(query, function (err, result) {
    if (err) throw err;
    console.log("Database and tables created");
  });
});
