// Pacotes necessarios:
// npm install mysql

const mysql = require('mysql');

const con = mysql.createConnection({
  host: "localhost",
  user: "usuario",
  password: "senha123"
});

con.connect(function(err) {
  if (err) throw err;
  console.log("Connected!");
});