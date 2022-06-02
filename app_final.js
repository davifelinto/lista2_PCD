// Programacao Concorrente e Distribuida
// Lista 2
// Alunos:   Davi Hugo Sateles Felinto – UC 21105738
//          Jackes Ridan da Silva Guedes Júnior – UC19106906

// consts para leitura e registro de dados no banco, chamado de listateste aqui
const fs = require("fs");
const { parse } = require("csv-parse");
const mysql = require('mysql');
const slug = require('slug')
const { VARCHAR, DATE, INT,  } = require('mysql/lib/protocol/constants/types');
const con = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "lista2", // Colocar senha do seu usuario
  database: "listateste",
  multipleStatements: true
});

// consts para criacao do arquivo de saida
const { stringify } = require("csv-stringify");
const filename = "saida.csv";
const writableStream = fs.createWriteStream(filename);
const columns = [
  "nome_fantasia",
  "slug",
  "inicio_atividades",
  "porte_empresa",
  "nome_cidade",
  "sigla_uf",
  "populacao_cidade",
  "latitude_cidade",
  "longitude_cidade",
];
const stringifier = stringify({ header: true, columns: columns });


// um unico connect para fazer todas as operacoes
con.connect(async function(err) {
    if (err) throw err;
    console.log("Connected!");

        // Criacao do banco;
        await new Promise((resolve, reject) => {
            var sql = "DROP DATABASE IF EXISTS ListaTeste;\
                        CREATE DATABASE ListaTeste;\
                        use ListaTeste;\
                        CREATE TABLE uf (\
                            id INT not NULL,\
                            sigla VARCHAR(2),\
                            nome_uf VARCHAR(50),\
                            primary key (id));\
                        CREATE TABLE cidade (\
                            id INT not NULL auto_increment,\
                            uf_id INT,\
                            nome VARCHAR(255),\
                            populacao INT,\
                            latitude VARCHAR(255),\
                            longitude VARCHAR(255),\
                            cod_ibge VARCHAR(255),\
                            cod_siafi VARCHAR(255),\
                            primary key (id),\
                            foreign key (uf_id)\
                                references uf (id));\
                        CREATE TABLE empresa (\
                            id INT not NULL auto_increment,\
                            cidade_id INT,\
                            slug VARCHAR(255),\
                            nome_fantasia VARCHAR(255),\
                            dt_inicioatividade DATE,\
                            cnae_fiscal VARCHAR(255),\
                            cep VARCHAR(255),\
                            porte INT,\
                            primary key (id),\
                            foreign key (cidade_id)\
                                references cidade (id));";
            con.query(sql, function (err, result) {
                if (err) throw err;
                console.log("Table empresa Database reseted");
                resolve();
            });
        })

    // primeira operacao: ler e inseir uf - check
    await new Promise((resolve, reject) => {
        var sql = "INSERT INTO listateste.uf (id, sigla) VALUES (?, ?)"
        fs.createReadStream("res/uf.csv")
        .pipe(parse({ delimiter: ",", from_line: 2 }))
        .on("data", function (row) {
            con.query(sql, [row[0],row[1]], function (err, result) {
                if (err) throw err;
                console.log("uf record inserted");
                resolve();
            });
        });
    });

    // segunda operacao: ler e inserir cidade_siafi - check
    await new Promise((resolve, reject) => {
        var sql = "INSERT INTO listateste.cidade (uf_id, nome, latitude, longitude, cod_ibge, cod_siafi) VALUES (?, ?, ?, ?, ?, ?)"
        fs.createReadStream("res/cidade_siafi.csv")
        .pipe(parse({ delimiter: ",", from_line: 2 }))
        .on("data", function (row) {
            con.query(sql, [row[4], row[1], row[2], row[3], row[0], row[5]], function (err, result) {
                if (err) throw err;
                console.log("cidade record inserted");
                resolve();
            });
        });
    })

    // Criacao de coluna auxiliar no banco - check
    await new Promise((resolve, reject) => {
        var sql = "ALTER TABLE listateste.empresa ADD municipio_siafi VARCHAR(255);\
                    SET SQL_SAFE_UPDATES = 0;";
        con.query(sql, function (err, result) {
            if (err) throw err;
            console.log("Table empresa altered");
            resolve();
        });
    })

    // primeira operacao: ler e inseir empresa - check
    await new Promise((resolve, reject) => {
        var sql = "INSERT INTO listateste.empresa (slug, nome_fantasia, dt_inicioatividade, cnae_fiscal, cep, porte, municipio_siafi) VALUES (?, ?, ?, ? , ?, ?, ?)"
        fs.createReadStream("res/empresas_bahia.csv")
        .pipe(parse({ delimiter: ",", from_line: 2 }))
        .on("data", function (row) {
            con.query(sql, [slug(row[0]), row[0], row[1], row[2], row[3], row[5], row[4]], function (err, result) {
                if (err) throw err;
                console.log("Empresa record inserted");
                resolve();
            });
        });
    })

    // quarta operacao: dar update nos valores faltantes arquivo cidade_populacao - check
    await new Promise((resolve, reject) => {
        //params = Number(String(row[0]).slice(0,2)),row[1],row[3],row[0]
        var sql = "UPDATE listateste.uf SET nome_uf = ? WHERE id = ?;\
                    UPDATE listateste.cidade SET populacao = ? WHERE cod_ibge = ?;\
                    UPDATE listateste.empresa SET cidade_id = (SELECT id FROM listateste.cidade WHERE cod_ibge = ?)\
                        WHERE municipio_siafi = (SELECT cod_siafi FROM listateste.cidade WHERE cod_ibge = ?);"
        fs.createReadStream("res/cidade_populacao.csv")
        .pipe(parse({ delimiter: ",", from_line: 2 }))
        .on("data", function (row) {
            con.query(sql, [row[3], Number(String(row[0]).slice(0,2)),
                             row[3], row[0],
                              row[0], row[0]] , function (err, result) {
                if (err) throw err;
                console.log("uf, cidade & empresa update inserted");
                resolve();
            });
        });
    })

    // Drop da coluna auxiliar criada na tabela do banco
    await new Promise((resolve, reject) => {
        var sql = "ALTER TABLE listateste.empresa DROP COLUMN municipio_siafi;\
                    SET SQL_SAFE_UPDATES = 1;";
        con.query(sql, function (err, result) {
            if (err) throw err;
            console.log("Table empresa altered");
            resolve();
        });
    })

    // Gravação de arquivo csv, nao ta pegando nada da query por algum motivo, nao deu pra resolver a tempo
    // O problema provavelmente é o método de gravação que tá todo errado.
    await new Promise((resolve, reject) => {
        
        sql = "SELECT empresa.nome_fantasia, empresa.slug, empresa.dt_inicioatividade as inicio_atividades, empresa.porte as porte_empresa, cidade.nome as nome_cidade, uf.sigla as sigla_uf, cidade.populacao as populacao_cidade, cidade.latitude as latitude_cidade, cidade.longitude as longitude_cidade\
        FROM uf\
        JOIN cidade\
            ON uf.id = cidade.uf_id\
        RIGHT JOIN empresa\
            ON cidade.id = empresa.cidade_id"

        con.each(query(sql, (err, row) => {
            if (error) {
              return console.log(error.message);
            }
            stringifier.write(row);
            resolve();
          }));
          stringifier.pipe(writableStream);
          console.log("Finished writing data");
    })

    await new Promise((resolve, reject) => {
        con.end();
        console.log("Disonnected!");
        resolve();
    })
});
