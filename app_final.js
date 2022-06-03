// Programacao Concorrente e Distribuida
// Lista 2
// Alunos:   Davi Hugo Sateles Felinto – UC 21105738
//          Jackes Ridan da Silva Guedes Júnior – UC19106906

// Imports
const fs = require("fs");
const { parse } = require("csv-parse");
const fastcsv = require("fast-csv")
const mysql = require('mysql');
const slug = require('slug')
const writableStream = fs.createWriteStream("saida.csv");

const con = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "lista2", // Colocar senha do seu usuario
  database: "listateste",
  multipleStatements: true
});

// Funcao do calculo da distancia baseado nas coordenadas, retorna kilometragem com 2 casas decimais
function distCoordenadasKm(position1, position2) {
    "use strict";
    var deg2rad = function (deg) { return deg * (Math.PI / 180); },
        R = 6371,
        dLat = deg2rad(position2.lat - position1.lat),
        dLng = deg2rad(position2.lng - position1.lng),
        a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
            + Math.cos(deg2rad(position1.lat))
            * Math.cos(deg2rad(position1.lat))
            * Math.sin(dLng / 2) * Math.sin(dLng / 2),
        c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return ((R * c).toFixed(2));
}

// Dicionarios das coordenadas das cidades "destino"
const coord_1 = {lat: -12.971111, lng: -38.510833}// Salvador
const coord_2 = {lat: -12.266667, lng: -38.966667}// Feira de Santana
const coord_3 = {lat: -14.866111, lng: -40.839444}// Vitória da Conquista
const coord_4 = {lat: -12.696389, lng: -38.323333}// Camacari

// Como o dict das coordenadas tem que ir pra função
// var distancia = (distCoordenadasKm(
//    {lat: -23.522490, lng: -46.736600},
//    {lat: -23.4446654, lng: -46.5319316}
// ));
// console.log(distancia);

// Um unico connect para fazer todas as operacoes
con.connect(async function(err) {
    if (err) throw err;
    console.log("Connected!");

        // Criar banco - ok
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
                            dist_1 DOUBLE,\
                            dist_2 DOUBLE,\
                            dist_3 DOUBLE,\
                            dist_4 DOUBLE,\
                            primary key (id),\
                            foreign key (cidade_id)\
                                references cidade (id));";
            
            con.query(sql, function (err, result) {
                if (err) throw err;
                console.log("Table empresa Database reseted");
                resolve();
            });
        })

    // Ler e inseir uf - ok
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

    // Ler e inserir cidade_siafi - ok
    await new Promise((resolve, reject) => {
        var sql = "INSERT INTO listateste.cidade (uf_id, nome, latitude, longitude, cod_ibge, cod_siafi)\
                    VALUES (?, ?, ?, ?, ?, ?)"

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

    // Criar de coluna auxiliar no banco - ok
    await new Promise((resolve, reject) => {
        var sql = "ALTER TABLE listateste.empresa ADD municipio_siafi VARCHAR(255);\
                    SET SQL_SAFE_UPDATES = 0;";

        con.query(sql, function (err, result) {
            if (err) throw err;
            console.log("Table empresa altered");
            resolve();
        });
    })

    //Ler e inseir empresas_bahia - nao entendo como a conta vai pra frente ainda
    await new Promise((resolve, reject) => {
        var sql = "INSERT INTO listateste.empresa (slug, nome_fantasia, dt_inicioatividade, cnae_fiscal, cep, porte, municipio_siafi)\
                     VALUES (?, ?, ?, ? , ?, ?, ?)"
                     // , dist_1, dist_2, dist_3, dist_4
        
        fs.createReadStream("res/empresas_bahia_lista_3.csv")
        .pipe(parse({ delimiter: ",", from_line: 2 }))
        .on("data", function (row) {
            con.query(sql, [slug(row[0]), row[0], row[1], row[2], row[3], row[5], row[4],
                            // distCoordenadasKm(,coord_1),distCoordenadasKm(,coord_2),
                            // distCoordenadasKm(,coord_3),distCoordenadasKm(,coord_4)
                            ], function (err, result) {
                if (err) throw err;
                console.log("Empresa record inserted");
                resolve();
            });
        });
    })

    // Update nos valores faltantes. Arquivo: cidade_populacao - ok
    await new Promise((resolve, reject) => {
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

    // Drop da coluna auxiliar criada na tabela do banco - ok
    await new Promise((resolve, reject) => {
        var sql = "ALTER TABLE listateste.empresa DROP COLUMN municipio_siafi;\
                    SET SQL_SAFE_UPDATES = 1;";
        con.query(sql, function (err, result) {
            if (err) throw err;
            console.log("Table empresa altered");
            resolve();
        });
    })

    // Gravacao de arquivo csv - ok
    await new Promise((resolve, reject) => {

        sql = "SELECT empresa.nome_fantasia, empresa.slug, empresa.dt_inicioatividade as inicio_atividades,\
                        empresa.porte as porte_empresa, cidade.nome as nome_cidade, uf.sigla as sigla_uf,\
                        cidade.populacao as populacao_cidade, cidade.latitude as latitude_cidade, cidade.longitude as longitude_cidade\
                FROM uf\
                JOIN cidade\
                    ON uf.id = cidade.uf_id\
                RIGHT JOIN empresa\
                    ON cidade.id = empresa.cidade_id"

        con.query(sql, function (err, data) {
        if (err) throw err;
    
        //JSON
        const jsonData = JSON.parse(JSON.stringify(data));
        console.log("jsonData", jsonData);
    
        //csv
        fastcsv
            .write(jsonData, { headers: true })
            .on("finish", function () {
            console.log("Finished writing data!");
            })
            .pipe(writableStream);
        });
    })

    await new Promise((resolve, reject) => {
        con.end((err) => {
            if (err) {
                console.error('Error during disconnection', err.stack)
                return reject(err)
            }
            console.log('Disconnected sucessfuly!')
            return resolve()
        })
    })
});


