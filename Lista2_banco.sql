CREATE DATABASE IF NOT EXISTS ListaTeste;

use ListaTeste;
CREATE TABLE IF NOT EXISTS uf (
id INT,
 sigla VARCHAR(2),
 nome_uf VARCHAR(50),
 primary key (id));
    
CREATE TABLE IF NOT EXISTS cidade (
id INT auto_increment,
 uf_id INT,
 nome VARCHAR(255),
 populacao INT,
 latitude VARCHAR(255),
 longitude VARCHAR(255),
 cod_ibge VARCHAR(255),
 cod_siafi VARCHAR(255),
 primary key (id),
 foreign key (uf_id)
	references uf (id));

CREATE TABLE IF NOT EXISTS empresa (
id INT auto_increment,
 cidade_id INT,
 slug VARCHAR(255),
 nome_fantasia VARCHAR(255),
 dt_inicioatividade DATE,
 cnae_fiscal VARCHAR(255),
 cep VARCHAR(255),
 porte INT,
 primary key (id),
 foreign key (cidade_id)
	references cidade (id));
