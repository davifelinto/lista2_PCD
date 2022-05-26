const fs = require('fs'); 
const {parse} = require('csv-parser');

fs.createReadStream("./cidade_populacao.csv")
.pipe(parse({ delimiter: ",", from_line: 2 }))
.on("data", function (row) {
  console.log(row);
})