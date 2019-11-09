"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const axios_1 = require("axios");

const parser = require('xml2json');
const fs = require('fs');
axios_1.default.get('https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/')
    .then(res => res.data)
    .then(data => parser.toJson(data))
    .then(data => fs.writeFileSync('testcycle.json', data));
//# sourceMappingURL=index.js.map