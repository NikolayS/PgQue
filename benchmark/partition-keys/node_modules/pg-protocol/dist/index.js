"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DatabaseError = exports.serialize = void 0;
exports.parse = parse;
const messages_1 = require("./messages");
Object.defineProperty(exports, "DatabaseError", { enumerable: true, get: function () { return messages_1.DatabaseError; } });
const serializer_1 = require("./serializer");
Object.defineProperty(exports, "serialize", { enumerable: true, get: function () { return serializer_1.serialize; } });
const parser_1 = require("./parser");
function parse(stream, callback) {
    const parser = new parser_1.Parser();
    stream.on('data', (buffer) => parser.parse(buffer, callback));
    return new Promise((resolve) => stream.on('end', () => resolve()));
}
//# sourceMappingURL=index.js.map