using System;
using System.Text.RegularExpressions;
using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class CreateIndex
    {
        public OperationStatus Execute(string sentence)
        {
            var store = Store.GetInstance();

            // sentencia CREATE INDEX
            const string pattern = @"CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(\s*(\w+)\s*\)\s+OF\s+TYPE\s+(BTREE|BST);?$";

            var match = Regex.Match(sentence, pattern, RegexOptions.IgnoreCase);

            if (!match.Success)
            {

                Console.WriteLine("Sintaxis incorrecta");
                return OperationStatus.Error;
            }

            // Extrae el nombre del índice, nombre de la tabla, nombre de la columna y tipo de índice
            var indexName = match.Groups[1].Value;
            var tableName = match.Groups[2].Value;
            var columnName = match.Groups[3].Value;
            var indexType = match.Groups[4].Value;

            // Llamar al método CreateIndex del almacén de datos y retornar el resultado
            return store.CreateIndex(indexName, tableName, columnName, indexType);
        }
    }
}

