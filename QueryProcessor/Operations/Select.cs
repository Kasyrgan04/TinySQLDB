using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        public OperationStatus Execute(string sentence, out object? data)
        {
            data = null;

            
            Console.WriteLine($"Comando recibido: {sentence}");

            
            var store = Store.GetInstance();

            
            const string pattern = @"SELECT\s+(\*|\w+(?:\s*,\s*\w+)*)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?)?;?$";
            var match = Regex.Match(sentence, pattern, RegexOptions.IgnoreCase);

            
            if (!match.Success)
            {
                Console.WriteLine("Error de sintaxis.");
                return OperationStatus.Error;
            }

            
            var columnsPart = match.Groups[1].Value;
            var tableName = match.Groups[2].Value;
            var whereClause = match.Groups[3].Success ? match.Groups[3].Value : null;
            var orderByColumn = match.Groups[4].Success ? match.Groups[4].Value : null;
            var orderByDirection = match.Groups[5].Success ? match.Groups[5].Value : "ASC"; 

            
            List<string>? columnsToSelect = columnsPart.Trim() == "*"
                ? null  // Null indica todas las columnas
                : columnsPart.Split(',').Select(c => c.Trim()).ToList();

            
            return store.Select(tableName, columnsToSelect, whereClause, orderByColumn, orderByDirection, out data);
        }
    }
}



