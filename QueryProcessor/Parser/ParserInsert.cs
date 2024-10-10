using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Entities;
using StoreDataManager;
using QueryProcessor;
using QueryProcessor.Operations;

namespace QueryProcessor.Parser
{
    internal class ParserInsert
    {
        public OperationStatus Parser(string sentence)
        {
            // Parsear la sentencia
            var match = Regex.Match(sentence, @"INSERT INTO\s+(\w+)\s*\((.+)\);?", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                Console.WriteLine("Sintaxis de INSERT INTO incorrecta.");
                return OperationStatus.Error;
            }

            string tableName = match.Groups[1].Value;
            string valuesPart = match.Groups[2].Value;

            // Separar los valores
            var values = ParseValues(valuesPart);

            if (values == null || values.Count == 0)
            {
                Console.WriteLine("No se encontraron valores para insertar.");
                return OperationStatus.Error;
            }

            return new Insert().Execute(tableName, values);
        }

        private List<string> ParseValues(string valuesPart)
        {
            var values = new List<string>();
            var current = new StringBuilder();
            bool inQuotes = false;
            char quoteChar = '\0';

            foreach (char c in valuesPart)
            {
                if ((c == '\'' || c == '\"') && !inQuotes)
                {
                    inQuotes = true;
                    quoteChar = c;
                }
                else if (c == quoteChar && inQuotes)
                {
                    inQuotes = false;
                }
                else if (c == ',' && !inQuotes)
                {
                    values.Add(current.ToString().Trim());
                    current.Clear();
                    continue;
                }
                current.Append(c);
            }

            if (current.Length > 0)
                values.Add(current.ToString().Trim());

            return values.Where(v => !string.IsNullOrWhiteSpace(v)).ToList();
        }
    }
}
