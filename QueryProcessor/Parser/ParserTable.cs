using System.Data.Common;
using System.Text.RegularExpressions;
using EntitiesDataType = Entities.DataType;

namespace QueryProcessor.Parser
{
    internal class ParserTable
    {
        internal string GetTableName(string sentence)
        {
            int tableNameEnd = sentence.IndexOf('(');
            return sentence.Substring(0, tableNameEnd).Trim();
        }

        internal List<Column> GetColumns(string columnsInfo)
        {
            var columns = new List<Column>();

            columnsInfo = columnsInfo.Trim().Substring(1, columnsInfo.Length - 2);
            string[] rawColumns = columnsInfo.Split(',');

            var rawColumnsMatrix = new List<string[]>();

            foreach (string column in rawColumns)
            {
                string trimmedColumn = column.Trim();
                Match match = Regex.Match(trimmedColumn, @"^(\S+)\s+(.+)$");

                if (match.Success)
                {
                    string columnName = match.Groups[1].Value.Trim();
                    string columnType = match.Groups[2].Value.Trim();

                    rawColumnsMatrix.Add(new string[] { columnName, columnType });
                }
                else
                {
                    Console.WriteLine("FORMATO INVÁLIDO");
                }
            }

            foreach (string[] column in rawColumnsMatrix)
            {
                var newColumn = new Column
                {
                    Name = column[0]
                };

                string rawColumnDataType = column[1];

                if (rawColumnDataType.StartsWith("INTEGER"))
                {
                    newColumn.DataType = EntitiesDataType.INTEGER; // Uso del alias
                }
                else if (rawColumnDataType.StartsWith("VARCHAR"))
                {
                    string varcharSize = rawColumnDataType.Substring("VARCHAR".Length).Trim('(', ')');

                    if (int.TryParse(varcharSize, out int maxSizeNumber))
                    {
                        newColumn.DataType = EntitiesDataType.VARCHAR; // Uso del alias
                        newColumn.MaxSize = maxSizeNumber;
                    }
                    else
                    {
                        Console.WriteLine("Error al parsear el tamaño de VARCHAR.");
                    }
                }
                else if (rawColumnDataType.StartsWith("DOUBLE"))
                {
                    newColumn.DataType = EntitiesDataType.DOUBLE; // Uso del alias
                }
                else if (rawColumnDataType.StartsWith("DATETIME"))
                {
                    newColumn.DataType = EntitiesDataType.DATETIME; // Uso del alias
                }

                columns.Add(newColumn);
            }

            return columns;
        }
    }
}
