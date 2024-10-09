using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;
using QueryProcessor.Parser;
using System.Data.Common;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        // Declarar el diccionario de manera que use la firma adecuada
        private static readonly Dictionary<string, Func<string, OperationStatus>> CommandHandlers
            = new Dictionary<string, Func<string, OperationStatus>>(StringComparer.OrdinalIgnoreCase)
        {
            { "CREATE DATABASE", sentence => new CreateDataBase().Execute(GetCommandArgument(sentence)) },
            { "SET DATABASE", sentence => new SetDataBase().Execute(GetCommandArgument(sentence)) },
            { "CREATE TABLE", ExecuteCreateTable },
            { "DROP TABLE", sentence => new DropTable().Execute(GetCommandArgument(sentence)) },
            { "CREATE INDEX", sentence => new CreateIndex().Execute(sentence) },
            { "SELECT", ExecuteSelect },
            { "INSERT INTO", sentence => new ParserInsert().Parser(sentence) },
            { "UPDATE", sentence => new Update().Execute(sentence) },
            { "DELETE", sentence => new Delete().Execute(sentence) }
        };

        public static OperationStatus Execute(string sentence, out object? data)
        {
            data = null; // Inicializar el parámetro out

            foreach (var handler in CommandHandlers)
            {
                if (sentence.StartsWith(handler.Key, StringComparison.OrdinalIgnoreCase))
                {
                    // Llamar al manejador y retornar el resultado
                    return handler.Value(sentence);
                }
            }

            throw new UnknownSQLSentenceException();
        }

        private static string GetCommandArgument(string sentence)
        {
            int index = sentence.IndexOf(' ') + 1; // Encuentra el primer espacio
            return index < sentence.Length ? sentence.Substring(index).Trim() : string.Empty;
        }

        private static OperationStatus ExecuteCreateTable(string sentence)
        {
            string tableInfo = GetCommandArgument(sentence);
            string tableName = new ParserTable().GetTableName(tableInfo);
            string tableColumnsInfo = tableInfo.Substring(tableName.Length).Trim();
            List<Column> tableColumns = new ParserTable().GetColumns(tableColumnsInfo);
            return new CreateTable().Execute(tableName, tableColumns);
        }

        private static OperationStatus ExecuteSelect(string sentence)
        {
            // Verificar si la consulta es sobre el System Catalog
            if (sentence.Contains("FROM SystemDatabases", StringComparison.OrdinalIgnoreCase))
            {
                return new SelectSystemDataBases().Execute();
            }
            else if (sentence.Contains("FROM SystemTables", StringComparison.OrdinalIgnoreCase))
            {
                return new SelectSystemTables().Execute();
            }
            else if (sentence.Contains("FROM SystemColumns", StringComparison.OrdinalIgnoreCase))
            {
                return new SelectSystemColumns().Execute();
            }
            else
            {
                // Implementar el SELECT normal sobre tablas de usuario
                return new Select().Execute(sentence, out _);
            }
        }
    }
}
