using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using QueryProcessor.Parser;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string sentence, out object? data)
        {

            data = null;

            if (sentence.StartsWith("CREATE DATABASE"))
            {
                string DataBaseName = sentence.Substring("CREATE DATABASE".Length).Trim();
                return new CreateDataBase().Execute(DataBaseName);
            }
            if (sentence.StartsWith("SET DATABASE"))
            {
                string SetDataBaseName = sentence.Substring("SET DATABASE".Length).Trim();
                return new SetDatabase().Execute(SetDataBaseName);

            }
            if (sentence.StartsWith("CREATE TABLE"))
            {
                string TableInfo = sentence.Substring("CREATE TABLE".Length).Trim();
                string TableName = new ParserTable().GetTableName(TableInfo);
                string TableColumnsInfo = TableInfo.Substring(TableName.Length).Trim();
                List<Column> TableColumns = new ParserTable().GetColumns(TableColumnsInfo);

                return new CreateTable().Execute(TableName, TableColumns);
            }

            if (sentence.StartsWith("DROP TABLE"))
            {
                string TableToDrop = sentence.Substring("DROP TABLE".Length).Trim();

                return new DropTable().Execute(TableToDrop);

            }

            if (sentence.StartsWith("CREATE INDEX", StringComparison.OrdinalIgnoreCase))
            {
                return new CreateIndex().Execute(sentence);
            }


            if (sentence.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
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
                    return new Select().Execute(sentence, out data);
                }
            }

            if (sentence.StartsWith("INSERT INTO"))
            {

                return new ParserInsert().Parser(sentence);

            }

            if (sentence.StartsWith("UPDATE"))
            {
                return new Update().Execute(sentence);
            }

            if (sentence.StartsWith("DELETE"))
            {
                return new Delete().Execute(sentence);
            }



            else
            {
                throw new UnknownSQLSentenceException();
            }
        }


    }

}
