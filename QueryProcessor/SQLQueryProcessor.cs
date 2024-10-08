using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string sentence)
        {
            if (sentence.StartsWith("CREATE TABLE"))
            {
                return new CreateTable().Execute();
            }
            if (sentence.StartsWith("SELECT"))
            {
                return new Select().Execute();
            }
            if (sentence.StartsWith("INSERT INTO"))
            {
                var parts = sentence.Split(new[] { "VALUES" }, StringSplitOptions.None);
                if (parts.Length == 2)
                {
                    var tableName = parts[0].Split(' ')[2]; // Asumiendo que la tabla es el tercer elemento
                    var valuesPart = parts[1].Trim(' ', '(', ')'); // Limpiamos paréntesis
                    var values = valuesPart.Split(','); // Divide los valores por comas
                    return new Insert(tableName, values).Execute();
                }
                else
                {
                    throw new UnknownSQLSentenceException();
                }
            }

            if (sentence.StartsWith("UPDATE"))
            {
                var parts = sentence.Split(new[] { ' ' }, 4);
                string tableName = parts[1];
                int id = int.Parse(parts[2]);
                string[] newValues = parts[3].Split(',');
                return new Update(tableName, id, newValues).Execute();
            }
            if (sentence.StartsWith("DELETE FROM"))
            {
                var parts = sentence.Split(new[] { ' ' }, 4);
                string tableName = parts[2];
                int id = int.Parse(parts[3]);
                return new Delete(tableName, id).Execute();
            }
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }

    }
}
