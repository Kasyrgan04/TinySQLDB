using StoreDataManager;
using System.Text.RegularExpressions;
using Entities;

namespace QueryProcessor.Operations
{
    internal class Delete
    {
        public OperationStatus Execute(string sentence)
        {
            var store = Store.GetInstance();

            // Sentencia DELETE
            const string pattern = @"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?$";

            var match = Regex.Match(sentence, pattern, RegexOptions.IgnoreCase);


            if (!match.Success)
            {
                
                Console.WriteLine("Sintaxis incorrecta");
                return OperationStatus.Error;
            }

            
            var tableName = match.Groups[1].Value;

            
            var where = match.Groups[2].Success ? match.Groups[2].Value : null;

            
            return store.Delete(tableName, where);
        }
    }
}


