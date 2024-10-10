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
                // Mostrar mensaje de error en la consola y retornar un estado de error
                Console.WriteLine("Error de sintaxis.");
                return OperationStatus.Error;
            }

            // Extraer el nombre de la tabla desde la sentencia
            var tableName = match.Groups[1].Value;

            // Verificar si se incluye WHERE
            var where = match.Groups[2].Success ? match.Groups[2].Value : null;

            // Llamar al método DeleteFromTable del almacén de datos y retornar el resultado
            return store.Delete(tableName, where);
        }
    }
}


