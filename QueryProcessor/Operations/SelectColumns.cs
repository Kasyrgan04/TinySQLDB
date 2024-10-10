using QueryProcessor.Parser;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;
using System.Data.Common;

namespace QueryProcessor.Operations
{
    internal class SelectColumns
    {
        public OperationStatus Execute()
        {
            var store = Store.GetInstance();
            List<string> bases = store.GetDataBases();

            Console.WriteLine("Columns:");
            foreach (var i in bases)
            {
                List<string> tables = store.GetTables(i);
                foreach (var table in tables)
                {
                    List<Column> columns = store.GetColumns(i, table);
                    foreach (var column in columns)
                    {
                        Console.WriteLine($"Base de datos: {i}, Tabla: {table}, Columna: {column.Name}, Tipo: {column.DataType}, Tamaño: {column.MaxSize}");
                    }
                }
            }
            return OperationStatus.Success;
        }
    }
}
