using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;

namespace QueryProcessor.Operations
{
    internal class SelectTables
    {
        public OperationStatus Execute()
        {
            var store = Store.GetInstance();
            List<string> databases = store.GetDataBases();

            Console.WriteLine("Tablas:");
            foreach (var i in databases)
            {
                List<string> tables = store.GetTables(i);
                foreach (var table in tables)
                {
                    Console.WriteLine($"Base de datos: {i}, Tabla: {table}");
                }
            }

            return OperationStatus.Success;
        }
    }
}
