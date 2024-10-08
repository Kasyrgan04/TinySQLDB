using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class SelectSystemDataBases
    {
        public OperationStatus Execute()
        {
            Store store = Store.GetInstance();
            List<string> bases = store.GetDataBases();

            // Mostrar los resultados
            Console.WriteLine("Base de datos:");
            foreach (var i in bases)
            {
                Console.WriteLine($"- {i}");
            }

            return OperationStatus.Success;
        }
    }
}
