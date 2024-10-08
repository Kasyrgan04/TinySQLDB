using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;

namespace QueryProcessor.Operations
{
    internal class DropTable
    {
        internal OperationStatus Execute(string TableToDrop)
        {
            return Store.GetInstance().DropTable(TableToDrop);
        }

    }
}
