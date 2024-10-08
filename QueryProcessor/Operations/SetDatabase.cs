using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;

namespace QueryProcessor.Operations
{
    internal class SetDatabase
    {
        internal OperationStatus Execute(string SetDataBaseName)
        {
            return Store.GetInstance().SetDatabase(SetDataBaseName);
        }
    }
}
