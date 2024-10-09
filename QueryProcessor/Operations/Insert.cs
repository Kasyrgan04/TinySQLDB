using Entities;
using StoreDataManager;
using System.IO;

namespace QueryProcessor.Operations
{
    internal class Insert
    {
        internal OperationStatus Execute(string TableName, List<String> Valores)
        {
            return Store.GetInstance().Insert(TableName, Valores);
        }
    }
}


