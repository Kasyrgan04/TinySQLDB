using Entities;
using QueryProcessor.Parser;
using StoreDataManager;
using System.Data.Common;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute(string TableName, List<Column> TableColumns)
        {
            return Store.GetInstance().CreateTable(TableName, TableColumns);
        }
    }
}
