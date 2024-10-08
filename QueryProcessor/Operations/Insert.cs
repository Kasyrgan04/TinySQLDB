using Entities;
using StoreDataManager;
using System.IO;

namespace QueryProcessor.Operations
{
    internal class Insert
    {
        private readonly string _tableName;
        private readonly string[] _values;

        public Insert(string tableName, string[] values)
        {
            _tableName = tableName;
            _values = values;
        }

        public OperationStatus Execute()
        {
            return Store.GetInstance().Insert(_tableName, _values);
        }
    }
}


