using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class Update
    {
        private readonly string _tableName;
        private readonly int _id;
        private readonly string[] _newValues;

        public Update(string tableName, int id, string[] newValues)
        {
            _tableName = tableName;
            _id = id;
            _newValues = newValues;
        }

        public OperationStatus Execute()
        {
            return Store.GetInstance().Update(_tableName, _id, _newValues);
        }
    }
}

