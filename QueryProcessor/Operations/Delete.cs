using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class Delete
    {
        private readonly string _tableName;
        private readonly int _id;

        public Delete(string tableName, int id)
        {
            _tableName = tableName;
            _id = id;
        }

        public OperationStatus Execute()
        {
            return Store.GetInstance().Delete(_tableName, _id);
        }
    }
}

