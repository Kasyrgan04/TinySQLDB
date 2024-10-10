using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateBase
    {
        internal OperationStatus Execute(string DataBaseName)
        {
            return Store.GetInstance().CreateDataBase(DataBaseName);
        }
    }
}
