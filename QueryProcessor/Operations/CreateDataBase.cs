using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateDataBase
    {
        internal OperationStatus Execute(string DataBaseName)
        {
            return Store.GetInstance().CreateDataBase(DataBaseName);
        }
    }
}
