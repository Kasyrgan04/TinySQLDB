using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        private readonly string _tableName;
        private readonly string[] _columnsDefinition;

        // Constructor que acepta el nombre de la tabla y las definiciones de columnas
        public CreateTable(string tableName, string[] columnsDefinition)
        {
            _tableName = tableName;
            _columnsDefinition = columnsDefinition;
        }

        // Método de ejecución que crea la tabla
        public OperationStatus Execute()
        {
            return Store.GetInstance().CreateTable(_tableName, _columnsDefinition);
        }
    }
}

