using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        private readonly string _tableName;
        private readonly List<string> _columns;
        private string tableName;
        private string[] columns;

        // Constructor que acepta el nombre de la tabla y una lista de columnas
        public Select(string tableName, List<string> columns)
        {
            _tableName = tableName;
            _columns = columns;
        }

        public Select(string tableName, string[] columns)
        {
            this.tableName = tableName;
            this.columns = columns;
        }

        // Método de ejecución que realiza la selección
        public OperationStatus Execute()
        {
            // Aquí puedes pasar el nombre de la tabla y columnas al Store
            return Store.GetInstance().Select(_tableName, _columns);
        }
    }
}


