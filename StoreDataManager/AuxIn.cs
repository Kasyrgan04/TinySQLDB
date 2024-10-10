using ApiInterface;
using ApiInterface.Indexes;
using Entities;
using QueryProcessor.Parser;
using StoreDataManager;
using System.Data.Common;
using System.Text.RegularExpressions;

public enum OperationStatus
{
    Success,
    Error
}

public class DatabaseManager
{

    public StoreDataManager.Store store = new StoreDataManager.Store();

    private string SystemIndexesFile = Store.SystemIndexesFile;
    public string CurrentDatabaseName;
    public string CurrentDatabasePath;
    public Dictionary<string, object> IndexTrees = new Dictionary<string, object>();
    public Dictionary<string, string> IndexesByColumns = new Dictionary<string, string>();
    public List<string> IndexedDatabases = new List<string>();
    public List<string> IndexedTables = new List<string>();
    public List<string> IndexedColumns = new List<string>();


    public List<object> GetColumn(string databaseName, string tableName, string columnName)
    {
        string originalDBName = CurrentDatabaseName;
        string originalDBPath = CurrentDatabasePath;
        var columnData = new List<object>();

        
        if (string.IsNullOrEmpty(databaseName))
        {
            Console.WriteLine("El nombre de la base de datos no puede estar vacío.");
            return columnData;
        }

        
        List<string> tables = store.GetTables(databaseName);
        if (!tables.Contains(tableName))
        {
            Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
            return columnData;
        }

       
        List<Column> allColumns = store.GetColumns(databaseName, tableName);

    
        var targetColumn = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
        if (targetColumn == null)
        {
            Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
            return columnData;
        }

        string dataBasePath = $@"{Store.DataPath}\{databaseName}";

        if (Directory.Exists(dataBasePath))
        {
            this.CurrentDatabasePath = dataBasePath;
            this.CurrentDatabaseName = databaseName;
        }

        
        string tablePath = Path.Combine(CurrentDatabasePath, $"{tableName}.table");

        if (!File.Exists(tablePath))
        {
            Console.WriteLine($"El archivo de la tabla '{tableName}' no existe.");
            return columnData;
        }

        int columnIndex = allColumns.IndexOf(targetColumn); 

        using (FileStream fs = new FileStream(tablePath, FileMode.Open, FileAccess.Read))
        using (BinaryReader reader = new BinaryReader(fs))
        {
            while (fs.Position < fs.Length)
            {
                for (int i = 0; i < allColumns.Count; i++)
                {
                    object value = new AuxSel().Read(reader, allColumns[i].DataType);

                    
                    if (i == columnIndex)
                    {
                        columnData.Add(value);
                    }
                }
            }
        }

        
        this.CurrentDatabasePath = originalDBPath;
        this.CurrentDatabaseName = originalDBName;

        return columnData;
    }


    public DataType? GetDatatype(string baseName, string tableName, string columnName)
    {

        if (string.IsNullOrEmpty(baseName))
        {
            Console.WriteLine("No es posible asignar un nombre en blanco.");
            return null;
        }


        List<string> tables = store.GetTables(baseName);
        if (!tables.Contains(tableName))
        {
            Console.WriteLine($"'{tableName}' no existe '{baseName}'.");
            return null;
        }


        List<Column> allColumns = store.GetColumns(baseName, tableName);


        var targetColumn = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
        if (targetColumn == null)
        {
            Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
            return null;
        }


        return targetColumn.DataType;
    }


    public string GetIndexes()
    {
        return SystemIndexesFile;
    }

        
    public string GetName(string baseName, string tableName, string columnName)
    {
        if (!File.Exists(SystemIndexesFile))
        {
            return null;    
        }

        using (FileStream stream = File.Open(SystemIndexesFile, FileMode.Open, FileAccess.Read))
        using (BinaryReader reader = new(stream))
        {
            while (stream.Position < stream.Length)     
            {
                string Base_Name = reader.ReadString();
                string Table_Name = reader.ReadString();
                string index_Name = reader.ReadString();
                string Column_Name = reader.ReadString();
                string Index_Type = reader.ReadString();

                    
                if (Base_Name.Equals(baseName, StringComparison.OrdinalIgnoreCase) && Table_Name.Equals(tableName, StringComparison.OrdinalIgnoreCase) && Column_Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return index_Name; 
                }
            }
        }

        return null; 
    }

    
    public string GetIndex(string baseName, string tableName, string columnName)
    {
        if (IndexesByColumns.TryGetValue(columnName, out string indexName))
        {
            Console.WriteLine($"Índice asociado encontrado: {indexName}");
            return indexName;
        }
        else
        {
            Console.WriteLine($"No se encontró un índice para la columna solicitada en {tableName}.");
            return null;
        }
    }

    
    public List<Dictionary<string, object>> GetRecords(string indexName)
    {
        if (IndexTrees.TryGetValue(indexName, out object tree))
        {
            
            switch (tree)
            {
                case BinarySearchTree<int> bstInt:
                    return bstInt.GetAllRecords();

                case BinarySearchTree<string> bstString:
                    return bstString.GetAllRecords();

                case BinarySearchTree<double> bstDouble:
                    return bstDouble.GetAllRecords();

                case BinarySearchTree<DateTime> bstDateTime:
                    return bstDateTime.GetAllRecords();

                case BTree<int> btreeInt:
                    return btreeInt.GetAllRecords();

                case BTree<string> btreeString:
                    return btreeString.GetAllRecords();

                case BTree<double> btreeDouble:
                    return btreeDouble.GetAllRecords();

                case BTree<DateTime> btreeDateTime:
                    return btreeDateTime.GetAllRecords();

                default:
                    Console.WriteLine("Tipo no soportado.");
                    return null;
            }
        }
        else
        {
            Console.WriteLine("El índice no existe.");
            return null;
        }
    }
}
