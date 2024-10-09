using Entities;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace StoreDataManager
{
    public sealed class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();
               
        public static Store GetInstance()
        {
            lock(_lock)
            {
                if (instance == null) 
                {
                    instance = new Store();
                }
                return instance;
            }
        }
        // Paths
        private const string DatabaseBasePath = @"C:\TinySql\";
        private const string DataPath = $@"{DatabaseBasePath}\Data";
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";
        private const string SystemColumnsFile = $@"{SystemCatalogPath}\SystemColumns.table";
        private const string SystemIndexesFile = $@"{SystemCatalogPath}\SystemIndexes.table";

        //Manejo de indices
        public Dictionary<string, object> IndexTrees = new Dictionary<string, object>();
        public Dictionary<string, string> IndexesByColumns = new Dictionary<string, string>();
        public List<string> IndexedDatabases = new List<string>();
        public List<string> IndexedTables = new List<string>();
        public List<string> IndexedColumns = new List<string>();

        //Manejo de bases de datos
        private string CurrentDatabasePath = string.Empty;
        private string CurrentDatabaseName = string.Empty;

        public Store()
        {
            this.InitializeSystemCatalog();
            
        }

        private void InitializeSystemCatalog()
        {
            // Always make sure that the system catalog and above folder
            // exist when initializing
            Directory.CreateDirectory(SystemCatalogPath);
        }

        //Funciones para la creacion y manejo de las bases de datos

        public OperationStatus CreateDataBase(string CreateDataBaseName)
        {
            //En caso de que la base ya exista
            if (Directory.Exists($@"{DataPath}\{CreateDataBaseName}"))
            {
                Console.WriteLine("La base ya existe");
                return OperationStatus.Error;
            }

            //Si la base no existe entonces la crea
            Directory.CreateDirectory($@"{DataPath}\{CreateDataBaseName}");

            //Se agrega la base al SystemCatalog
            AddBaseToSystem(CreateDataBaseName);

            Console.WriteLine("Base de datos creada correctamente");

            return OperationStatus.Success;
        }

        public OperationStatus SetDatabase(string DataBaseToSet)
        {
            string DataBasePath = $@"{DataPath}\{DataBaseToSet}";

            if (Directory.Exists(DataBasePath))
            {
                Console.WriteLine($"Establecido en {DataBaseToSet} ");
                this.CurrentDatabasePath = DataBasePath;
                this.CurrentDatabaseName = DataBaseToSet;
                Console.WriteLine($"Ruta {DataBasePath}");
                return OperationStatus.Success;

            }
            else
            {
                Console.WriteLine("La base de datos no existe");
                return OperationStatus.Error;
            }
        }

        public void AddBaseToSystem(string DataBaseName)
        {
            
            using (FileStream stream = File.Open(SystemDatabasesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                // Posiciona el puntero al final del archivo para añadir el nombre
                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(DataBaseName);
                }
            }
        }

        public List<string> GetDataBases()
        {
            try
            {
                using (FileStream stream = new FileStream(SystemDatabasesFile, FileMode.Open, FileAccess.Read))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    List<string> Bases = new List<string>();

                    // Leer todos los nombres de bases de datos
                    while (stream.Position < stream.Length)
                    {
                        string databaseName = reader.ReadString();
                        Bases.Add(databaseName);
                    }

                    return Bases;
                }
            }
            catch (FileNotFoundException)
            {
                // Si el archivo no existe entonces se retorna una lista vacía
                return new List<string>();
            }
            catch (IOException ex)
            {
                
                Console.WriteLine($"Error al leer el archivo: {ex.Message}");
                return new List<string>();
            }
        }

        //Funciones para la creacion y manejo de las tablas

        public OperationStatus CreateTable(string TableName, List<Column> TableColumns)
        {
            // Verificar si hay una base de datos activa
            if (!IsDatabaseSet())
            {
                Console.WriteLine("No existe una base de datos establecida");
                return OperationStatus.Error;
            }

            
            string tablePath = Path.Combine(CurrentDatabasePath, $"{TableName}.table");

            // Verificar si la tabla ya existe
            if (File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla '{TableName}' ya existe en: '{CurrentDatabasePath}'.");
                return OperationStatus.Error; 
            }

            try
            {
               
                using (FileStream stream = File.Open(tablePath, FileMode.Create))


                // Agregar la tabla y las columnas al SystemCatalog
                AddTableToSystem(TableName);
                AddColumnToSystem(TableName, TableColumns);


                Console.WriteLine($"Tabla '{TableName}' creada exitosamente en: '{CurrentDatabaseName}'.");
                return OperationStatus.Success;
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al crear la tabla '{TableName}': {ex.Message}");
                return OperationStatus.Error;
            }
        }

        // Verifica si la base de datos está activa
        private bool IsDatabaseSet()
        {
            return !string.IsNullOrEmpty(CurrentDatabasePath);
        }

        private void AddTableToSystem(string tableName)
        {
            try
            {
                using (FileStream stream = File.Open(SystemTablesFile, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    
                    stream.Seek(0, SeekOrigin.End);

                    using (BinaryWriter writer = new BinaryWriter(stream))
                    {
                        
                        writer.Write(CurrentDatabaseName);
                        writer.Write(tableName);
                    }
                }
            }
            catch (IOException ex)
            {
                
                Console.WriteLine($"Error al agregar '{tableName}' al SystemCatalog: {ex.Message}");
            }
        }

        public List<string> GetTables(string databaseName)
        {
            // Almacena los nombres de las tablas
            List<string> tables = new List<string>();

            
            if (!File.Exists(SystemTablesFile))
            {
                return tables; 
            }

            try
            {
                using (FileStream stream = new FileStream(SystemTablesFile, FileMode.Open, FileAccess.Read))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    while (stream.Position < stream.Length)
                    {
                        string baseName = reader.ReadString(); 
                        string tableName = reader.ReadString(); 

                        
                        if (baseName.Equals(databaseName, StringComparison.OrdinalIgnoreCase))
                        {
                            tables.Add(tableName);
                        }
                    }
                }
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al leer las tablas existentes: {ex.Message}");
            }

            return tables; 
        }

        public OperationStatus DropTable(string TableToDrop)
        {            
            if (string.IsNullOrEmpty(CurrentDatabasePath))
            {
                Console.WriteLine("No hay una base de datos establecida.");
                return OperationStatus.Error;
            }

            string tablePath = $@"{CurrentDatabasePath}\{TableToDrop}";

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla '{TableToDrop}' no existe.");
                return OperationStatus.Error;
            }

            try
            {
                // Verifica que la tabla está vacía
                if (new FileInfo(tablePath).Length == 0)
                {
                    //Si esta vacia procede a eliminarla
                    Console.WriteLine("La tabla vacía, procediendo a su eliminación.");
                    File.Delete(tablePath);
                    RemoveTableFromSystem(TableToDrop);
                    RemoveColumnsFromSystem(TableToDrop);
                    Console.WriteLine($"'{TableToDrop}' ha sido eliminada exitosamente.");
                    return OperationStatus.Success;
                }
                else
                {
                    Console.WriteLine("La tabla no está vacía, no es posible eliminarla.");
                    return OperationStatus.Error;
                }
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al eliminar la tabla: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private void RemoveTableFromSystem(string TableToDrop)
        {
            string tempPath = $@"{SystemCatalogPath}\SystemTables_Temp.table";
                        
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }

            try
            {
                using (FileStream fs = new FileStream(SystemTablesFile, FileMode.Open, FileAccess.Read))
                using (FileStream fsTemp = new FileStream(tempPath, FileMode.Create, FileAccess.Write))
                using (BinaryReader reader = new BinaryReader(fs))
                using (BinaryWriter writer = new BinaryWriter(fsTemp))
                {
                    while (fs.Position < fs.Length)
                    {
                        string baseName = reader.ReadString();
                        string tableName = reader.ReadString();
                                                
                        if (!(baseName == CurrentDatabaseName && tableName == TableToDrop))
                        {
                            writer.Write(baseName);
                            writer.Write(tableName);
                        }
                    }
                }

                File.Delete(SystemTablesFile);
                File.Move(tempPath, SystemTablesFile);
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al eliminar la tabla: {ex.Message}");
            }
        }

        //Funciones para la creacion y manejo de columnas

        private void AddColumnToSystem(string TableName, List<Column> Columns)
        {
            // Valida que la lista de columnas no esté vacía
            if (Columns == null || Columns.Count == 0)
            {
                Console.WriteLine("No hay columnas para agregar.");
                return;
            }

            try
            {
                using (FileStream stream = File.Open(SystemColumnsFile, FileMode.OpenOrCreate, FileAccess.Write))
                {
                    stream.Seek(0, SeekOrigin.End);

                    using (BinaryWriter writer = new(stream))
                    {
                        foreach (Column column in Columns)
                        {
                            writer.Write(CurrentDatabaseName);
                            writer.Write(TableName);
                            writer.Write(column.Name);
                            writer.Write(column.DataType.ToString());
                            writer.Write(column.MaxSize.HasValue ? column.MaxSize.Value : 0); 
                        }
                    }
                }

                Console.WriteLine($"Se han agregado {Columns.Count} columnas a la tabla '{TableName}' en '{CurrentDatabaseName}'.");
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al agregar columnas: {ex.Message}");
            }
        }

        public List<Column> GetColumns(string databaseName, string tableName)
        {
            List<Column> columns = new List<Column>();

            // Valida que los nombres no sean nulos o vacíos
            if (string.IsNullOrEmpty(databaseName) || string.IsNullOrEmpty(tableName))
            {
                Console.WriteLine("El nombre de base de datos o tabla no puede ser nulo o vacío.");
                return columns;
            }

            if (!File.Exists(SystemColumnsFile))
            {
                Console.WriteLine("El archivo de columnas no existe en el SystemCatalog.");
                return columns;
            }

            try
            {
                using (FileStream stream = new FileStream(SystemColumnsFile, FileMode.Open, FileAccess.Read))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    while (stream.Position < stream.Length)
                    {
                        string baseName = reader.ReadString();
                        string tableName_ = reader.ReadString();
                        string columnName = reader.ReadString();
                        string dataTypeStr = reader.ReadString();
                        int maxSize = reader.ReadInt32();

                        // Filtra por base de datos y tabla
                        if (baseName == databaseName && tableName_ == tableName)
                        {
                            // Usar Enum.TryParse para manejar posibles errores
                            if (Enum.TryParse<DataType>(dataTypeStr, out DataType dataType))
                            {
                                Column column = new Column
                                {
                                    Name = columnName,
                                    DataType = dataType,
                                    MaxSize = maxSize > 0 ? maxSize : (int?)null
                                };
                                columns.Add(column);
                            }
                            else
                            {
                                Console.WriteLine($"Tipo de dato: {dataTypeStr} no válido para la columna {columnName}");
                            }
                        }
                    }
                }
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al leer el archivo: {ex.Message}");
            }

            return columns;
        }

        private void RemoveColumnsFromSystem(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                Console.WriteLine("El nombre de la tabla no puede ser nulo o vacío.");
                return;
            }

            string tempPath = $@"{SystemCatalogPath}\SystemColumns_Temp.table";

            try
            {
                using (FileStream fs = new FileStream(SystemColumnsFile, FileMode.OpenOrCreate, FileAccess.Read))
                using (FileStream fsTemp = new FileStream(tempPath, FileMode.Create, FileAccess.Write))
                using (BinaryReader reader = new BinaryReader(fs))
                using (BinaryWriter writer = new BinaryWriter(fsTemp))
                {
                    while (fs.Position < fs.Length)
                    {
                        string baseName = reader.ReadString();
                        string tableName_ = reader.ReadString();
                        string columnName = reader.ReadString();
                        string dataTypeStr = reader.ReadString();
                        int maxSize = reader.ReadInt32();

                        if (!(baseName == CurrentDatabaseName && tableName_ == tableName))
                        {
                            writer.Write(baseName);
                            writer.Write(tableName_);
                            writer.Write(columnName);
                            writer.Write(dataTypeStr);
                            writer.Write(maxSize);
                        }
                    }
                }

                File.Delete(SystemColumnsFile);
                File.Move(tempPath, SystemColumnsFile);
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al eliminar columnas de la tabla: {ex.Message}");
            }
            finally
            {
                if (File.Exists(tempPath))
                {
                    File.Delete(tempPath);
                }
            }
        }

        public OperationStatus Insert(string tableName, List<string> values)
        {
            if (string.IsNullOrEmpty(CurrentDatabaseName))
            {
                Console.WriteLine("La base de datos no existe.");
                return OperationStatus.Error;
            }

            // Verifica que la tabla exista
            if (!GetTables(CurrentDatabaseName).Contains(tableName))
            {
                Console.WriteLine($"La tabla: '{tableName}', no existe en: '{CurrentDatabaseName}'.");
                return OperationStatus.Error;
            }

            // Obtiene las columnas de la tabla
            List<Column> columns = GetColumns(CurrentDatabaseName, tableName);

            // Valida el número de valores
            if (values.Count != columns.Count)
            {
                Console.WriteLine("El número de valores proporcionados no coincide con el número de columnas.");
                return OperationStatus.Error;
            }

            // Valida y convierte los valores
            var convertedValues = new List<object>();
            for (int i = 0; i < values.Count; i++)
            {
                string valueStr = values[i];
                Column column = columns[i];

                try
                {
                    var convertedValue = ConvertValue(valueStr, column.DataType, column.MaxSize);
                    convertedValues.Add(convertedValue);

                    // Verifica duplicados en columnas indexadas
                    if (GetIndexNameIfExist(CurrentDatabaseName, tableName, column.Name) != null)
                    {
                        var columnData = GetColumnData(CurrentDatabaseName, tableName, column.Name);
                        if (columnData.Contains(convertedValue))
                        {
                            Console.WriteLine($"El valor '{convertedValue}' ya existe en la columna '{column.Name}' con un índice asociado.");
                            return OperationStatus.Error;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al convertir el valor '{valueStr}' para la columna '{column.Name}': {ex.Message}");
                    return OperationStatus.Error;
                }
            }

            string tablePath = Path.Combine(CurrentDatabasePath, $"{tableName}.table");

            try
            {
                using (FileStream fs = new FileStream(tablePath, FileMode.Append, FileAccess.Write))
                using (BinaryWriter writer = new BinaryWriter(fs))
                {
                    foreach (var value in convertedValues)
                    {
                        WriteValue(writer, value);
                    }
                }

                Console.WriteLine("Valores insertados correctamente.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al insertar los valores en la tabla '{tableName}': {ex.Message}");
                return OperationStatus.Error;
            }

            // Regenera los índices si la inserción fue exitosa
            try
            {
                var indexGenerator = new IndexGenerator();
                indexGenerator.RegenerateIndexes();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al regenerar los índices: {ex.Message}");
                return OperationStatus.Error;
            }

            return OperationStatus.Success;
        }








    }
}
