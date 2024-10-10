﻿using Entities;
using System.ComponentModel.DataAnnotations;
using ApiInterface.Indexes;
using StoreDataManager;
using System.Data.Common;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using QueryProcessor.Parser;
using DataType = Entities.DataType;
using ApiInterface;

namespace StoreDataManager
{
    public class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();

        private DatabaseManager databaseManager = new();
        private AuxInsert auxInsert = new();
        public AuxSel auxSel = new();

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
        public const string DatabaseBasePath = @"C:\TinySql\";
        public const string DataPath = $@"{DatabaseBasePath}\Data";
        public const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        public const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        public const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";
        public const string SystemColumnsFile = $@"{SystemCatalogPath}\SystemColumns.table";
        public const string SystemIndexesFile = $@"{SystemCatalogPath}\SystemIndexes.table";


        //Manejo de indices
        //public Dictionary<string, object> IndexTrees = new Dictionary<string, object>();
        //public Dictionary<string, string> IndexesByColumns = new Dictionary<string, string>();
        //public List<string> IndexedDatabases = new List<string>();
        //public List<string> IndexedTables = new List<string>();
        //public List<string> IndexedColumns = new List<string>();

        //Manejo de bases de datos
        public string CurrentDatabasePath = string.Empty;
        public string CurrentDatabaseName = string.Empty;

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

        public OperationStatus SetDatabase(string toSet)
        {
            string DataBasePath = $@"{DataPath}\{toSet}";

            if (Directory.Exists(DataBasePath))
            {
                Console.WriteLine($"Establecido en {toSet} ");
                this.CurrentDatabasePath = DataBasePath;
                this.CurrentDatabaseName = toSet;
                this.databaseManager.CurrentDatabaseName = DataBasePath;
                this.databaseManager.CurrentDatabasePath = toSet;
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
                AddTable(TableName);
                AddColumn(TableName, TableColumns);


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

        private void AddTable(string tableName)
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
                    RemoveTable(TableToDrop);
                    RemoveColumns(TableToDrop);
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

        private void RemoveTable(string TableToDrop)
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

        private void AddColumn(string TableName, List<Column> Columns)
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

        private void RemoveColumns(string tableName)
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
                Console.WriteLine("La base no existe.");
                return OperationStatus.Error;
            }

            // Verifica que la tabla exista
            if (!GetTables(CurrentDatabaseName).Contains(tableName))
            {
                Console.WriteLine($"La tabla solicitada no existe en la base especificada'.");
                return OperationStatus.Error;
            }

            // Obtiene las columnas de la tabla
            List<Column> columns = GetColumns(CurrentDatabaseName, tableName);

            // Valida el número de valores
            if (values.Count != columns.Count)
            {
                Console.WriteLine("No existen coincidencias.");
                return OperationStatus.Error;
            }

            
            var converted = new List<object>();
            for (int i = 0; i < values.Count; i++)
            {
                string valueStr = values[i];
                Column column = columns[i];

                try
                {
                    var convertedValue = auxInsert.Convert(valueStr, column.DataType, column.MaxSize);
                    converted.Add(convertedValue);

                    // Verifica duplicados en columnas indexadas
                    if (databaseManager.GetName(CurrentDatabaseName, tableName, column.Name) != null)
                    {
                        var columnData = databaseManager.GetColumn(CurrentDatabaseName, tableName, column.Name);
                        if (columnData.Contains(convertedValue))
                        {
                            Console.WriteLine($"El valor ya existe en la columna.");
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
                    foreach (var value in converted)
                    {
                        
                        auxInsert.Write(writer, value);
                    }
                }

                Console.WriteLine("La inserción fue exitosa.");
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

        //Funcion para la creacion y manejo de indices

        public OperationStatus CreateIndex(string indexName, string tableName, string columnName, string indexType)
        {
            
            if (string.IsNullOrEmpty(CurrentDatabaseName))
            {
                Console.WriteLine("No se ha creado una base .");
                return OperationStatus.Error;
            }

           
            var tables = GetTables(CurrentDatabaseName);
            if (!tables.Contains(tableName, StringComparer.OrdinalIgnoreCase))
            {
                Console.WriteLine($"La tabla solicitada no existe en la base especificada.");
                return OperationStatus.Error;
            }

            
            if (!indexType.Equals("BST", StringComparison.OrdinalIgnoreCase) &&
                !indexType.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"El tipo solicitado no es válido. Los comandos permitidos son 'BST' o 'BTREE'.");
                return OperationStatus.Error;
            }

            
            var allColumns = GetColumns(CurrentDatabaseName, tableName);

            
            var column = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (column == null)
            {
                Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
                return OperationStatus.Error;
            }

            
            var existingIndex = databaseManager.GetIndex(CurrentDatabaseName, tableName, columnName);
            if (existingIndex != null)
            {
                Console.WriteLine($"Ya existe un índice asociado a la columna '{columnName}' en la tabla '{tableName}'. Índice existente: {existingIndex}");
                return OperationStatus.Error;
            }

            // Obtener los datos de la columna
            var columnData = databaseManager.GetColumn(CurrentDatabaseName, tableName, columnName);

            // Verificar si hay datos duplicados
            if (columnData.Count != columnData.Distinct().Count())
            {
                Console.WriteLine($"La columna '{columnName}' contiene datos duplicados. No se puede crear el índice.");
                return OperationStatus.Error; // Retornar error si se encuentran datos duplicados
            }

            // Si no hay duplicados, proceder a crear el índice
            using (var stream = File.Open(SystemIndexesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End); // Mover el puntero al final para agregar nuevo índice

                using (var writer = new BinaryWriter(stream))
                {
                    writer.Write(CurrentDatabaseName);
                    writer.Write(tableName);
                    writer.Write(indexName);
                    writer.Write(columnName);
                    writer.Write(indexType);
                }
            }

            Console.WriteLine($"Índice '{indexName}' creado exitosamente para la columna '{columnName}' en la tabla '{tableName}'.");

            return OperationStatus.Success;
        }

        //REVISAR A PARTIR DE AQUI
        //Función para seleccionar datos

        public OperationStatus Select(string tableName, List<string> columnsToSelect, string whereClause, string orderByColumn, string orderByDirection, out object? data)
        {
            data = null;
            string mode = "DEFAULT";

            
            if (string.IsNullOrEmpty(CurrentDatabaseName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return OperationStatus.Error;
            }

            
            List<string> tables = GetTables(CurrentDatabaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{CurrentDatabaseName}'.");
                return OperationStatus.Error;
            }

            
            List<Column> allColumns = GetColumns(CurrentDatabaseName, tableName);

            
            List<Column> selectedColumns;
            if (columnsToSelect == null || columnsToSelect.Count == 0)
            {
                // Seleccionar todas las columnas
                selectedColumns = allColumns;
            }
            else
            {
                
                selectedColumns = new List<Column>();
                foreach (var colName in columnsToSelect)
                {
                    var col = allColumns.FirstOrDefault(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                    if (col == null)
                    {
                        Console.WriteLine($"La columna '{colName}' no existe en la tabla '{tableName}'.");
                        return OperationStatus.Error;
                    }
                    selectedColumns.Add(col);
                }
            }

            var records = new List<Dictionary<string, object>>();

           
            if (databaseManager.IndexedDatabases.Contains(CurrentDatabaseName) && databaseManager.IndexedTables.Contains(tableName))
            {
                foreach (var col in selectedColumns)
                {
                    string? indexName = databaseManager.GetIndex(CurrentDatabaseName, tableName, col.Name);
                    if (indexName != null)
                    {
                        records = databaseManager.GetRecords(indexName);
                        Console.WriteLine("USANDO ÍNDICES EN MEMORIA PARA ESTE REQUEST");
                        break; // Utiliza solo el primer índice encontrado
                    }
                }
            }

            if (records == null || records.Count == 0)
            {
                Console.WriteLine("No se encontraron índices asociados. Leyendo toda la tabla.");
                auxSel.CurrentDatabasePath = CurrentDatabasePath;
                auxSel.CurrentDatabaseName = CurrentDatabaseName;
                records = auxSel.GetRecords(tableName, allColumns);
            }

            if (records == null)
            {
                Console.WriteLine("No se pudieron leer los registros de la tabla.");
                return OperationStatus.Error;
            }

            // Aplicar cláusula WHERE si es necesario
            if (!string.IsNullOrEmpty(whereClause))
            {
                records = auxSel.FilteredByWhere(records, whereClause, tableName, allColumns, mode, null, null);
                if (records == null)
                {
                    Console.WriteLine("Error al aplicar la cláusula WHERE.");
                    return OperationStatus.Error;
                }
            }

            // Ordenar los registros si hay cláusula ORDER BY
            if (!string.IsNullOrEmpty(orderByColumn))
            {
                var orderColumn = allColumns.FirstOrDefault(c => c.Name.Equals(orderByColumn, StringComparison.OrdinalIgnoreCase));
                if (orderColumn == null)
                {
                    Console.WriteLine($"La columna '{orderByColumn}' no existe en la tabla '{tableName}'.");
                    return OperationStatus.Error;
                }

                records = auxSel.FilteredByOrder(records, orderByColumn, orderByDirection, tableName, allColumns);
                if (records == null)
                {
                    Console.WriteLine("Error al aplicar el ordenamiento.");
                    return OperationStatus.Error;
                }
            }

            // Filtrar los registros seleccionados
            var filteredRecords = records.Select(record =>
            {
                var filteredRecord = new Dictionary<string, object>();
                foreach (var column in selectedColumns)
                {
                    filteredRecord[column.Name] = record[column.Name];
                }
                return filteredRecord;
            }).ToList();

            // Asignar los datos al parámetro de salida
            data = new
            {
                Columns = selectedColumns.Select(c => c.Name).ToList(),
                Rows = filteredRecords
            };

            // Mostrar los registros en formato de tabla
            auxSel.PrintRecords(selectedColumns, records);

            return OperationStatus.Success;
        }

        // Función para actualizar datos
        public OperationStatus Update(string tableName, string columnName, string newValue, string whereClause)
        {
            // Verificar que la base de datos está establecida
            if (string.IsNullOrEmpty(CurrentDatabaseName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return OperationStatus.Error;
            }

            // Verificar que la tabla existe
            List<string> tables = GetTables(CurrentDatabaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{CurrentDatabaseName}'.");
                return OperationStatus.Error;
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumns(CurrentDatabaseName, tableName);
            List<Dictionary<string, object>> records = auxSel.GetRecords(tableName, allColumns);
            string mode = "UPDATE";
            object convertedValue;

            if (records == null)
            {
                Console.WriteLine("No se pudieron leer los registros de la tabla.");
                return OperationStatus.Error;
            }

            // Validar que la columna a actualizar existe
            var targetColumn = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (targetColumn == null)
            {
                Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
                return OperationStatus.Error;
            }

            try
            {
                convertedValue = auxInsert.Convert(newValue, targetColumn.DataType, targetColumn.MaxSize);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al convertir el valor '{newValue}' para la columna '{columnName}': {ex.Message}");
                return OperationStatus.Error;
            }

            // Leer los registros de la tabla
            string tablePath = Path.Combine(CurrentDatabasePath, $"{tableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"El archivo de la tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            // Aplicar la cláusula WHERE si existe
            if (!string.IsNullOrEmpty(whereClause))
            {
                records = auxSel.FilteredByWhere(records, whereClause, tableName, allColumns, mode, columnName, convertedValue);
                if (records == null)
                {
                    return OperationStatus.Error;
                }
            }
            else
            {
                // Si no hay WHERE, actualizamos todos los registros
                foreach (var record in records)
                {
                    record[columnName] = convertedValue;
                }
            }

            try
            {
                using (FileStream fs = new FileStream(tablePath, FileMode.Create, FileAccess.Write))
                using (BinaryWriter writer = new BinaryWriter(fs))
                {
                    foreach (var record in records)
                    {
                        foreach (var column in allColumns)
                        {
                            object value = record[column.Name];
                            auxInsert.Write(writer, value);
                        }
                    }
                }

                Console.WriteLine("Valores actualizados correctamente.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al escribir en la tabla: {ex.Message}");
                return OperationStatus.Error;
            }

            // Mantener el estado de la base de datos después de actualizar índices
            string SettedDBNAME = CurrentDatabaseName;
            string SettedDBPATH = CurrentDatabasePath;

            // Regenerar los índices
            IndexGenerator indexGenerator = new IndexGenerator();
            indexGenerator.RegenerateIndexes();

            // Restaurar el estado de la base de datos
            this.CurrentDatabaseName = SettedDBPATH;
            this.CurrentDatabasePath = SettedDBNAME;

            return OperationStatus.Success;
        }

        //Función para eliminar datos

        public OperationStatus Delete(string tableName, string whereClause)
        {
            string mode = "DEFAULT";

            // Verificar que la base de datos está establecida
            if (string.IsNullOrEmpty(CurrentDatabaseName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return OperationStatus.Error;
            }

            // Verificar que la tabla existe
            List<string> tables = GetTables(CurrentDatabaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{CurrentDatabaseName}'.");
                return OperationStatus.Error;
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumns(CurrentDatabaseName, tableName);
            List<Dictionary<string, object>> records = auxSel.GetRecords(tableName, allColumns);
            List<Dictionary<string, object>> recordsToDelete = new List<Dictionary<string, object>>();

            if (records == null)
            {
                Console.WriteLine("No se pudieron leer los registros de la tabla.");
                return OperationStatus.Error;
            }

            // Filtrar registros si hay cláusula WHERE
            if (!string.IsNullOrEmpty(whereClause))
            {
                recordsToDelete = auxSel.FilteredByWhere(records, whereClause, tableName, allColumns, mode, null, null);
                if (records == null)
                {
                    return OperationStatus.Error;
                }
            }

            // Eliminar registros seleccionados
            records = records.Except(recordsToDelete).ToList();

            string tablePath = Path.Combine(CurrentDatabasePath, $"{tableName}.table");

            try
            {
                using (FileStream fs = new FileStream(tablePath, FileMode.Create, FileAccess.Write))
                using (BinaryWriter writer = new BinaryWriter(fs))
                {
                    foreach (var record in records)
                    {
                        foreach (var column in allColumns)
                        {
                            object value = record[column.Name];
                            auxInsert.Write(writer, value);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al escribir en la tabla: {ex.Message}");
                return OperationStatus.Error;
            }

            // Mantener el estado de la base de datos tras la actualización de índices
            string CurrentDBNAME = CurrentDatabaseName;
            string CurrentDBPATH = CurrentDatabasePath;

            // Regenerar índices
            IndexGenerator indexGenerator = new IndexGenerator();
            indexGenerator.RegenerateIndexes();

            // Restaurar el estado de la base de datos
            this.CurrentDatabasePath = CurrentDBPATH;
            this.CurrentDatabaseName = CurrentDBNAME;

            Console.WriteLine("Registros eliminados correctamente.");
            return OperationStatus.Success;
        }




    }
}
