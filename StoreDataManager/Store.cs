using ApiInterface;
using ApiInterface.Indexes;
using Entities;
using QueryProcessor.Parser;
using System.Text.RegularExpressions;
using DataType = Entities.DataType;

namespace StoreDataManager
{
    public class Store
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
        public const string DatabaseBasePath = @"C:\TinySql\";
        public const string DataPath = $@"{DatabaseBasePath}\Data";
        public const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        public const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        public const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";
        public const string SystemColumnsFile = $@"{SystemCatalogPath}\SystemColumns.table";
        public const string SystemIndexesFile = $@"{SystemCatalogPath}\SystemIndexes.table";


        //Manejo de indices
        public Dictionary<string, object> IndexTrees = new Dictionary<string, object>();
        public Dictionary<string, string> IndexesByColumns = new Dictionary<string, string>();
        public List<string> IndexedDatabases = new List<string>();
        public List<string> IndexedTables = new List<string>();
        public List<string> IndexedColumns = new List<string>();

        //Manejo de bases de datos
        public string CurrentPath = string.Empty;
        public string CurrentName = string.Empty;

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

        //Funciones para el manejo de la base de datos

        public OperationStatus CreateDataBase(string CreateDataBaseName)
        {
            // Creates a default DB called TESTDB
            if (Directory.Exists($@"{DataPath}\{CreateDataBaseName}"))
            {
                Console.WriteLine("La base ya existe");
                return OperationStatus.Error;
            }

            Directory.CreateDirectory($@"{DataPath}\{CreateDataBaseName}");

            AddDataBase(CreateDataBaseName);

            Console.WriteLine("Base de datos creada correctamente");

            return OperationStatus.Success;
        }

        public OperationStatus SetDatabase(string SetDataBaseName)
        {
            string DataBasePath = $@"{DataPath}\{SetDataBaseName}";

            if (Directory.Exists(DataBasePath))
            {
                Console.WriteLine("Establecido en la base");
                this.CurrentPath = DataBasePath;
                this.CurrentName = SetDataBaseName;
                Console.WriteLine($"Actualmente en: {DataBasePath}");
                return OperationStatus.Success;

            }
            else
            {
                Console.WriteLine("La base especificada no ha sido creada");
                return OperationStatus.Error;
            }
        }

        public void AddDataBase(string DataBaseName)
        {
            using (FileStream stream = File.Open(SystemDatabasesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {

                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(DataBaseName);
                }
            }
        }

        public List<string> GetDataBases()
        {

            List<string> databases = new List<string>();


            if (!File.Exists(SystemDatabasesFile))
            {
                return databases;
            }

            using (FileStream stream = new FileStream(SystemDatabasesFile, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new(stream))
            {
                while (stream.Position < stream.Length)
                {
                    string databaseName = reader.ReadString();
                    databases.Add(databaseName);
                }
            }

            return databases;
        }




        //Funciones para el manejo de las tablas

        public OperationStatus CreateTable(string TableName, List<Column> TableColumns)
        {

            if (string.IsNullOrEmpty(CurrentPath))
            {
                Console.WriteLine("No existe una base establecida");
                return OperationStatus.Error;
            }

            string tablePath = $@"{CurrentPath}\{TableName}.table";

            if (File.Exists(tablePath))
            {
                Console.WriteLine("Tabla ya existente en otra base");
                return OperationStatus.Error;
            }

            using (FileStream stream = File.Open(tablePath, FileMode.Create))


                // Agrega la tabla al SystemCatalog
                AddTable(TableName);
            AddColums(TableName, TableColumns);

            Console.WriteLine("La tabla fue creada.");
            return OperationStatus.Success;
        }

        private void AddTable(string TableName)
        {
            using (FileStream stream = File.Open(SystemTablesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(CurrentName);
                    writer.Write(TableName);
                }
            }
        }

        public List<string> GetTables(string BaseName)
        {
            List<string> tables = new List<string>();


            if (!File.Exists(SystemTablesFile))
            {
                return tables;
            }

            using (FileStream stream = new FileStream(SystemTablesFile, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new(stream))
            {
                while (stream.Position < stream.Length)
                {
                    string dbName = reader.ReadString();
                    string tableName = reader.ReadString();

                    if (dbName == BaseName)
                    {
                        tables.Add(tableName);
                    }
                }
            }

            return tables;
        }

        public OperationStatus Drop(string TableToDrop)
        {

            if (string.IsNullOrEmpty(CurrentPath))
            {
                Console.WriteLine("No existe una base establecida");
                return OperationStatus.Error;
            }

            string tablePath = $@"{CurrentPath}\{TableToDrop}";

            if (File.Exists(tablePath))
            {

                string[] TableContent = File.ReadAllLines(tablePath);


                if (TableContent.Length == 0)
                {
                    Console.WriteLine("Procediendo a eliminar tabla");
                    File.Delete(tablePath);
                }
                else
                {
                    Console.WriteLine("No es posible eliminar una tabla con contenido.");
                    return OperationStatus.Error;
                }
            }

            else
            {
                Console.WriteLine($"La tabla solicitada no existe.");
                return OperationStatus.Error;
            }

            //Elimina las referencias del SystemCatalog
            RemoveTable(TableToDrop);

            RemoveColumns(TableToDrop);

            return OperationStatus.Success;
        }

        private void RemoveTable(string TableToDrop)
        {
            string tempPath = $@"{SystemCatalogPath}\SystemTables_Temp.table";
            using (FileStream fs = new FileStream(SystemTablesFile, FileMode.OpenOrCreate, FileAccess.Read))
            using (FileStream fsTemp = new FileStream(tempPath, FileMode.OpenOrCreate, FileAccess.Write))
            using (BinaryReader reader = new BinaryReader(fs))
            using (BinaryWriter writer = new BinaryWriter(fsTemp))
            {
                while (fs.Position < fs.Length)
                {
                    string dbName = reader.ReadString();
                    string tblName = reader.ReadString();

                    if (!(dbName == CurrentName && tblName == TableToDrop))
                    {
                        writer.Write(dbName);
                        writer.Write(tblName);
                    }
                }
            }
            File.Delete(SystemTablesFile);
            File.Move(tempPath, SystemTablesFile);
        }


        //Funciones para el manejo de las columnas

        private void AddColums(string TableName, List<Column> Columns)
        {
            using (FileStream stream = File.Open(SystemColumnsFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new(stream))
                {
                    foreach (Column Column in Columns)
                    {
                        writer.Write(CurrentName);
                        writer.Write(TableName);
                        writer.Write(Column.Name);
                        writer.Write(Column.DataType.ToString());
                        writer.Write(Column.MaxSize.HasValue ? Column.MaxSize.Value : 0);
                    }
                }
            }
        }

        public List<Column> GetColumns(string BaseName, string TableName)
        {

            List<Column> columns = new List<Column>();

            if (!File.Exists(SystemColumnsFile))
            {
                return columns;
            }

            using (FileStream stream = new FileStream(SystemColumnsFile, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                while (stream.Position < stream.Length)
                {
                    string dbName = reader.ReadString();
                    string tblName = reader.ReadString();
                    string columnName = reader.ReadString();
                    string dataTypeStr = reader.ReadString();
                    int maxSize = reader.ReadInt32();

                    if (dbName == BaseName && tblName == TableName)
                    {
                        Column column = new Column
                        {
                            Name = columnName,
                            DataType = Enum.Parse<DataType>(dataTypeStr),
                            MaxSize = maxSize > 0 ? maxSize : null
                        };
                        columns.Add(column);
                    }
                }
            }



            return columns;
        }

        private void RemoveColumns(string TableName)
        {
            string tempPath = $@"{SystemCatalogPath}\SystemColumns_Temp.table";
            using (FileStream fs = new FileStream(SystemColumnsFile, FileMode.OpenOrCreate, FileAccess.Read))
            using (FileStream fsTemp = new FileStream(tempPath, FileMode.OpenOrCreate, FileAccess.Write))
            using (BinaryReader reader = new BinaryReader(fs))
            using (BinaryWriter writer = new BinaryWriter(fsTemp))
            {
                while (fs.Position < fs.Length)
                {
                    string dbName = reader.ReadString();
                    string tblName = reader.ReadString();
                    string columnName = reader.ReadString();
                    string dataTypeStr = reader.ReadString();
                    int maxSize = reader.ReadInt32();

                    if (!(dbName == CurrentName && tblName == TableName))
                    {

                        writer.Write(dbName);
                        writer.Write(tblName);
                        writer.Write(columnName);
                        writer.Write(dataTypeStr);
                        writer.Write(maxSize);
                    }
                }
            }


            File.Delete(SystemColumnsFile);
            File.Move(tempPath, SystemColumnsFile);
        }

        //Funcion para insertar valores en una tabla
        public OperationStatus Insert(string TableName, List<string> Values)
        {
            if (string.IsNullOrEmpty(CurrentName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return OperationStatus.Error;
            }



            List<string> tables = GetTables(CurrentName);
            if (!tables.Contains(TableName))
            {
                Console.WriteLine("La tabla solicitada no existe en la base actual'.");
                return OperationStatus.Error;
            }


            List<Column> columns = GetColumns(CurrentName, TableName);

            if (Values.Count != columns.Count)
            {
                Console.WriteLine("El número de valores proporcionados no coincide con el número de columnas.");
                return OperationStatus.Error;
            }


            var convertedValues = new List<object>();
            for (int i = 0; i < Values.Count; i++)
            {
                string valueStr = Values[i];
                Column column = columns[i];
                object convertedValue;

                try
                {
                    convertedValue = Convert(valueStr, column.DataType, column.MaxSize);
                    convertedValues.Add(convertedValue);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al convertir el valor '{valueStr}' para la columna '{column.Name}': {ex.Message}");
                    return OperationStatus.Error;
                }




                string existingIndex = GetNameOfIndex(CurrentName, TableName, column.Name);
                if (existingIndex != null)
                {

                    List<object> columnData = GetData(CurrentName, TableName, column.Name);


                    if (columnData.Contains(convertedValue))
                    {
                        Console.WriteLine($"El valor '{convertedValue}' ya existe en la columna '{column.Name}'. No es posible insertar duplicados.");
                        return OperationStatus.Error; // Retornar error si el valor ya existe en la columna
                    }
                }
            }


            string tablePath = Path.Combine(CurrentPath, $"{TableName}.table");

            try
            {
                using (FileStream fs = new FileStream(tablePath, FileMode.Append, FileAccess.Write))
                using (BinaryWriter writer = new BinaryWriter(fs))
                {
                    foreach (var value in convertedValues)
                    {
                        Write(writer, value);
                    }
                }

                Console.WriteLine("Inserción exitosa.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al insertar los valores en la tabla '{TableName}': {ex.Message}");
                return OperationStatus.Error;
            }


            try
            {
                string originalDBName = CurrentName;
                string originalDBPath = CurrentPath;

                Generator indexGenerator = new Generator();
                indexGenerator.RegenerateIndexes();


                this.CurrentName = originalDBName;
                this.CurrentPath = originalDBPath;


            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al regenerar los índices: {ex.Message}");
                return OperationStatus.Error;
            }

            return OperationStatus.Success;
        }

        // Función para crear los indices
        public OperationStatus CreateIndex(string IndexName, string TableName, string ColumnName, string TypeOfIndex)
        {

            if (string.IsNullOrEmpty(CurrentName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return OperationStatus.Error;
            }


            List<string> tables = GetTables(CurrentName);
            if (!tables.Contains(TableName))
            {
                Console.WriteLine($"La tabla '{TableName}' no existe en la base de datos '{CurrentName}'.");
                return OperationStatus.Error;
            }


            if (!TypeOfIndex.Equals("BST", StringComparison.OrdinalIgnoreCase) &&
                !TypeOfIndex.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"El tipo de insice solicitado no es valido, unicamente se soportan indices de tipo 'BST' o 'BTREE'.");
                return OperationStatus.Error;
            }


            List<Column> allColumns = GetColumns(CurrentName, TableName);


            var column = allColumns.FirstOrDefault(c => c.Name.Equals(ColumnName, StringComparison.OrdinalIgnoreCase));
            if (column == null)
            {
                Console.WriteLine("La columna especificada no existe en la tabla.");
                return OperationStatus.Error;
            }


            string existingIndex = GetNameOfIndex(CurrentName, TableName, ColumnName);
            if (existingIndex != null)
            {
                Console.WriteLine($"Ya existe un índice asociado a la columna actual.\n  Nombre del índice: {existingIndex}");
                return OperationStatus.Error;
            }


            List<object> columnData = GetData(CurrentName, TableName, ColumnName);


            if (columnData.Count != columnData.Distinct().Count())
            {
                Console.WriteLine($"Existen datos duplicados en la columna, es imposible crear el indice");
                return OperationStatus.Error;
            }


            using (FileStream stream = File.Open(SystemIndexesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new(stream))
                {
                    writer.Write(CurrentName);
                    writer.Write(TableName);
                    writer.Write(IndexName);
                    writer.Write(ColumnName);
                    writer.Write(TypeOfIndex);
                }
            }

            Console.WriteLine("Creación del índice exitosa.");

            return OperationStatus.Success;
        }


        public OperationStatus Select(string TableName, List<string> ToSelect, string Where, string ColumnOrder, string DirectionOrder, out object? Data)

        {

            Data = null;
            string mode = "DEFAULT";

            if (string.IsNullOrEmpty(CurrentName))
            {
                Console.WriteLine("No existe una base de datos establecida");
                return OperationStatus.Error;
            }


            List<string> tables = GetTables(CurrentName);
            if (!tables.Contains(TableName))
            {
                Console.WriteLine("La tabla especificada no existe en la base actual");
                return OperationStatus.Error;
            }


            List<Column> allColumns = GetColumns(CurrentName, TableName);


            List<Column> selectedColumns;
            if (ToSelect == null)
            {

                selectedColumns = allColumns;
            }
            else
            {

                selectedColumns = new List<Column>();
                foreach (var colName in ToSelect)
                {
                    var col = allColumns.FirstOrDefault(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                    if (col == null)
                    {
                        Console.WriteLine("La columna especificada no existe en la tabla");
                        return OperationStatus.Error;
                    }
                    selectedColumns.Add(col);
                }
            }

            var records = new List<Dictionary<string, object>>();

            if (IndexedDatabases.Contains(CurrentName) && IndexedTables.Contains(TableName))
            {
                foreach (var col in selectedColumns)
                {
                    string? indexName = GetAssociated(CurrentName, TableName, col.Name);
                    if (indexName != null)
                    {
                        records = GetIndexRecord(indexName);

                        break;
                    }
                }
            }


            if (records == null || records.Count == 0)

            {
                Console.WriteLine("No se encontraron indices");
                records = GetTableRecords(TableName, allColumns);
            }


            if (records == null)
            {
                Console.WriteLine("No se pudo leer la tabla.");
                return OperationStatus.Error;
            }

            if (!string.IsNullOrEmpty(Where))
            {
                records = FilteredByWhere(records, Where, TableName, allColumns, mode, null, null);
                if (records == null)
                {
                    return OperationStatus.Error;
                }
            }


            if (!string.IsNullOrEmpty(ColumnOrder))
            {
                records = FilteredByOrder(records, ColumnOrder, DirectionOrder, TableName, allColumns);
                if (records == null)
                {
                    return OperationStatus.Error;
                }
            }

            var filteredRecords = records.Select(record =>
            {
                var filteredRecord = new Dictionary<string, object>();
                foreach (var column in selectedColumns)
                {
                    filteredRecord[column.Name] = record[column.Name];
                }
                return filteredRecord;
            }).ToList();


            Data = new
            {
                Columns = selectedColumns.Select(c => c.Name).ToList(),
                Rows = filteredRecords
            };


            PrintRecords(selectedColumns, records);

            return OperationStatus.Success;
        }

        //Función para actualizar datos de una tabla
        public OperationStatus Update(string TableName, string ColumnName, string InsertedValue, string Where)
        {

            if (string.IsNullOrEmpty(CurrentName))
            {
                Console.WriteLine("No existe una base de datos establecida");
                return OperationStatus.Error;
            }


            List<string> tables = GetTables(CurrentName);
            if (!tables.Contains(TableName))
            {
                Console.WriteLine("La tabla especificada no existe en la base actual");
                return OperationStatus.Error;
            }


            List<Column> allColumns = GetColumns(CurrentName, TableName);
            List<Dictionary<string, object>> records = GetTableRecords(TableName, allColumns);
            string mode = "UPDATE";
            object convertedValue;

            if (records == null)
            {
                Console.WriteLine("No se pudo leer la tabla");
                return OperationStatus.Error;
            }

            // Validar que la columna a actualizar existe
            var targetColumn = allColumns.FirstOrDefault(c => c.Name.Equals(ColumnName, StringComparison.OrdinalIgnoreCase));
            if (targetColumn == null)
            {
                Console.WriteLine($"La columna especificada no existe en la tabla");
                return OperationStatus.Error;
            }

            try
            {
                convertedValue = Convert(InsertedValue, targetColumn.DataType, targetColumn.MaxSize);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al convertir el valor '{InsertedValue}' para la columna '{ColumnName}': {ex.Message}");
                return OperationStatus.Error;
            }


            string tablePath = Path.Combine(CurrentPath, $"{TableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla especificada no existe en la base actual");
                return OperationStatus.Error;
            }

            if (!string.IsNullOrEmpty(Where))
            {
                records = FilteredByWhere(records, Where, TableName, allColumns, mode, ColumnName, convertedValue);
                if (records == null)
                {
                    return OperationStatus.Error;
                }
            }
            else
            {
                foreach (var record in records)
                {
                    record[ColumnName] = convertedValue;
                }
            }

            using (FileStream fs = new FileStream(tablePath, FileMode.Create, FileAccess.Write))
            using (BinaryWriter writer = new BinaryWriter(fs))
            {
                foreach (var record in records)
                {
                    foreach (var column in allColumns)
                    {
                        object value = record[column.Name];
                        Write(writer, value);
                    }
                }
            }

            string SettedDBNAME = CurrentName;
            string SettedDBPATH = CurrentPath;


            Console.WriteLine("Actualización exitosa.");
            Generator indexGenerator = new Generator();


            indexGenerator.RegenerateIndexes();

            this.CurrentPath = SettedDBPATH;
            this.CurrentName = SettedDBNAME;

            return OperationStatus.Success;

        }

        //Función para eliminar datos de una tabla
        public OperationStatus Delete(string TableName, string Where)
        {

            string mode = "DEFAULT";



            if (string.IsNullOrEmpty(CurrentName))
            {
                Console.WriteLine("No existe una base de datos establecida");
                return OperationStatus.Error;
            }

            // Verificar que la tabla existe
            List<string> tables = GetTables(CurrentName);
            if (!tables.Contains(TableName))
            {
                Console.WriteLine($"La tabla especificada no existe en la base actual");
                return OperationStatus.Error;
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumns(CurrentName, TableName);
            List<Dictionary<string, object>> records = GetTableRecords(TableName, allColumns);
            List<Dictionary<string, object>> recordsToDelete = new List<Dictionary<string, object>>();
            if (records == null)
            {
                Console.WriteLine("No se pudo la tabla");
                return OperationStatus.Error;
            }

            if (!string.IsNullOrEmpty(Where))
            {
                recordsToDelete = FilteredByWhere(records, Where, TableName, allColumns, mode, null, null);
                if (records == null)
                {
                    return OperationStatus.Error;
                }
            }

            records = records.Except(recordsToDelete).ToList();

            string tablePath = Path.Combine(CurrentPath, $"{TableName}.table");

            using (FileStream fs = new FileStream(tablePath, FileMode.Create, FileAccess.Write))
            using (BinaryWriter writer = new BinaryWriter(fs))
            {
                foreach (var record in records)
                {
                    foreach (var column in allColumns)
                    {
                        object value = record[column.Name];
                        Write(writer, value);
                    }
                }
            }

            string SettedDBNAME = CurrentName;
            string SettedDBPATH = CurrentPath;

            Generator indexGenerator = new Generator();
            indexGenerator.RegenerateIndexes();

            this.CurrentPath = SettedDBPATH;
            this.CurrentName = SettedDBNAME;

            return OperationStatus.Success;
        }


        private object Convert(string ValueStr, DataType Type, int? Max)
        {
            switch (Type)
            {
                case DataType.INTEGER:
                    if (int.TryParse(ValueStr, out int intValue))
                    {
                        return intValue;
                    }
                    else
                    {
                        throw new Exception("El valor no es un entero válido");
                    }
                case DataType.DOUBLE:
                    if (double.TryParse(ValueStr, out double doubleValue))
                    {
                        return doubleValue;
                    }
                    else
                    {
                        throw new Exception("El valor no es un double válido");
                    }
                case DataType.VARCHAR:
                    ValueStr = ValueStr.Trim('\'', '\"');
                    if (ValueStr.Length > Max)
                    {
                        throw new Exception($"Se está excediendo tamaño máximo. Tamaño maximo es de: {Max}");
                    }
                    return ValueStr;
                case DataType.DATETIME:
                    ValueStr = ValueStr.Trim('\'', '\"');
                    if (DateTime.TryParse(ValueStr, out DateTime dateTimeValue))
                    {
                        return dateTimeValue;
                    }
                    else
                    {
                        throw new Exception("El valor no es una fecha válida");
                    }
                default:
                    throw new Exception("Tipo de dato no soportado");
            }
        }

        private void Write(BinaryWriter Writer, object ValueToWrite)
        {
            if (ValueToWrite is int intValue)
            {
                Writer.Write(intValue);
            }
            else if (ValueToWrite is double doubleValue)
            {
                Writer.Write(doubleValue);
            }
            else if (ValueToWrite is string strValue)
            {
                Writer.Write(strValue);
            }
            else if (ValueToWrite is DateTime dateTimeValue)
            {
                Writer.Write(dateTimeValue.ToBinary()); // Almacenar como ticks
            }
            else
            {
                throw new Exception("Tipo de dato no soportado para escritura.");
            }
        }

        public List<object> GetData(string BaseName, string TableName, string ColumnName)
        {

            string originalName = CurrentName;
            string originalPath = CurrentPath;

            var Data = new List<object>();

            // Verificar que la base de datos existe
            if (string.IsNullOrEmpty(BaseName))
            {
                Console.WriteLine("La base debe de tener un nombre");
                return Data;
            }


            List<string> tables = GetTables(BaseName);
            if (!tables.Contains(TableName))
            {
                Console.WriteLine("La tabla especificada no existe en la base actual");
                return Data;
            }

            List<Column> Columns = GetColumns(BaseName, TableName);

            var TargetColumn = Columns.FirstOrDefault(c => c.Name.Equals(ColumnName, StringComparison.OrdinalIgnoreCase));
            if (TargetColumn == null)
            {
                Console.WriteLine("La columna especificada no existe en la tabla");
                return Data;
            }

            string BasePath = $@"{DataPath}\{BaseName}";

            if (Directory.Exists(BasePath))
            {



                this.CurrentPath = BasePath;
                this.CurrentName = BaseName;

            }


            string PathOfTable = Path.Combine(CurrentPath, $"{TableName}.table");

            if (!File.Exists(PathOfTable))
            {
                Console.WriteLine("La tabla no existe");
                return Data;
            }

            using (FileStream fs = new FileStream(PathOfTable, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    var record = new Dictionary<string, object>();
                    foreach (var column in Columns)
                    {
                        object value = Read(reader, column.DataType);
                        record[column.Name] = value;
                    }
                    // Agregar el valor de la columna deseada a la lista
                    Data.Add(record[TargetColumn.Name]);
                }
            }

            this.CurrentPath = originalPath;
            this.CurrentName = originalName;

            return Data;
        }

        public DataType? GetType(string BaseName, string TableName, string ColumnName)
        {

            if (string.IsNullOrEmpty(BaseName))
            {
                Console.WriteLine("La base debe de tener un nombre");
                return null;
            }


            List<string> tables = GetTables(BaseName);
            if (!tables.Contains(TableName))
            {
                Console.WriteLine("La tabla especificada no existe en la base actual");
                return null;
            }


            List<Column> AllColumns = GetColumns(BaseName, TableName);


            var TargetColumn = AllColumns.FirstOrDefault(c => c.Name.Equals(ColumnName, StringComparison.OrdinalIgnoreCase));
            if (TargetColumn == null)
            {
                Console.WriteLine("La columna especificada no existe en la tabla");
                return null;
            }


            return TargetColumn.DataType;
        }

        public string GetIndexes()
        {
            return SystemIndexesFile;
        }

        public string GetNameOfIndex(string BaseName, string TableName, string ColumnName)
        {

            if (!File.Exists(SystemIndexesFile))
            {

                return null;
            }

            using (FileStream stream = File.Open(SystemIndexesFile, FileMode.Open, FileAccess.Read))
            {
                using (BinaryReader reader = new(stream))
                {
                    while (stream.Position < stream.Length)
                    {

                        string baseName = reader.ReadString();
                        string tableName = reader.ReadString();
                        string indexName = reader.ReadString();
                        string columnName = reader.ReadString();
                        string TypeOfIndex = reader.ReadString();


                        if (baseName.Equals(BaseName, StringComparison.OrdinalIgnoreCase) &&
                            tableName.Equals(TableName, StringComparison.OrdinalIgnoreCase) &&
                            columnName.Equals(ColumnName, StringComparison.OrdinalIgnoreCase))
                        {

                            return indexName;
                        }
                    }
                }
            }


            return null;
        }

        public string GetAssociated(string BaseName, string TableName, string ColumnName)
        {

            if (IndexesByColumns.TryGetValue(ColumnName, out string indexName))
            {
                Console.WriteLine("Se ha encontrado el índice");
                return indexName;
            }
            else
            {
                Console.WriteLine("No se encontró un índice asociado para la columna especificada");
                return null;
            }
        }

        public List<Dictionary<string, object>> GetIndexRecord(string indexName)
        {
            if (IndexTrees.TryGetValue(indexName, out object tree))
            {

                switch (tree)
                {
                    case BST<int> bstInt:
                        return bstInt.GetAllRecords();

                    case BST<string> bstString:
                        return bstString.GetAllRecords();

                    case BST<double> bstDouble:
                        return bstDouble.GetAllRecords();

                    case BST<DateTime> bstDateTime:
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
                        Console.WriteLine("Tipo de árbol no soportado");
                        return null;
                }
            }
            else
            {
                Console.WriteLine("El índice especificado no existe");
                return null;
            }
        }

        public List<Dictionary<string, object>> GetTableRecords(string TableName, List<Column> AllColumns)


        {
            string tablePath = Path.Combine(CurrentPath, $"{TableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine("La tabla no existe");
                return new List<Dictionary<string, object>>();
            }

            var records = new List<Dictionary<string, object>>();

            using (FileStream fs = new FileStream(tablePath, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    var record = new Dictionary<string, object>();
                    foreach (var column in AllColumns)
                    {
                        object value = Read(reader, column.DataType);
                        record[column.Name] = value;
                    }
                    records.Add(record);


                    foreach (var key in record.Keys)
                    {
                        Console.WriteLine($"Columna: {key}, Valor: {record[key]}");
                    }
                }
            }

            return records;
        }

        public List<Dictionary<string, object>> GetTableData(string BaseName, string TableName, List<Column> AllColumns)


        {
            string DataBasePath = $@"{DataPath}\{BaseName}";

            if (Directory.Exists(DataBasePath))
            {

                this.CurrentPath = DataBasePath;
                this.CurrentName = BaseName;

            }


            string tablePath = Path.Combine(CurrentPath, $"{TableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"La tabla no existe");
                return new List<Dictionary<string, object>>();
            }

            var records = new List<Dictionary<string, object>>();

            using (FileStream fs = new FileStream(tablePath, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    var record = new Dictionary<string, object>();
                    foreach (var column in AllColumns)
                    {
                        object value = Read(reader, column.DataType);
                        record[column.Name] = value;
                    }
                    records.Add(record);


                    foreach (var key in record.Keys)
                    {
                        //Console.WriteLine($"Columna: {key}, Valor: {record[key]}");
                    }
                }
            }

            this.CurrentPath = string.Empty;
            this.CurrentName = string.Empty;

            return records;
        }

        private List<Dictionary<string, object>> FilteredByWhere(List<Dictionary<string, object>> Records, string Where, string TableName, List<Column> AllColumns, string Mode, string? CurrentColumn, object? ValueToUpdate)
        {

            string WhereColumn = null;
            string WhereOperator = null;
            string WhereValue = null;

            var whereMatch = Regex.Match(Where, @"(\w+)\s*(=|>|<|LIKE|NOT)\s*(.+)", RegexOptions.IgnoreCase);
            if (!whereMatch.Success)
            {
                Console.WriteLine("Error de sintaxis");
                return Records = null;
            }

            WhereColumn = whereMatch.Groups[1].Value;
            WhereOperator = whereMatch.Groups[2].Value.ToUpper();
            WhereValue = whereMatch.Groups[3].Value.Trim('\'', '\"');

            // Validar la columna
            var whereCol = AllColumns.FirstOrDefault(c => c.Name.Equals(WhereColumn, StringComparison.OrdinalIgnoreCase));
            if (whereCol == null)
            {
                Console.WriteLine("La columna especificada no existe en la tabla");
                return Records = null;
            }

            // Aplicar el filtro
            if (Mode == "DEFAULT")
            {
                return Records = Records.Where(record => WhereCondition(record, WhereColumn, WhereOperator, WhereValue)).ToList();
            }

            else if (Mode == "UPDATE")
            {


                foreach (var record in Records)
                {
                    if (WhereCondition(record, WhereColumn, WhereOperator, WhereValue))
                    {
                        record[CurrentColumn] = ValueToUpdate;
                    }
                }

                return Records;
            }

            else
            {
                return Records = null;
            }

        }

        private List<Dictionary<string, object>> FilteredByOrder(List<Dictionary<string, object>> Records, string ColumnOrder, string DirectionOrder, string TableName, List<Column> AllColumns)
        {
            var orderByCol = AllColumns.FirstOrDefault(c => c.Name.Equals(ColumnOrder, StringComparison.OrdinalIgnoreCase));
            if (orderByCol == null)
            {
                Console.WriteLine("La columna especificada no existe en la tabla");
                return null;
            }

            bool descending = DirectionOrder.Equals("DESC", StringComparison.OrdinalIgnoreCase);

            // Llamar a Quicksort
            Sort(Records, 0, Records.Count - 1, orderByCol.Name, descending);

            return Records;
        }

        private object Read(BinaryReader Reader, DataType Type)
        {
            switch (Type)
            {
                case DataType.INTEGER:
                    return Reader.ReadInt32();
                case DataType.DOUBLE:
                    return Reader.ReadDouble();
                case DataType.VARCHAR:
                    return Reader.ReadString();
                case DataType.DATETIME:
                    long ticks = Reader.ReadInt64();
                    return DateTime.FromBinary(ticks);
                default:
                    throw new Exception("Tipo de dato no soportado");
            }
        }

        private void PrintRecords(List<Column> SelectedColumns, List<Dictionary<string, object>> Records)
        {

            var table = new List<Dictionary<string, object>>();

            foreach (var record in Records)
            {
                var row = new Dictionary<string, object>();
                foreach (var column in SelectedColumns)
                {
                    row[column.Name] = record[column.Name];
                }
                table.Add(row);
            }


            var columnNames = SelectedColumns.Select(c => c.Name).ToList();


            PrintTable(table, columnNames);
        }

        private void PrintTable(List<Dictionary<string, object>> Table, List<string> Columns)
        {

            Dictionary<string, int> ColumnWidth = new Dictionary<string, int>();


            foreach (var col in Columns)
            {
                ColumnWidth[col] = col.Length;
            }


            foreach (var row in Table)
            {
                foreach (var col in Columns)
                {
                    string valueStr = row[col]?.ToString() ?? "NULL";
                    if (valueStr.Length > ColumnWidth[col])
                    {
                        ColumnWidth[col] = valueStr.Length;
                    }
                }
            }


            foreach (var col in Columns)
            {
                Console.Write($"| {col.PadRight(ColumnWidth[col])} ");
            }
            Console.WriteLine("|");


            Console.WriteLine(new string('-', Columns.Sum(col => ColumnWidth[col] + 3) + 1));


            foreach (var row in Table)
            {
                foreach (var col in Columns)
                {
                    string valueStr = row[col]?.ToString() ?? "NULL";
                    Console.Write($"| {valueStr.PadRight(ColumnWidth[col])} ");
                }
                Console.WriteLine("|");
            }
        }

        private bool WhereCondition(Dictionary<string, object> Record, string ColumnName, string Operator, string Value)
        {
            if (!Record.ContainsKey(ColumnName))
                return false;

            var recordValue = Record[ColumnName];

            switch (Operator)
            {
                case "=":
                    return recordValue.ToString().Equals(Value, StringComparison.OrdinalIgnoreCase);
                case ">":
                    return Compare(recordValue, Value) > 0;
                case "<":
                    return Compare(recordValue, Value) < 0;
                case "LIKE":
                    return recordValue.ToString().Contains(Value, StringComparison.OrdinalIgnoreCase);
                case "NOT":
                    return !recordValue.ToString().Equals(Value, StringComparison.OrdinalIgnoreCase);
                default:
                    throw new Exception("Operador no soportado.");
            }
        }

        private int Compare(object Value_1, object Value_2)
        {
            if (Value_1 == null && Value_2 == null)
                return 0;
            if (Value_1 == null)
                return -1;
            if (Value_2 == null)
                return 1;

            if (Value_1 is int int1 && Value_2 is int int2)
            {
                return int1.CompareTo(int2);
            }
            else if (Value_1 is double double1 && Value_2 is double double2)
            {
                return double1.CompareTo(double2);
            }
            else if (Value_1 is string str1 && Value_2 is string str2)
            {
                return string.Compare(str1, str2, StringComparison.OrdinalIgnoreCase);
            }
            else if (Value_1 is DateTime date1 && Value_2 is DateTime date2)
            {
                return date1.CompareTo(date2);
            }
            else
            {
                throw new Exception("Tipos incompatibles.");
            }
        }

        private void Sort(List<Dictionary<string, object>> Records, int Low, int High, string ColumnName, bool Desc)
        {
            if (Low < High)
            {
                int pivotIndex = Partition(Records, Low, High, ColumnName, Desc);
                Sort(Records, Low, pivotIndex - 1, ColumnName, Desc);
                Sort(Records, pivotIndex + 1, High, ColumnName, Desc);
            }
        }

        private int Partition(List<Dictionary<string, object>> records, int Low, int high, string ColumnName, bool Desc)
        {
            var pivotValue = records[high][ColumnName];
            int i = Low - 1;

            for (int j = Low; j < high; j++)
            {
                int comparisonResult = Compare(records[j][ColumnName], pivotValue);
                if (Desc)
                {
                    if (comparisonResult > 0)
                    {
                        i++;
                        Swap(records, i, j);
                    }
                }
                else
                {
                    if (comparisonResult < 0)
                    {
                        i++;
                        Swap(records, i, j);
                    }
                }
            }

            Swap(records, i + 1, high);
            return i + 1;
        }

        private void Swap(List<Dictionary<string, object>> Records, int Index_1, int Index_2)
        {
            var temp = Records[Index_1];
            Records[Index_1] = Records[Index_2];
            Records[Index_2] = temp;
        }
    }
}
