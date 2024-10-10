using ApiInterface.Indexes;
using System.Data.Common;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using ApiInterface;
using System.Text.RegularExpressions;

public enum OperationStatus
{
    Success,
    Error
}

public class DatabaseManager
{
    private string SystemIndexesFile = "path/to/indexes/file"; // Asegúrate de ajustar la ruta
    private string SettedDataBaseName;
    private HashSet<string> DataBasesWithIndexes = new HashSet<string>();
    private HashSet<string> TablesWithIndexes = new HashSet<string>();
    private Dictionary<string, string> AssociatedIndexesToColumns = new Dictionary<string, string>();
    private Dictionary<string, object> IndexTrees = new Dictionary<string, object>();

    // Método para obtener el archivo de índices del sistema
    public string GetSystemIndexesFile()
    {
        return SystemIndexesFile;
    }

    // Método para obtener el nombre del índice si existe
    public string GetIndexNameIfExist(string databaseName, string tableName, string columnName)
    {
        if (!File.Exists(SystemIndexesFile))
        {
            return null; // Si el archivo no existe, no puede haber ningún índice
        }

        using (FileStream stream = File.Open(SystemIndexesFile, FileMode.Open, FileAccess.Read))
        using (BinaryReader reader = new(stream))
        {
            while (stream.Position < stream.Length) // Leer hasta el final del archivo
            {
                string dbName = reader.ReadString();
                string tblName = reader.ReadString();
                string idxName = reader.ReadString();
                string colName = reader.ReadString();
                string idxType = reader.ReadString();

                // Comprobar si coinciden la base de datos, la tabla y la columna
                if (dbName.Equals(databaseName, StringComparison.OrdinalIgnoreCase) &&
                    tblName.Equals(tableName, StringComparison.OrdinalIgnoreCase) &&
                    colName.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                {
                    return idxName; // Si se encuentra una coincidencia, devolver el nombre del índice
                }
            }
        }

        return null; // Si no se encontró el índice
    }

    // Método para obtener el índice asociado a una columna
    public string GetAssociatedIndex(string dataBaseName, string tableName, string columnName)
    {
        if (AssociatedIndexesToColumns.TryGetValue(columnName, out string indexName))
        {
            Console.WriteLine($"Índice asociado encontrado: {indexName}");
            return indexName;
        }
        else
        {
            Console.WriteLine($"No se encontró un índice asociado para la columna {columnName} en la tabla {tableName} de la base de datos {dataBaseName}.");
            return null;
        }
    }

    // Método para obtener todos los registros de un índice dado su nombre
    public List<Dictionary<string, object>> GetRecordsFromIndex(string indexName)
    {
        if (IndexTrees.TryGetValue(indexName, out object tree))
        {
            // Verificar el tipo de árbol y llamar a GetAllRecords
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
                    Console.WriteLine("Tipo de árbol no soportado.");
                    return null;
            }
        }
        else
        {
            Console.WriteLine("El índice especificado no existe.");
            return null;
        }
    }

    // Método para seleccionar registros de una tabla
    public OperationStatus SelectFromTable(
        string tableName,
        List<string> columnsToSelect,
        string whereClause,
        string orderByColumn,
        string orderByDirection,
        out object? data)
    {
        data = null;

        // Verificar que la base de datos está establecida
        if (string.IsNullOrEmpty(SettedDataBaseName))
        {
            Console.WriteLine("No se ha establecido una base de datos.");
            return OperationStatus.Error;
        }

        // Verificar que la tabla existe
        List<string> tables = GetTablesInDataBase(SettedDataBaseName);
        if (!tables.Contains(tableName))
        {
            Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'.");
            return OperationStatus.Error;
        }

        // Obtener las columnas de la tabla
        List<Column> allColumns = GetColumnsOfTable(SettedDataBaseName, tableName);

        // Determinar las columnas a seleccionar
        List<Column> selectedColumns = GetSelectedColumns(columnsToSelect, allColumns, tableName);
        if (selectedColumns == null)
        {
            return OperationStatus.Error; // Error ya manejado en GetSelectedColumns
        }

        var records = new List<Dictionary<string, object>>();

        // Usar índices si están disponibles
        if (DataBasesWithIndexes.Contains(SettedDataBaseName) && TablesWithIndexes.Contains(tableName))
        {
            records = GetRecordsUsingIndexes(selectedColumns);
        }

        // Si no se encontraron registros con índices, obtener de la tabla directamente
        if (records == null || records.Count == 0)
        {
            Console.WriteLine("No hay índices asociados. Leyendo registros de la tabla.");
            records = GetRecordsFromTable(tableName, allColumns);
        }

        if (records == null)
        {
            Console.WriteLine("No se pudieron leer los registros de la tabla.");
            return OperationStatus.Error;
        }

        // Aplicar filtro WHERE si existe
        if (!string.IsNullOrEmpty(whereClause))
        {
            records = FilteredRecordsWhere(records, whereClause, tableName, allColumns, "DEFAULT", null, null);
            if (records == null)
            {
                return OperationStatus.Error;
            }
        }

        // Ordenar los registros si hay cláusula ORDER BY
        if (!string.IsNullOrEmpty(orderByColumn))
        {
            records = FilteredRecordsOrderBy(records, orderByColumn, orderByDirection, tableName, allColumns);
            if (records == null)
            {
                return OperationStatus.Error;
            }
        }

        // Filtrar los registros según las columnas seleccionadas
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
        PrintRecords(selectedColumns, records);

        return OperationStatus.Success;
    }

    // Método para obtener las columnas seleccionadas
    private List<Column> GetSelectedColumns(List<string> columnsToSelect, List<Column> allColumns, string tableName)
    {
        if (columnsToSelect == null)
        {
            // Seleccionar todas las columnas si no se especifica ninguna
            return allColumns;
        }

        var selectedColumns = new List<Column>();
        foreach (var colName in columnsToSelect)
        {
            var col = allColumns.FirstOrDefault(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
            if (col == null)
            {
                Console.WriteLine($"La columna '{colName}' no existe en la tabla '{tableName}'.");
                return null; // Retorna null para indicar error
            }
            selectedColumns.Add(col);
        }

        return selectedColumns;
    }

    // Método para obtener registros usando índices
    private List<Dictionary<string, object>> GetRecordsUsingIndexes(List<Column> selectedColumns)
    {
        foreach (var col in selectedColumns)
        {
            string? indexName = GetAssociatedIndex(SettedDataBaseName, tableName, col.Name);
            if (indexName != null)
            {
                Console.WriteLine("USANDO ÍNDICES EN MEMORIA PARA ESTE REQUEST");
                return GetRecordsFromIndex(indexName);
            }
        }

        Console.WriteLine("No se encontraron índices asociados.");
        return null;
    }

    // Método para imprimir registros en formato de tabla
    private void PrintRecords(List<Column> selectedColumns, List<Dictionary<string, object>> records)
    {
        // Aquí debes implementar la lógica para mostrar los registros en formato de tabla
        foreach (var record in records)
        {
            foreach (var column in selectedColumns)
            {
                Console.Write($"{record[column.Name]} \t");
            }
            Console.WriteLine();
        }
    }

    // Métodos adicionales que necesitas implementar
    private List<string> GetTablesInDataBase(string databaseName)
    {
        // Implementa la lógica para obtener las tablas de la base de datos
        throw new NotImplementedException();
    }

    private List<Column> GetColumnsOfTable(string databaseName, string tableName)
    {
        // Implementa la lógica para obtener las columnas de la tabla
        throw new NotImplementedException();
    }

    private List<Dictionary<string, object>> GetRecordsFromTable(string tableName, List<Column> allColumns)
    {
        List<Dictionary<string, object>> records = new List<Dictionary<string, object>>();

        // Lógica para abrir la conexión a la base de datos (esto depende de cómo estés manejando las conexiones)
        using (var connection = new DbConnection()) // Asegúrate de reemplazar DbConnection con tu conexión real
        {
            connection.Open();

            // Lógica para ejecutar una consulta para obtener todos los registros de la tabla
            string query = $"SELECT * FROM {tableName}";

            using (var command = new DbCommand(query, connection)) // Reemplaza DbCommand con tu comando real
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var record = new Dictionary<string, object>();

                        // Leer cada columna del registro
                        foreach (var column in allColumns)
                        {
                            record[column.Name] = reader[column.Name];
                        }

                        records.Add(record);
                    }
                }
            }
        }

        return records;
    }
    public OperationStatus DeleteFromTable(string tableName, string whereClause)
    {
        // Verificar que la base de datos está establecida
        if (string.IsNullOrEmpty(SettedDataBaseName))
        {
            Console.WriteLine("No se ha establecido una base de datos.");
            return OperationStatus.Error;
        }

        // Verificar que la tabla existe
        List<string> tables = GetTablesInDataBase(SettedDataBaseName);
        if (!tables.Contains(tableName))
        {
            Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'.");
            return OperationStatus.Error;
        }

        // Obtener las columnas de la tabla
        List<Column> allColumns = GetColumnsOfTable(SettedDataBaseName, tableName);
        List<Dictionary<string, object>> records = GetRecordsFromTable(tableName, allColumns);

        // Verificar si se pudieron leer los registros de la tabla
        if (records == null)
        {
            Console.WriteLine("No se pudieron leer los registros de la tabla.");
            return OperationStatus.Error;
        }

        // Filtrar registros según la cláusula WHERE si está presente
        List<Dictionary<string, object>> recordsToDelete = new List<Dictionary<string, object>>();
        if (!string.IsNullOrEmpty(whereClause))
        {
            recordsToDelete = FilteredRecordsWhere(records, whereClause, tableName, allColumns, "DEFAULT", null, null);
            if (recordsToDelete == null || recordsToDelete.Count == 0)
            {
                Console.WriteLine("No se encontraron registros que coincidan con la cláusula WHERE.");
                return OperationStatus.Error;
            }
        }
        else
        {
            // Si no hay cláusula WHERE, se eliminan todos los registros
            recordsToDelete = new List<Dictionary<string, object>>(records);
        }

        // Eliminar registros de la lista original
        records = records.Except(recordsToDelete).ToList();

        // Guardar los registros restantes de nuevo en el archivo
        string tablePath = Path.Combine(SettedDataBasePath, $"{tableName}.table");

        using (FileStream fs = new FileStream(tablePath, FileMode.Create, FileAccess.Write))
        using (BinaryWriter writer = new BinaryWriter(fs))
        {
            foreach (var record in records)
            {
                foreach (var column in allColumns)
                {
                    object value = record[column.Name];
                    WriteValue(writer, value);
                }
            }
        }

        // Actualizar índices
        IndexGenerator indexGenerator = new IndexGenerator();
        indexGenerator.RegenerateIndexes();

        return OperationStatus.Success;
    }

    public List<Dictionary<string, object>> FilteredRecordsWhere(List<Dictionary<string, object>> records, string whereClause, string tableName, List<Column> allColumns, string mode, string? settedColumn, object? updateValue)
    {
        // Parsear y evaluar la cláusula WHERE
        string whereColumn = null;
        string whereOperator = null;
        string whereValue = null;

        var whereMatch = Regex.Match(whereClause, @"(\w+)\s*(=|>|<|LIKE|NOT)\s*(.+)", RegexOptions.IgnoreCase);
        if (!whereMatch.Success)
        {
            Console.WriteLine("Sintaxis de WHERE incorrecta.");
            return null;
        }

        whereColumn = whereMatch.Groups[1].Value;
        whereOperator = whereMatch.Groups[2].Value.ToUpper();
        whereValue = whereMatch.Groups[3].Value.Trim('\'', '\"');

        // Validar la columna
        var whereCol = allColumns.FirstOrDefault(c => c.Name.Equals(whereColumn, StringComparison.OrdinalIgnoreCase));
        if (whereCol == null)
        {
            Console.WriteLine($"La columna '{whereColumn}' no existe en la tabla '{tableName}'.");
            return null;
        }

        // Aplicar el filtro
        if (mode == "DEFAULT")
        {
            return records.Where(record => EvaluateWhereCondition(record, whereColumn, whereOperator, whereValue)).ToList();
        }
        else if (mode == "UPDATE")
        {
            foreach (var record in records)
            {
                if (EvaluateWhereCondition(record, whereColumn, whereOperator, whereValue))
                {
                    record[settedColumn] = updateValue;
                }
            }
            return records;
        }
        else
        {
            return null;
        }
    }

    public List<Dictionary<string, object>> FilteredRecordsOrderBy(List<Dictionary<string, object>> records, string orderByColumn, string orderByDirection, string tableName, List<Column> allColumns)
    {
        var orderByCol = allColumns.FirstOrDefault(c => c.Name.Equals(orderByColumn, StringComparison.OrdinalIgnoreCase));
        if (orderByCol == null)
        {
            Console.WriteLine($"La columna '{orderByColumn}' no existe en la tabla '{tableName}'.");
            return null;
        }

        bool descending = orderByDirection.Equals("DESC", StringComparison.OrdinalIgnoreCase);
        QuickSort(records, 0, records.Count - 1, orderByCol.Name, descending);
        return records;
    }

    private void PrintRecords(List<Column> selectedColumns, List<Dictionary<string, object>> records)
    {
        // Crear la tabla en forma de lista de diccionarios
        var table = new List<Dictionary<string, object>>();

        foreach (var record in records)
        {
            var row = new Dictionary<string, object>();
            foreach (var column in selectedColumns)
            {
                row[column.Name] = record[column.Name];
            }
            table.Add(row);
        }

        // Obtener los nombres de las columnas
        var columnNames = selectedColumns.Select(c => c.Name).ToList();

        // Llamar al método PrintTable
        PrintTable(table, columnNames);
    }

    private void PrintTable(List<Dictionary<string, object>> table, List<string> columns)
    {
        // Calcular el ancho máximo de cada columna
        Dictionary<string, int> columnWidths = new Dictionary<string, int>();

        // Inicializar los anchos con la longitud de los nombres de las columnas
        foreach (var col in columns)
        {
            columnWidths[col] = col.Length;
        }

        // Actualizar los anchos según los datos
        foreach (var row in table)
        {
            foreach (var col in columns)
            {
                string valueStr = row[col]?.ToString() ?? "NULL";
                if (valueStr.Length > columnWidths[col])
                {
                    columnWidths[col] = valueStr.Length;
                }
            }
        }

        // Imprimir los encabezados con formato
        foreach (var col in columns)
        {
            Console.Write($"| {col.PadRight(columnWidths[col])} ");
        }
        Console.WriteLine("|");

        // Imprimir línea separadora
        Console.WriteLine(new string('-', columns.Sum(col => columnWidths[col] + 3) + 1));

        // Paso 3: Imprimir las filas con formato
        foreach (var row in table)
        {
            foreach (var col in columns)
            {
                string valueStr = row[col]?.ToString() ?? "NULL";
                Console.Write($"| {valueStr.PadRight(columnWidths[col])} ");
            }
            Console.WriteLine("|");
        }
    }

    private bool EvaluateWhereCondition(Dictionary<string, object> record, string columnName, string operatorStr, string valueStr)
    {
        if (!record.ContainsKey(columnName))
            return false;

        var recordValue = record[columnName];

        switch (operatorStr)
        {
            case "=":
                return recordValue.ToString().Equals(valueStr, StringComparison.OrdinalIgnoreCase);
            case ">":
                return CompareValues(recordValue, valueStr) > 0;
            case "<":
                return CompareValues(recordValue, valueStr) < 0;
            case "LIKE":
                return recordValue.ToString().Contains(valueStr, StringComparison.OrdinalIgnoreCase);
            case "NOT":
                return !recordValue.ToString().Equals(valueStr, StringComparison.OrdinalIgnoreCase);
            default:
                throw new Exception("Operador no soportado en WHERE.");
        }
    }

    private int CompareValues(object value1, object value2)
    {
        if (value1 == null && value2 == null)
            return 0;
        if (value1 == null)
            return -1;
        if (value2 == null)
            return 1;

        if (value1 is int int1 && value2 is int int2)
        {
            return int1.CompareTo(int2);
        }
        else if (value1 is double double1 && value2 is double double2)
        {
            return double1.CompareTo(double2);
        }
        else if (value1 is string str1 && value2 is string str2)
        {
            return string.Compare(str1, str2, StringComparison.OrdinalIgnoreCase);
        }
        else if (value1 is DateTime date1 && value2 is DateTime date2)
        {
            return date1.CompareTo(date2);
        }
        else
        {
            throw new Exception("Tipos de datos no comparables o incompatibles.");
        }
    }

    private void QuickSort(List<Dictionary<string, object>> records, int low, int high, string columnName, bool descending)
    {
        if (low < high)
        {
            int pivotIndex = Partition(records, low, high, columnName, descending);
            QuickSort(records, low, pivotIndex - 1, columnName, descending);
            QuickSort(records, pivotIndex + 1, high, columnName, descending);
        }
    }

    private int Partition(List<Dictionary<string, object>> records, int low, int high, string columnName, bool descending)
    {
        var pivotValue = records[high][columnName];
        int i = low - 1;

        for (int j = low; j < high; j++)
        {
            int comparisonResult = CompareValues(records[j][columnName], pivotValue);
            if ((descending && comparisonResult > 0) || (!descending && comparisonResult < 0))
            {
                i++;
                Swap(records, i, j);
            }
        }

        Swap(records, i + 1, high);
        return i + 1;
    }

    private void Swap(List<Dictionary<string, object>> records, int index1, int index2)
    {
        (records[index1], records[index2]) = (records[index2], records[index1]);
    }
}