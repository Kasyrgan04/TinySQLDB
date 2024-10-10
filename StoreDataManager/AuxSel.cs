using Entities;
using QueryProcessor.Parser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace StoreDataManager
{
    public class AuxSel
    {
        public string CurrentDatabasePath;
        public string CurrentDatabaseName;

        public List<Dictionary<string, object>> GetRecords(string tableName, List<Column> allColumns)
        {
            string tablePath = Path.Combine(CurrentDatabasePath, $"{tableName}.table");


            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"El archivo de la tabla '{tableName}' no existe.");
                return new List<Dictionary<string, object>>();
            }

            var records = new List<Dictionary<string, object>>();

            try
            {
                using (FileStream fs = new FileStream(tablePath, FileMode.Open, FileAccess.Read))
                using (BinaryReader reader = new BinaryReader(fs))
                {
                    while (fs.Position < fs.Length)
                    {
                        var record = new Dictionary<string, object>();
                        foreach (var column in allColumns)
                        {
                            object value = Read(reader, column.DataType);
                            record[column.Name] = value;
                        }
                        records.Add(record);

                        // Depuración: Imprimir registro leído
                        Console.WriteLine("Registro leído:");
                        foreach (var key in record.Keys)
                        {
                            Console.WriteLine($"Columna: {key}, Valor: {record[key]}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer los registros de la tabla '{tableName}': {ex.Message}");
                return new List<Dictionary<string, object>>();
            }

            return records;
        }

        public List<Dictionary<string, object>> GetData(string DataBaseName, string tableName, List<Column> allColumns)
        {
            string DataBasePath = $@"{Store.DataPath}\{DataBaseName}";

            if (!Directory.Exists(DataBasePath))
            {
                Console.WriteLine($"La base de datos '{DataBaseName}' no existe.");
                return new List<Dictionary<string, object>>();
            }

            this.CurrentDatabasePath = DataBasePath;
            this.CurrentDatabaseName = DataBaseName;

            string tablePath = Path.Combine(CurrentDatabasePath, $"{tableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"El archivo de la tabla '{tableName}' no existe.");
                return new List<Dictionary<string, object>>();
            }

            var records = new List<Dictionary<string, object>>();

            try
            {
                using (FileStream fs = new FileStream(tablePath, FileMode.Open, FileAccess.Read))
                using (BinaryReader reader = new BinaryReader(fs))
                {
                    while (fs.Position < fs.Length)
                    {
                        var record = new Dictionary<string, object>();
                        foreach (var column in allColumns)
                        {
                            object value = Read(reader, column.DataType);
                            record[column.Name] = value;
                        }
                        records.Add(record);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer los registros de la tabla '{tableName}': {ex.Message}");
                return new List<Dictionary<string, object>>();
            }
            finally
            {
                // Restablecer las propiedades de la base de datos después de la operación
                this.CurrentDatabasePath = string.Empty;
                this.CurrentDatabaseName = string.Empty;
            }

            return records;
        }

        public List<Dictionary<string, object>> FilteredByWhere(List<Dictionary<string, object>> records, string whereClause, string tableName, List<Column> allColumns, string mode, string? settedColumn, object? updateValue)
        {
            // Parsear y evaluar la cláusula WHERE
            var whereMatch = Regex.Match(whereClause, @"(\w+)\s*(=|>|<|LIKE|NOT)\s*(.+)", RegexOptions.IgnoreCase);
            if (!whereMatch.Success)
            {
                Console.WriteLine("Sintaxis de WHERE incorrecta.");
                return null;
            }

            string whereColumn = whereMatch.Groups[1].Value;
            string whereOperator = whereMatch.Groups[2].Value.ToUpper();
            string whereValue = whereMatch.Groups[3].Value.Trim('\'', '\"');

            // Validar la columna
            var whereCol = allColumns.FirstOrDefault(c => c.Name.Equals(whereColumn, StringComparison.OrdinalIgnoreCase));
            if (whereCol == null)
            {
                Console.WriteLine($"La columna '{whereColumn}' no existe en la tabla '{tableName}'.");
                return null;
            }

            // Aplicar el filtro según el modo
            if (mode == "DEFAULT")
            {
                return records.Where(record => WhereCondition(record, whereColumn, whereOperator, whereValue)).ToList();
            }
            else if (mode == "UPDATE" && settedColumn != null && updateValue != null)
            {
                foreach (var record in records)
                {
                    if (WhereCondition(record, whereColumn, whereOperator, whereValue))
                    {
                        record[settedColumn] = updateValue;
                    }
                }
                return records;
            }

            Console.WriteLine("Modo desconocido o parámetros incorrectos.");
            return null;
        }

        public int CompareValues(object value1, object value2)
        {
            if (value1 == null && value2 == null)
                return 0;
            if (value1 == null)
                return -1;
            if (value2 == null)
                return 1;

            switch (value1)
            {
                case int int1 when value2 is int int2:
                    return int1.CompareTo(int2);
                case double double1 when value2 is double double2:
                    return double1.CompareTo(double2);
                case string str1 when value2 is string str2:
                    return string.Compare(str1, str2, StringComparison.OrdinalIgnoreCase);
                case DateTime date1 when value2 is DateTime date2:
                    return date1.CompareTo(date2);
                // Agregar más tipos aquí según sea necesario
                default:
                    throw new InvalidOperationException("Tipos de datos no comparables o incompatibles.");
            }
        }

        public void QuickSort(List<Dictionary<string, object>> records, int low, int high, string columnName, bool descending)
        {
            if (low < high)
            {
                int pivotIndex = Partition(records, low, high, columnName, descending);
                QuickSort(records, low, pivotIndex - 1, columnName, descending);
                QuickSort(records, pivotIndex + 1, high, columnName, descending);
            }
        }

        public int Partition(List<Dictionary<string, object>> records, int low, int high, string columnName, bool descending)
        {
            // Verificar que la columna existe
            if (!records.Any() || !records[0].ContainsKey(columnName))
            {
                throw new ArgumentException($"La columna '{columnName}' no existe en los registros.");
            }

            var pivotValue = records[high][columnName];
            int i = low - 1;

            for (int j = low; j < high; j++)
            {
                // Manejo de valores nulos en comparación
                if (records[j][columnName] == null || pivotValue == null)
                {
                    // Puedes definir cómo quieres tratar los nulos aquí
                    continue; // O lanzar una excepción, según tu lógica
                }

                int comparisonResult = CompareValues(records[j][columnName], pivotValue);

                if (descending)
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

        public void Swap(List<Dictionary<string, object>> records, int index1, int index2)
        {
            var temp = records[index1];
            records[index1] = records[index2];
            records[index2] = temp;
        }

        public List<Dictionary<string, object>> FilteredByOrder(List<Dictionary<string, object>> records, string orderByColumn, string orderByDirection, string tableName, List<Column> allColumns)
        {
            // Verificar que la lista de registros no esté vacía
            if (records == null || records.Count == 0)
            {
                Console.WriteLine("No hay registros para ordenar.");
                return records; // O lanzar una excepción
            }

            // Validar que la columna existe
            var orderByCol = allColumns.FirstOrDefault(c => c.Name.Equals(orderByColumn, StringComparison.OrdinalIgnoreCase));
            if (orderByCol == null)
            {
                throw new ArgumentException($"La columna '{orderByColumn}' no existe en la tabla '{tableName}'.");
            }

            // Determinar la dirección de orden
            bool descending = orderByDirection.Equals("DESC", StringComparison.OrdinalIgnoreCase);

            // Llamar al método QuickSort
            QuickSort(records, 0, records.Count - 1, orderByCol.Name, descending);

            return records;
        }

        public object Read(BinaryReader reader, DataType dataType)
        {
            switch (dataType)
            {
                case DataType.INTEGER:
                    return reader.ReadInt32();
                case DataType.DOUBLE:
                    return reader.ReadDouble();
                case DataType.VARCHAR:
                    return reader.ReadString();
                case DataType.DATETIME:
                    long ticks = reader.ReadInt64();
                    return DateTime.FromBinary(ticks);
                default:
                    throw new Exception("Tipo de dato no soportado para lectura.");
            }
        }

        public void PrintRecords(List<Column> selected, List<Dictionary<string, object>> records)
        {
            
            var table = new List<Dictionary<string, object>>();

            foreach (var record in records)
            {
                var row = new Dictionary<string, object>();
                foreach (var column in selected)
                {
                    row[column.Name] = record[column.Name];
                }
                table.Add(row);
            }

           
            var Names = selected.Select(c => c.Name).ToList();

            
            PrintTable(table, Names);
        }

        public void PrintTable(List<Dictionary<string, object>> table, List<string> columns)
        {
            
            Dictionary<string, int> Widths = new Dictionary<string, int>();

            
            foreach (var col in columns)
            {
                Widths[col] = col.Length;
            }

            
            foreach (var row in table)
            {
                foreach (var col in columns)
                {
                    string valueStr = row[col]?.ToString() ?? "NULL";
                    if (valueStr.Length > Widths[col])
                    {
                        Widths[col] = valueStr.Length;
                    }
                }
            }

            
            foreach (var col in columns)
            {
                Console.Write($"| {col.PadRight(Widths[col])} ");
            }
            Console.WriteLine("|");

            
            Console.WriteLine(new string('-', columns.Sum(col => Widths[col] + 3) + 1));

            
            foreach (var row in table)
            {
                foreach (var col in columns)
                {
                    string valueStr = row[col]?.ToString() ?? "NULL";
                    Console.Write($"| {valueStr.PadRight(Widths[col])} ");
                }
                Console.WriteLine("|");
            }
        }

        public bool WhereCondition(Dictionary<string, object> record, string column, string operatorStr, string valueStr)
        {
            if (!record.ContainsKey(column))
                return false;

            var recordValue = record[column];

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
                    throw new Exception("El operador ingresado no es valido.");
            }
        }
    }
}
