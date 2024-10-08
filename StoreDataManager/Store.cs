using Entities;
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

        private const string DatabaseBasePath = @"C:\TinySql\";
        private const string DataPath = $@"{DatabaseBasePath}\Data";
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";

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

        public OperationStatus CreateTable(string tableName, string[] columnsDefinition)
        {
            var tablePath = $@"{DataPath}\TESTDB\{tableName}.Table";

            using (FileStream stream = File.Open(tablePath, FileMode.Create))
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                // Escribir las definiciones de las columnas en el archivo de la tabla
                foreach (var column in columnsDefinition)
                {
                    writer.Write(column);
                }
            }
            return OperationStatus.Success;
        }


        public OperationStatus Select(string tableName, List<string> columns)
        {
            var tablePath = $@"{DataPath}\TESTDB\{tableName}.Table";
            using (FileStream stream = File.Open(tablePath, FileMode.OpenOrCreate))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                while (stream.Position < stream.Length)
                {
                    // Suponiendo que siempre se lee el mismo número de columnas
                    int id = reader.ReadInt32();
                    string nombre = reader.ReadString();
                    string apellido = reader.ReadString();

                    // Aquí puedes decidir qué columnas imprimir según los nombres en `columns`
                    if (columns.Contains("id"))
                    {
                        Console.WriteLine(id);
                    }
                    if (columns.Contains("nombre"))
                    {
                        Console.WriteLine(nombre);
                    }
                    if (columns.Contains("apellido"))
                    {
                        Console.WriteLine(apellido);
                    }
                }
                return OperationStatus.Success;
            }
        }



        public OperationStatus Insert(string tableName, string[] values)
        {
            var tablePath = $@"{DataPath}\TESTDB\{tableName}.Table";

            using (FileStream stream = new FileStream(tablePath, FileMode.Append))
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                // Aquí asumo que tienes tres campos, ajusta según tus necesidades
                writer.Write(int.Parse(values[0])); // Asume que el primer valor es un int
                writer.Write(values[1].PadRight(30)); // Asume que el segundo valor es un string de tamaño 30
                writer.Write(values[2].PadRight(50)); // Asume que el tercer valor es un string de tamaño 50
            }
            return OperationStatus.Success;
        }


        public OperationStatus Update(string tableName, int id, string[] newValues)
        {
            var tablePath = $@"{DataPath}\TESTDB\{tableName}.Table";
            var tempPath = $@"{DataPath}\TESTDB\Temp.Table";

            using (FileStream readStream = new FileStream(tablePath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(readStream))
            using (FileStream writeStream = new FileStream(tempPath, FileMode.Create))
            using (BinaryWriter writer = new BinaryWriter(writeStream))
            {
                while (readStream.Position < readStream.Length)
                {
                    int recordId = reader.ReadInt32();
                    string nombre = reader.ReadString();
                    string apellido = reader.ReadString();

                    // Si encontramos el registro con el ID correspondiente, lo actualizamos
                    if (recordId == id)
                    {
                        writer.Write(id); // Mantener el ID
                        writer.Write(newValues[0].PadRight(30)); // Actualizar nombre
                        writer.Write(newValues[1].PadRight(50)); // Actualizar apellido
                    }
                    else
                    {
                        // Escribir el registro sin cambios
                        writer.Write(recordId);
                        writer.Write(nombre);
                        writer.Write(apellido);
                    }
                }
            }

            // Reemplazar el archivo original con el temporal
            File.Delete(tablePath);
            File.Move(tempPath, tablePath);
            return OperationStatus.Success;
        }

        public OperationStatus Delete(string tableName, int id)
        {
            var tablePath = $@"{DataPath}\TESTDB\{tableName}.Table";
            var tempPath = $@"{DataPath}\TESTDB\Temp.Table";

            using (FileStream readStream = new FileStream(tablePath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(readStream))
            using (FileStream writeStream = new FileStream(tempPath, FileMode.Create))
            using (BinaryWriter writer = new BinaryWriter(writeStream))
            {
                while (readStream.Position < readStream.Length)
                {
                    int recordId = reader.ReadInt32();
                    string nombre = reader.ReadString();
                    string apellido = reader.ReadString();

                    // Solo escribimos el registro si no es el que queremos eliminar
                    if (recordId != id)
                    {
                        writer.Write(recordId);
                        writer.Write(nombre);
                        writer.Write(apellido);
                    }
                }
            }

            // Reemplazar el archivo original con el temporal
            File.Delete(tablePath);
            File.Move(tempPath, tablePath);
            return OperationStatus.Success;
        }


    }
}
