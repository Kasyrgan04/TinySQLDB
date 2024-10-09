using ApiInterface.Indexes;
using QueryProcessor.Parser;
using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface
{
    public class IndexGenerator
    {

        

        public void LoadIndexesAndGenerateTrees()
        {
            var store = Store.GetInstance();
            string systemIndexesFile = store.GetSystemIndexesFile();

            if (!File.Exists(systemIndexesFile))
            {
                Console.WriteLine($"El archivo de índices no existe: {systemIndexesFile}");
                // Aquí puedes manejar la lógica para crear un nuevo índice si es necesario
                return; // Salimos del método ya que no hay índices para cargar
            }

            // Abrimos el archivo de índices
            using (FileStream stream = File.Open(systemIndexesFile, FileMode.Open, FileAccess.Read))
            {
                using (BinaryReader reader = new(stream))
                {
                    while (stream.Position < stream.Length)
                    {
                        // Leer la metadata desde el archivo de índices
                        string DataBaseName = reader.ReadString();
                        string tableName = reader.ReadString();
                        string indexName = reader.ReadString();
                        string columnName = reader.ReadString();
                        string indexType = reader.ReadString();

                        DataType? columnDatatype = store.GetColumnDataType(DataBaseName, tableName, columnName);

                        List<Column> allColumns = store.GetColumnsOfTable(DataBaseName, tableName);

                        // Obtener los registros completos de la tabla
                        List<Dictionary<string, object>> records = store.GetDataFromTable(DataBaseName, tableName, allColumns);


                        // Acceder a la columna en el disco duro y obtener los datos
                        List<object> columnData = store.GetColumnData(DataBaseName, tableName, columnName);


                        // Agregar los datos a las listas del Store
                       

                        if (!store.DataBasesWithIndexes.Contains(DataBaseName))
                        {
                            store.DataBasesWithIndexes.Add(DataBaseName);
                        }

                        if (!store.TablesWithIndexes.Contains(tableName))
                        {
                            store.TablesWithIndexes.Add(tableName);
                        }

                        if (!store.ColumnsWithIndexes.Contains(columnName))
                        {
                            store.ColumnsWithIndexes.Add(columnName);
                        }

                        // Asociar columna con nombre del índice
                        store.AssociatedIndexesToColumns[columnName] = indexName;


                        // Verificar el tipo de índice y crear el árbol correspondiente
                        if (indexType.Equals("BST", StringComparison.OrdinalIgnoreCase))
                        {
                            if (columnDatatype == DataType.INTEGER)
                            {
                                var bst = new BinarySearchTree<int>();
                                foreach (var record in records)
                                {
                                    int value = (int)record[columnName];
                                    bst.insert(value,record); // Insertar valores en el BST
                                    //Console.WriteLine($"Valor {value} agregado al arbol del indice: {indexName}");
                                }

                                // Guardar el árbol en el diccionario en memoria
                                Store.GetInstance().IndexTrees[indexName] = bst;

                            }

                            else if (columnDatatype == DataType.VARCHAR)
                            {
                                var bst = new BinarySearchTree<string>();
                                foreach (var record in records)
                                {
                                    string value = (string)record[columnName];
                                    bst.insert(value, record);
                                    //Console.WriteLine($"Valor {value} agregado al árbol del índice: {indexName}");
                                }
                                Store.GetInstance().IndexTrees[indexName] = bst;

                            }

                            else if (columnDatatype == DataType.DOUBLE)
                            {
                                var bst = new BinarySearchTree<double>();
                                foreach (var record in records)
                                {
                                    double value = (double)record[columnName];
                                    bst.insert(value, record);
                                    //Console.WriteLine($"Valor {value} agregado al árbol del índice: {indexName}");
                                }
                                Store.GetInstance().IndexTrees[indexName] = bst;

                            }

                            else if (columnDatatype == DataType.DATETIME)
                            {
                                var bst = new BinarySearchTree<DateTime>();
                                foreach (var record in records)
                                {
                                    DateTime value = (DateTime)record[columnName];
                                    bst.insert(value, record);
                                    //Console.WriteLine($"Valor {value} agregado al árbol del índice: {indexName}");
                                }
                                Store.GetInstance().IndexTrees[indexName] = bst;

                            }


                        }
                        else if (indexType.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
                        {

                            if (columnDatatype == DataType.INTEGER)
                            {
                                var bTree = new BTree<int>(3);
                                foreach (var record in records)
                                {
                                    int value = (int)record[columnName];
                                    bTree.Insert(value, record);
                                    //Console.WriteLine($"Valor {value} agregado al árbol del índice: {indexName}");
                                }
                                Store.GetInstance().IndexTrees[indexName] = bTree;
                            }

                            else if (columnDatatype == DataType.DOUBLE)
                            {
                                var bTree = new BTree<double>(3);
                                foreach (var record in records)
                                {
                                    double value = (double)record[columnName];
                                    bTree.Insert(value, record);
                                    //Console.WriteLine($"Valor {value} agregado al árbol del índice: {indexName}");
                                }
                                Store.GetInstance().IndexTrees[indexName] = bTree;
                            }

                            else if (columnDatatype == DataType.VARCHAR)
                            {
                                var bTree = new BTree<string>(3);
                                foreach (var record in records)
                                {
                                    string value = (string)record[columnName];
                                    bTree.Insert(value, record);
                                    //Console.WriteLine($"Valor {value} agregado al árbol del índice: {indexName}");
                                }
                                Store.GetInstance().IndexTrees[indexName] = bTree;
                            }

                            else if (columnDatatype == DataType.DATETIME)
                            {
                                var bTree = new BTree<DateTime>(3);
                                foreach (var record in records)
                                {
                                    DateTime value = (DateTime)record[columnName];
                                    bTree.Insert(value, record);
                                    //Console.WriteLine($"Valor {value} agregado al árbol del índice: {indexName}");
                                }
                                Store.GetInstance().IndexTrees[indexName] = bTree;
                            }


                        }
                        else
                        {
                            Console.WriteLine($"Tipo de índice no válido: {indexType}");
                        }
                    }
                }
            }

            
        }

        public void RegenerateIndexes()
        {
            var store = Store.GetInstance();

            // Limpiar los árboles de índices en memoria
            store.IndexTrees.Clear(); // Esto borra todos los árboles de índices en memoria

            // Limpiar las listas de bases de datos, tablas y columnas con índices
            store.DataBasesWithIndexes.Clear();
            store.TablesWithIndexes.Clear();
            store.ColumnsWithIndexes.Clear();
            store.AssociatedIndexesToColumns.Clear();

            // Volver a generar los índices
            LoadIndexesAndGenerateTrees();
            Console.WriteLine("Actualización de índices completada.");
        }



    }

}
