using ApiInterface.Indexes;
using QueryProcessor.Parser;
using StoreDataManager;
using DataType = Entities.DataType;

namespace ApiInterface
{
    public class Generator
    {



        public void LoadIndexesAndGenerateTrees()
        {
            var store = Store.GetInstance();

            string IndexFile = store.GetIndexes();

            if (!File.Exists(IndexFile))
            {
                Console.WriteLine("No se encontraron indices");
                
                return;
            }

            
            using (FileStream stream = File.Open(IndexFile, FileMode.Open, FileAccess.Read))
            {
                using (BinaryReader reader = new(stream))
                {
                    while (stream.Position < stream.Length)
                    {
                        
                        string BaseName = reader.ReadString();
                        string TableName = reader.ReadString();
                        string IndexName = reader.ReadString();
                        string columnName = reader.ReadString();
                        string TypeOfIndex = reader.ReadString();

                        DataType? ColumnType = store.GetType(BaseName, TableName, columnName);

                        List<Column> AllColumns = store.GetColumns(BaseName, TableName);
                        
                        List<Dictionary<string, object>> records = store.GetTableData(BaseName, TableName, AllColumns);

                        List<object> columnData = store.GetData(BaseName, TableName, columnName);

                        if (store.IndexedDatabases.Contains(BaseName))
                        {
                            store.IndexedDatabases.Add(BaseName);
                        }

                        if (!store.IndexedTables.Contains(TableName))
                        {
                            store.IndexedTables.Add(TableName);
                        }

                        if (!store.IndexedColumns.Contains(columnName))
                        {
                            store.IndexedColumns.Add(columnName);
                        }

                        store.IndexesByColumns[columnName] = IndexName;



                        if (TypeOfIndex.Equals("BST", StringComparison.OrdinalIgnoreCase))
                        {
                            if (ColumnType == DataType.INTEGER)
                            {
                                var bst = new BST<int>();
                                foreach (var record in records)
                                {
                                    int value = (int)record[columnName];
                                    bst.insert(value, record);

                                }
                                store.IndexTrees[IndexName] = bst;
                            }

                            else if (ColumnType == DataType.VARCHAR)
                            {
                                var bst = new BST<string>();
                                foreach (var record in records)
                                {
                                    string value = (string)record[columnName];
                                    bst.insert(value, record);
                                }
                                store.IndexTrees[IndexName] = bst;

                            }

                            else if (ColumnType == DataType.DOUBLE)
                            {
                                var bst = new BST<double>();
                                foreach (var record in records)
                                {
                                    double value = (double)record[columnName];
                                    bst.insert(value, record);
                                }
                                store.IndexTrees[IndexName] = bst;
                            }

                            else if (ColumnType == DataType.DATETIME)
                            {
                                var bst = new BST<DateTime>();
                                foreach (var record in records)
                                {
                                    DateTime value = (DateTime)record[columnName];
                                    bst.insert(value, record);
                                }
                                    store.IndexTrees[IndexName] = bst;
                            }

                        }
                        else if (TypeOfIndex.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
                        {

                            if (ColumnType == DataType.INTEGER)
                            {
                                var bTree = new BTree<int>(3);
                                foreach (var record in records)
                                {
                                    int value = (int)record[columnName];
                                    bTree.Insert(value, record);
                                }
                                store.IndexTrees[IndexName] = bTree;
                            }

                            else if (ColumnType == DataType.DOUBLE)
                            {
                                var bTree = new BTree<double>(3);
                                foreach (var record in records)
                                {
                                    double value = (double)record[columnName];
                                    bTree.Insert(value, record);
                                }
                                store.IndexTrees[IndexName] = bTree;
                            }

                            else if (ColumnType == DataType.VARCHAR)
                            {
                                var bTree = new BTree<string>(3);
                                foreach (var record in records)
                                {
                                    string value = (string)record[columnName];
                                    bTree.Insert(value, record);
                                }
                                store.IndexTrees[IndexName] = bTree;
                            }

                            else if (ColumnType == DataType.DATETIME)
                            {
                                var bTree = new BTree<DateTime>(3);
                                foreach (var record in records)
                                {
                                    DateTime value = (DateTime)record[columnName];
                                    bTree.Insert(value, record);
                                }
                                store.IndexTrees[IndexName] = bTree;
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Tipo no válido");
                        }
                    }
                }
            }
        }

        public void RegenerateIndexes()
        {
            var store = Store.GetInstance();


            store.IndexTrees.Clear(); 


            store.IndexedDatabases.Clear();
            store.IndexedTables.Clear();
            store.IndexedColumns.Clear();
            store.IndexesByColumns.Clear();


            LoadIndexesAndGenerateTrees();
        }



    }

}
