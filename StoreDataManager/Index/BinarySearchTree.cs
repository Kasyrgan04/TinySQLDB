using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface.Indexes
{
    public class BinarySearchTree<T> where T : IComparable<T>
    {
        TreeNode<T> root;

        public BinarySearchTree()
        {
            root = null;
        }

        // Funciones de búsqueda

        public Dictionary<string, object> search(T key)
        {
            return searchRecursive(root, key);
        }

        private Dictionary<string, object> searchRecursive(TreeNode<T> root, T key)
        {
            if (root == null)
                return null; // Si no encuentra el nodo, devuelve null

            if (root.key.CompareTo(key) == 0)
                return root.record; // Si encuentra la clave, devuelve el registro

            if (root.key.CompareTo(key) < 0)
                return searchRecursive(root.right, key); // Buscar en el subárbol derecho

            return searchRecursive(root.left, key); // Buscar en el subárbol izquierdo
        }

        // Función de inserción actualizada para aceptar un registro asociado
        public void insert(T key, Dictionary<string, object> record)
        {
            root = insertRecursive(root, key, record);
        }

        private TreeNode<T> insertRecursive(TreeNode<T> root, T key, Dictionary<string, object> record)
        {
            if (root == null)
            {
                root = new TreeNode<T>(key, record); // Crear un nodo con la clave y el registro
                return root;
            }

            if (key.CompareTo(root.key) < 0)
                root.left = insertRecursive(root.left, key, record);
            else if (key.CompareTo(root.key) > 0)
                root.right = insertRecursive(root.right, key, record);

            return root;
        }

        // Función de eliminación (sin cambios en este caso)
        public void delete(T key)
        {
            root = deleteRecursive(root, key);
        }

        private TreeNode<T> deleteRecursive(TreeNode<T> root, T key)
        {
            if (root == null)
                return root;

            if (key.CompareTo(root.key) < 0)
                root.left = deleteRecursive(root.left, key);
            else if (key.CompareTo(root.key) > 0)
                root.right = deleteRecursive(root.right, key);
            else
            {
                if (root.left == null)
                    return root.right;
                else if (root.right == null)
                    return root.left;

                root.key = minValue(root.right);
                root.right = deleteRecursive(root.right, root.key);
            }
            return root;
        }

        private T minValue(TreeNode<T> root)
        {
            T minValue = root.key;
            while (root.left != null)
            {
                minValue = root.left.key;
                root = root.left;
            }
            return minValue;
        }

        // Función para obtener todos los registros en una lista de diccionarios
        public List<Dictionary<string, object>> GetAllRecords()
        {
            List<Dictionary<string, object>> recordsList = new List<Dictionary<string, object>>();
            InOrderTraversal(root, recordsList);
            return recordsList;
        }

        // Recorrido en orden (In-Order Traversal) para agregar los registros a la lista
        private void InOrderTraversal(TreeNode<T> node, List<Dictionary<string, object>> recordsList)
        {
            if (node != null)
            {
                InOrderTraversal(node.left, recordsList);
                recordsList.Add(node.record);
                InOrderTraversal(node.right, recordsList);
            }
        }


    }

}