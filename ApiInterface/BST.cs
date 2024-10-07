using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface
{
    internal class BSTNode<T> where T : IComparable<T> // IComparable<T> compara el valor de un objeto con otro objeto del mismo tipo
    {
        public T Value { get; set; } //Le asigna el valor a nodo, en este caso la columna
        public BSTNode<T>? Left { get; set; }
        public BSTNode<T>? Right { get; set; }

        public BSTNode(T value)
        {
            Value = value;
            Left = null;
            Right = null;
        }
    }

    internal class BST<T> where T : IComparable<T>
    {
        private BSTNode<T>? root;

        public void BinarySearchTree()
        {
            root = null;
        }

        // Método para insertar un valor en el BST
        public void Insert(T value)
        {
            root = InsertRec(root, value);
        }

        private BSTNode<T> InsertRec(BSTNode<T>? node, T value)
        {
            if (node == null)
                return new BSTNode<T>(value);

            if (value.CompareTo(node.Value) < 0)
                node.Left = InsertRec(node.Left, value);
            else if (value.CompareTo(node.Value) > 0)
                node.Right = InsertRec(node.Right, value);

            return node;
        }

        // Método para buscar un valor en el BST
        public bool Search(T value)
        {
            return SearchRec(root, value);
        }

        private bool SearchRec(BSTNode<T>? node, T value)
        {
            if (node == null)
                return false;
            if (value.CompareTo(node.Value) == 0)
                return true;
            else if (value.CompareTo(node.Value) < 0)
                return SearchRec(node.Left, value);
            else
                return SearchRec(node.Right, value);
        }

        // Método para eliminar un valor en el BST
        public void Delete(T value)
        {
            root = DeleteRec(root, value);
        }

        private BSTNode<T>? DeleteRec(BSTNode<T>? node, T value)
        {
            if (node == null)
                return null;

            if (value.CompareTo(node.Value) < 0)
                node.Left = DeleteRec(node.Left, value);
            else if (value.CompareTo(node.Value) > 0)
                node.Right = DeleteRec(node.Right, value);
            else
            {
                if (node.Left == null)
                    return node.Right;
                else if (node.Right == null)
                    return node.Left;

                node.Value = MinValue(node.Right);
                node.Right = DeleteRec(node.Right, node.Value);
            }
            return node;
        }

        private T MinValue(BSTNode<T> node)
        {
            T minValue = node.Value;
            while (node.Left != null)
            {
                minValue = node.Left.Value;
                node = node.Left;
            }
            return minValue;
        }

    }
}
