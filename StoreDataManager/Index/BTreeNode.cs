using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface.Indexes
{
    public class BTreeNode<T> where T : IComparable<T>
    {
        public T[] keys;  // Las claves (valores) en el nodo
        public Dictionary<string, object>[] records;  // Los registros asociados a cada clave
        public BTreeNode<T>[] children;  // Los hijos del nodo
        public int numKeys;  // Número actual de claves en el nodo
        public bool isLeaf;  // Si es una hoja o no

        // Constructor
        public BTreeNode(int t, bool isLeaf)
        {
            this.isLeaf = isLeaf;
            keys = new T[2 * t - 1];  // Cada nodo puede contener hasta 2t - 1 claves
            records = new Dictionary<string, object>[2 * t - 1];  // Los registros asociados
            children = new BTreeNode<T>[2 * t];  // Cada nodo puede tener hasta 2t hijos
            numKeys = 0;  // Inicialmente no hay claves en el nodo
        }
    }

}
