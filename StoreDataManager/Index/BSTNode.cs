using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface.Indexes
{
    public class TreeNode<T> where T : IComparable<T>
    {
        public T key;
        public Dictionary<string, object> record; // Diccionario para almacenar los datos del registro
        public TreeNode<T> left, right;

        public TreeNode(T key, Dictionary<string, object> record)
        {
            this.key = key;
            this.record = record; // Guardar el registro
            left = right = null;
        }
    }
}
