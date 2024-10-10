using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities
{
    internal class DataColumn
    {
        public DataType DataType { get; set; }
        public Object Data { get; set; }

        public void ValidateData(object NewData)
        {
            if (this.DataType == DataType.INTEGER)
            {

            }
        }
    }
}
