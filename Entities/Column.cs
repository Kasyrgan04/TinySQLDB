using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;

namespace QueryProcessor.Parser
{
    public class Column
    {
        public string Name { get; set; }
        public DataType DataType { get; set; }
        public int? MaxSize { get; set; }

    }
}