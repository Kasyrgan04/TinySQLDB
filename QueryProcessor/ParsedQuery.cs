namespace QueryProcessor
{
    public enum QueryType
    {
        CreateTable,
        Insert,
        Select,
        Update,
        Delete
    }

    public class ParsedQuery
    {
        public QueryType QueryType { get; set; }
        public string TableName { get; set; }
        public string[] Columns { get; set; }
        public string[] Values { get; set; }
        public string[] ColumnsDefinition { get; set; }
        public int Id { get; set; }
    }


}
