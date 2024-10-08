using System;
using System;
using System.Text.RegularExpressions;

namespace QueryProcessor
{
    public class Parser
    {
        public ParsedQuery Parse(string query)
        {
            query = query.Trim().ToUpper();

            if (query.StartsWith("CREATE TABLE"))
            {
                return ParseCreateTable(query);
            }
            if (query.StartsWith("INSERT INTO"))
            {
                return ParseInsert(query);
            }
            if (query.StartsWith("SELECT"))
            {
                return ParseSelect(query);
            }
            if (query.StartsWith("UPDATE"))
            {
                return ParseUpdate(query);
            }
            if (query.StartsWith("DELETE FROM"))
            {
                return ParseDelete(query);
            }

            throw new Exception("Unknown SQL Query");
        }

        private ParsedQuery ParseCreateTable(string query)
        {
            var match = Regex.Match(query, @"CREATE TABLE (\w+)\s*\((.+)\)");
            if (!match.Success)
                throw new Exception("Invalid CREATE TABLE syntax");

            string tableName = match.Groups[1].Value;
            string columnsDefinition = match.Groups[2].Value;

            var parsedQuery = new ParsedQuery
            {
                QueryType = QueryType.CreateTable,
                TableName = tableName,
                ColumnsDefinition = columnsDefinition.Split(','),
            };

            return parsedQuery;
        }

        private ParsedQuery ParseInsert(string query)
        {
            var match = Regex.Match(query, @"INSERT INTO (\w+)\s*\((.+)\)\s*VALUES\s*\((.+)\)");
            if (!match.Success)
                throw new Exception("Invalid INSERT INTO syntax");

            string tableName = match.Groups[1].Value;
            string columns = match.Groups[2].Value;
            string values = match.Groups[3].Value;

            var parsedQuery = new ParsedQuery
            {
                QueryType = QueryType.Insert,
                TableName = tableName,
                Columns = columns.Split(','),
                Values = values.Split(',')
            };

            return parsedQuery;
        }

        private ParsedQuery ParseSelect(string query)
        {
            var match = Regex.Match(query, @"SELECT\s+(.+)\s+FROM\s+(\w+)");
            if (!match.Success)
                throw new Exception("Invalid SELECT syntax");

            string columns = match.Groups[1].Value;
            string tableName = match.Groups[2].Value;

            var parsedQuery = new ParsedQuery
            {
                QueryType = QueryType.Select,
                TableName = tableName,
                Columns = columns.Split(',')
            };

            return parsedQuery;
        }

        private ParsedQuery ParseUpdate(string query)
        {
            var match = Regex.Match(query, @"UPDATE (\w+) SET (.+) WHERE ID = (\d+)");
            if (!match.Success)
                throw new Exception("Invalid UPDATE syntax");

            string tableName = match.Groups[1].Value;
            string setValues = match.Groups[2].Value;
            int id = int.Parse(match.Groups[3].Value);

            var parsedQuery = new ParsedQuery
            {
                QueryType = QueryType.Update,
                TableName = tableName,
                Values = setValues.Split(','),
                Id = id
            };

            return parsedQuery;
        }

        private ParsedQuery ParseDelete(string query)
        {
            var match = Regex.Match(query, @"DELETE FROM (\w+) WHERE ID = (\d+)");
            if (!match.Success)
                throw new Exception("Invalid DELETE syntax");

            string tableName = match.Groups[1].Value;
            int id = int.Parse(match.Groups[2].Value);

            var parsedQuery = new ParsedQuery
            {
                QueryType = QueryType.Delete,
                TableName = tableName,
                Id = id
            };

            return parsedQuery;
        }
    }
}

