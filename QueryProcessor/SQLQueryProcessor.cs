using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;
using StoreDataManager;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static OperationStatus Execute(string sentence)
        {
            Parser parser = new Parser();
            ParsedQuery parsedQuery = parser.Parse(sentence);

            switch (parsedQuery.QueryType)
            {
                case QueryType.CreateTable:
                    return new CreateTable(parsedQuery.TableName, parsedQuery.ColumnsDefinition).Execute();
                case QueryType.Insert:
                    return new Insert(parsedQuery.TableName, parsedQuery.Values).Execute();
                case QueryType.Select:
                    return new Select(parsedQuery.TableName, parsedQuery.Columns).Execute();
                case QueryType.Update:
                    return new Update(parsedQuery.TableName, parsedQuery.Id, parsedQuery.Values).Execute();
                case QueryType.Delete:
                    return new Delete(parsedQuery.TableName, parsedQuery.Id).Execute();
                default:
                    throw new UnknownSQLSentenceException();
            }
        }

    }
}
