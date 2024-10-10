using ApiInterface.InternalModels;
using ApiInterface.Models;
using Entities;
using QueryProcessor;

namespace ApiInterface.Processors
{
    internal class SQLSentenceProcessor(Request request) : IProcessor
    {
        public Request Request { get; } = request;

        public Response Process()
        {
            var sentence = this.Request.RequestBody;
            var result = SQLQueryProcessor.Execute(sentence, out object? data);
            var response = this.ConvertToResponse(result, data);
            return response;
        }

        private Response ConvertToResponse(OperationStatus result, object? data)
        {
            return new Response
            {
                Status = result,
                Request = this.Request,
                ResponseBody = "Comando Ejecutado", // Aqui se envia la informacion de la respuesta
                ResponseData = data

            };
        }
    }
}
