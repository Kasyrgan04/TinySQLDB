using Entities;

namespace StoreDataManager
{
    class AuxInsert
    {
        public object Convert(string value, DataType Type, int? Size)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new Exception("El valor .");
            }

            switch (Type)
            {
                case DataType.INTEGER:
                    if (int.TryParse(value, out int intValue))
                    {
                        return intValue;
                    }
                    else
                    {
                        throw new Exception("El valor no es válido.");
                    }

                case DataType.DOUBLE:
                    if (double.TryParse(value, out double doubleValue))
                    {
                        return doubleValue;
                    }
                    else
                    {
                        throw new Exception("El valor no válido.");
                    }

                case DataType.VARCHAR:
                    value = value.Trim('\'', '\"');
                    if (Size.HasValue && value.Length > Size.Value)
                    {
                        throw new Exception($"Se está excediendo el tamaño maximo. El tamaño es de {Size}.");
                    }
                    return value;

                case DataType.DATETIME:
                    value = value.Trim('\'', '\"'); 
                    if (DateTime.TryParse(value, out DateTime dateTimeValue))
                    {
                        return dateTimeValue;
                    }
                    else
                    {
                        throw new Exception("El valor no es válido.");
                    }

                default:
                    throw new Exception("Tipo no soportado.");
            }
        }

        public void Write(BinaryWriter writer, object value)
        {
            if (value == null)
            {
                writer.Write(false);
                return;
            }
            else
            {
                writer.Write(true); 
            }

            if (value is int intValue)
            {
                writer.Write(intValue);
            }
            else if (value is double doubleValue)
            {
                writer.Write(doubleValue);
            }
            else if (value is string strValue)
            {
                writer.Write(strValue);
            }
            else if (value is DateTime dateTimeValue)
            {
                writer.Write(dateTimeValue.ToBinary()); 
            }
            else
            {
                throw new Exception("Tipo no soportado.");
            }
        }


    }
}
