using System;

namespace Spearing.Utilities.Data.UpsertCore
{
    internal static class TableFieldExtensions
    {
        /// <summary>
        /// Returns the data type size / precision
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        internal static string DataTypeSize(this TableField field)
        {
            // TODO - figure out what to do with all of the data types
            // image, rowversion, sql_variant, timestamp
            switch (field.DataType)
            {
                case "nchar":
                case "nvarchar":
                    return "(" + (field.Max_length == -1 ? "MAX" : (field.Max_length / 2).ToString()) + ")";
                case "char":
                case "varchar":
                    return "(" + (field.Max_length == -1 ? "MAX" : field.Max_length.ToString()) + ")";
                case "binary": // TODO - test this
                case "varbinary": // TODO - test this
                case "datetimeoffset":
                case "time":
                    return "(" + field.Max_length.ToString() + ")";
                case "bigint":
                case "bit":
                case "date":
                case "datetime":
                case "datetime2":
                case "float":
                case "int":
                case "money":
                case "ntext":
                case "real": // TODO - test this
                case "smalldatetime":
                case "smallint":
                case "smallmoney":
                case "text": // TODO - test this
                case "tinyint":
                case "uniqueidentifier": // TODO - test this
                case "xml": // TODO - test this
                    return String.Empty;
                case "decimal":
                case "numeric":
                    return "(" + field.Precision.ToString() + "," + field.Scale.ToString() + ")";
            }
            throw new Exception(field.DataType + "is not defined");
        }
    }
}
