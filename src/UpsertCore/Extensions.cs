using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Linq;

namespace Spearing.Utilities.Data.UpsertCore
{
    // These should be in a separate project for re-use
    internal static class Extensions
    {
        public static PropertyKey[] GetPropertyKeys<T>(List<string> propertiesToInclude = null)
        {
            var properties = typeof(T)
                .GetProperties()
                .Select(prop =>
                {
                    ColumnAttribute[] attributes = (ColumnAttribute[])prop.GetCustomAttributes(typeof(ColumnAttribute), false);
                    var data = new PropertyKey()
                    {
                        Name = attributes.Length == 0 ? prop.Name : attributes[0].Name,
                        Property = prop
                    };
                    return data;
                })
                .Where(prop => propertiesToInclude == null || propertiesToInclude.Contains(prop.Name))
                .ToArray();



            return properties;
        }

        /// <summary>
        /// Converts a generic list to a data table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="propertiesToInclude"></param>
        /// <returns></returns>
        internal static DataTable ToDataTable<T>(this IEnumerable<T> list, List<string> propertiesToInclude = null)
        {

            var properties = GetPropertyKeys<T>(propertiesToInclude);

            DataTable table = new DataTable();

            foreach (var prop in properties)
            {
                table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.Property.PropertyType) ?? prop.Property.PropertyType);
            }
                

            foreach (T item in list)
            {
                DataRow row = table.NewRow();
                foreach (var prop in properties)
                    row[prop.Name] = prop.Property.GetValue(item) ?? DBNull.Value;
                table.Rows.Add(row);
            }

            return table;
        }


        /// <summary>
        /// Joins a collection of strings with a separator / delimiter
        /// </summary>
        /// <param name="values"></param>
        /// <param name="separator"></param>
        /// <returns></returns>
        internal static string Join(this IEnumerable<string> values, string separator)
        {
            return String.Join(separator, values);
        }
    }
}
