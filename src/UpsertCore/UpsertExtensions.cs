using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace Spearing.Utilities.Data.UpsertCore
{
    public static class UpsertExtensions
    {

        private static int Timeout { get; set; }

        #region Public

        /// <summary>
        /// Performs an update / insert on a typed collection based on the primary key in the database
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <param name="connectionString"></param>
        public static void Upsert<T>(this IEnumerable<T> list, string schemaName, string tableName, string connectionString)
        {
            string tableTypeName = tableName + "_" + Guid.NewGuid().ToString().Replace("-", "");
            string parameterName = "@" + tableTypeName;

            try
            {

                var properties = Extensions.GetPropertyKeys<T>();
                string[] propNames = properties.Select(p => p.Name).ToArray();

                // Get the database fields for the table
                List<TableField> fields = GetTableFields(schemaName, tableName, connectionString)
                        .Where(field => propNames.Contains(field.ColumnName))
                        .OrderBy(field => field.ColumnId)
                        .ToList();


                DataTable table = CreateObjects(list, fields, tableTypeName, connectionString);

                // Run the sql merge statement
                string sql = GetMergeSql(parameterName, schemaName, tableName, fields);
                ExecuteSQL(sql, table, tableTypeName, parameterName, schemaName, tableName, fields, connectionString);
            }
            catch (Exception ex)
            {
                throw;// new Exception("Update failed", ex);
            }
            finally
            {
                // Delete the user defined table type if it exists(may blow up here, too)
                DeleteTableType(tableTypeName, connectionString);
            }
        }

        /// <summary>
        /// Performs an update / insert on a typed collection based on the key passed in getKey
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="list"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <param name="connectionString"></param>
        /// <param name="getKey"></param>
        public static void Upsert<T, S>(this IEnumerable<T> list, string schemaName, string tableName, string connectionString, Func<T, S> getKey)
        {
            var propNames = Extensions.GetPropertyKeys<T>()
                .Select(p => p.Name).ToArray();

            var keyPropNames = Extensions.GetPropertyKeys<S>()
                .Select(p => p.Name).ToArray();

            string tableTypeName = tableName + "_" + Guid.NewGuid().ToString().Replace("-", "");
            string parameterName = "@" + tableTypeName;


            try
            {
                // Get the database fields for the table
                var fields = GetTableFields(schemaName, tableName, connectionString)
                    .Where(field => propNames == null || propNames.Contains(field.ColumnName))
                    .OrderBy(field => field.ColumnId)
                    .ToList();

                var keyFields = fields
                    .Where(f => keyPropNames.Contains(f.ColumnName))
                    .ToArray();

                if (keyFields.Length != keyPropNames.Length)
                    throw new Exception("Missing fields");



                DataTable table = CreateObjects(list, fields, tableTypeName, connectionString);

                // Run the sql merge statement
                string sql = GetMergeKeySql(parameterName, schemaName, tableName, fields, keyFields);

                ExecuteSQL(sql, table, tableTypeName, parameterName, schemaName, tableName, fields, connectionString);
            }
            catch (Exception ex)
            {
                throw;// new Exception("Update failed", ex);
            }
            finally
            {
                //Delete the user defined table type if it exists(may blow up here, too)
                DeleteTableType(tableTypeName, connectionString);
            }
        }





        /// <summary>
        /// Performs a mass delete where a collection of keys matches records in a database table
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="keys"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <param name="connectionString"></param>
        public static void Deleter<T>(this IEnumerable<T> keys, string schemaName, string tableName, string connectionString)
        {
            var properties = Extensions.GetPropertyKeys<T>();
            var propNames = properties.Select(p => p.Name).ToArray();

            string tableTypeName = tableName + "_" + Guid.NewGuid().ToString().Replace("-", "");
            string parameterName = "@" + tableTypeName;

            try
            {
                var sql = GetDeleteSql(parameterName, schemaName, tableName, propNames);

                // Delete the user defined table type (shouldn't be there, but just in case)
                DeleteTableType(tableTypeName, connectionString);

                var fields = GetTableFields(schemaName, tableName, connectionString)
                    .Where(field => propNames == null || propNames.Contains(field.ColumnName))
                    .OrderBy(field => field.ColumnId)
                    .ToList();

                // Create the user defined table used to pass the data
                CreateTableType(fields, tableTypeName, connectionString);

                DataTable table =
                        keys.ToDataTable(
                            fields
                                .Select(field => field.ColumnName)
                                .ToList()
                            );

                ExecuteSQL(sql, table, tableTypeName, parameterName, schemaName, tableName, fields, connectionString);
            }
            catch(Exception ex)
            {
                throw;
            }
            finally
            {
                // Delete the user defined table type if it exists (may blow up here, too)
                DeleteTableType(tableTypeName, connectionString);
            }
        }



        #endregion


        /// <summary>
        /// Creates a datatable that represents the data to upsert and that can be passed as a parameter to SQL Server
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="fields"></param>
        /// <param name="tableTypeName"></param>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        private static DataTable CreateObjects<T>(this IEnumerable<T> list, List<TableField> fields, string tableTypeName, string connectionString)
        {
            // Convert the list into a data table
            DataTable table =
                list.ToDataTable(
                    fields
                        .Select(field => field.ColumnName)
                        .ToList()
                    );

            // Make sure the order of the columns in the data table are the same as the fields list
            SetColumnsOrder(table, fields.Select(field => field.ColumnName).ToArray());

            // Delete the user defined table type (shouldn't be there, but just in case)
            DeleteTableType(tableTypeName, connectionString);

            // Create the user defined table used to pass the data
            CreateTableType(fields, tableTypeName, connectionString);

            return table;
        }





        /// <summary>
        /// Runs the Merge SQL statement that Updates or Inserts
        /// </summary>
        /// <param name="table"></param>
        /// <param name="tableTypeName"></param>
        /// <param name="parameterName"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <param name="fields"></param>
        /// <param name="connectionString"></param>
        private static void ExecuteSQL(string sql, DataTable table, string tableTypeName, string parameterName, string schemaName, string tableName, List<TableField> fields, string connectionString)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                using (var cmd = new SqlCommand(sql))
                {
                    cmd.Connection = conn;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = Timeout;
                    cmd.Parameters.AddWithValue(parameterName, table);
                    cmd.Parameters[0].SqlDbType = SqlDbType.Structured;
                    cmd.Parameters[0].TypeName = tableTypeName;

                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Creates the SQL Server User Defined Type that stores the data to Upsert
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="tableTypeName"></param>
        /// <param name="connectionString"></param>
        private static void CreateTableType(List<TableField> fields, string tableTypeName, string connectionString)
        {
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();

                string tableColumns =
                    fields
                        .OrderBy(field => field.ColumnId)
                        .Select(field =>
                            field.ColumnName + " "
                            + field.DataType + " "
                            + field.DataTypeSize() + " "
                            + (field.IsNullable ? "" : "NOT NULL")
                        )
                        .Join(",");

                string tableTypeSql = String.Format(@"
                    CREATE TYPE {0} AS TABLE(
                        {1}
                    )
                    ", tableTypeName, tableColumns);

                // Create the table type
                using (var tableTypeCmd = new SqlCommand(tableTypeSql, conn))
                {
                    tableTypeCmd.CommandType = CommandType.Text;
                    tableTypeCmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Deletes tehy User Defined Type used to store the data
        /// </summary>
        /// <param name="tableTypeName"></param>
        /// <param name="connectionString"></param>
        private static void DeleteTableType(string tableTypeName, string connectionString)
        {
            string sql = String.Format(@"
                IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '{0}')
                  DROP TYPE {0}; 
                ", tableTypeName);

            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var tableTypeCmd = new SqlCommand(sql, conn))
                {
                    tableTypeCmd.CommandType = CommandType.Text;
                    tableTypeCmd.ExecuteNonQuery();
                }
            }
        }


        /// <summary>
        /// Returns a list of TableFields associated with the Schema.Table
        /// </summary>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        private static List<TableField> GetTableFields(string schemaName, string tableName, string connectionString)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand command = new SqlCommand(GetTableFieldsSQL(schemaName, tableName), conn))
                {
                    List<TableField> columns = new List<TableField>();
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            columns.Add(new TableField()
                            {
                                TableName = reader["TableName"] as string,
                                SchemaName = reader["SchemaName"] as string,
                                ColumnName = reader["ColumnName"] as string,
                                DataType = reader["DataType"] as string,
                                Max_length = Convert.ToInt32(reader["Max_length"]),
                                ColumnId = Convert.ToInt32(reader["ColumnId"]),
                                Precision = Convert.ToInt32(reader["Precision"]),
                                Scale = Convert.ToInt32(reader["Scale"]),
                                IsPrimaryKey = Convert.ToBoolean(reader["IsPrimaryKey"]),
                                IsNullable = Convert.ToBoolean(reader["Is_Nullable"]),
                                IsIdentity = Convert.ToBoolean(reader["is_identity"])
                            });
                        }
                    }
                    return columns;
                }
            }
        }

        /// <summary>
        /// Returns the SQL used to query the SQL Server systems tables to get the relevant data to perform the Upsert
        /// </summary>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private static string GetTableFieldsSQL(string schemaName, String tableName)
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("                select  ");
            sql.Append("                t.name TableName,  ");
            sql.Append("                s.name SchemaName,");
            sql.Append("                c.name ColumnName, ");
            sql.Append("                y.name DataType, ");
            sql.Append("                c.Max_length, ");
            sql.Append("                c.Precision, ");
            sql.Append("                c.Scale, ");
            sql.Append("                c.Is_Nullable, ");
            sql.Append("                c.is_identity,");
            sql.Append("                case when ic.id is null then 0 ");
            sql.Append("                else 1 ");
            sql.Append("                end IsPrimaryKey, ");
            sql.Append("                c.column_id ColumnId ");
            sql.Append("                from  ");
            sql.Append("                sys.tables t ");
            sql.Append("                join sys.schemas s");
            sql.Append("                on t.schema_id = s.schema_id");
            sql.Append("                join sys.columns c ");
            sql.Append("                on t.object_id = c.object_id ");
            sql.Append("                join sys.types y ");
            sql.Append("                on c.system_type_id = y.system_type_id ");
            sql.Append("                left outer join ( ");
            sql.Append("                select  ");
            sql.Append("                c.id, ");
            sql.Append("                c.name ");
            sql.Append("                from  ");
            sql.Append("                sysindexes i ");
            sql.Append("                join  ");
            sql.Append("                sysobjects o  ");
            sql.Append("                ON i.id = o.id ");
            sql.Append("                join  ");
            sql.Append("                sysobjects pk  ");
            sql.Append("                ON i.name = pk.name ");
            sql.Append("                AND pk.parent_obj = i.id ");
            sql.Append("                AND pk.xtype = 'PK' ");
            sql.Append("                join  ");
            sql.Append("                sysindexkeys ik  ");
            sql.Append("                on i.id = ik.id ");
            sql.Append("                and i.indid = ik.indid ");
            sql.Append("                join  ");
            sql.Append("                syscolumns c  ");
            sql.Append("                ON ik.id = c.id ");
            sql.Append("                AND ik.colid = c.colid ");
            sql.Append("                ) ic ");
            sql.Append("                on c.object_id = ic.id ");
            sql.Append("                and c.name = ic.name ");
            sql.Append("                where  ");
            sql.Append("                t.name = '" + tableName + "' ");
            sql.Append("                and s.name = '" + schemaName + "'");
            sql.Append("                and y.name <> 'sysname'");
            sql.Append("                order by  ");
            sql.Append("                t.name,  ");
            sql.Append("                c.column_id ");

            return sql.ToString();
        }


        internal static string KeyFields(this IEnumerable<TableField> fields)
        {
            return
                fields
                    .Where(field => field.IsPrimaryKey)
                    .OrderBy(field => field.ColumnId)
                    .Select(field => "t." + field.ColumnName + " = s." + field.ColumnName)
                    .Join(" AND ");
        }

        private static string UpdateFields(this IEnumerable<TableField> fields, string setColumnTableIdentifier = "")
        {
            return
                fields
                    .Where(field => field.IsPrimaryKey == false && field.IsIdentity == false)// && (updateFieldsToSkip == null || updateFieldsToSkip.Contains(field.ColumnName) == false)) // don't update the key fields
                    .OrderBy(field => field.ColumnId)
                    .Select(field => setColumnTableIdentifier + field.ColumnName + " = s." + field.ColumnName)
                    .Join(",");
        }


        internal static string GetDeleteSql(string parameterName, string schema, string tableName, string[] keyFields)
        {
            string join = String.Join(" AND ", keyFields.Select(field => $"t.{field} = s.{field}"));
            string sql = $@"
BEGIN
    MERGE {schema}.{tableName} AS T
    USING {parameterName} AS S
    ON ({join}) 
    WHEN MATCHED 
        then delete
    ;
END
            ";

            return sql;
        }

        internal static string GetMergeKeySql(string parameterName, string schema, string tableName, IEnumerable<TableField> fields, IEnumerable<TableField> keys)
        {
            string keyFields = String.Join(" AND ", keys.Select(key => $"t.{key.ColumnName} = s.{key.ColumnName}"));

            string updateFields = fields.UpdateFields("t.");

            string insertFields =
                fields
                    .Where(w => w.IsIdentity == false)
                    .OrderBy(field => field.ColumnId)
                    .Select(field => field.ColumnName)
                    .Join(",");

            string insertValueFields =
                fields
                    .Where(w => w.IsIdentity == false)
                    .OrderBy(field => field.ColumnId)
                    .Select(field => "S." + field.ColumnName)
                    .Join(",");


            string mergeSql = String.Format(@"                
                BEGIN
                        MERGE INTO {0}.{1} AS T
                        USING {2} AS S
                        ON 
                            {3}
                        WHEN MATCHED THEN UPDATE 
                            SET 
                                {4}
                        WHEN NOT MATCHED THEN INSERT 
                            ({5})
                            VALUES(	
		                    {6}
                            );
                    END
                    ", schema, tableName, parameterName, keyFields, updateFields, insertFields, insertValueFields);

            return mergeSql;
        }


        /// <summary>
        /// Returns the SQL Merge statement to Upsert the data
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="schema"></param>
        /// <param name="tableName"></param>
        /// <param name="fields"></param>
        /// <param name="updateFieldsToSkip">List of fields that shluld be excluded from the UPDATE portion of the MERGE statement</param>
        /// <returns></returns>
        private static string GetMergeSql(string parameterName, string schema, string tableName, List<TableField> fields)
        {
            string keyFields = fields.KeyFields();

            string updateFields = fields.UpdateFields("t.");

            string insertFields =
                fields
                    .Where(w => w.IsIdentity == false)
                    .OrderBy(field => field.ColumnId)
                    .Select(field => field.ColumnName)
                    .Join(",");

            string insertValueFields =
                fields
                    .Where(w => w.IsIdentity == false)
                    .OrderBy(field => field.ColumnId)
                    .Select(field => "S." + field.ColumnName)
                    .Join(",");


            string mergeSql = String.Format(@"                
                BEGIN
                        MERGE INTO {0}.{1} AS T
                        USING {2} AS S
                        ON 
                            {3}
                        WHEN MATCHED THEN UPDATE 
                            SET 
                                {4}
                        WHEN NOT MATCHED THEN INSERT 
                            ({5})
                            VALUES(	
		                    {6}
                            );
                    END
                    ", schema, tableName, parameterName, keyFields, updateFields, insertFields, insertValueFields);

            return mergeSql;
        }



        /// <summary>
        /// Sets the order of the columns in a data table
        /// </summary>
        /// <param name="table"></param>
        /// <param name="columnNames"></param>
        private static void SetColumnsOrder(DataTable table, params String[] columnNames)
        {
            for (int columnIndex = 0; columnIndex < columnNames.Length; columnIndex++)
            {
                table.Columns[columnNames[columnIndex]].SetOrdinal(columnIndex);
            }
        }

    }
}
