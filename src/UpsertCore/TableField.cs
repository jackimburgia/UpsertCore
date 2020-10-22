namespace Spearing.Utilities.Data.UpsertCore
{
    internal class TableField
    {
        public string TableName { get; set; }
        public string SchemaName { get; set; }
        public string ColumnName { get; set; }
        public string DataType { get; set; }
        public int Max_length { get; set; }
        public int ColumnId { get; set; }
        public int Precision { get; set; }
        public int Scale { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsNullable { get; set; }
        public bool IsIdentity { get; set; }
        public TableField() { }
    }
}
