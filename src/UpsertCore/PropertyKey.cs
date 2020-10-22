using System.Reflection;

namespace Spearing.Utilities.Data.UpsertCore
{
    public class PropertyKey
    {
        public string Name { get; set; }
        public PropertyInfo Property { get; set; }
    }
}
