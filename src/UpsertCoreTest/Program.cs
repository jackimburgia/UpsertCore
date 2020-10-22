using System;
using System.Collections.Generic;
using System.Linq;
using Spearing.Utilities.Data.UpsertCore;


namespace UpsertCoreTest
{
    class Program
    {
        static void Main(string[] args)
        {
            string connStr = @"Data Source=LAPTOP-KF78KIA6\SQLEXPRESS;Initial Catalog=Tester;User Id=SomeUser; Password=password1;";

            // Attempts to match on database primary key CustomerId
            // Updates existing customer Joe Smith
            // Inserts new customer Jane West
            List<Customer> customers= new List<Customer>()
            {
                new Customer() { CustomerID = 1, FirstName = "Joseph", LastName = "Smith", City = "Philadelphia", State = "PA" },
                new Customer() { CustomerID = 4, FirstName = "Jane", LastName = "West", City = "Denver", State = "CO" }
            };

            customers.Upsert("Sales", "Customer", connStr);



            // update Joe Smith's city
            customers[0].City = "Pittsburgh";
            // Create a new customer
            customers.Add(new Customer() { CustomerID = 5, FirstName = "Betsy", LastName = "Collins", City = "Denver", State = "CO" });

            // Attempt to match on surrogate key of FirstName, LastName
            customers.Upsert("Sales", "Customer", connStr, c => new { c.FirstName, c.LastName });


            var keys = customers
                .Where(c => c.FirstName.StartsWith("J"))
                .Select(c => new { c.CustomerID });

            keys.Deleter("Sales", "Customer", connStr);


            Console.WriteLine("Done");
        }
    }

    public class Customer
    {
        public int CustomerID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string City { get; set; }
        public string State { get; set; }
    }
}
