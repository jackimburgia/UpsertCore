# The .NET Upsert Nuget package is for combined insert and update operations of strongly typed collections into SQL Server.  Multirow deletes are possible, too.

# Setup database

```sql
CREATE TABLE Sales.Customer (
	CustomerID INT NOT NULL PRIMARY KEY,
	FirstName VARCHAR(20) NOT NULL,
	LastName VARCHAR(20) NOT NULL,
	City VARCHAR(20) NOT NULL,
	State CHAR(2) NOT NULL
);


INSERT INTO Sales.Customer (CustomerID, FirstName, LastName, City, State)
VALUES (1, 'Joe', 'Smith', 'Philadelphia', 'PA'),
	(2, 'Mary', 'Jones', 'New York', 'NY'),
	(3, 'Mike', 'Andersen', 'Raleigh', 'NC');

```

# Attempts to match on database primary key CustomerId

```csharp

string connStr = @"Data Source=ServerName;Initial Catalog=Tester;User Id=SomeUser; Password=password1;";

// Updates existing customer Joe Smith
// Inserts new customer Jane West
List<Customer> customers= new List<Customer>()
{
    new Customer() { CustomerID = 1, FirstName = "Joseph", LastName = "Smith", City = "Philadelphia", State = "PA" },
    new Customer() { CustomerID = 4, FirstName = "Jane", LastName = "West", City = "Denver", State = "CO" }
};

customers.Upsert("Sales", "Customer", connStr);

```


# Upsert based on surrogate key
```csharp 

// update Joe Smith's city
customers[0].City = "Pittsburgh";
// Create a new customer
customers.Add(new Customer() { CustomerID = 5, FirstName = "Betsy", LastName = "Collins", City = "Denver", State = "CO" });

// Attempt to match on surrogate key of FirstName, LastName
customers.Upsert("Sales", "Customer", connStr, c => new { c.FirstName, c.LastName });

```

# Delete multiple records based on key
```csharp 

var keys = customers
    .Where(c => c.FirstName.StartsWith("J"))
    .Select(c => new { c.CustomerID });

keys.Deleter("Sales", "Customer", connStr);

```
