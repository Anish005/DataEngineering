class Customer:
    def __init__(self, name, account_type):
        """
        Default constructor called automatically when a new object is created.
        Initializes instance variables and can perform setup logic.
        """
        self.name = name
        self.account_type = account_type
        print(f"New customer created: {self.name}, {self.account_type} account")

    # Alternative constructor: create a Customer with only name, defaulting account_type
    @classmethod
    def from_name(cls, name):
        print("Creating customer using from_name() constructor...")
        return cls(name, "Savings")

    # Another alternative constructor: create a Customer from a dictionary
    @classmethod
    def from_dict(cls, data):
        print("Creating customer using from_dict() constructor...")
        return cls(data.get("name"), data.get("account_type", "Checking"))

# --- Usage examples ---

# Normal object creation (calls __init__ directly)
c1 = Customer("Alice", "Premium")

# Alternative constructor using classmethod
c2 = Customer.from_name("Bob")

# Alternative constructor using dictionary input
customer_data = {"name": "Charlie", "account_type": "Gold"}
c3 = Customer.from_dict(customer_data)