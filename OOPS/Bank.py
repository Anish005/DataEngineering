'''
Technical Explanation:

Instance attributes: Stored per-object (self.attribute). 
Each object gets its own copy of the data.
Class attributes: Shared across all instances (ClassName.attribute). Useful for constants or shared data.
Instance methods: Operate on specific object data, accessing instance attributes through self.

'''

class Bank:
    bank_name = "National Bank"  # Class variable (shared by all instances)

    def __init__(self, account_holder):
        self.account_holder = account_holder  # Instance variable (unique to each object)
    
    def welcome(self):
        print(f"Welcome {self.account_holder} to {Bank.bank_name}!")

# --- Usage examples ---
# Creating two objects (each with its own instance attribute)
customer1 = Bank("Alice")
customer2 = Bank("Bob")
customer1.welcome()  # Output: Welcome Alice to National Bank!
customer2.welcome()  # Output: Welcome Bob to National Bank!

# Changing the class variable affects all instances
Bank.bank_name = "Global Trust Bank"
customer1.welcome()  # Output: Welcome Alice to Global Trust Bank!
customer2.welcome()  # Output: Welcome Bob to Global Trust Bank!


'''
Technical Detail:

Each instance’s attributes are stored in its internal __dict__.
When you access an attribute (e.g., self.bank_name), Python first checks the instance’s __dict__. If it’s not found, it looks up the class 
and parent classes — this is the class namespace resolution chain.
Key Insight:
Use instance attributes for data unique to each object and class attributes for values common to all objects. This design keeps memory efficient and behavior consistent across instances.

'''