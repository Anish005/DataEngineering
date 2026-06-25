class BankAccount:
    def __init__(self , owner, balance=0):
        self.owner = owner
        self.balance = balance

    def deposit(self ,amount):
        self.balance += amount
        print(f"Added {amount} to the balance. New Balance : {self.balance}" )

    def withdraw(self, amount):
        if amount > self.balance:
            print("Insufficient balance")
        else:
            self.balance -= amount
            print(f"Withdrawn {amount} from the balance. New Balance : {self.balance}")
acc1 = BankAccount("John", 1000)
acc2 = BankAccount("Alice", 500)
acc1.deposit(500) # Added 500 to the balance. New Balance : 1500
acc2.withdraw(200) # Withdrawn 200 from the balance. New Balance : 300

