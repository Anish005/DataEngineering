class Car:
    def __init__(self , registration_no , chasis_no):
        self.registration_no = registration_no
        self.chasis_no = chasis_no
    
    def description(self):
        return f"Car with registration number {self.registration_no} and chasis number {self.chasis_no}"
    
    def number_plate(self, plate):
        return f"Car with registration number {self.registration_no} has number plate {plate}"
    
lamborghini = Car("MH12AB1234", "CHS123456789") 
print(lamborghini.description()) # Car with registration number MH12AB1234 and chasis number CHS123456789
print(lamborghini.number_plate("MH12 AB 1234"))