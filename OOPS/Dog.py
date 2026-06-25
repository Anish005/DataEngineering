class Dog:
    species = "Labrador" # class attribute
    def __init__(self, name , age):
        self.name = name # instance attribute
        self.age = age # instance attribute
    # instance method
    def description(self):
        return f"{self.name} is {self.age} years old"
    
    def speak(self, sound):
        return f"{self.name} says {sound}"
class Bulldog(Dog):
    pass

miles = Dog("Milo", 5)
toddy = Dog("Toddy", 3)
print(miles.name) # Milo
print(toddy.species) # Labrador
print(miles.description()) # Milo is 5 years old
print(toddy.speak("bow wow"))