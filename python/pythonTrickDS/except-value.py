def main(): 
    try:
        my_var = float(input("Please enter a number: "))
        print(f"You entered {my_var}")
    except ValueError:
        print("That's not a number")

if __name__ == "__main__":
    main()